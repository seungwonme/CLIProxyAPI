// Package adapter connects native authenticated HTTP lifecycles to the isolated store.
package adapter

import (
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/httpwire"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/transcript/core"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v8/sdk/access"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/session"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/httptransport"
)

// Controller is owned by one server. Configuration changes take effect on restart.
type Controller struct {
	store   *core.Store
	users   map[[32]byte]string
	enabled atomic.Bool
	failed  atomic.Bool
	skipped atomic.Uint64
}

func New(cfg config.Transcripts) (*Controller, error) {
	c := &Controller{}
	c.enabled.Store(cfg.Enabled)
	if !cfg.Enabled {
		return c, nil
	}
	users, err := bindings(cfg.Users)
	if err != nil {
		c.failed.Store(true)
		return c, err
	}
	root := cfg.Root
	if root == "" {
		root = "/transcripts"
	}
	if !filepath.IsAbs(root) {
		c.failed.Store(true)
		return c, errors.New("transcripts.root must be absolute")
	}
	store, err := core.New(core.Config{Root: root, MaxBytes: core.DefaultMaxBytes})
	if err != nil {
		c.failed.Store(true)
		return c, errors.New("transcript store unavailable")
	}
	c.store, c.users = store, users
	return c, nil
}

func bindings(items []config.TranscriptUser) (map[[32]byte]string, error) {
	out := make(map[[32]byte]string, len(items))
	if len(items) == 0 || len(items) > 1024 {
		return nil, errors.New("transcripts require 1 to 1024 user bindings")
	}
	seen := make(map[string]bool, len(items))
	for _, item := range items {
		if item.EnvName == "" || strings.TrimSpace(item.UserID) != item.UserID || item.UserID == "" || len(item.UserID) > 128 || seen[item.EnvName] {
			return nil, errors.New("invalid transcript user binding")
		}
		seen[item.EnvName] = true
		key := strings.TrimSpace(os.Getenv(item.EnvName))
		if key == "" {
			return nil, errors.New("transcript user environment binding is unavailable")
		}
		digest := sha256.Sum256([]byte(key))
		if _, exists := out[digest]; exists {
			return nil, errors.New("duplicate transcript credential binding")
		}
		out[digest] = item.UserID
	}
	return out, nil
}

func (c *Controller) Close() error {
	if c != nil && c.store != nil {
		return c.store.Close()
	}
	return nil
}
func (c *Controller) Stop() {
	if c != nil && c.store != nil {
		c.store.Stop()
	}
}

// Middleware installs no body wrapper, copy, allocation or reservation when off.
// The observer is called by Manager only after its first successful provider.
func (c *Controller) Middleware() gin.HandlerFunc {
	return func(g *gin.Context) {
		if c == nil || !c.enabled.Load() || c.store == nil {
			g.Next()
			return
		}
		var capture *exchange
		var observed atomic.Bool
		returned := false
		requestContext := g.Request.Context()
		ctx := sdkaccess.WithAuthenticationObserver(g.Request.Context(), func(_ *http.Request, result *sdkaccess.Result) {
			if result == nil {
				return
			}
			principal := sha256.Sum256([]byte(result.Principal))
			if !observed.CompareAndSwap(false, true) {
				if capture != nil && capture.principal != principal {
					capture.revoked.Store(true)
					capture.broken.Store(true)
				}
				return
			}
			user := c.users[principal]
			if user == "" {
				c.skipped.Add(1)
				return
			}
			r := g.Request
			if metadataBytes(r.Method, r.URL.String(), r.Header) > 16<<10 {
				c.skipped.Add(1)
				return
			}
			sessionInfo, _ := session.ExtractSessionInfo(r.Header, nil, nil)
			h := c.store.Begin(core.Metadata{RequestID: logging.GetGinRequestID(g), UserID: user,
				SessionID: sessionInfo.SessionID, Method: r.Method, URL: maskURL(r.URL.String()),
				Headers: maskHeaders(r.Header), Started: time.Now()})
			if h == nil {
				return
			}
			capture = &exchange{handle: h, remaining: r.ContentLength, principal: principal}
			g.Set(logging.TranscriptSinkContextKey, logging.TranscriptSink(capture))
			captureCtx := logging.WithTranscriptSink(g.Request.Context(), capture)
			g.Request = g.Request.WithContext(httptransport.WithDecorator(captureCtx, capture.decorate))
			if r.Body != nil && r.Body != http.NoBody {
				g.Request.Body = &bodyReader{ReadCloser: r.Body, capture: capture}
			}
			g.Writer = &responseWriter{ResponseWriter: g.Writer, capture: capture}
		})
		g.Request = g.Request.WithContext(ctx)
		defer func() {
			if capture != nil {
				capture.HTTP("response.headers", "", "", g.Writer.Status(), g.Writer.Header(), nil)
				capture.handle.End(returned && !capture.broken.Load() && requestContext.Err() == nil && (capture.remaining == 0 || (capture.remaining < 0 && capture.sawEOF)))
			}
		}()
		g.Next()
		returned = true
	}
}

type exchange struct {
	handle    *core.Handle
	broken    atomic.Bool
	revoked   atomic.Bool
	principal [32]byte
	remaining int64
	sawEOF    bool
	attempt   atomic.Uint64
}

func (e *exchange) Capture(kind string, data []byte) {
	if e.revoked.Load() {
		e.handle.Drop(1, uint64(len(data)))
		return
	}
	e.handle.Capture(kind, data)
}
func (e *exchange) Drop(chunks, bytes uint64) { e.handle.Drop(chunks, bytes) }
func (e *exchange) HTTP(kind, method, url string, status int, headers http.Header, body []byte) {
	if size := metadataBytes(method, url, headers); size > 16<<10 {
		e.Drop(1, uint64(size))
	} else {
		e.Capture(kind+".metadata", httpMetadata(method, url, status, headers))
	}
	if len(body) > 0 {
		e.Capture(kind+".body", body)
	}
}
func (e *exchange) Failure(kind string, err error) {
	if err == nil {
		return
	}
	// Errors can embed credentials in URLs or transport diagnostics. Classify,
	// never persist their arbitrary message; upstream error bodies remain raw.
	e.Capture(kind, []byte(logging.SafeErrorDiagnostic(err)))
}

type bodyReader struct {
	io.ReadCloser
	capture *exchange
}

func (r *bodyReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if r.capture.remaining > 0 {
		r.capture.remaining -= int64(n)
	}
	if err == io.EOF {
		r.capture.sawEOF = true
	}
	if n > 0 {
		r.capture.Capture("request.body", p[:n])
	}
	if err != nil && err != io.EOF {
		r.capture.broken.Store(true)
		r.capture.Failure("request.error", err)
	}
	return n, err
}

type responseWriter struct {
	gin.ResponseWriter
	capture *exchange
}

func (w *responseWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	if n > 0 {
		w.capture.Capture("response.body", p[:n])
	}
	if err != nil || n != len(p) {
		w.capture.broken.Store(true)
		w.capture.Failure("response.error", err)
	}
	return n, err
}
func (w *responseWriter) WriteString(s string) (int, error) {
	n, err := w.ResponseWriter.WriteString(s)
	for offset := 0; offset < n; {
		end := min(offset+(32<<10), n)
		w.capture.Capture("response.body", []byte(s[offset:end]))
		offset = end
	}
	if err != nil || n != len(s) {
		w.capture.broken.Store(true)
		w.capture.Failure("response.error", err)
	}
	return n, err
}
func (w *responseWriter) FlushError() error {
	err := httpwire.FlushResponse(w.ResponseWriter)
	if err != nil {
		w.capture.broken.Store(true)
		w.capture.Failure("response.flush.error", err)
	}
	return err
}
