package adapter

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/buildinfo"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/transcript/core"
)

func sensitive(name string) bool {
	name = strings.ToLower(name)
	for _, part := range []string{"authorization", "cookie", "token", "key", "secret", "credential", "signature", "password", "oauth", "sec-websocket-protocol", "session", "conversation"} {
		if strings.Contains(name, part) {
			return true
		}
	}
	return name == "code" || name == "state"
}

func metadataBytes(method, raw string, headers http.Header) int64 {
	n := int64(len(method)) + int64(len(raw))
	for key, values := range headers {
		n += int64(len(key))
		for _, value := range values {
			n += int64(len(value)) + 4
		}
	}
	return n
}
func maskHeaders(headers http.Header) map[string][]string {
	out := make(map[string][]string, len(headers))
	for key, values := range headers {
		if sensitive(key) {
			out[key] = []string{"[REDACTED]"}
			continue
		}
		for _, value := range values {
			if strings.EqualFold(key, "Location") || strings.EqualFold(key, "Referer") {
				value = maskURL(value)
			}
			out[key] = append(out[key], value)
		}
	}
	return out
}
func maskURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "[INVALID URL]"
	}
	u.User = nil
	u.Fragment = ""
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		u.RawQuery = "[REDACTED]"
		return u.String()
	}
	for key := range q {
		if sensitive(key) {
			q[key] = []string{"[REDACTED]"}
		}
	}
	u.RawQuery = q.Encode()
	return u.String()
}
func httpMetadata(method, raw string, status int, headers http.Header) []byte {
	var b strings.Builder
	if method != "" {
		fmt.Fprintf(&b, "Method: %s\n", method)
	}
	if raw != "" {
		fmt.Fprintf(&b, "URL: %s\n", maskURL(raw))
	}
	if status != 0 {
		fmt.Fprintf(&b, "Status: %d\n", status)
	}
	masked := maskHeaders(headers)
	keys := make([]string, 0, len(masked))
	for key := range masked {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		for _, value := range masked[key] {
			fmt.Fprintf(&b, "%s: %s\n", key, value)
		}
	}
	return []byte(b.String())
}

// Download is mounted only behind the existing v8 management authentication.
// The existing log handler remains responsible for the disabled feature path.
func (c *Controller) Download(fallback gin.HandlerFunc) gin.HandlerFunc {
	return func(g *gin.Context) {
		if c == nil || !c.enabled.Load() {
			fallback(g)
			return
		}
		id := strings.TrimSpace(g.Param("id"))
		if id == "" {
			id = strings.TrimSpace(g.Query("id"))
		}
		if id == "" {
			g.JSON(http.StatusBadRequest, gin.H{"error": "missing request ID"})
			return
		}
		if strings.ContainsAny(id, "/\\") || len(id) > 256 {
			g.JSON(http.StatusBadRequest, gin.H{"error": "invalid request ID"})
			return
		}
		if c.store == nil {
			g.JSON(http.StatusServiceUnavailable, gin.H{"error": "transcript store unavailable"})
			return
		}
		r, err := c.store.Open(id)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				g.JSON(http.StatusNotFound, gin.H{"error": "log file not found for the given request ID"})
			} else {
				g.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read log file"})
			}
			return
		}
		defer func() {
			if errClose := r.Close(); errClose != nil {
				c.failed.Store(true)
			}
		}()
		g.Header("Content-Type", "text/plain; charset=utf-8")
		g.Header("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": r.Name()}))
		g.Header("Cache-Control", "no-store")
		g.Header("X-Transcript-State", r.Status().State)
		g.Status(http.StatusOK)
		if errRender := render(g.Writer, r); errRender != nil {
			c.failed.Store(true)
		}
	}
}

func (c *Controller) Stats(g *gin.Context) {
	var stats core.Stats
	if c != nil && c.store != nil {
		stats = c.store.Stats()
	}
	if c != nil && c.failed.Load() {
		stats.Degraded = true
	}
	if c != nil {
		stats.Skipped += c.skipped.Load()
		stats.Degraded = stats.Degraded || stats.Skipped > 0
	}
	g.Header("Cache-Control", "no-store")
	g.JSON(http.StatusOK, gin.H{"enabled": c != nil && c.enabled.Load(), "stats": stats, "best_effort": true, "physical_limit": false})
}

func render(w io.Writer, r *core.Reader) error {
	last := ""
	corrupt := false
	for {
		frame, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			_, writeErr := io.WriteString(w, "\n[TRANSCRIPT GAP: invalid or incomplete frame]\n")
			if writeErr != nil {
				return writeErr
			}
			corrupt = true
			break
		}
		if m := frame.Metadata; m != nil {
			if _, err := fmt.Fprintf(w, "=== REQUEST INFO ===\nVersion: %s\nURL: %s\nMethod: %s\nTimestamp: %s\nRequest ID: %s\nUser: %s\nSession: %s\n\n=== HEADERS ===\n%s\n", buildinfo.Version, m.URL, m.Method, m.Started.Format(time.RFC3339Nano), m.RequestID, m.UserID, m.SessionID, httpMetadata("", "", 0, m.Headers)); err != nil {
				return err
			}
			continue
		}
		if frame.Status != nil {
			if frame.Kind == "gap" {
				if _, err := fmt.Fprintf(w, "\n[TRANSCRIPT GAP: dropped %d chunks / %d bytes]\n", frame.Status.DroppedChunks, frame.Status.DroppedBytes); err != nil {
					return err
				}
			}
			continue
		}
		section := nativeSection(frame.Kind)
		if section != last {
			if _, err := fmt.Fprintf(w, "\n%s\nTimestamp: %s\n", section, frame.At.Format(time.RFC3339Nano)); err != nil {
				return err
			}
			last = section
		}
		if strings.HasPrefix(frame.Kind, "websocket.") {
			if _, err := fmt.Fprintf(w, "\nTimestamp: %s\nEvent: %s\n", frame.At.Format(time.RFC3339Nano), frame.Kind); err != nil {
				return err
			}
		}
		if utf8.Valid(frame.Data) {
			if _, err := w.Write(frame.Data); err != nil {
				return err
			}
		} else {
			if _, err := fmt.Fprintf(w, "[BINARY base64]\n%s\n", base64.StdEncoding.EncodeToString(frame.Data)); err != nil {
				return err
			}
		}
	}
	s := r.Status()
	if corrupt {
		s.State = core.EndedIncomplete
		s.Failure = "invalid frame"
	}
	_, err := fmt.Fprintf(w, "\n\n=== TRANSCRIPT STATUS ===\nState: %s\nDropped chunks: %d\nDropped bytes: %d\nEnd approximate: %t\nFailure: %s\nBest-effort capture; crash-lost counters are not recoverable.\n", s.State, s.DroppedChunks, s.DroppedBytes, s.ApproximateEnd, s.Failure)
	return err
}

func nativeSection(kind string) string {
	switch {
	case kind == "request.body":
		return "=== REQUEST BODY ==="
	case strings.HasPrefix(kind, "response."):
		return "=== RESPONSE ==="
	case strings.HasPrefix(kind, "websocket.downstream."):
		return "=== WEBSOCKET TIMELINE ==="
	case strings.HasPrefix(kind, "websocket.upstream."):
		return "=== API WEBSOCKET TIMELINE ==="
	case strings.HasPrefix(kind, "upstream.request"):
		return "=== API REQUEST ==="
	case strings.HasPrefix(kind, "upstream.response"):
		return "=== API RESPONSE ==="
	case strings.HasPrefix(kind, "http.upstream."):
		parts := strings.Split(kind, ".")
		if len(parts) > 3 {
			if _, err := strconv.ParseUint(parts[2], 10, 64); err == nil {
				if parts[3] == "request" {
					return "=== API REQUEST " + parts[2] + " ==="
				}
				return "=== API RESPONSE " + parts[2] + " ==="
			}
		}
	}
	return "=== " + strings.ToUpper(strings.ReplaceAll(kind, ".", " ")) + " ==="
}
