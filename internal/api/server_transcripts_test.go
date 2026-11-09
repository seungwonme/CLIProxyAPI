package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/transcript/core"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/access"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/httptransport"
	"golang.org/x/crypto/bcrypt"
)

func TestTranscriptsNativeServerAuthQueuesAndAdmin(t *testing.T) {
	t.Setenv("TRANSCRIPT_FIXTURE_A", "fixture-client-a")
	t.Setenv("MANAGEMENT_PASSWORD", "fixture-admin")
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{AuthDir: t.TempDir(), RemoteManagement: config.RemoteManagement{AllowRemote: true}, SDKConfig: config.SDKConfig{APIKeys: []string{"fixture-client-a"}, RequestLog: true, MaxRequestBodyBytes: 32 << 20, Transcripts: config.Transcripts{Enabled: true, Root: root, Users: []config.TranscriptUser{{EnvName: "TRANSCRIPT_FIXTURE_A", UserID: "fixture-a"}}}}}
	manager := access.NewManager()
	factoryCalls := 0
	var requestID string
	s := NewServer(cfg, nil, manager, filepath.Join(t.TempDir(), "config.yaml"), WithLocalManagementPassword("fixture-admin"), WithRequestLoggerFactory(func(*config.Config, string) logging.RequestLogger { factoryCalls++; return nil }), WithRouterConfigurator(func(e *gin.Engine, _ *handlers.BaseAPIHandler, _ *config.Config) {
		e.POST("/v1/transcript-fixture", AuthMiddleware(manager), func(c *gin.Context) {
			requestID = logging.GetGinRequestID(c)
			body, errRead := io.ReadAll(c.Request.Body)
			if errRead != nil {
				c.Status(400)
				return
			}
			c.Header("Content-Type", "text/event-stream")
			c.Header("X-Request-Id", requestID)
			c.Status(200)
			_, _ = c.Writer.WriteString("data: ")
			_, _ = c.Writer.Write(body)
			_, _ = c.Writer.WriteString("\n\n")
			c.Writer.Flush()
		})
	}))
	defer func() { _ = s.Stop(context.Background()) }()
	if factoryCalls != 0 {
		t.Fatalf("native queue logger instantiated %d times", factoryCalls)
	}
	if s.requestLogger != nil {
		t.Fatal("enabled transcripts must bypass both native logging queues")
	}
	rejected := httptest.NewRecorder()
	s.Handler().ServeHTTP(rejected, httptest.NewRequest(http.MethodPost, "/v1/transcript-fixture", strings.NewReader("never-store-this")))
	if rejected.Code != http.StatusUnauthorized {
		t.Fatalf("rejected status %d", rejected.Code)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/transcript-fixture?auth_token=masked-query", strings.NewReader("visible-body-secret"))
	r.Header.Set("Authorization", "Bearer fixture-client-a")
	r.Header.Set("X-User", "forged-user")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 200 || w.Body.String() != "data: visible-body-secret\n\n" {
		t.Fatalf("wire response: %d %q", w.Code, w.Body.String())
	}
	if err := s.transcripts.Close(); err != nil {
		t.Fatal(err)
	}
	path := "/v8/management/observability/logs/requests/" + logging.ShortRequestID(requestID)
	denied := httptest.NewRecorder()
	s.Handler().ServeHTTP(denied, httptest.NewRequest(http.MethodGet, path, nil))
	if denied.Code == 200 {
		t.Fatal("admin download accepted no management credential")
	}
	adminReq := httptest.NewRequest(http.MethodGet, path, nil)
	adminReq.RemoteAddr = "127.0.0.1:12345"
	adminReq.Header.Set("Authorization", "Bearer fixture-admin")
	admin := httptest.NewRecorder()
	s.Handler().ServeHTTP(admin, adminReq)
	if admin.Code != 200 {
		t.Fatalf("download %d: %s", admin.Code, admin.Body.String())
	}
	text := admin.Body.String()
	for _, want := range []string{"=== REQUEST INFO ===", "visible-body-secret", "fixture-a", "ENDED_COMPLETE"} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q in %s", want, text)
		}
	}
	for _, secret := range []string{"fixture-client-a", "masked-query", "never-store-this"} {
		if strings.Contains(text, secret) {
			t.Errorf("secret/unauthenticated body leaked: %s", secret)
		}
	}
	if !strings.HasPrefix(admin.Header().Get("Content-Disposition"), "attachment;") {
		t.Fatal("native attachment header missing")
	}
}

func TestTranscriptsTransportDecoratorRecordsExactSSE(t *testing.T) {
	t.Setenv("TRANSCRIPT_FIXTURE_TRANSPORT", "fixture-client")
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(append([]byte("event: reply\r\ndata: "), append(body, []byte("\r\n\r\n")...)...))
	}))
	defer upstream.Close()
	cfg := &config.Config{AuthDir: t.TempDir(), SDKConfig: config.SDKConfig{APIKeys: []string{"fixture-client"}, Transcripts: config.Transcripts{Enabled: true, Root: root, Users: []config.TranscriptUser{{EnvName: "TRANSCRIPT_FIXTURE_TRANSPORT", UserID: "test-user"}}}}}
	manager := access.NewManager()
	var id string
	s := NewServer(cfg, nil, manager, "", WithRouterConfigurator(func(e *gin.Engine, _ *handlers.BaseAPIHandler, _ *config.Config) {
		e.POST("/v1/transcript-transport", AuthMiddleware(manager), func(c *gin.Context) {
			id = logging.GetGinRequestID(c)
			client := httptransport.Decorate(c.Request.Context(), upstream.Client())
			r, _ := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, upstream.URL+"?key=upstream-secret", strings.NewReader("wire-payload"))
			r.Header.Set("Authorization", "Bearer upstream-auth")
			resp, errDo := client.Do(r)
			if errDo != nil {
				t.Error(errDo)
				c.Status(502)
				return
			}
			defer func() { _ = resp.Body.Close() }()
			_, _ = io.Copy(c.Writer, resp.Body)
		})
	}))
	defer func() { _ = s.Stop(context.Background()) }()
	r := httptest.NewRequest(http.MethodPost, "/v1/transcript-transport", nil)
	r.Header.Set("Authorization", "Bearer fixture-client")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("wire status %d", w.Code)
	}
	if err := s.transcripts.Close(); err != nil {
		t.Fatal(err)
	}
	output := httptest.NewRecorder()
	g, _ := gin.CreateTestContext(output)
	g.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	g.Params = gin.Params{{Key: "id", Value: id}}
	// Invoke the actual download adapter; auth is covered separately above.
	s.transcripts.Download(func(*gin.Context) { t.Fatal("unexpected native fallback") })(g)
	text := output.Body.String()
	if !strings.Contains(text, "event: reply\r\ndata: wire-payload\r\n\r\n") {
		t.Fatalf("SSE wire bytes missing: %q", text)
	}
	if strings.Contains(text, "upstream-secret") || strings.Contains(text, "upstream-auth") {
		t.Fatal("upstream credentials leaked")
	}
}

func TestTranscriptsStatsManagementAccessAndRedaction(t *testing.T) {
	const (
		envName   = "TRANSCRIPT_FIXTURE_STATS"
		clientKey = "fixture-stats-client-secret"
		adminKey  = "fixture-stats-admin-secret"
		userID    = "fixture-stats-user"
		body      = "fixture-stats-request-body-secret"
		statsPath = "/v8/management/observability/transcripts"
	)
	t.Setenv("MANAGEMENT_PASSWORD", "")
	t.Setenv(envName, clientKey)
	t.Setenv("TRANSCRIPT_FIXTURE_STATS_UNSET", "")
	secretHash, err := bcrypt.GenerateFromPassword([]byte(adminKey), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name                         string
		invalidRoot, invalidBindings bool
		wantDegraded                 bool
	}{
		{name: "healthy"},
		{name: "invalid root", invalidRoot: true, wantDegraded: true},
		{name: "invalid bindings", invalidBindings: true, wantDegraded: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, errRoot := filepath.EvalSymlinks(t.TempDir())
			if errRoot != nil {
				t.Fatal(errRoot)
			}
			transcripts := config.Transcripts{Enabled: true, Root: root, Users: []config.TranscriptUser{{EnvName: envName, UserID: userID}}}
			if tc.invalidRoot {
				transcripts.Root = "relative-transcript-root"
			}
			if tc.invalidBindings {
				transcripts.Users[0].EnvName = "TRANSCRIPT_FIXTURE_STATS_UNSET"
			}
			cfg := &config.Config{RemoteManagement: config.RemoteManagement{AllowRemote: true, SecretKey: string(secretHash)}, SDKConfig: config.SDKConfig{APIKeys: []string{clientKey}, Transcripts: transcripts}}
			s := newTranscriptManagementTestServer(t, cfg)
			serveTranscriptManagementInference(t, s, clientKey, body)
			for i, auth := range []struct {
				name, key string
				status    int
			}{
				{"missing", "", http.StatusUnauthorized},
				{"wrong", "fixture-wrong-management-key", http.StatusUnauthorized},
				{"client", clientKey, http.StatusUnauthorized},
				{"management", adminKey, http.StatusOK},
			} {
				r := httptest.NewRequest(http.MethodGet, statsPath, nil)
				r.RemoteAddr = fmt.Sprintf("198.51.100.%d:12345", i+1)
				if auth.key != "" {
					r.Header.Set("Authorization", "Bearer "+auth.key)
				}
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, r)
				if w.Code != auth.status {
					t.Fatalf("%s credential: status=%d want=%d body=%s", auth.name, w.Code, auth.status, w.Body.String())
				}
				if auth.status != http.StatusOK {
					continue
				}
				if got := w.Header().Get("Cache-Control"); got != "no-store" {
					t.Errorf("Cache-Control=%q, want no-store", got)
				}
				var response struct {
					Enabled       *bool      `json:"enabled"`
					BestEffort    *bool      `json:"best_effort"`
					PhysicalLimit *bool      `json:"physical_limit"`
					Stats         core.Stats `json:"stats"`
				}
				if errDecode := json.Unmarshal(w.Body.Bytes(), &response); errDecode != nil {
					t.Fatal(errDecode)
				}
				if response.Enabled == nil || !*response.Enabled || response.BestEffort == nil || !*response.BestEffort || response.PhysicalLimit == nil || *response.PhysicalLimit {
					t.Fatalf("incorrect enabled/best-effort/logical-limit contract: %s", w.Body.String())
				}
				stats := response.Stats
				if stats.Degraded != tc.wantDegraded || stats.Active != 0 || stats.ReservedBytes != 0 {
					t.Fatalf("stats after inference barrier=%+v, want degraded=%t and no active/reserved work", stats, tc.wantDegraded)
				}
				if tc.wantDegraded {
					if stats.Records != 0 || stats.LogicalBytes != 0 {
						t.Errorf("unavailable store claims captured records: %+v", stats)
					}
				} else if stats.LimitBytes != 10<<30 || stats.Records != 1 || stats.LogicalBytes <= 0 || stats.Failures != 0 || stats.Skipped != 0 || stats.DroppedChunks != 0 || stats.DroppedBytes != 0 {
					t.Errorf("healthy inference was not reflected in aggregate stats: %+v", stats)
				}
				for _, secret := range []string{body, clientKey, adminKey, envName, "TRANSCRIPT_FIXTURE_STATS_UNSET", userID, root, string(secretHash)} {
					if strings.Contains(w.Body.String(), secret) || strings.Contains(w.Body.String(), fmt.Sprintf("%x", sha256.Sum256([]byte(secret)))) {
						t.Errorf("stats disclose synthetic sensitive value or its hash: %q", secret)
					}
				}
			}
			for _, path := range []string{"/v0/management/observability/transcripts", "/v0/management/transcripts"} {
				r := httptest.NewRequest(http.MethodGet, path, nil)
				r.RemoteAddr = "127.0.0.1:12345"
				r.Header.Set("Authorization", "Bearer "+adminKey)
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, r)
				if w.Code != http.StatusNotFound {
					t.Errorf("v8-only stats exposed at %s: status=%d body=%s", path, w.Code, w.Body.String())
				}
			}
		})
	}
	for _, tc := range []struct {
		name                                    string
		configured, disabled, home, allowRemote bool
		remoteAddr                              string
		status                                  int
	}{
		{name: "unconfigured management", remoteAddr: "127.0.0.1:12345", status: http.StatusNotFound},
		{name: "disabled management", configured: true, disabled: true, remoteAddr: "127.0.0.1:12345", status: http.StatusNotFound},
		{name: "Home", configured: true, home: true, allowRemote: true, remoteAddr: "127.0.0.1:12345", status: http.StatusNotFound},
		{name: "local management", configured: true, remoteAddr: "127.0.0.1:12345", status: http.StatusOK},
		{name: "remote disabled", configured: true, remoteAddr: "203.0.113.1:12345", status: http.StatusForbidden},
		{name: "remote enabled", configured: true, allowRemote: true, remoteAddr: "203.0.113.2:12345", status: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{RemoteManagement: config.RemoteManagement{AllowRemote: tc.allowRemote}, SDKConfig: config.SDKConfig{APIKeys: []string{clientKey}}}
			if tc.configured {
				cfg.RemoteManagement.SecretKey = string(secretHash)
			}
			cfg.Home.Enabled = tc.home
			s := newTranscriptManagementTestServer(t, cfg)
			if tc.disabled {
				s.managementRoutesEnabled.Store(false)
			}
			r := httptest.NewRequest(http.MethodGet, statsPath, nil)
			r.RemoteAddr = tc.remoteAddr
			r.Header.Set("Authorization", "Bearer "+adminKey)
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", w.Code, tc.status, w.Body.String())
			}
		})
	}
}

func TestTranscriptsPreserveNativeV0RequestLogContract(t *testing.T) {
	const (
		clientKey = "fixture-v0-client"
		adminKey  = "fixture-v0-admin"
		logName   = "v1_chat_completions-2026-09-28T100000-1234abcd.log"
		logBody   = "=== REQUEST INFO ===\r\nRequest ID: 1234abcd\r\n\r\n=== REQUEST BODY ===\r\n  native fixture body\t\r\n"
	)
	t.Setenv("MANAGEMENT_PASSWORD", "")
	t.Setenv("TRANSCRIPT_FIXTURE_V0", clientKey)
	secretHash, err := bcrypt.GenerateFromPassword([]byte(adminKey), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	logDir := t.TempDir()
	if errWrite := os.WriteFile(filepath.Join(logDir, logName), []byte(logBody), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	for _, tc := range []struct {
		name    string
		enabled bool
	}{{"transcripts disabled", false}, {"transcripts enabled", true}} {
		t.Run(tc.name, func(t *testing.T) {
			root, errRoot := filepath.EvalSymlinks(t.TempDir())
			if errRoot != nil {
				t.Fatal(errRoot)
			}
			cfg := &config.Config{RemoteManagement: config.RemoteManagement{SecretKey: string(secretHash)}, SDKConfig: config.SDKConfig{APIKeys: []string{clientKey}, RequestLog: true, Transcripts: config.Transcripts{Enabled: tc.enabled, Root: root, Users: []config.TranscriptUser{{EnvName: "TRANSCRIPT_FIXTURE_V0", UserID: "fixture-v0-user"}}}}}
			s := newTranscriptManagementTestServer(t, cfg)
			s.mgmt.SetLogDirectory(logDir)
			serveTranscriptManagementInference(t, s, clientKey, "fixture-v0-inference-body")
			for _, route := range []struct {
				path                           string
				status                         int
				body, contentType, disposition string
			}{
				{"/v0/management/request-log", http.StatusOK, `{"request-log":true}`, "application/json; charset=utf-8", ""},
				{"/v0/management/request-log-by-id/1234abcd", http.StatusOK, logBody, "text/plain; charset=utf-8", `attachment; filename="` + logName + `"`},
				{"/v0/management/request-log-by-id/018f3a5b-1234-7abc-def0-00001234abcd", http.StatusOK, logBody, "text/plain; charset=utf-8", `attachment; filename="` + logName + `"`},
				{"/v0/management/request-log-by-id/deadbeef", http.StatusNotFound, `{"error":"log file not found for the given request ID"}`, "application/json; charset=utf-8", ""},
			} {
				r := httptest.NewRequest(http.MethodGet, route.path, nil)
				r.RemoteAddr = "127.0.0.1:12345"
				r.Header.Set("Authorization", "Bearer "+adminKey)
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, r)
				if w.Code != route.status || w.Body.String() != route.body || w.Header().Get("Content-Type") != route.contentType || w.Header().Get("Content-Disposition") != route.disposition {
					t.Errorf("%s: status=%d body=%q Content-Type=%q Content-Disposition=%q; want status=%d body=%q Content-Type=%q Content-Disposition=%q", route.path, w.Code, w.Body.String(), w.Header().Get("Content-Type"), w.Header().Get("Content-Disposition"), route.status, route.body, route.contentType, route.disposition)
				}
			}
		})
	}
}

func newTranscriptManagementTestServer(t *testing.T, cfg *config.Config) *Server {
	t.Helper()
	cfg.AuthDir = t.TempDir()
	manager := access.NewManager()
	s := NewServer(cfg, nil, manager, filepath.Join(t.TempDir(), "config.yaml"), WithRequestLoggerFactory(func(*config.Config, string) logging.RequestLogger { return nil }), WithRouterConfigurator(func(e *gin.Engine, _ *handlers.BaseAPIHandler, _ *config.Config) {
		e.POST("/v1/transcript-management-fixture", AuthMiddleware(manager), func(c *gin.Context) {
			body, errRead := io.ReadAll(c.Request.Body)
			if errRead != nil {
				c.Status(http.StatusBadRequest)
				return
			}
			c.Data(http.StatusOK, "text/plain; charset=utf-8", body)
		})
	}))
	t.Cleanup(func() {
		if errStop := s.Stop(context.Background()); errStop != nil {
			t.Error(errStop)
		}
		if errClose := s.transcripts.Close(); errClose != nil {
			t.Error(errClose)
		}
	})
	return s
}

func serveTranscriptManagementInference(t *testing.T, s *Server, clientKey, body string) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/v1/transcript-management-fixture", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+clientKey)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusOK || w.Body.String() != body {
		t.Fatalf("authenticated inference: status=%d body=%q, want 200 and %q", w.Code, w.Body.String(), body)
	}
	if errClose := s.transcripts.Close(); errClose != nil {
		t.Fatal(errClose)
	}
}
