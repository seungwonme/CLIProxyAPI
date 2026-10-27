package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/access"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/httptransport"
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
