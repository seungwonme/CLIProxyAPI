package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/access"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

func TestTranscriptsActualBidirectionalResponsesWebsocket(t *testing.T) {
	t.Setenv("TRANSCRIPT_WEBSOCKET_FIXTURE", "synthetic-websocket-client")
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const model = "transcript-local-ws-fixture"
	const authID = "transcript-local-ws-auth"
	created := []byte(" \n{\"type\":\"response.created\",\"response\":{\"id\":\"transcript-r1\"}} \n")
	completed := []byte(`{"type":"response.completed","response":{"id":"transcript-r1","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"fixture-output"}]}]}}`)
	upstreamBody := make(chan []byte, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Error(errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()
		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Error(errRead)
			return
		}
		upstreamBody <- payload
		if errWrite := conn.WriteMessage(websocket.TextMessage, created); errWrite != nil {
			t.Error(errWrite)
			return
		}
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			t.Error(errWrite)
			return
		}
		_, _, _ = conn.ReadMessage()
	}))
	defer upstream.Close()
	cfg := &config.Config{AuthDir: t.TempDir()}
	cfg.APIKeys = []string{"synthetic-websocket-client"}
	cfg.Transcripts = config.Transcripts{Enabled: true, Root: root, Users: []config.TranscriptUser{{EnvName: "TRANSCRIPT_WEBSOCKET_FIXTURE", UserID: "websocket-user"}}}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	manager.RegisterExecutor(executor.NewCodexAutoExecutor(cfg))
	if _, errRegister := manager.Register(context.Background(), &coreauth.Auth{ID: authID, Provider: "codex", Status: coreauth.StatusActive, Attributes: map[string]string{"api_key": "synthetic-provider-key", "base_url": upstream.URL, "websockets": "true"}}); errRegister != nil {
		t.Fatal(errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
	defer registry.GetGlobalRegistry().UnregisterClient(authID)
	ended := make(chan string, 1)
	s := NewServer(cfg, manager, access.NewManager(), "", WithMiddleware(func(c *gin.Context) {
		c.Next()
		if c.Request.URL.Path == "/v1/responses" {
			ended <- logging.GetGinRequestID(c)
		}
	}))
	defer func() { _ = s.Stop(context.Background()) }()
	downstream := httptest.NewServer(s.Handler())
	defer downstream.Close()
	headers := http.Header{"Authorization": {"Bearer synthetic-websocket-client"}, "Session_id": {"same-client-session"}}
	client, _, errDial := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(downstream.URL, "http")+"/v1/responses", headers)
	if errDial != nil {
		t.Fatal(errDial)
	}
	defer func() { _ = client.Close() }()
	if err := client.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	request := []byte(`{"type":"response.create","model":"` + model + `","input":[{"role":"user","content":"fixture-input"}]}`)
	if err := client.WriteMessage(websocket.TextMessage, request); err != nil {
		t.Fatal(err)
	}
	for {
		_, data, errRead := client.ReadMessage()
		if errRead != nil {
			t.Fatal(errRead)
		}
		if gjson.GetBytes(data, "type").String() == "response.completed" {
			break
		}
	}
	_ = client.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
	_ = client.Close()
	var id string
	select {
	case id = <-ended:
	case <-time.After(10 * time.Second):
		t.Fatal("actual websocket handler did not finish")
	}
	if err := s.transcripts.Close(); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	g, _ := gin.CreateTestContext(response)
	g.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	g.Params = gin.Params{{Key: "id", Value: id}}
	s.transcripts.Download(func(*gin.Context) { t.Fatal("native fallback") })(g)
	if response.Code != http.StatusOK {
		t.Fatalf("download %d %s", response.Code, response.Body.String())
	}
	text := response.Body.String()
	for _, want := range []string{string(request), string(created), "fixture-output", "websocket.downstream.input.1", "websocket.downstream.output.1", "websocket.upstream.input.1", "websocket.upstream.output.1", "=== API WEBSOCKET TIMELINE ==="} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q in transcript %s", want, text)
		}
	}
	if strings.Contains(text, "synthetic-provider-key") || strings.Contains(text, "synthetic-websocket-client") {
		t.Fatal("websocket credentials leaked")
	}
	select {
	case actual := <-upstreamBody:
		if !strings.Contains(string(actual), "fixture-input") {
			t.Fatalf("upstream request altered: %s", actual)
		}
	default:
		t.Fatal("upstream attempt not observed")
	}
}

func TestTranscriptsActualResponsesWebsocketOperationalRedaction(t *testing.T) {
	const clientKey = "synthetic-websocket-redaction-client"
	t.Setenv("TRANSCRIPT_WEBSOCKET_REDACTION", clientKey)
	t.Setenv("TRANSCRIPT_WEBSOCKET_UNMAPPED", "synthetic-other-websocket-client")
	t.Setenv("MANAGEMENT_PASSWORD", "")
	previousGinMode := gin.Mode()
	defer gin.SetMode(previousGinMode)
	// Keep this test and its cases serial: they temporarily replace the shared logger.
	for _, tc := range []struct {
		name         string
		invalidRoot  bool
		unmapped     bool
		wantDownload int
	}{
		{name: "healthy", wantDownload: http.StatusOK},
		{name: "invalid_root", invalidRoot: true, wantDownload: http.StatusServiceUnavailable},
		{name: "unmapped", unmapped: true, wantDownload: http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			capture, err := os.CreateTemp(t.TempDir(), "operational-*.log")
			if err != nil {
				t.Fatal(err)
			}
			logger := log.StandardLogger()
			previousOutput, previousFormatter, previousLevel := logger.Out, logger.Formatter, logger.GetLevel()
			previousHooks := logger.ReplaceHooks(make(log.LevelHooks))
			logger.SetOutput(capture)
			logger.SetFormatter(&logging.LogFormatter{})
			logger.SetLevel(log.DebugLevel)
			defer func() {
				logger.SetOutput(previousOutput)
				logger.SetFormatter(previousFormatter)
				logger.SetLevel(previousLevel)
				logger.ReplaceHooks(previousHooks)
				_ = capture.Close()
			}()
			root, errRoot := filepath.EvalSymlinks(t.TempDir())
			if errRoot != nil {
				t.Fatal(errRoot)
			}
			transcripts := config.Transcripts{Enabled: true, Root: root, Users: []config.TranscriptUser{{EnvName: "TRANSCRIPT_WEBSOCKET_REDACTION", UserID: "websocket-redaction-user"}}}
			if tc.invalidRoot {
				transcripts.Root = "relative-websocket-transcript-root"
			}
			if tc.unmapped {
				transcripts.Users[0].EnvName = "TRANSCRIPT_WEBSOCKET_UNMAPPED"
			}
			cfg := &config.Config{AuthDir: t.TempDir()}
			cfg.APIKeys, cfg.Transcripts = []string{clientKey}, transcripts
			var nativeID string
			s := NewServer(cfg, nil, access.NewManager(), "", WithMiddleware(func(c *gin.Context) {
				nativeID = logging.GetGinRequestID(c)
				c.Next()
			}))
			defer func() {
				_ = s.Stop(context.Background())
				_ = s.transcripts.Close()
			}()
			ended := make(chan struct{})
			downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				s.Handler().ServeHTTP(w, r)
				close(ended)
			}))
			defer downstream.Close()
			dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
			client, _, errDial := dialer.Dial("ws"+strings.TrimPrefix(downstream.URL, "http")+"/v1/responses", http.Header{"Authorization": {"Bearer " + clientKey}})
			if errDial != nil {
				t.Fatal(errDial)
			}
			defer func() {
				_ = client.Close()
				select {
				case <-ended:
				case <-time.After(5 * time.Second):
					t.Error("actual websocket handler did not finish during cleanup")
				}
			}()
			if errDeadline := client.SetReadDeadline(time.Now().Add(5 * time.Second)); errDeadline != nil {
				t.Fatal(errDeadline)
			}
			if errDeadline := client.SetWriteDeadline(time.Now().Add(5 * time.Second)); errDeadline != nil {
				t.Fatal(errDeadline)
			}
			bodySentinel, tokenSentinel := "ws-body-secret-"+tc.name, "ws-token-secret-"+tc.name
			closeSentinel := "ws-close-secret-" + tc.name
			attackerType := "response." + bodySentinel + "." + tokenSentinel
			messages := []struct{ request, errorMessage string }{
				{` {"input":"` + bodySentinel + `","token":"` + tokenSentinel + `"`, "unsupported websocket request type: "},
				{" \n" + `{"type":"` + attackerType + `","input":"` + bodySentinel + `","token":"` + tokenSentinel + `"}` + " \n", "unsupported websocket request type: " + attackerType},
			}
			var received []string
			for _, message := range messages {
				if errWrite := client.WriteMessage(websocket.TextMessage, []byte(message.request)); errWrite != nil {
					t.Fatal(errWrite)
				}
				kind, payload, errRead := client.ReadMessage()
				if errRead != nil {
					t.Fatal(errRead)
				}
				want := `{"type":"error","status":400,"error":{"message":"` + message.errorMessage + `","type":"invalid_request_error"}}`
				if kind != websocket.TextMessage || string(payload) != want {
					t.Fatalf("websocket rejection type=%d payload=%q, want text %q", kind, payload, want)
				}
				received = append(received, string(payload))
			}
			if errClose := client.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, closeSentinel), time.Now().Add(time.Second)); errClose != nil {
				t.Fatal(errClose)
			}
			if _, _, errClose := client.ReadMessage(); !websocket.IsCloseError(errClose, websocket.CloseNormalClosure) {
				t.Fatalf("normal websocket close handshake: %v", errClose)
			}
			select {
			case <-ended:
			case <-time.After(5 * time.Second):
				t.Fatal("actual websocket handler did not finish")
			}
			if nativeID == "" {
				t.Fatal("actual handler did not assign a native request ID")
			}
			if errClose := s.transcripts.Close(); errClose != nil {
				t.Fatal(errClose)
			}
			output, errRead := os.ReadFile(capture.Name())
			if errRead != nil {
				t.Fatal(errRead)
			}
			operational := string(output)
			for _, secret := range []string{bodySentinel, tokenSentinel, closeSentinel, clientKey, "unsupported websocket request type:"} {
				if strings.Contains(operational, secret) {
					t.Errorf("operational log leaked %q: %s", secret, operational)
				}
			}
			for i, message := range messages {
				if strings.Contains(operational, message.request) || strings.Contains(operational, received[i]) {
					t.Errorf("operational log contains raw websocket request or error: %s", operational)
				}
				if want := fmt.Sprintf("type=1 event=error status=400 bytes=%d", len(received[i])); !strings.Contains(operational, want) {
					t.Errorf("missing safe error metadata %q: %s", want, operational)
				}
			}
			if !strings.Contains(operational, "responses websocket: client disconnected") {
				t.Errorf("normal-close diagnostic not exercised: %s", operational)
			}
			response := httptest.NewRecorder()
			g, _ := gin.CreateTestContext(response)
			g.Request = httptest.NewRequest(http.MethodGet, "/", nil)
			g.Params = gin.Params{{Key: "id", Value: nativeID}}
			s.transcripts.Download(func(*gin.Context) { t.Fatal("unexpected native fallback") })(g)
			if response.Code != tc.wantDownload {
				t.Fatalf("native download status=%d want=%d body=%s", response.Code, tc.wantDownload, response.Body.String())
			}
			if tc.wantDownload == http.StatusOK {
				for i, message := range messages {
					for _, raw := range []string{message.request, received[i]} {
						if !strings.Contains(response.Body.String(), raw) {
							t.Errorf("exact websocket wire bytes missing from native transcript: %q", raw)
						}
					}
				}
			}
		})
	}
}
