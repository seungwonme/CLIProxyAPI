package api

import (
	"context"
	"net/http"
	"net/http/httptest"
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
