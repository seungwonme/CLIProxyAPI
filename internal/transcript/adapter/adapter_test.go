package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	configaccess "github.com/router-for-me/CLIProxyAPI/v8/internal/access/config_access"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/transcript/core"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v8/sdk/access"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

const (
	adapterAlphaEnv   = "TRANSCRIPT_ADAPTER_TEST_ALPHA"
	adapterBetaEnv    = "TRANSCRIPT_ADAPTER_TEST_BETA"
	adapterRotatedEnv = "TRANSCRIPT_ADAPTER_TEST_ROTATED"
	adapterOtherEnv   = "TRANSCRIPT_ADAPTER_TEST_OTHER"
)

func adapterTestUsers(t *testing.T) []config.TranscriptUser {
	t.Helper()
	t.Setenv(adapterAlphaEnv, "synthetic-transcript-alpha-key")
	t.Setenv(adapterBetaEnv, "synthetic-transcript-beta-key")
	t.Setenv(adapterRotatedEnv, "synthetic-transcript-alpha-rotated-key")
	t.Setenv(adapterOtherEnv, "synthetic-transcript-unmapped-key")
	return []config.TranscriptUser{
		{EnvName: adapterAlphaEnv, UserID: "alpha"},
		{EnvName: adapterBetaEnv, UserID: "beta"},
		{EnvName: adapterRotatedEnv, UserID: "alpha"},
	}
}

func adapterTestRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("EvalSymlinks(TempDir): %v", err)
	}
	return filepath.Join(root, "records")
}

func adapterTestController(t *testing.T, users []config.TranscriptUser) (*Controller, string, func()) {
	t.Helper()
	root := adapterTestRoot(t)
	controller, err := New(config.Transcripts{Enabled: true, Root: root, Users: users})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	closed := false
	closeStore := func() {
		t.Helper()
		if !closed {
			closed = true
			if errClose := controller.Close(); errClose != nil {
				t.Errorf("Close: %v", errClose)
			}
		}
	}
	t.Cleanup(closeStore)
	return controller, root, closeStore
}

func adapterTestManager(t *testing.T) *sdkaccess.Manager {
	t.Helper()
	var previous sdkaccess.Provider
	for _, provider := range sdkaccess.RegisteredProviders() {
		if provider.Identifier() == sdkaccess.DefaultAccessProviderName {
			previous = provider
		}
	}
	configaccess.Register(&sdkconfig.SDKConfig{APIKeys: []string{
		os.Getenv(adapterAlphaEnv), os.Getenv(adapterBetaEnv), os.Getenv(adapterRotatedEnv), os.Getenv(adapterOtherEnv),
	}})
	t.Cleanup(func() {
		configaccess.Register(nil)
		if previous != nil {
			sdkaccess.RegisterProvider(sdkaccess.AccessProviderTypeConfigAPIKey, previous)
		}
	})
	manager := sdkaccess.NewManager()
	for _, provider := range sdkaccess.RegisteredProviders() {
		if provider.Identifier() == sdkaccess.DefaultAccessProviderName {
			manager.SetProviders([]sdkaccess.Provider{provider})
			return manager
		}
	}
	t.Fatal("registered native config-access provider was not found")
	return nil
}

func adapterTestRouter(controller *Controller, manager *sdkaccess.Manager, handler gin.HandlerFunc) *gin.Engine {
	router := gin.New()
	router.Use(gin.CustomRecoveryWithWriter(io.Discard, func(g *gin.Context, _ any) {
		g.AbortWithStatus(http.StatusInternalServerError)
	}))
	router.Use(func(g *gin.Context) {
		logging.SetGinRequestID(g, logging.GetRequestID(g.Request.Context()))
		g.Next()
	})
	router.Use(controller.Middleware())
	router.Use(func(g *gin.Context) {
		result, authErr := manager.Authenticate(g.Request.Context(), g.Request)
		if authErr != nil {
			g.AbortWithStatus(authErr.HTTPStatusCode())
			return
		}
		if result == nil {
			g.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		g.Set("adapter-test-auth-result", result)
		g.Next()
	})
	router.Any("/v1/responses", handler)
	return router
}

func adapterTestRequest(id, envName, target string, body io.Reader) *http.Request {
	req := httptest.NewRequest(http.MethodPost, target, body)
	req = req.WithContext(logging.WithRequestID(req.Context(), id))
	if envName != "" {
		req.Header.Set("Authorization", "Bearer "+os.Getenv(envName))
	}
	req.Header.Set("Session_id", "shared-client-session")
	return req
}

type adapterTestRecord struct {
	metadata       core.Metadata
	metadataFrames int
	parts          map[string][]byte
	state          string
	name           string
	droppedChunks  uint64
	droppedBytes   uint64
}

func adapterReadRecord(t *testing.T, controller *Controller, id string) adapterTestRecord {
	t.Helper()
	reader, err := controller.store.Open(id)
	if err != nil {
		t.Fatalf("Open(%q): %v", id, err)
	}
	defer func() {
		if errClose := reader.Close(); errClose != nil {
			t.Errorf("reader.Close: %v", errClose)
		}
	}()
	record := adapterTestRecord{parts: make(map[string][]byte), name: reader.Name()}
	for {
		frame, errNext := reader.Next()
		if errNext == io.EOF {
			break
		}
		if errNext != nil {
			t.Fatalf("Next(%q): %v", id, errNext)
		}
		if frame.Metadata != nil {
			record.metadata = *frame.Metadata
			record.metadataFrames++
		} else if frame.Status == nil {
			record.parts[frame.Kind] = append(record.parts[frame.Kind], frame.Data...)
		}
	}
	status := reader.Status()
	record.state = string(status.State)
	record.droppedChunks = status.DroppedChunks
	record.droppedBytes = status.DroppedBytes
	return record
}

func adapterAssertMissing(t *testing.T, controller *Controller, id string) {
	t.Helper()
	reader, err := controller.store.Open(id)
	if reader != nil {
		if errClose := reader.Close(); errClose != nil {
			t.Errorf("unexpected reader.Close: %v", errClose)
		}
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Open(%q) error = %v, want os.ErrNotExist", id, err)
	}
}

type adapterCountingBody struct {
	reader io.Reader
	reads  int
	closes int
}

func (b *adapterCountingBody) Read(p []byte) (int, error) {
	b.reads++
	return b.reader.Read(p)
}

func (b *adapterCountingBody) Close() error {
	b.closes++
	return nil
}

func TestAdapterVerifiedPrincipalPartitionsSessionsAndKeyRotation(t *testing.T) {
	users := adapterTestUsers(t)
	controller, _, closeStore := adapterTestController(t, users)
	manager := adapterTestManager(t)
	router := adapterTestRouter(controller, manager, func(g *gin.Context) {
		body, err := io.ReadAll(g.Request.Body)
		if err != nil {
			t.Errorf("ReadAll: %v", err)
			g.AbortWithStatus(http.StatusBadRequest)
			return
		}
		if logging.TranscriptFromContext(g.Request.Context()) == nil {
			t.Error("verified mapped principal has no transcript sink")
		}
		g.Data(http.StatusOK, "application/json", body)
	})

	requests := []struct {
		id   string
		env  string
		user string
	}{
		{id: "partition-alpha", env: adapterAlphaEnv, user: "alpha"},
		{id: "partition-beta", env: adapterBetaEnv, user: "beta"},
		{id: "partition-alpha-rotated", env: adapterRotatedEnv, user: "alpha"},
	}
	for _, request := range requests {
		body := `{"message":"` + request.id + `","secret":"raw-body-must-survive"}`
		req := adapterTestRequest(request.id, request.env, "/v1/responses", strings.NewReader(body))
		req.Header.Set("X-User", "spoofed-user")
		req.Header.Set("X-User-ID", "spoofed-user")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		if response.Code != http.StatusOK || response.Body.String() != body {
			t.Fatalf("request %q = (%d, %q), want unchanged JSON body", request.id, response.Code, response.Body.String())
		}
	}
	// ServeHTTP is synchronous; Close is the persistence barrier, not a timer.
	closeStore()
	records := make([]adapterTestRecord, len(requests))
	for i, request := range requests {
		records[i] = adapterReadRecord(t, controller, request.id)
		record := records[i]
		if record.metadataFrames != 1 || record.metadata.RequestID != request.id || record.metadata.UserID != request.user {
			t.Fatalf("record %q metadata = %#v, frames=%d", request.id, record.metadata, record.metadataFrames)
		}
		if record.state != core.EndedComplete || record.droppedChunks != 0 || record.droppedBytes != 0 {
			t.Fatalf("record %q state=%q dropped=(%d,%d)", request.id, record.state, record.droppedChunks, record.droppedBytes)
		}
		if record.metadata.SessionID == "" || record.metadata.SessionID == "shared-client-session" {
			t.Fatalf("record %q persisted an unhashed/empty session identifier", request.id)
		}
		if !bytes.Equal(record.parts["request.body"], record.parts["response.body"]) || !bytes.Contains(record.parts["request.body"], []byte(request.id)) {
			t.Fatalf("record %q did not retain its own exact request/response bytes", request.id)
		}
	}
	if records[0].metadata.SessionID == records[1].metadata.SessionID {
		t.Fatal("different verified users with the same client session shared a session hash")
	}
	if records[0].metadata.SessionID != records[2].metadata.SessionID {
		t.Fatal("rotating a credential changed the stable user's session partition")
	}
	if records[0].name == records[1].name || records[0].name == records[2].name {
		t.Fatal("independent requests shared a physical record name")
	}
}

func TestAdapterNativeCredentialPrecedence(t *testing.T) {
	users := adapterTestUsers(t)
	controller, _, closeStore := adapterTestController(t, users)
	manager := adapterTestManager(t)
	var sources []string
	router := adapterTestRouter(controller, manager, func(g *gin.Context) {
		result, _ := g.Get("adapter-test-auth-result")
		sources = append(sources, result.(*sdkaccess.Result).Metadata["source"])
		g.Status(http.StatusNoContent)
	})
	tests := []struct {
		id       string
		headers  http.Header
		query    string
		wantUser string
		source   string
	}{
		{id: "precedence-authorization", headers: http.Header{"Authorization": {"Bearer " + os.Getenv(adapterAlphaEnv)}, "X-Goog-Api-Key": {os.Getenv(adapterBetaEnv)}, "X-Api-Key": {os.Getenv(adapterBetaEnv)}}, query: "?key=" + os.Getenv(adapterBetaEnv) + "&auth_token=" + os.Getenv(adapterBetaEnv), wantUser: "alpha", source: "authorization"},
		{id: "precedence-google", headers: http.Header{"Authorization": {"Bearer " + os.Getenv(adapterOtherEnv) + "-invalid"}, "X-Goog-Api-Key": {os.Getenv(adapterBetaEnv)}, "X-Api-Key": {os.Getenv(adapterAlphaEnv)}}, query: "?key=" + os.Getenv(adapterAlphaEnv), wantUser: "beta", source: "x-goog-api-key"},
		{id: "precedence-anthropic", headers: http.Header{"X-Api-Key": {os.Getenv(adapterAlphaEnv)}}, query: "?key=" + os.Getenv(adapterBetaEnv), wantUser: "alpha", source: "x-api-key"},
		{id: "precedence-query-key", query: "?key=" + os.Getenv(adapterBetaEnv) + "&auth_token=" + os.Getenv(adapterAlphaEnv), wantUser: "beta", source: "query-key"},
		{id: "precedence-query-auth-token", query: "?auth_token=" + os.Getenv(adapterRotatedEnv), wantUser: "alpha", source: "query-auth-token"},
	}
	for _, tt := range tests {
		req := adapterTestRequest(tt.id, "", "/v1/responses"+tt.query, nil)
		for name, values := range tt.headers {
			req.Header[name] = values
		}
		req.Header.Set("X-User", "spoofed-user")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		if response.Code != http.StatusNoContent {
			t.Fatalf("%s HTTP status = %d", tt.id, response.Code)
		}
	}
	closeStore()
	for i, tt := range tests {
		record := adapterReadRecord(t, controller, tt.id)
		if record.metadata.UserID != tt.wantUser || sources[i] != tt.source {
			t.Errorf("%s user/source = (%q, %q), want (%q, %q)", tt.id, record.metadata.UserID, sources[i], tt.wantUser, tt.source)
		}
		for _, env := range []string{adapterAlphaEnv, adapterBetaEnv, adapterRotatedEnv, adapterOtherEnv} {
			if strings.Contains(record.metadata.URL, os.Getenv(env)) {
				t.Errorf("%s metadata URL retained a credential", tt.id)
			}
		}
	}
}

func TestAdapterUnauthenticatedRequestDoesNotReadOrPersistBody(t *testing.T) {
	users := adapterTestUsers(t)
	controller, _, closeStore := adapterTestController(t, users)
	manager := adapterTestManager(t)
	called := false
	router := adapterTestRouter(controller, manager, func(g *gin.Context) {
		called = true
		g.Status(http.StatusOK)
	})
	for _, id := range []string{"unauthenticated-missing", "unauthenticated-invalid"} {
		body := &adapterCountingBody{reader: strings.NewReader("unauthenticated-body-must-not-be-read")}
		req := adapterTestRequest(id, "", "/v1/responses", nil)
		req.Body = body
		req.ContentLength = int64(len("unauthenticated-body-must-not-be-read"))
		req.Header.Set("X-User", "alpha")
		if strings.HasSuffix(id, "invalid") {
			req.Header.Set("Authorization", "Bearer "+os.Getenv(adapterOtherEnv)+"-invalid")
		}
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		if response.Code != http.StatusUnauthorized || body.reads != 0 || body.closes != 0 || called {
			t.Fatalf("%s status=%d body reads/closes=(%d,%d) handler=%t", id, response.Code, body.reads, body.closes, called)
		}
	}
	closeStore()
	adapterAssertMissing(t, controller, "unauthenticated-missing")
	adapterAssertMissing(t, controller, "unauthenticated-invalid")
}

func TestAdapterAdmissionUsesFirstVerifiedPrincipalOnly(t *testing.T) {
	users := adapterTestUsers(t)
	controller, _, closeStore := adapterTestController(t, users)
	manager := adapterTestManager(t)
	router := adapterTestRouter(controller, manager, func(g *gin.Context) {
		initial := logging.TranscriptFromContext(g.Request.Context())
		g.Request.Header.Set("Authorization", "Bearer "+os.Getenv(adapterBetaEnv))
		result, authErr := manager.Authenticate(g.Request.Context(), g.Request)
		if authErr != nil || result == nil || result.Principal != os.Getenv(adapterBetaEnv) {
			t.Errorf("native reauthentication failed: result=%#v error=%v", result, authErr)
		}
		if logging.TranscriptFromContext(g.Request.Context()) != initial {
			t.Error("reauthentication replaced the original transcript sink")
		}
		body, err := io.ReadAll(g.Request.Body)
		if err != nil {
			t.Errorf("ReadAll: %v", err)
		}
		g.Data(http.StatusOK, "text/plain", body)
	})
	for _, request := range []struct{ id, env string }{
		{id: "reauthentication-bound", env: adapterAlphaEnv},
		{id: "reauthentication-unmapped", env: adapterOtherEnv},
	} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, adapterTestRequest(request.id, request.env, "/v1/responses", strings.NewReader("read-once")))
		if response.Code != http.StatusOK || response.Body.String() != "read-once" {
			t.Fatalf("%s changed application behavior: (%d, %q)", request.id, response.Code, response.Body.String())
		}
	}
	closeStore()
	record := adapterReadRecord(t, controller, "reauthentication-bound")
	if record.metadata.UserID != "alpha" || record.metadataFrames != 1 {
		t.Fatalf("reauthentication duplicated or reassigned the capture: %#v", record)
	}
	// A different verified principal cannot append content to the first user's
	// transcript. Keep the original admission and make the omitted bytes visible.
	if len(record.parts["request.body"]) != 0 || record.state != core.EndedIncomplete || record.droppedChunks == 0 || record.droppedBytes < uint64(len("read-once")) {
		t.Fatalf("changed principal content was attributed to the original user or loss hidden: %#v", record)
	}
	adapterAssertMissing(t, controller, "reauthentication-unmapped")
	if controller.skipped.Load() != 1 {
		t.Fatalf("unmapped principal skips = %d, want one per request", controller.skipped.Load())
	}
}

func TestAdapterCapturesSSEUpstreamEventsAndBinaryBytes(t *testing.T) {
	users := adapterTestUsers(t)
	controller, _, closeStore := adapterTestController(t, users)
	manager := adapterTestManager(t)
	binary := []byte{0, 0xff, 0x80, '\n', 0x01}
	stream := "data: {\"delta\":\"hello\"}\n\ndata: [DONE]\n\n"
	router := adapterTestRouter(controller, manager, func(g *gin.Context) {
		body, err := io.ReadAll(g.Request.Body)
		if err != nil {
			t.Errorf("ReadAll: %v", err)
		}
		sink := logging.TranscriptFromContext(g.Request.Context())
		if sink == nil {
			t.Error("missing transcript sink")
			g.AbortWithStatus(http.StatusInternalServerError)
			return
		}
		sink.HTTP("http.upstream.1.request", http.MethodPost, "https://provider.test/v1/respond?key="+os.Getenv(adapterAlphaEnv), 0, http.Header{"Authorization": {"Bearer " + os.Getenv(adapterAlphaEnv)}, "Content-Type": {"application/json"}}, []byte(`{"raw":"upstream-request-secret"}`))
		sink.HTTP("http.upstream.1.response", "", "", http.StatusAccepted, http.Header{"Set-Cookie": {os.Getenv(adapterBetaEnv)}, "Content-Type": {"text/event-stream"}}, []byte("upstream-response-secret"))
		sink.Capture("provider.ws.response", binary)
		sink.Failure("http.upstream.1.error", errors.New("failed https://provider.test/?key="+os.Getenv(adapterAlphaEnv)))
		if !bytes.Equal(body, binary) {
			t.Error("request body changed")
		}
		g.Header("Content-Type", "text/event-stream")
		for _, chunk := range []string{"data: {\"delta\":\"hello\"}\n\n", "data: [DONE]\n\n"} {
			if _, errWrite := g.Writer.WriteString(chunk); errWrite != nil {
				t.Errorf("WriteString: %v", errWrite)
			}
			g.Writer.Flush()
		}
	})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, adapterTestRequest("stream-and-binary", adapterAlphaEnv, "/v1/responses", bytes.NewReader(binary)))
	if response.Code != http.StatusOK || response.Body.String() != stream || !response.Flushed {
		t.Fatalf("SSE response = (%d, %q, flushed=%t)", response.Code, response.Body.String(), response.Flushed)
	}
	closeStore()
	record := adapterReadRecord(t, controller, "stream-and-binary")
	if !bytes.Equal(record.parts["request.body"], binary) || !bytes.Equal(record.parts["provider.ws.response"], binary) || string(record.parts["response.body"]) != stream {
		t.Fatalf("binary request/provider or SSE response bytes changed: %#v", record.parts)
	}
	if string(record.parts["http.upstream.1.request.body"]) != `{"raw":"upstream-request-secret"}` || string(record.parts["http.upstream.1.response.body"]) != "upstream-response-secret" {
		t.Fatal("upstream body events were not captured verbatim")
	}
	for _, kind := range []string{"http.upstream.1.request.metadata", "http.upstream.1.response.metadata", "http.upstream.1.error"} {
		if len(record.parts[kind]) == 0 {
			t.Errorf("missing upstream event %q", kind)
		}
		for _, env := range []string{adapterAlphaEnv, adapterBetaEnv} {
			if bytes.Contains(record.parts[kind], []byte(os.Getenv(env))) {
				t.Errorf("upstream event %q leaked a credential", kind)
			}
		}
	}
	if !bytes.Contains(record.parts["http.upstream.1.response.metadata"], []byte("Status: 202")) || record.state != core.EndedComplete {
		t.Fatalf("upstream status or final transcript state missing: %#v", record)
	}
}

func TestAdapterCancellationPanicAndUnreadBodiesEndIncomplete(t *testing.T) {
	users := adapterTestUsers(t)
	controller, _, closeStore := adapterTestController(t, users)
	manager := adapterTestManager(t)
	for _, mode := range []string{"cancellation", "panic", "unread-body", "short-body"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			router := adapterTestRouter(controller, manager, func(g *gin.Context) {
				if mode == "unread-body" {
					g.Status(http.StatusAccepted)
					return
				}
				if _, err := io.ReadAll(g.Request.Body); err != nil {
					t.Errorf("ReadAll: %v", err)
				}
				switch mode {
				case "panic":
					panic("synthetic handler panic")
				case "cancellation":
					cancel()
				}
				g.Status(http.StatusAccepted)
			})
			req := adapterTestRequest("incomplete-"+mode, adapterAlphaEnv, "/v1/responses", strings.NewReader("payload"))
			req = req.WithContext(logging.WithRequestID(ctx, "incomplete-"+mode))
			if mode == "short-body" {
				req.ContentLength += 10
			}
			response := httptest.NewRecorder()
			router.ServeHTTP(response, req)
			wantStatus := http.StatusAccepted
			if mode == "panic" {
				wantStatus = http.StatusInternalServerError
			}
			if response.Code != wantStatus {
				t.Fatalf("HTTP status = %d, want %d", response.Code, wantStatus)
			}
		})
	}
	closeStore()
	for _, mode := range []string{"cancellation", "panic", "unread-body", "short-body"} {
		record := adapterReadRecord(t, controller, "incomplete-"+mode)
		if record.state != core.EndedIncomplete {
			t.Errorf("%s state = %q, want %q", mode, record.state, core.EndedIncomplete)
		}
	}
}

func TestAdapterRejectsInvalidBindingsWithoutCreatingStore(t *testing.T) {
	users := adapterTestUsers(t)
	t.Setenv("TRANSCRIPT_ADAPTER_TEST_MISSING", "")
	t.Setenv("TRANSCRIPT_ADAPTER_TEST_WHITESPACE", " \t ")
	t.Setenv("TRANSCRIPT_ADAPTER_TEST_DUPLICATE", os.Getenv(adapterAlphaEnv))
	tests := []struct {
		name  string
		users []config.TranscriptUser
		root  string
	}{
		{name: "no users"},
		{name: "missing environment name", users: []config.TranscriptUser{{UserID: "alpha"}}},
		{name: "missing environment value", users: []config.TranscriptUser{{EnvName: "TRANSCRIPT_ADAPTER_TEST_MISSING", UserID: "alpha"}}},
		{name: "blank environment value", users: []config.TranscriptUser{{EnvName: "TRANSCRIPT_ADAPTER_TEST_WHITESPACE", UserID: "alpha"}}},
		{name: "missing user", users: []config.TranscriptUser{{EnvName: adapterAlphaEnv}}},
		{name: "user whitespace", users: []config.TranscriptUser{{EnvName: adapterAlphaEnv, UserID: " alpha "}}},
		{name: "oversized user", users: []config.TranscriptUser{{EnvName: adapterAlphaEnv, UserID: strings.Repeat("x", 129)}}},
		{name: "duplicate environment", users: []config.TranscriptUser{users[0], users[0]}},
		{name: "duplicate credential", users: []config.TranscriptUser{users[0], {EnvName: "TRANSCRIPT_ADAPTER_TEST_DUPLICATE", UserID: "beta"}}},
		{name: "too many bindings", users: make([]config.TranscriptUser, 1025)},
		{name: "relative root", users: users, root: "relative-transcript-root"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := adapterTestRoot(t)
			if tt.root != "" {
				root = tt.root
			}
			controller, err := New(config.Transcripts{Enabled: true, Root: root, Users: tt.users})
			if err == nil || controller == nil || controller.store != nil {
				if controller != nil {
					if errClose := controller.Close(); errClose != nil {
						t.Errorf("Close: %v", errClose)
					}
				}
				t.Fatalf("New invalid configuration = (%#v, %v), want failed controller without store", controller, err)
			}
			if !controller.failed.Load() {
				t.Error("rejected configuration did not expose degraded capture")
			}
			if filepath.IsAbs(root) {
				if _, errStat := os.Stat(root); !errors.Is(errStat, os.ErrNotExist) {
					t.Fatalf("invalid binding created filesystem state: Stat error=%v", errStat)
				}
			}
		})
	}
}

func TestAdapterDisabledLeavesRequestBodyWriterAndFilesystemUntouched(t *testing.T) {
	root := adapterTestRoot(t)
	controller, err := New(config.Transcripts{Root: root, Users: []config.TranscriptUser{{EnvName: "MISSING_DISABLED_BINDING"}}})
	if err != nil {
		t.Fatalf("disabled New validated unused bindings: %v", err)
	}
	if controller.store != nil {
		t.Fatal("disabled adapter created a store/reservations")
	}
	body := &adapterCountingBody{reader: strings.NewReader("do-not-read")}
	req := httptest.NewRequest(http.MethodPost, "/disabled", nil)
	req.Body = body
	originalContext := req.Context()
	var originalWriter gin.ResponseWriter
	router := gin.New()
	router.Use(func(g *gin.Context) {
		originalWriter = g.Writer
		g.Next()
	})
	router.Use(controller.Middleware())
	router.POST("/disabled", func(g *gin.Context) {
		if g.Request != req || g.Request.Body != body || g.Request.Context() != originalContext || g.Writer != originalWriter {
			t.Error("disabled middleware replaced a request, body, context, or writer")
		}
		if logging.TranscriptFromContext(g.Request.Context()) != nil {
			t.Error("disabled adapter installed a transcript sink")
		}
		g.Status(http.StatusNoContent)
	})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, req)
	if response.Code != http.StatusNoContent || body.reads != 0 || body.closes != 0 {
		t.Fatalf("disabled request changed: status=%d body reads/closes=(%d,%d)", response.Code, body.reads, body.closes)
	}
	if _, errStat := os.Stat(root); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("disabled adapter created filesystem state: Stat error=%v", errStat)
	}
	if errClose := controller.Close(); errClose != nil {
		t.Fatalf("disabled Close: %v", errClose)
	}

	statsRouter := gin.New()
	statsRouter.GET("/stats", controller.Stats)
	statsResponse := httptest.NewRecorder()
	statsRouter.ServeHTTP(statsResponse, httptest.NewRequest(http.MethodGet, "/stats", nil))
	var stats struct {
		Enabled    bool `json:"enabled"`
		BestEffort bool `json:"best_effort"`
	}
	if errDecode := json.Unmarshal(statsResponse.Body.Bytes(), &stats); errDecode != nil {
		t.Fatalf("decode stats: %v", errDecode)
	}
	if stats.Enabled || !stats.BestEffort {
		t.Fatalf("disabled stats = %#v", stats)
	}
}

func TestAdapterDisabledMiddlewareDoesNotAddPerRequestAllocations(t *testing.T) {
	controller, err := New(config.Transcripts{})
	if err != nil {
		t.Fatalf("New disabled: %v", err)
	}
	response := httptest.NewRecorder()
	g, _ := gin.CreateTestContext(response)
	g.Request = httptest.NewRequest(http.MethodGet, "/disabled", nil)
	baseline := func(g *gin.Context) { g.Next() }
	middleware := controller.Middleware()
	baselineAllocs := testing.AllocsPerRun(10, func() { baseline(g) })
	disabledAllocs := testing.AllocsPerRun(10, func() { middleware(g) })
	if disabledAllocs > baselineAllocs {
		t.Fatalf("disabled middleware allocates beyond the identical Gin continuation: got %g, baseline %g", disabledAllocs, baselineAllocs)
	}
}
