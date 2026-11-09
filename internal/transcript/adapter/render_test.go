package adapter

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/transcript/core"
)

func renderTestDownload(t *testing.T, controller *Controller, id string) *httptest.ResponseRecorder {
	t.Helper()
	router := gin.New()
	router.GET("/admin/logs", controller.Download(func(g *gin.Context) {
		t.Error("enabled adapter unexpectedly delegated a native log download")
		g.AbortWithStatus(http.StatusTeapot)
	}))
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/logs?id="+url.QueryEscape(id), nil))
	return response
}

func TestDownloadRendersNativeTextAndMasksOnlyMetadata(t *testing.T) {
	users := adapterTestUsers(t)
	controller, _, closeStore := adapterTestController(t, users)
	manager := adapterTestManager(t)
	binary := []byte{0xff, 0x80, 0x00, '\n', 0xfe}
	body := `{"secret":"raw-body-secret-must-survive","token":"raw-body-token-must-survive","binary":"unchanged"}`
	router := adapterTestRouter(controller, manager, func(g *gin.Context) {
		requestBody, err := io.ReadAll(g.Request.Body)
		if err != nil {
			t.Errorf("ReadAll: %v", err)
		}
		sink := logging.TranscriptFromContext(g.Request.Context())
		if sink == nil {
			t.Error("missing native transcript sink")
			g.AbortWithStatus(http.StatusInternalServerError)
			return
		}
		sink.HTTP("http.upstream.1.request", http.MethodPost, "https://userinfo:"+os.Getenv(adapterBetaEnv)+"@provider.test/v1/respond?access_token="+os.Getenv(adapterAlphaEnv)+"&model=synthetic", 0, http.Header{"Authorization": {"Bearer " + os.Getenv(adapterAlphaEnv)}, "X-Api-Key": {os.Getenv(adapterBetaEnv)}, "Content-Type": {"application/json"}}, []byte(`{"secret":"upstream-body-secret-must-survive"}`))
		sink.HTTP("http.upstream.1.response", "", "", http.StatusCreated, http.Header{"Set-Cookie": {"session=" + os.Getenv(adapterBetaEnv)}, "Location": {"https://provider.test/final?auth_token=" + os.Getenv(adapterAlphaEnv) + "&ok=yes"}}, []byte("upstream-response-raw-secret"))
		sink.Capture("provider.binary", binary)
		g.Header("Set-Cookie", "session="+os.Getenv(adapterBetaEnv))
		g.Header("X-Api-Key", os.Getenv(adapterAlphaEnv))
		g.Data(http.StatusCreated, "application/json", requestBody)
	})
	query := url.Values{
		"api_key":    {os.Getenv(adapterAlphaEnv)},
		"auth_token": {os.Getenv(adapterBetaEnv)},
		"model":      {"synthetic-model"},
	}
	req := adapterTestRequest("render-native-text", adapterAlphaEnv, "/v1/responses?"+query.Encode(), strings.NewReader(body))
	req.Header.Set("X-Goog-Api-Key", os.Getenv(adapterBetaEnv))
	req.Header.Set("Cookie", "session="+os.Getenv(adapterBetaEnv))
	req.Header.Set("Referer", "https://browser.test/start?key="+os.Getenv(adapterAlphaEnv)+"&view=chat")
	req.Header.Set("X-Trace-Label", "visible-nonsecret-header")
	applicationResponse := httptest.NewRecorder()
	router.ServeHTTP(applicationResponse, req)
	if applicationResponse.Code != http.StatusCreated || applicationResponse.Body.String() != body {
		t.Fatalf("application behavior changed: (%d, %q)", applicationResponse.Code, applicationResponse.Body.String())
	}
	closeStore()
	record := adapterReadRecord(t, controller, "render-native-text")
	response := renderTestDownload(t, controller, "render-native-text")
	if response.Code != http.StatusOK {
		t.Fatalf("download status = %d: %s", response.Code, response.Body.String())
	}
	mediaType, _, errContentType := mime.ParseMediaType(response.Header().Get("Content-Type"))
	if errContentType != nil || mediaType != "text/plain" {
		t.Fatalf("download content type = %q (%v), want text/plain", response.Header().Get("Content-Type"), errContentType)
	}
	disposition, parameters, errDisposition := mime.ParseMediaType(response.Header().Get("Content-Disposition"))
	if errDisposition != nil || disposition != "attachment" || parameters["filename"] != record.name {
		t.Fatalf("download disposition = %q (%v), want native attachment %q", response.Header().Get("Content-Disposition"), errDisposition, record.name)
	}
	if parameters["filename"] == "" || filepath.Base(parameters["filename"]) != parameters["filename"] || strings.ContainsAny(parameters["filename"], "/\\\r\n") {
		t.Fatalf("unsafe attachment filename %q", parameters["filename"])
	}
	if response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("X-Transcript-State") != core.EndedComplete {
		t.Fatalf("download cache/state headers = (%q, %q)", response.Header().Get("Cache-Control"), response.Header().Get("X-Transcript-State"))
	}
	text := response.Body.String()
	for _, want := range []string{
		"=== REQUEST INFO ===", "Request ID: render-native-text", "User: alpha", "=== HEADERS ===", "X-Trace-Label: visible-nonsecret-header",
		"=== REQUEST BODY ===", body, "=== RESPONSE ===", "=== API REQUEST 1 ===", "=== API RESPONSE 1 ===", "Status: 201",
		"upstream-body-secret-must-survive", "upstream-response-raw-secret", "[BINARY base64]\n" + base64.StdEncoding.EncodeToString(binary),
		"[REDACTED]", "synthetic-model", "=== TRANSCRIPT STATUS ===", "State: " + core.EndedComplete, "Best-effort capture",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("native text download missing %q", want)
		}
	}
	for _, env := range []string{adapterAlphaEnv, adapterBetaEnv, adapterRotatedEnv} {
		if strings.Contains(text, os.Getenv(env)) {
			t.Errorf("rendered metadata leaked synthetic credential from %s", env)
		}
	}
	if bytes.Contains(response.Body.Bytes(), binary) || strings.Contains(text, `"kind":"metadata"`) || strings.Contains(text, `"data":"`) {
		t.Fatal("download exposed binary/raw framed storage rather than native text")
	}
	if strings.Contains(text, "shared-client-session") {
		t.Fatal("download exposed the unhashed client session identifier")
	}
}

func TestDownloadMissingTraversalAndUnavailableStore(t *testing.T) {
	users := adapterTestUsers(t)
	controller, _, closeStore := adapterTestController(t, users)
	closeStore()
	for _, tt := range []struct {
		id   string
		want int
	}{
		{id: "", want: http.StatusBadRequest},
		{id: "   ", want: http.StatusBadRequest},
		{id: "../outside", want: http.StatusBadRequest},
		{id: `..\outside`, want: http.StatusBadRequest},
		{id: strings.Repeat("x", 257), want: http.StatusBadRequest},
		{id: "native-record-not-present", want: http.StatusNotFound},
	} {
		response := renderTestDownload(t, controller, tt.id)
		if response.Code != tt.want {
			t.Errorf("download %q status=%d body=%q, want %d", tt.id, response.Code, response.Body.String(), tt.want)
		}
		if response.Header().Get("Content-Disposition") != "" {
			t.Errorf("failed download %q exposed attachment headers", tt.id)
		}
	}
	failed, err := New(config.Transcripts{Enabled: true, Root: adapterTestRoot(t), Users: []config.TranscriptUser{{EnvName: "", UserID: "alpha"}}})
	if err == nil || failed == nil {
		t.Fatalf("invalid config did not produce a failed controller: (%#v, %v)", failed, err)
	}
	if response := renderTestDownload(t, failed, "valid-request-id"); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unavailable store status=%d body=%q", response.Code, response.Body.String())
	}
}

func TestDownloadUsesNativePathIDAndDisabledFallback(t *testing.T) {
	users := adapterTestUsers(t)
	controller, _, closeStore := adapterTestController(t, users)
	manager := adapterTestManager(t)
	router := adapterTestRouter(controller, manager, func(g *gin.Context) { g.Status(http.StatusNoContent) })
	router.ServeHTTP(httptest.NewRecorder(), adapterTestRequest("path-native-id", adapterAlphaEnv, "/v1/responses", nil))
	closeStore()
	downloadRouter := gin.New()
	downloadRouter.GET("/admin/logs/:id", controller.Download(func(g *gin.Context) {
		t.Error("enabled adapter called fallback")
		g.Status(http.StatusTeapot)
	}))
	response := httptest.NewRecorder()
	downloadRouter.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/admin/logs/path-native-id?id=wrong-query-id", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "Request ID: path-native-id") {
		t.Fatalf("native path ID not used: status=%d body=%q", response.Code, response.Body.String())
	}

	root := adapterTestRoot(t)
	disabled, err := New(config.Transcripts{Root: root})
	if err != nil {
		t.Fatalf("New disabled: %v", err)
	}
	for _, candidate := range []*Controller{disabled, nil} {
		called := 0
		fallbackRouter := gin.New()
		fallbackRouter.GET("/admin/logs/:id", candidate.Download(func(g *gin.Context) {
			called++
			if g.Param("id") != "native-fallback" {
				t.Errorf("fallback path ID changed to %q", g.Param("id"))
			}
			g.String(http.StatusAccepted, "native-log-content")
		}))
		fallbackResponse := httptest.NewRecorder()
		fallbackRouter.ServeHTTP(fallbackResponse, httptest.NewRequest(http.MethodGet, "/admin/logs/native-fallback", nil))
		if called != 1 || fallbackResponse.Code != http.StatusAccepted || fallbackResponse.Body.String() != "native-log-content" {
			t.Fatalf("disabled fallback calls=%d response=(%d,%q)", called, fallbackResponse.Code, fallbackResponse.Body.String())
		}
	}
	if _, errStat := os.Stat(root); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("disabled download created filesystem state: %v", errStat)
	}
}

func TestRenderMakesDropsAndEndedIncompleteVisible(t *testing.T) {
	users := adapterTestUsers(t)
	controller, _, closeStore := adapterTestController(t, users)
	for _, tt := range []struct {
		id       string
		drop     bool
		complete bool
	}{
		{id: "render-explicit-gap", drop: true, complete: true},
		{id: "render-ended-incomplete", complete: false},
	} {
		handle := controller.store.Begin(core.Metadata{RequestID: tt.id, UserID: "alpha", SessionID: "shared-client-session", Method: http.MethodPost, URL: "/v1/responses", Started: time.Unix(1_700_000_000, 123)})
		if handle == nil {
			t.Fatalf("Begin(%q) refused a small test capture", tt.id)
		}
		handle.Capture("request.body", []byte("body-before-gap"))
		if tt.drop {
			handle.Drop(3, 17)
		}
		handle.Capture("response.body", []byte("body-after-gap"))
		handle.End(tt.complete)
	}
	closeStore()
	for _, id := range []string{"render-explicit-gap", "render-ended-incomplete"} {
		response := renderTestDownload(t, controller, id)
		text := response.Body.String()
		if response.Code != http.StatusOK || response.Header().Get("X-Transcript-State") != core.EndedIncomplete || !strings.Contains(text, "State: "+core.EndedIncomplete) {
			t.Fatalf("%s download hid incomplete state: status=%d header=%q body=%q", id, response.Code, response.Header().Get("X-Transcript-State"), text)
		}
		if !strings.Contains(text, "body-before-gap") || !strings.Contains(text, "body-after-gap") || !strings.Contains(text, "=== TRANSCRIPT STATUS ===") {
			t.Errorf("%s lost accepted bytes or final status: %q", id, text)
		}
		if id == "render-explicit-gap" {
			for _, want := range []string{"TRANSCRIPT GAP", "Dropped chunks: 3", "Dropped bytes: 17"} {
				if !strings.Contains(text, want) {
					t.Errorf("gap download missing %q: %q", want, text)
				}
			}
		}
	}
}

type renderFailWriter struct{ err error }

func (w renderFailWriter) Write([]byte) (int, error) { return 0, w.err }

func TestRenderPropagatesDownloadWriterFailure(t *testing.T) {
	users := adapterTestUsers(t)
	controller, _, closeStore := adapterTestController(t, users)
	handle := controller.store.Begin(core.Metadata{RequestID: "render-writer-failure", UserID: "alpha", Method: http.MethodGet, URL: "/v1/responses", Started: time.Unix(1_700_000_000, 0)})
	if handle == nil {
		t.Fatal("Begin refused a small test capture")
	}
	handle.Capture("response.body", []byte("accepted"))
	handle.End(true)
	closeStore()
	reader, err := controller.store.Open("render-writer-failure")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() {
		if errClose := reader.Close(); errClose != nil {
			t.Errorf("reader.Close: %v", errClose)
		}
	}()
	writeErr := errors.New("synthetic download writer failure")
	if errRender := render(renderFailWriter{err: writeErr}, reader); !errors.Is(errRender, writeErr) {
		t.Fatalf("render error = %v, want original writer failure", errRender)
	}
}

func TestMetadataMaskingDoesNotMutateHeadersOrEraseRawBodies(t *testing.T) {
	adapterTestUsers(t)
	headers := http.Header{
		"Authorization":          {"Bearer " + os.Getenv(adapterAlphaEnv)},
		"Proxy-Authorization":    {"Basic " + os.Getenv(adapterBetaEnv)},
		"Cookie":                 {"sid=" + os.Getenv(adapterBetaEnv)},
		"Set-Cookie":             {"sid=" + os.Getenv(adapterBetaEnv)},
		"X-Api-Key":              {os.Getenv(adapterAlphaEnv)},
		"X-Auth-Token":           {os.Getenv(adapterAlphaEnv)},
		"Sec-Websocket-Protocol": {os.Getenv(adapterAlphaEnv)},
		"Location":               {"https://userinfo:" + os.Getenv(adapterBetaEnv) + "@provider.test/done?access_token=" + os.Getenv(adapterAlphaEnv) + "&ok=yes#fragment"},
		"Referer":                {"https://browser.test/?key=" + os.Getenv(adapterAlphaEnv) + "&view=chat"},
		"X-Visible":              {"first", "second"},
	}
	original := headers.Clone()
	masked := maskHeaders(headers)
	if !reflect.DeepEqual(headers, original) {
		t.Fatal("maskHeaders mutated live HTTP headers")
	}
	metadata := httpMetadata(http.MethodPost, "https://userinfo:"+os.Getenv(adapterBetaEnv)+"@provider.test/v1/respond?api_key="+os.Getenv(adapterAlphaEnv)+"&auth_token="+os.Getenv(adapterBetaEnv)+"&model=visible-model#fragment", http.StatusCreated, headers)
	for _, env := range []string{adapterAlphaEnv, adapterBetaEnv} {
		if bytes.Contains(metadata, []byte(os.Getenv(env))) {
			t.Errorf("metadata leaked synthetic credential from %s", env)
		}
	}
	for _, want := range []string{"Method: POST", "Status: 201", "model=visible-model", "X-Visible: first\nX-Visible: second", "[REDACTED]"} {
		if !bytes.Contains(metadata, []byte(want)) {
			t.Errorf("masked metadata missing %q: %q", want, metadata)
		}
	}
	masked["X-Visible"][0] = "changed-copy"
	if headers.Get("X-Visible") != "first" {
		t.Error("masked metadata aliases original header slices")
	}
	for _, rawURL := range []string{
		"https://provider.test/?key=%zz&model=visible",
		"https://provider.test/?key=" + os.Getenv(adapterAlphaEnv) + ";model=visible",
		"https://provider.test/%zz?key=" + os.Getenv(adapterAlphaEnv),
	} {
		maskedURL := maskURL(rawURL)
		if strings.Contains(maskedURL, os.Getenv(adapterAlphaEnv)) || strings.Contains(maskedURL, "%zz") {
			t.Errorf("malformed URL retained credential-bearing raw input: %q", maskedURL)
		}
	}
}
