package helps

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
)

type helperTranscriptEvent struct {
	op      string
	kind    string
	method  string
	url     string
	status  int
	headers http.Header
	body    []byte
	err     error
	chunks  uint64
	dropped uint64
}

type helperTranscriptSink struct {
	events []helperTranscriptEvent
}

func (s *helperTranscriptSink) Capture(kind string, data []byte) {
	s.events = append(s.events, helperTranscriptEvent{op: "capture", kind: kind, body: bytes.Clone(data)})
}

func (s *helperTranscriptSink) Drop(chunks, count uint64) {
	s.events = append(s.events, helperTranscriptEvent{op: "drop", chunks: chunks, dropped: count})
}

func (s *helperTranscriptSink) HTTP(kind, method, url string, status int, headers http.Header, body []byte) {
	s.events = append(s.events, helperTranscriptEvent{
		op: "http", kind: kind, method: method, url: url, status: status,
		headers: headers.Clone(), body: bytes.Clone(body),
	})
}

func (s *helperTranscriptSink) Failure(kind string, err error) {
	s.events = append(s.events, helperTranscriptEvent{op: "failure", kind: kind, err: err})
}

func newHelperTranscriptContext(t *testing.T) (context.Context, *gin.Context, []*logging.FileBodySource) {
	t.Helper()
	g, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx := logging.WithResponseHeadersHolder(context.WithValue(context.Background(), "gin", g))
	var sources []*logging.FileBodySource
	for _, key := range []string{logging.APIRequestSourceContextKey, logging.APIResponseSourceContextKey, logging.APIWebsocketTimelineSourceContextKey} {
		source, err := logging.NewFileBodySourceInDir(t.TempDir(), "transcript-helper")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if errCleanup := source.Cleanup(); errCleanup != nil {
				t.Errorf("cleanup native source: %v", errCleanup)
			}
		})
		g.Set(key, source)
		sources = append(sources, source)
	}
	return ctx, g, sources
}

func assertHelperTranscriptNoNativeCapture(t *testing.T, g *gin.Context, sources []*logging.FileBodySource) {
	t.Helper()
	for _, key := range []string{apiAttemptsKey, apiRequestKey, apiResponseKey, apiWebsocketTimelineKey, deferredAPIRequestBytesKey, logging.DeferredAPIRequestContextKey, logging.APIResponseCapturedContextKey} {
		if _, exists := g.Get(key); exists {
			t.Errorf("native capture key %s was populated", key)
		}
	}
	for _, source := range sources {
		if paths := source.Paths(); len(paths) != 0 {
			t.Errorf("native log parts created: %v", paths)
		}
	}
}

func TestTranscriptSharedHelpersCaptureBeforeNativeFlags(t *testing.T) {
	payload := []byte{' ', '\t', 0, 0xff, 'x', '\r', '\n'}
	requestHeaders := http.Header{"X-Request-Test": {"first", "second"}}
	responseHeaders := http.Header{"X-Response-Test": {"received"}}
	transportErr := errors.New("synthetic transport failure with credential=not-a-real-key")
	info := UpstreamRequestLog{
		URL: "https://provider.invalid/endpoint", Method: http.MethodPost,
		Headers: requestHeaders, Body: payload, Provider: "synthetic-provider",
		AuthID: "synthetic-auth-id", AuthLabel: "synthetic-label", AuthType: "api_key", AuthValue: "must-not-be-captured",
	}
	providerEvent := helperTranscriptEvent{op: "capture", kind: "upstream.provider", body: []byte(info.Provider)}
	requestEvent := func(kind string) helperTranscriptEvent {
		return helperTranscriptEvent{op: "http", kind: kind, method: info.Method, url: info.URL, headers: requestHeaders, body: payload}
	}
	responseEvent := func(kind string, body []byte) helperTranscriptEvent {
		return helperTranscriptEvent{op: "http", kind: kind, status: http.StatusBadGateway, headers: responseHeaders, body: body}
	}
	tests := []struct {
		name   string
		record func(context.Context, *config.Config)
		want   []helperTranscriptEvent
	}{
		{"request", func(ctx context.Context, cfg *config.Config) { RecordAPIRequest(ctx, cfg, info) }, []helperTranscriptEvent{providerEvent, requestEvent("upstream.request")}},
		{"response_metadata", func(ctx context.Context, cfg *config.Config) {
			RecordAPIResponseMetadata(ctx, cfg, http.StatusBadGateway, responseHeaders)
		}, []helperTranscriptEvent{responseEvent("upstream.response", nil)}},
		{"response_error", func(ctx context.Context, cfg *config.Config) { RecordAPIResponseError(ctx, cfg, transportErr) }, []helperTranscriptEvent{{op: "failure", kind: "upstream.response.error", err: transportErr}}},
		{"response_chunk", func(ctx context.Context, cfg *config.Config) { AppendAPIResponseChunk(ctx, cfg, payload) }, []helperTranscriptEvent{{op: "capture", kind: "upstream.response.body", body: payload}}},
		{"websocket_request", func(ctx context.Context, cfg *config.Config) { RecordAPIWebsocketRequest(ctx, cfg, info) }, []helperTranscriptEvent{providerEvent, requestEvent("websocket.upstream.request")}},
		{"websocket_handshake", func(ctx context.Context, cfg *config.Config) {
			RecordAPIWebsocketHandshake(ctx, cfg, http.StatusBadGateway, responseHeaders)
		}, []helperTranscriptEvent{responseEvent("websocket.upstream.handshake", nil)}},
		{"websocket_upgrade_rejection", func(ctx context.Context, cfg *config.Config) {
			RecordAPIWebsocketUpgradeRejection(ctx, cfg, info, http.StatusBadGateway, responseHeaders, payload)
		}, []helperTranscriptEvent{providerEvent, requestEvent("websocket.upstream.upgrade_rejection.request"), responseEvent("websocket.upstream.upgrade_rejection.response", payload)}},
		{"websocket_response", func(ctx context.Context, cfg *config.Config) { AppendAPIWebsocketResponse(ctx, cfg, payload) }, []helperTranscriptEvent{{op: "capture", kind: "websocket.upstream.response", body: payload}}},
		{"codex_websocket_response", func(ctx context.Context, cfg *config.Config) { AppendCodexAPIWebsocketResponse(ctx, cfg, payload) }, []helperTranscriptEvent{{op: "capture", kind: "websocket.upstream.response", body: payload}}},
		{"websocket_error", func(ctx context.Context, cfg *config.Config) { RecordAPIWebsocketError(ctx, cfg, "dial", transportErr) }, []helperTranscriptEvent{{op: "failure", kind: "websocket.upstream.error.dial", err: transportErr}}},
	}
	configs := []struct {
		name string
		cfg  *config.Config
	}{
		{name: "nil_config"},
		{name: "native_disabled", cfg: &config.Config{}},
		{name: "commercial", cfg: &config.Config{SDKConfig: config.SDKConfig{RequestLog: true}, CommercialMode: true}},
		{name: "native_enabled", cfg: &config.Config{SDKConfig: config.SDKConfig{RequestLog: true}}},
	}
	for _, mode := range configs {
		for _, test := range tests {
			t.Run(mode.name+"/"+test.name, func(t *testing.T) {
				ctx, g, sources := newHelperTranscriptContext(t)
				sink := &helperTranscriptSink{}
				if mode.name == "commercial" {
					g.Set(logging.TranscriptSinkContextKey, logging.TranscriptSink(sink))
				} else {
					ctx = logging.WithTranscriptSink(ctx, sink)
				}
				test.record(ctx, mode.cfg)
				if !reflect.DeepEqual(sink.events, test.want) {
					t.Fatalf("sink events = %#v, want %#v", sink.events, test.want)
				}
				assertHelperTranscriptNoNativeCapture(t, g, sources)
			})
		}
	}
}

func TestTranscriptProviderTruncationAccountsOmittedBytes(t *testing.T) {
	for _, size := range []int{0, 255, 256, 263} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			sink := &helperTranscriptSink{}
			provider := strings.Repeat("p", size)
			RecordAPIRequest(logging.WithTranscriptSink(context.Background(), sink), nil, UpstreamRequestLog{Provider: provider, AuthValue: "must-not-be-captured"})
			var providers [][]byte
			var droppedChunks, droppedBytes uint64
			for _, event := range sink.events {
				if event.kind == "upstream.provider" {
					providers = append(providers, event.body)
				}
				if event.op == "drop" {
					droppedChunks += event.chunks
					droppedBytes += event.dropped
				}
			}
			if size == 0 {
				if len(providers) != 0 {
					t.Fatalf("empty provider captured: %q", providers)
				}
			} else if len(providers) != 1 || string(providers[0]) != provider[:min(size, 256)] {
				t.Fatalf("provider captures = %q", providers)
			}
			wantBytes := uint64(max(size-256, 0))
			wantChunks := uint64(0)
			if wantBytes > 0 {
				wantChunks = 1
			}
			if droppedChunks != wantChunks || droppedBytes != wantBytes {
				t.Fatalf("drop = (%d, %d), want (%d, %d)", droppedChunks, droppedBytes, wantChunks, wantBytes)
			}
		})
	}
}

func TestTranscriptMetadataPreservesResponseHeaders(t *testing.T) {
	for _, withSink := range []bool{false, true} {
		for _, helper := range []string{"http", "handshake", "rejection", "codex"} {
			t.Run(fmt.Sprintf("sink_%t/%s", withSink, helper), func(t *testing.T) {
				ctx := logging.WithResponseHeadersHolder(context.Background())
				if withSink {
					ctx = logging.WithTranscriptSink(ctx, &helperTranscriptSink{})
				}
				cfg := &config.Config{}
				cfg.Transcripts.Enabled = true
				headers := http.Header{"X-Response-Test": {"original"}}
				logging.SetResponseHeaders(ctx, http.Header{"X-Preserved": {"existing"}})
				switch helper {
				case "http":
					RecordAPIResponseMetadata(ctx, cfg, http.StatusOK, headers)
				case "handshake":
					RecordAPIWebsocketHandshake(ctx, cfg, http.StatusSwitchingProtocols, headers)
				case "rejection":
					RecordAPIWebsocketUpgradeRejection(ctx, cfg, UpstreamRequestLog{}, http.StatusForbidden, headers, nil)
				case "codex":
					AppendCodexAPIWebsocketResponse(ctx, cfg, []byte(" \n"+`{"type":"codex.rate_limits","rate_limits":{"primary":{"used_percent":2,"window_minutes":5,"reset_after_seconds":30}}}`+"\r\n"))
					got := logging.GetResponseHeaders(ctx)
					if got.Get("X-Preserved") != "existing" || got.Get("X-Codex-Primary-Used-Percent") != "2" {
						t.Fatalf("quota header merge = %#v", got)
					}
					return
				}
				headers.Set("X-Response-Test", "mutated")
				if got := logging.GetResponseHeaders(ctx); got.Get("X-Response-Test") != "original" {
					t.Fatalf("response headers = %#v, want independent snapshot", got)
				}
			})
		}
	}
}

func TestTranscriptEnabledWithoutSinkDisablesAllNativeHelpers(t *testing.T) {
	for _, requestLog := range []bool{false, true} {
		t.Run(fmt.Sprintf("request_log_%t", requestLog), func(t *testing.T) {
			ctx, g, sources := newHelperTranscriptContext(t)
			cfg := &config.Config{SDKConfig: config.SDKConfig{RequestLog: requestLog}}
			cfg.Transcripts.Enabled = true
			payload := []byte(" \nsynthetic-body\r\n")
			headers := http.Header{"X-Synthetic": {"header"}}
			info := UpstreamRequestLog{URL: "https://provider.invalid", Method: http.MethodPost, Body: payload}
			err := errors.New("synthetic transport error")
			RecordAPIRequest(ctx, cfg, info)
			RecordAPIResponseMetadata(ctx, cfg, http.StatusBadGateway, headers)
			RecordAPIResponseError(ctx, cfg, err)
			AppendAPIResponseChunk(ctx, cfg, payload)
			RecordAPIWebsocketRequest(ctx, cfg, info)
			RecordAPIWebsocketHandshake(ctx, cfg, http.StatusSwitchingProtocols, headers)
			RecordAPIWebsocketUpgradeRejection(ctx, cfg, info, http.StatusForbidden, headers, payload)
			AppendAPIWebsocketResponse(ctx, cfg, payload)
			AppendCodexAPIWebsocketResponse(ctx, cfg, payload)
			RecordAPIWebsocketError(ctx, cfg, "read", err)
			assertHelperTranscriptNoNativeCapture(t, g, sources)
		})
	}
}

func TestTranscriptNilErrorsDoNotCreateFailures(t *testing.T) {
	sink := &helperTranscriptSink{}
	ctx := logging.WithTranscriptSink(context.Background(), sink)
	RecordAPIResponseError(ctx, nil, nil)
	RecordAPIWebsocketError(ctx, nil, "read", nil)
	if len(sink.events) != 0 {
		t.Fatalf("nil error events = %#v", sink.events)
	}
}
