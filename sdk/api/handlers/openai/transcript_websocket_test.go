package openai

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	requestlogging "github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
	log "github.com/sirupsen/logrus"
)

type websocketTranscriptTestEvent struct {
	op    string
	kind  string
	data  []byte
	class string
}

type websocketTranscriptTestSink struct {
	mu     sync.Mutex
	events []websocketTranscriptTestEvent
}

func (s *websocketTranscriptTestSink) Capture(kind string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, websocketTranscriptTestEvent{op: "capture", kind: kind, data: bytes.Clone(data)})
}

func (s *websocketTranscriptTestSink) Drop(chunks, count uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, websocketTranscriptTestEvent{op: "drop", class: fmt.Sprintf("%d/%d", chunks, count)})
}

func (s *websocketTranscriptTestSink) HTTP(kind, method, url string, status int, headers http.Header, body []byte) {
	s.Capture(kind, body)
}

func (s *websocketTranscriptTestSink) Failure(kind string, err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, websocketTranscriptTestEvent{op: "failure", kind: kind, class: fmt.Sprintf("%T", err)})
}

func (s *websocketTranscriptTestSink) snapshot() []websocketTranscriptTestEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]websocketTranscriptTestEvent(nil), s.events...)
}

func newTranscriptWebsocketPeer(t *testing.T) (*websocket.Conn, *websocket.Conn) {
	t.Helper()
	type accepted struct {
		conn *websocket.Conn
		err  error
	}
	connections := make(chan accepted, 1)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
		connections <- accepted{conn: conn, err: err}
		if err == nil {
			<-release
			_ = conn.Close()
		}
	}))
	t.Cleanup(func() {
		close(release)
		server.Close()
	})
	client, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		t.Fatalf("dial transcript websocket: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	var peer *websocket.Conn
	select {
	case connection := <-connections:
		if connection.err != nil {
			t.Fatal(connection.err)
		}
		peer = connection.conn
	case <-time.After(5 * time.Second):
		t.Fatal("websocket was not accepted")
	}
	t.Cleanup(func() { _ = peer.Close() })
	if err = client.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return client, peer
}

func TestTranscriptWebsocketWriterCapturesDeliveredBytesWithoutTimeline(t *testing.T) {
	client, peer := newTranscriptWebsocketPeer(t)
	sink := &websocketTranscriptTestSink{}
	writer := newResponsesWebsocketWriter(peer)
	writer.sink = sink
	payload := []byte(" \t{\"type\":\"response.output_text.delta\",\"delta\":\"synthetic\"}\r\n")
	if err := writeResponsesWebsocketPayload(writer, nil, payload, time.Time{}); err != nil {
		t.Fatal(err)
	}
	kind, received, err := client.ReadMessage()
	if err != nil || kind != websocket.TextMessage || !bytes.Equal(received, payload) {
		t.Fatalf("peer received kind=%d payload=%q error=%v", kind, received, err)
	}
	want := []websocketTranscriptTestEvent{{op: "capture", kind: "websocket.downstream.output.1", data: payload}}
	if events := sink.snapshot(); !reflect.DeepEqual(events, want) {
		t.Fatalf("successful write events = %#v, want %#v", events, want)
	}
}

func TestTranscriptWebsocketTerminalWriteCapturesDeliveredBytes(t *testing.T) {
	client, peer := newTranscriptWebsocketPeer(t)
	sink := &websocketTranscriptTestSink{}
	writer := newResponsesWebsocketWriter(peer)
	writer.sink = sink
	payload := []byte(" \n{\"type\":\"error\",\"message\":\"synthetic rejection\"}\r\n")
	wrote, err := writer.closeWithPayload(payload)
	if err != nil || !wrote {
		t.Fatalf("terminal write = %t, %v", wrote, err)
	}
	_, received, err := client.ReadMessage()
	if err != nil || !bytes.Equal(received, payload) {
		t.Fatalf("terminal peer payload=%q error=%v", received, err)
	}
	want := []websocketTranscriptTestEvent{{op: "capture", kind: "websocket.downstream.output.1", data: payload}}
	if events := sink.snapshot(); !reflect.DeepEqual(events, want) {
		t.Fatalf("terminal events = %#v, want %#v", events, want)
	}
}

func TestTranscriptWebsocketFailedWriteUsesFailureNotDeliveredCapture(t *testing.T) {
	_, peer := newTranscriptWebsocketPeer(t)
	sink := &websocketTranscriptTestSink{}
	writer := newResponsesWebsocketWriter(peer)
	writer.sink = sink
	timeline := newWebsocketTimelineLog(false, nil)
	timeline.sink = sink
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	payload := []byte(" \nsynthetic attempted message\r\n")
	err := writeResponsesWebsocketPayload(writer, timeline, payload, time.Time{})
	if err == nil {
		t.Fatal("write on closed websocket succeeded")
	}
	want := []websocketTranscriptTestEvent{
		{op: "capture", kind: "websocket.downstream.response", data: payload},
		{op: "failure", kind: "websocket.downstream.write.error", class: fmt.Sprintf("%T", err)},
	}
	if events := sink.snapshot(); !reflect.DeepEqual(events, want) {
		t.Fatalf("failed write events = %#v, want attempted diagnostic and classified failure %#v", events, want)
	}
}

func TestTranscriptWebsocketTimelineBypassesNativeStorageAndPreservesBytes(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("native_%t", enabled), func(t *testing.T) {
			source, err := requestlogging.NewFileBodySourceInDir(t.TempDir(), "transcript-websocket")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if errCleanup := source.Cleanup(); errCleanup != nil {
					t.Errorf("cleanup timeline source: %v", errCleanup)
				}
			})
			g, _ := gin.CreateTestContext(httptest.NewRecorder())
			sink := &websocketTranscriptTestSink{}
			timeline := newWebsocketTimelineLog(enabled, source)
			timeline.sink = sink
			timeline.BeginRequest()
			payload := []byte{' ', '\t', 0, 0xff, 'x', '\r', '\n'}
			timeline.Append("response", payload, time.Time{})
			timeline.SetContext(g)
			want := []websocketTranscriptTestEvent{{op: "capture", kind: "websocket.downstream.response", data: payload}}
			if events := sink.snapshot(); !reflect.DeepEqual(events, want) {
				t.Fatalf("timeline events = %#v, want %#v", events, want)
			}
			if paths := source.Paths(); len(paths) != 0 {
				t.Fatalf("native timeline parts created: %v", paths)
			}
			if body := timeline.String(); body != "" {
				t.Fatalf("native timeline body = %q", body)
			}
			if _, exists := g.Get(wsTimelineBodyKey); exists {
				t.Fatal("native timeline body populated")
			}
			if _, exists := g.Get(requestlogging.WebsocketTimelineSourceContextKey); exists {
				t.Fatal("native timeline source populated")
			}
		})
	}
}

func TestTranscriptWebsocketDisconnectClassifiesErrorWithoutSecretText(t *testing.T) {
	sink := &websocketTranscriptTestSink{}
	timeline := newWebsocketTimelineLog(false, nil)
	timeline.sink = sink
	err := errors.New("synthetic https://provider.invalid?api_key=not-a-real-secret")
	appendWebsocketTimelineDisconnect(timeline, err, time.Time{})
	appendWebsocketTimelineDisconnect(timeline, nil, time.Time{})
	want := []websocketTranscriptTestEvent{{op: "failure", kind: "websocket.downstream.disconnect", class: fmt.Sprintf("%T", err)}}
	if events := sink.snapshot(); !reflect.DeepEqual(events, want) {
		t.Fatalf("disconnect events = %#v, want classified error %#v", events, want)
	}
}

func TestTranscriptWebsocketInputCapturesRawInterruptBeforeDiagnostics(t *testing.T) {
	for _, messageKind := range []int{websocket.TextMessage, websocket.BinaryMessage} {
		t.Run(fmt.Sprint(messageKind), func(t *testing.T) {
			client, peer := newTranscriptWebsocketPeer(t)
			sink := &websocketTranscriptTestSink{}
			ctx, cancel := context.WithCancelCause(requestlogging.WithTranscriptSink(context.Background(), sink))
			defer cancel(nil)
			timeline := newWebsocketTimelineLog(false, nil)
			timeline.sink = sink
			writer := newResponsesWebsocketWriter(peer)
			writer.sink = sink
			forwarded := make(chan []byte, 1)
			_, done := readResponsesWebsocketInput(ctx, cancel, peer, func(payload []byte) error {
				forwarded <- bytes.Clone(payload)
				return nil
			}, newResponsesLocalInterrupt(), writer, timeline)
			t.Cleanup(func() {
				_ = peer.Close()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("input reader did not stop")
				}
			})
			payload := []byte(" \t{\"type\":\"response.interrupt\",\"response_id\":\"synthetic-id\",\"mode\":\"synthetic-mode\",\"extension\":\"keep-exact\"}\r\n")
			if err := client.WriteMessage(messageKind, payload); err != nil {
				t.Fatal(err)
			}
			select {
			case received := <-forwarded:
				if !bytes.Equal(received, payload) {
					t.Fatalf("forwarded interrupt = %q, want %q", received, payload)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("interrupt not forwarded")
			}
			events := sink.snapshot()
			if len(events) < 2 {
				t.Fatalf("missing input or diagnostic event: %#v", events)
			}
			if first := events[0]; first.op != "capture" || first.kind != fmt.Sprintf("websocket.downstream.input.%d", messageKind) || !bytes.Equal(first.data, payload) {
				t.Fatalf("first event must be exact raw input, got %#v", first)
			}
			if diagnostic := events[1]; diagnostic.kind != "websocket.downstream.interrupt" || bytes.Contains(diagnostic.data, []byte("synthetic-id")) || bytes.Contains(diagnostic.data, []byte("keep-exact")) || bytes.Equal(diagnostic.data, payload) {
				t.Fatalf("interrupt diagnostic must follow raw input and remain sanitized: %#v", diagnostic)
			}
		})
	}
}

func TestTranscriptWebsocketOperationalDiagnosticsExcludeBodiesAndErrors(t *testing.T) {
	payload := []byte(`{"type":"error","status":400,"error":{"message":"private-body-token"}}`)
	for _, writer := range []*responsesWebsocketWriter{
		{transcriptEnabled: true},
		{sink: &websocketTranscriptTestSink{}},
	} {
		diagnostic := writer.payloadDiagnostic(payload)
		if diagnostic != fmt.Sprintf("event=error status=400 bytes=%d", len(payload)) {
			t.Fatalf("unexpected safe diagnostic: %q", diagnostic)
		}
		unknown := []byte(`{"type":"private-body-token","status":9999}`)
		if got := writer.payloadDiagnostic(unknown); got != fmt.Sprintf("event=other status=0 bytes=%d", len(unknown)) {
			t.Fatalf("untrusted event/status leaked: %q", got)
		}
		if got := writer.errorDiagnostic(&websocket.CloseError{Code: websocket.CloseNormalClosure, Text: "private-body-token"}); strings.Contains(got, "private-body-token") || got == "" {
			t.Fatalf("close diagnostic leaks or loses classification: %q", got)
		}
	}
	legacy := &responsesWebsocketWriter{}
	if got := legacy.payloadDiagnostic(payload); got != "event=error payload="+string(payload) {
		t.Fatalf("disabled native payload diagnostics changed: %q", got)
	}
	err := errors.New("legacy-diagnostic")
	if got := legacy.errorDiagnostic(err); got != err.Error() {
		t.Fatalf("disabled native error diagnostics changed: %q", got)
	}
}

func TestTranscriptWebsocketForwardErrorsKeepRawBytesOutOfOperationalLog(t *testing.T) {
	logger := log.StandardLogger()
	previousOutput, previousLevel := logger.Out, logger.GetLevel()
	var output bytes.Buffer
	logger.SetOutput(&output)
	logger.SetLevel(log.DebugLevel)
	t.Cleanup(func() { logger.SetOutput(previousOutput); logger.SetLevel(previousLevel) })
	for _, mode := range []string{"error-channel", "error-payload", "disconnect", "closed-data", "closed-data-duplex"} {
		t.Run(mode, func(t *testing.T) {
			output.Reset()
			client, peer := newTranscriptWebsocketPeer(t)
			sink := &websocketTranscriptTestSink{}
			writer := newResponsesWebsocketWriter(peer)
			writer.transcriptEnabled, writer.sink = true, sink
			const sentinel = "synthetic-upstream-body-secret"
			payload := []byte(`{"type":"error","status":400,"error":{"type":"invalid_request_error","message":"` + sentinel + `"}}`)
			errMsg := &interfaces.ErrorMessage{StatusCode: http.StatusBadRequest, Error: errors.New(string(payload))}
			if mode == "disconnect" {
				writer.closeForUpstreamDisconnect(errMsg.Error)
			} else {
				g, _ := gin.CreateTestContext(httptest.NewRecorder())
				g.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
				var data chan []byte
				var errs chan *interfaces.ErrorMessage
				var opts responsesWebsocketForwardOptions
				switch mode {
				case "error-channel":
					errs = make(chan *interfaces.ErrorMessage, 1)
					errs <- errMsg
				case "closed-data", "closed-data-duplex":
					data = make(chan []byte)
					errs = make(chan *interfaces.ErrorMessage, 1)
					close(data)
					opts.duplexStream = func() bool {
						// Queue the error only after select takes the closed-data branch.
						// This forces PendingStreamError to handle it without a select race.
						errs <- errMsg
						close(errs)
						return mode == "closed-data-duplex"
					}
				default:
					data = make(chan []byte, 1)
					data <- payload
				}
				h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
				_, _, _, terminalErr, err := h.forwardResponsesWebsocket(g, writer, func(...interface{}) {}, data, errs, nil, "synthetic-session", opts)
				if err != nil && !errors.Is(err, websocket.ErrCloseSent) {
					t.Fatal(err)
				}
				if mode != "error-payload" && terminalErr != errMsg {
					t.Fatalf("terminal error = %p, want original upstream error %p", terminalErr, errMsg)
				}
			}
			kind, received, err := client.ReadMessage()
			if err != nil || kind != websocket.TextMessage || !bytes.Contains(received, []byte(sentinel)) {
				t.Fatalf("wire error changed: %d %q %v", kind, received, err)
			}
			if mode != "disconnect" && !bytes.Equal(received, payload) {
				t.Fatalf("wire error bytes = %q, want %q", received, payload)
			}
			captured := false
			for _, event := range sink.snapshot() {
				if event.kind == "websocket.downstream.output.1" && bytes.Equal(event.data, received) {
					captured = true
				}
			}
			if !captured {
				t.Fatal("exact delivered upstream error missing from transcript sink")
			}
			if strings.Contains(output.String(), sentinel) || bytes.Contains(output.Bytes(), received) || strings.Contains(output.String(), "payload=") {
				t.Fatalf("raw operational log leak: %s", output.String())
			}
			if !strings.Contains(output.String(), "event=error") || !strings.Contains(output.String(), "bytes=") {
				t.Fatalf("safe operational classification missing: %s", output.String())
			}
		})
	}
}
