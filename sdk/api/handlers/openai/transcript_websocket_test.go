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
	requestlogging "github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
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
