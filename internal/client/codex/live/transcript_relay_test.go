package live

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
)

type relayTranscriptTestSink struct {
	mu            sync.Mutex
	data          map[string][]byte
	failures      map[string]error
	droppedChunks uint64
	droppedBytes  uint64
}

var _ logging.TranscriptSink = (*relayTranscriptTestSink)(nil)

func (s *relayTranscriptTestSink) Capture(kind string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data == nil {
		s.data = make(map[string][]byte)
	}
	s.data[kind] = append(s.data[kind], data...)
}

func (s *relayTranscriptTestSink) Drop(chunks, count uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.droppedChunks += chunks
	s.droppedBytes += count
}

func (s *relayTranscriptTestSink) HTTP(kind, _, _ string, _ int, _ http.Header, body []byte) {
	s.Capture(kind, body)
}

func (s *relayTranscriptTestSink) Failure(kind string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failures == nil {
		s.failures = make(map[string]error)
	}
	s.failures[kind] = err
}

func transcriptRelayTestPair(t *testing.T) (*websocket.Conn, *websocket.Conn) {
	t.Helper()
	type acceptedConnection struct {
		conn *websocket.Conn
		err  error
	}
	accepted := make(chan acceptedConnection, 1)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		accepted <- acceptedConnection{conn: conn, err: err}
	}))
	t.Cleanup(server.Close)
	dialer := websocket.Dialer{}
	client, response, errDial := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if errDial != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		t.Fatalf("dial local websocket peer: %v", errDial)
	}
	t.Cleanup(func() { _ = client.Close() })
	select {
	case peer := <-accepted:
		if peer.err != nil {
			t.Fatalf("upgrade local websocket peer: %v", peer.err)
		}
		t.Cleanup(func() { _ = peer.conn.Close() })
		return client, peer.conn
	case <-time.After(5 * time.Second):
		t.Fatal("local websocket peer did not finish upgrading")
		return nil, nil
	}
}

func TestTranscriptRelayPreservesTextAndBinaryBothDirectionsAndCloses(t *testing.T) {
	downstreamClient, downstream := transcriptRelayTestPair(t)
	upstream, upstreamPeer := transcriptRelayTestPair(t)
	sink := &relayTranscriptTestSink{}
	ctx := logging.WithTranscriptSink(context.Background(), sink)
	relayDone := make(chan error, 1)
	go func() { relayDone <- relayWebsockets(ctx, downstream, upstream) }()

	frames := []struct {
		source      *websocket.Conn
		destination *websocket.Conn
		opcode      int
		data        []byte
		received    string
		sent        string
	}{
		{downstreamClient, upstreamPeer, websocket.TextMessage, []byte(" \t{ \"type\": \"input\" }\r\n "), "websocket.downstream.request", "websocket.upstream.request"},
		{downstreamClient, upstreamPeer, websocket.BinaryMessage, []byte{0, 0xff, ' ', '\t', 0x80, '\n'}, "websocket.downstream.request", "websocket.upstream.request"},
		{upstreamPeer, downstreamClient, websocket.TextMessage, []byte("\r\n { \"type\": \"output\" } \t"), "websocket.upstream.response", "websocket.downstream.response"},
		{upstreamPeer, downstreamClient, websocket.BinaryMessage, []byte{0xfe, 0, '\r', ' ', 0x81}, "websocket.upstream.response", "websocket.downstream.response"},
	}
	want := make(map[string][]byte)
	for i, frame := range frames {
		type receivedFrame struct {
			opcode int
			data   []byte
			err    error
		}
		received := make(chan receivedFrame, 1)
		go func(conn *websocket.Conn) {
			opcode, data, err := conn.ReadMessage()
			received <- receivedFrame{opcode: opcode, data: data, err: err}
		}(frame.destination)
		if errWrite := frame.source.WriteMessage(frame.opcode, frame.data); errWrite != nil {
			t.Fatalf("write frame %d: %v", i, errWrite)
		}
		select {
		case got := <-received:
			if got.err != nil || got.opcode != frame.opcode || !bytes.Equal(got.data, frame.data) {
				t.Fatalf("wire frame %d: opcode=%d data=%q error=%v, want opcode=%d data=%q", i, got.opcode, got.data, got.err, frame.opcode, frame.data)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("relay did not deliver frame %d", i)
		}
		suffix := "." + strconv.Itoa(frame.opcode)
		want[frame.received+suffix] = frame.data
		want[frame.sent+suffix] = frame.data
	}

	if errClose := downstreamClient.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "test complete")); errClose != nil {
		t.Fatalf("send close frame: %v", errClose)
	}
	if errClose := downstreamClient.Close(); errClose != nil {
		t.Fatalf("close downstream client: %v", errClose)
	}
	select {
	case errRelay := <-relayDone:
		if !isNormalWebsocketClose(errRelay) {
			t.Errorf("relay returned unexpected error after normal close: %v", errRelay)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not terminate both relay directions")
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.data) != len(want) || len(sink.failures) != 0 || sink.droppedChunks != 0 || sink.droppedBytes != 0 {
		t.Errorf("relay observations: kinds=%d want=%d failures=%v dropped=(%d,%d)", len(sink.data), len(want), sink.failures, sink.droppedChunks, sink.droppedBytes)
	}
	for kind, data := range want {
		if !bytes.Equal(sink.data[kind], data) {
			t.Errorf("sink %q=%q, want unchanged wire bytes %q", kind, sink.data[kind], data)
		}
	}
}
