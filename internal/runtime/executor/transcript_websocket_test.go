package executor

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
)

type websocketTranscriptTestEvent struct {
	kind string
	data []byte
	err  error
}

type websocketTranscriptTestSink struct {
	mu            sync.Mutex
	events        []websocketTranscriptTestEvent
	droppedChunks uint64
	droppedBytes  uint64
}

var _ logging.TranscriptSink = (*websocketTranscriptTestSink)(nil)

func (s *websocketTranscriptTestSink) Capture(kind string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, websocketTranscriptTestEvent{kind: kind, data: bytes.Clone(data)})
}

func (s *websocketTranscriptTestSink) Drop(chunks, count uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.droppedChunks += chunks
	s.droppedBytes += count
}

func (s *websocketTranscriptTestSink) HTTP(kind, _, _ string, _ int, _ http.Header, body []byte) {
	s.Capture(kind, body)
}

func (s *websocketTranscriptTestSink) Failure(kind string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, websocketTranscriptTestEvent{kind: kind, err: err})
}

func (s *websocketTranscriptTestSink) snapshot() ([]websocketTranscriptTestEvent, uint64, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]websocketTranscriptTestEvent(nil), s.events...), s.droppedChunks, s.droppedBytes
}

func TestWebsocketTranscriptStableOwnerKeepsRawOpcodeAndBytes(t *testing.T) {
	var session codexWebsocketSession
	conn := &websocket.Conn{}
	sink := &websocketTranscriptTestSink{}
	binding := session.bindRequestTranscript(logging.WithTranscriptSink(context.Background(), sink))
	if binding == nil || session.transcriptBindingForConn(conn) != nil {
		t.Fatal("request binding attributed a reusable connection before activation")
	}
	session.bindTranscriptConnection(conn)
	if session.transcriptBindingForConn(conn) != binding {
		t.Fatal("new connection did not bind to its dialing request")
	}
	ch := make(chan codexWebsocketRead, 1)
	session.setActive(conn, ch)
	defer session.clearActive(conn, ch)
	session.bindActiveTranscript(binding, conn, ch)
	if session.transcriptBindingForConn(&websocket.Conn{}) != nil {
		t.Fatal("request binding attributed another connection")
	}
	text := []byte(" \t{ \"type\": \"response.delta\" }\r\n ")
	binary := []byte{' ', 0, 0xff, '\t', 0x80, '\n'}
	for _, frame := range []struct {
		opcode int
		data   []byte
	}{{websocket.TextMessage, text}, {websocket.BinaryMessage, binary}} {
		owner := session.transcriptBindingForConn(conn)
		if owner != binding || session.captureTranscriptRead(conn, owner, frame.opcode, frame.data, nil) != sink {
			t.Fatalf("stable read opcode=%d did not retain its owner", frame.opcode)
		}
	}
	events, chunks, count := sink.snapshot()
	if chunks != 0 || count != 0 || len(events) != 2 {
		t.Fatalf("stable read observations: events=%#v dropped=(%d,%d)", events, chunks, count)
	}
	for i, want := range []websocketTranscriptTestEvent{{kind: "websocket.upstream.input.1", data: text}, {kind: "websocket.upstream.input.2", data: binary}} {
		if events[i].kind != want.kind || !bytes.Equal(events[i].data, want.data) || events[i].err != nil {
			t.Errorf("event %d=%#v, want opcode and unmodified bytes %#v", i, events[i], want)
		}
	}
}

func TestWebsocketTranscriptChangedOwnerDropsOnlyOldSinkAndIgnoresStaleCleanup(t *testing.T) {
	var session codexWebsocketSession
	conn := &websocket.Conn{}
	oldSink, newSink := &websocketTranscriptTestSink{}, &websocketTranscriptTestSink{}
	oldBinding := session.bindRequestTranscript(logging.WithTranscriptSink(context.Background(), oldSink))
	oldCh := make(chan codexWebsocketRead, 1)
	session.setActive(conn, oldCh)
	session.bindActiveTranscript(oldBinding, conn, oldCh)
	ownerAtReadStart := session.transcriptBindingForConn(conn)
	if ownerAtReadStart != oldBinding || oldBinding == nil {
		t.Fatal("old request was not bound before the read started")
	}

	newBinding := session.bindRequestTranscript(logging.WithTranscriptSink(context.Background(), newSink))
	if newBinding == nil || session.transcriptBindingForConn(conn) != nil {
		t.Fatal("new request inherited the old request's active read")
	}
	newCh := make(chan codexWebsocketRead, 1)
	session.setActive(conn, newCh)
	defer session.clearActive(conn, newCh)
	session.bindActiveTranscript(newBinding, conn, newCh)
	session.bindActiveTranscript(oldBinding, conn, oldCh)
	session.clearRequestTranscript(oldBinding)
	if session.clearActive(conn, oldCh) {
		t.Fatal("stale cleanup removed the new active channel")
	}
	if session.transcriptBindingForConn(conn) != newBinding {
		t.Fatal("stale binding or cleanup replaced the new transcript owner")
	}

	payload := []byte{' ', '\t', 0, 0xff, '\r', '\n'}
	readErr := errors.New("synthetic partial websocket read")
	if got := session.captureTranscriptRead(conn, ownerAtReadStart, websocket.BinaryMessage, payload, readErr); got != nil {
		t.Fatalf("changed-owner read returned sink %T, want no attribution", got)
	}
	events, chunks, count := oldSink.snapshot()
	if chunks != 1 || count != uint64(len(payload)) || len(events) != 0 {
		t.Errorf("old sink observations=%#v dropped=(%d,%d), want only its exact loss", events, chunks, count)
	}
	if events, chunks, count := newSink.snapshot(); len(events) != 0 || chunks != 0 || count != 0 {
		t.Errorf("read begun by old owner was charged to new sink: events=%#v dropped=(%d,%d)", events, chunks, count)
	}
	if session.transcriptBindingForConn(conn) != newBinding {
		t.Fatal("changed-owner read cleared the new binding")
	}
}

func TestWebsocketTranscriptIdleReadIsNotAssignedToNextRequest(t *testing.T) {
	var session codexWebsocketSession
	conn := &websocket.Conn{}
	ownerAtReadStart := session.transcriptBindingForConn(conn)
	sink := &websocketTranscriptTestSink{}
	binding := session.bindRequestTranscript(logging.WithTranscriptSink(context.Background(), sink))
	ch := make(chan codexWebsocketRead, 1)
	session.setActive(conn, ch)
	defer session.clearActive(conn, ch)
	session.bindActiveTranscript(binding, conn, ch)
	if binding == nil || ownerAtReadStart != nil || session.transcriptBindingForConn(conn) != binding {
		t.Fatal("idle-to-active request setup failed")
	}
	payload := []byte(" \tlate idle response\r\n")
	if got := session.captureTranscriptRead(conn, ownerAtReadStart, websocket.TextMessage, payload, nil); got != nil {
		t.Fatalf("idle read retroactively returned sink %T", got)
	}
	session.clearRequestTranscript(binding)
	if session.transcriptBindingForConn(conn) != nil {
		t.Fatal("completed request retained transcript ownership")
	}
	if got := session.captureTranscriptRead(conn, nil, websocket.BinaryMessage, []byte{0, 0xff}, nil); got != nil {
		t.Fatalf("unbound idle read returned sink %T", got)
	}
	if events, chunks, count := sink.snapshot(); len(events) != 0 || chunks != 0 || count != 0 {
		t.Fatalf("idle reads attributed data or loss to a request: events=%#v dropped=(%d,%d)", events, chunks, count)
	}
}
