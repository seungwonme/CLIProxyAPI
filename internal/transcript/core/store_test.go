package core

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func coreTestRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("EvalSymlinks: %v", err)
	}
	return filepath.Join(root, "records")
}

func coreTestConfig(t *testing.T) Config {
	t.Helper()
	return Config{Root: coreTestRoot(t), MaxBytes: 8 << 20, MaxActive: 128, QueueBytes: 2 << 20, MaxRecords: 256}
}

func coreTestStore(t *testing.T, cfg Config) (*Store, func()) {
	t.Helper()
	store, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var once sync.Once
	closeStore := func() {
		t.Helper()
		once.Do(func() {
			if errClose := store.Close(); errClose != nil {
				t.Errorf("Close: %v", errClose)
			}
		})
	}
	t.Cleanup(closeStore)
	return store, closeStore
}

func coreTestBegin(t *testing.T, store *Store, id, user string) *Handle {
	t.Helper()
	return coreTestBeginAt(t, store, id, user, time.Unix(1700000000, 123))
}

func coreTestBeginAt(t *testing.T, store *Store, id, user string, started time.Time) *Handle {
	t.Helper()
	h := store.Begin(Metadata{RequestID: id, UserID: user, SessionID: "synthetic-client-session", Method: "POST", URL: "/v1/responses", Started: started})
	if h == nil {
		t.Fatalf("Begin(%q) unexpectedly rejected", id)
	}
	// End is idempotent and must precede the store's cleanup Close, including
	// when a later assertion fails.
	t.Cleanup(func() { h.End(false) })
	return h
}

type coreTestRecord struct {
	frames         []Frame
	metadata       Metadata
	metadataFrames int
	status         Status
	name           string
}

func coreTestRead(t *testing.T, store *Store, id string) coreTestRecord {
	t.Helper()
	reader, err := store.Open(id)
	if err != nil {
		t.Fatalf("Open(%q): %v", id, err)
	}
	defer func() {
		if errClose := reader.Close(); errClose != nil {
			t.Errorf("Reader.Close: %v", errClose)
		}
	}()
	return coreTestReadReader(t, reader)
}

func coreTestReadReader(t *testing.T, reader *Reader) coreTestRecord {
	t.Helper()
	result := coreTestRecord{name: reader.Name()}
	for {
		frame, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Reader.Next: %v", err)
		}
		result.frames = append(result.frames, frame)
		if frame.Metadata != nil {
			result.metadata = *frame.Metadata
			result.metadataFrames++
		}
	}
	result.status = reader.Status()
	return result
}

func (r coreTestRecord) body(kind string) []byte {
	var out []byte
	for _, frame := range r.frames {
		if frame.Kind == kind && frame.Metadata == nil && frame.Status == nil {
			out = append(out, frame.Data...)
		}
	}
	return out
}

func coreTestMissing(t *testing.T, store *Store, id string) {
	t.Helper()
	reader, err := store.Open(id)
	if reader != nil {
		if errClose := reader.Close(); errClose != nil {
			t.Errorf("unexpected reader Close: %v", errClose)
		}
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Open(%q) = %v, want os.ErrNotExist", id, err)
	}
}

func coreTestAwait(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		t.Fatalf("%s did not finish while the worker was deliberately blocked", what)
	}
}

func TestStoreExactBodyFramingAndCopiedBuffers(t *testing.T) {
	cfg := coreTestConfig(t)
	store, closeStore := coreTestStore(t, cfg)
	h := coreTestBegin(t, store, "body-framing", "alpha")
	want := make(map[string][]byte)
	for _, size := range []int{0, 1, maxChunkBytes - 1, maxChunkBytes, maxChunkBytes + 1, 2*maxChunkBytes + 7} {
		kind := fmt.Sprintf("body.%d", size)
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i*131 + size)
		}
		want[kind] = bytes.Clone(data)
		h.Capture(kind, data)
		// The caller can reuse its transport buffer immediately after Capture.
		for i := range data {
			data[i] ^= 0xff
		}
	}
	h.End(true)
	closeStore()
	record := coreTestRead(t, store, "body-framing")
	if record.status.State != EndedComplete || record.metadataFrames != 1 {
		t.Fatalf("status=%+v metadata frames=%d", record.status, record.metadataFrames)
	}
	for kind, body := range want {
		if got := record.body(kind); !bytes.Equal(got, body) {
			t.Errorf("%s roundtrip: got %d bytes, want %d exact binary bytes", kind, len(got), len(body))
		}
		frames := 0
		for _, frame := range record.frames {
			if frame.Kind == kind {
				frames++
				if len(frame.Data) > maxChunkBytes {
					t.Errorf("%s frame has %d bytes, maximum %d", kind, len(frame.Data), maxChunkBytes)
				}
			}
		}
		if frames == 0 {
			t.Errorf("Capture(%q) disappeared, including the empty frame", kind)
		}
	}
}

func TestStoreMetadataSessionPartitionAndNativeRequestIDReuse(t *testing.T) {
	store, closeStore := coreTestStore(t, coreTestConfig(t))
	type request struct{ id, user, body string }
	requests := []request{
		{"018f3a5b-1234-7abc-def0-123411111111", "alpha", "first-alpha"},
		{"018f3a5b-1234-7abc-def0-123422222222", "beta", "beta"},
		{"018f3a5b-1234-7abc-def0-123433333333", "alpha", "same-user-session"},
		{"018f3a5b-1234-7abc-def0-123411111111", "alpha", "latest-alpha"},
	}
	for i, request := range requests {
		h := store.Begin(Metadata{RequestID: request.id, UserID: request.user, SessionID: "synthetic-client-session", Method: "POST", URL: "/v1/responses", Started: time.Unix(1700000000+int64(i), 123)})
		if h == nil {
			t.Fatalf("Begin(%q) rejected", request.id)
		}
		t.Cleanup(func() { h.End(false) })
		h.Capture("request.body", []byte(request.body))
		h.End(true)
	}
	closeStore()
	alpha := coreTestRead(t, store, requests[0].id)
	beta := coreTestRead(t, store, requests[1].id)
	sameUser := coreTestRead(t, store, requests[2].id)
	if string(alpha.body("request.body")) != "latest-alpha" {
		t.Fatalf("reused native request ID did not resolve to latest admission: %q", alpha.body("request.body"))
	}
	for _, record := range []coreTestRecord{alpha, beta, sameUser} {
		if record.metadataFrames != 1 || record.metadata.SessionID == "" || record.metadata.SessionID == "synthetic-client-session" {
			t.Fatalf("raw/empty session or missing metadata: %+v", record.metadata)
		}
		if strings.ContainsAny(record.name, "/\\\r\n") || !strings.HasSuffix(record.name, ".log") {
			t.Fatalf("unsafe generated download name %q", record.name)
		}
	}
	if alpha.metadata.RequestID != requests[0].id || alpha.metadata.UserID != "alpha" || beta.metadata.UserID != "beta" {
		t.Fatalf("native identity lost: alpha=%+v beta=%+v", alpha.metadata, beta.metadata)
	}
	if alpha.metadata.SessionID != sameUser.metadata.SessionID || alpha.metadata.SessionID == beta.metadata.SessionID {
		t.Fatal("session hashing is not stable within a user and partitioned across users")
	}
	short := coreTestRead(t, store, "11111111")
	if string(short.body("request.body")) != "latest-alpha" {
		t.Fatalf("short native request ID resolved incorrectly: %q", short.body("request.body"))
	}
}

func TestStoreStopRejectsNewButPreservesActiveUntilActualEnd(t *testing.T) {
	store, closeStore := coreTestStore(t, coreTestConfig(t))
	h := coreTestBegin(t, store, "stop-active", "alpha")
	store.Stop()
	if other := store.Begin(Metadata{RequestID: "after-stop", UserID: "alpha"}); other != nil {
		other.End(false)
		t.Fatal("Stop admitted a new request")
	}
	if stats := store.Stats(); stats.Active != 1 {
		t.Fatalf("Stop released a still-running inference handle: %+v", stats)
	}
	h.Capture("response.body", []byte("completed after Stop"))
	h.End(true)
	closeStore()
	got := coreTestRead(t, store, "stop-active")
	if got.status.State != EndedComplete || string(got.body("response.body")) != "completed after Stop" {
		t.Fatalf("Stop truncated an existing handle: %+v", got)
	}
	coreTestMissing(t, store, "after-stop")
}

func TestStoreDropForcesIncompleteAndLateCallbacksDoNotChangeRecord(t *testing.T) {
	store, closeStore := coreTestStore(t, coreTestConfig(t))
	h := coreTestBegin(t, store, "explicit-drop", "alpha")
	h.Capture("request.body", []byte("kept"))
	h.Drop(2, 123)
	h.End(true)
	closeStore()
	before := coreTestRead(t, store, "explicit-drop")
	statsBefore := store.Stats()
	h.Capture("late", []byte("late-body"))
	h.Drop(3, 17)
	h.End(false)
	after := coreTestRead(t, store, "explicit-drop")
	statsAfter := store.Stats()
	if before.status.State != EndedIncomplete || before.status.DroppedChunks != 2 || before.status.DroppedBytes != 123 {
		t.Fatalf("End(true) erased explicit loss: %+v", before.status)
	}
	if after.status != before.status || len(after.frames) != len(before.frames) || len(after.body("late")) != 0 {
		t.Fatalf("late callback mutated a finalized record: before=%+v after=%+v", before.status, after.status)
	}
	if statsAfter.DroppedChunks <= statsBefore.DroppedChunks || statsAfter.DroppedBytes < statsBefore.DroppedBytes+26 {
		t.Fatalf("late losses missing from global counters: before=%+v after=%+v", statsBefore, statsAfter)
	}
}

func TestStoreDisabledEmptyRootHasNoAdmissionOrFilesystem(t *testing.T) {
	store, closeStore := coreTestStore(t, Config{MaxBytes: 1, MaxActive: 1, QueueBytes: 1, MaxRecords: 1})
	if h := store.Begin(Metadata{RequestID: "disabled", UserID: "alpha"}); h != nil {
		h.Capture("body", []byte("must not persist"))
		h.End(true)
		t.Fatal("empty-root disabled store admitted capture")
	}
	store.Stop()
	closeStore()
	coreTestMissing(t, store, "disabled")
	if store.root != nil || store.Stats().LogicalBytes != 0 || store.Stats().ReservedBytes != 0 {
		t.Fatalf("disabled store opened filesystem or reserved bytes: %+v", store.Stats())
	}
}

func TestStoreConcurrentCaptureAndEnd(t *testing.T) {
	cfg := coreTestConfig(t)
	cfg.MaxBytes = 16 << 20
	cfg.QueueBytes = 8 << 20
	store, closeStore := coreTestStore(t, cfg)
	const handles = 32
	const producers = 4
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := 0; i < handles; i++ {
		h := coreTestBegin(t, store, fmt.Sprintf("concurrent-%02d", i), "alpha")
		for producer := 0; producer < producers; producer++ {
			workers.Add(1)
			go func(h *Handle, producer int) {
				defer workers.Done()
				<-start
				for chunk := 0; chunk < 16; chunk++ {
					h.Capture("response.body", []byte{byte(producer), byte(chunk), 0, 255})
				}
			}(h, producer)
		}
		workers.Add(1)
		go func(h *Handle) {
			defer workers.Done()
			<-start
			h.End(true)
			h.End(false)
		}(h)
	}
	close(start)
	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	coreTestAwait(t, done, "concurrent Capture/End")
	closeStore()
	for i := 0; i < handles; i++ {
		got := coreTestRead(t, store, fmt.Sprintf("concurrent-%02d", i))
		if got.status.State == Active || got.status.Ended.IsZero() {
			t.Fatalf("request stayed active after concurrent End: %+v", got.status)
		}
		for _, frame := range got.frames {
			if frame.Kind != "response.body" {
				continue
			}
			if len(frame.Data) != 4 || frame.Data[0] >= producers || frame.Data[1] >= 16 || frame.Data[2] != 0 || frame.Data[3] != 255 {
				t.Fatalf("concurrent body was torn or reused: %v", frame.Data)
			}
		}
	}
	if stats := store.Stats(); stats.Active != 0 {
		t.Fatalf("concurrent End leaked active slots: %+v", stats)
	}
}

func TestStoreBeginSnapshotsMutableMetadata(t *testing.T) {
	store, closeStore := coreTestStore(t, coreTestConfig(t))
	headers := map[string][]string{"X-Synthetic": {"original", "second"}}
	metadata := Metadata{RequestID: "metadata-copy", UserID: "alpha", SessionID: "original-session", Method: "POST", URL: "/original", Headers: headers, Started: time.Unix(1700000000, 1)}
	h := store.Begin(metadata)
	if h == nil {
		t.Fatal("Begin rejected metadata snapshot")
	}
	t.Cleanup(func() { h.End(false) })
	headers["X-Synthetic"][0] = "mutated"
	headers["New-Header"] = []string{"must not appear"}
	metadata.RequestID = "different"
	metadata.URL = "/mutated"
	h.End(true)
	closeStore()
	got := coreTestRead(t, store, "metadata-copy")
	if got.metadata.URL != "/original" || len(got.metadata.Headers["X-Synthetic"]) != 2 || got.metadata.Headers["X-Synthetic"][0] != "original" {
		t.Fatalf("caller mutated admitted metadata: %+v", got.metadata)
	}
	if _, exists := got.metadata.Headers["New-Header"]; exists {
		t.Fatal("caller mutated admitted header map")
	}
	coreTestMissing(t, store, "different")
}
