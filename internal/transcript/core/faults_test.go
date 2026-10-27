package core

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func coreTestHookStore(t *testing.T, cfg Config, hooks *faultHooks) (*Store, func() error) {
	t.Helper()
	store, err := newStore(cfg, hooks)
	if err != nil {
		t.Fatalf("newStore: %v", err)
	}
	var once sync.Once
	var closeErr error
	closeStore := func() error {
		once.Do(func() { closeErr = store.Close() })
		return closeErr
	}
	t.Cleanup(func() { _ = closeStore() })
	return store, closeStore
}

// Each pass is released explicitly. A closed stop channel makes cleanup safe
// even if an assertion fails while the single mutation worker is parked.
type coreTestWorkerGate struct {
	entered chan struct{}
	resume  chan struct{}
	stop    chan struct{}
	once    sync.Once
}

func coreTestNewGate() *coreTestWorkerGate {
	return &coreTestWorkerGate{entered: make(chan struct{}), resume: make(chan struct{}), stop: make(chan struct{})}
}

func (g *coreTestWorkerGate) beforeWorker() {
	select {
	case g.entered <- struct{}{}:
	case <-g.stop:
		return
	}
	select {
	case <-g.resume:
	case <-g.stop:
	}
}

func (g *coreTestWorkerGate) release() {
	g.once.Do(func() { close(g.stop) })
}

func (g *coreTestWorkerGate) advance(t *testing.T, store *Store) {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case g.resume <- struct{}{}:
	case <-timer.C:
		t.Fatal("mutation worker did not accept an explicit pass release")
	}
	store.notify()
	coreTestAwait(t, g.entered, "next explicitly synchronized worker pass")
}

func TestStoreWorkerBarrierBoundsQueueAndSlotsWithoutBlockingInference(t *testing.T) {
	cfg := coreTestConfig(t)
	cfg.QueueBytes = 1024
	cfg.MaxActive = 2
	gate := coreTestNewGate()
	store, closeStore := coreTestHookStore(t, cfg, &faultHooks{beforeWorker: gate.beforeWorker})
	t.Cleanup(gate.release)
	coreTestAwait(t, gate.entered, "initial worker barrier")
	first := coreTestBegin(t, store, "barrier-first", "alpha")
	second := coreTestBegin(t, store, "barrier-second", "beta")
	returned := make(chan struct{})
	go func() {
		first.Capture("body", bytes.Repeat([]byte{0xa1}, 700))
		first.Capture("body", []byte{0xb2})
		first.Drop(2, 17)
		first.End(true)
		second.End(true)
		close(returned)
	}()
	coreTestAwait(t, returned, "Capture/Drop/End before worker release")
	if queued := store.queuedBytes.Load(); queued <= 0 || queued > cfg.QueueBytes {
		t.Fatalf("queued charge=%d limit=%d", queued, cfg.QueueBytes)
	}
	if got := store.Begin(Metadata{RequestID: "barrier-third", UserID: "gamma"}); got != nil {
		got.End(false)
		t.Fatal("actual End released admission before worker retired its record")
	}
	if stats := store.Stats(); stats.Active != 2 || stats.DroppedChunks != 3 || stats.DroppedBytes != 18 {
		t.Fatalf("blocked worker lost bounded admission/counters: %+v", stats)
	}
	gate.release()
	if err := closeStore(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := coreTestRead(t, store, "barrier-first")
	if got.status.State != EndedIncomplete || got.status.DroppedChunks != 3 || got.status.DroppedBytes != 18 || !bytes.Equal(got.body("body"), bytes.Repeat([]byte{0xa1}, 700)) {
		t.Fatalf("full queue lost independent terminal state: status=%+v bytes=%d", got.status, len(got.body("body")))
	}
	coreTestAssertBudget(t, store, cfg.Root)
}

func TestStoreAdmissionDoesNotSpawnPerRequestWorkers(t *testing.T) {
	cfg := coreTestConfig(t)
	cfg.MaxActive = 256
	cfg.MaxRecords = 512
	gate := coreTestNewGate()
	store, closeStore := coreTestHookStore(t, cfg, &faultHooks{beforeWorker: gate.beforeWorker})
	t.Cleanup(gate.release)
	coreTestAwait(t, gate.entered, "single worker barrier")
	before := runtime.NumGoroutine()
	for i := 0; i < cfg.MaxActive; i++ {
		h := coreTestBegin(t, store, fmt.Sprintf("bounded-worker-%03d", i), "alpha")
		h.Capture("body", []byte{byte(i)})
		h.End(true)
	}
	if growth := runtime.NumGoroutine() - before; growth > 8 {
		t.Fatalf("%d requests spawned %d additional goroutines while worker stayed parked", cfg.MaxActive, growth)
	}
	gate.release()
	if err := closeStore(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreFullQueueLossSurvivesIndependentEndPublication(t *testing.T) {
	cfg := coreTestConfig(t)
	cfg.QueueBytes = 1
	cfg.MaxActive = 1
	store, closeStore := coreTestStore(t, cfg)
	h := coreTestBegin(t, store, "full-byte-queue", "alpha")
	if rejected := store.Begin(Metadata{RequestID: "full-active-slots", UserID: "beta"}); rejected != nil {
		rejected.End(false)
		t.Fatal("full active admission was not rejected")
	}
	body := bytes.Repeat([]byte{0, 255, '\n'}, 100)
	returned := make(chan struct{})
	go func() {
		h.Capture("request.body", body)
		h.Drop(3, 17)
		h.End(true)
		close(returned)
	}()
	coreTestAwait(t, returned, "Capture/Drop/End against exhausted queue bytes")
	closeStore()
	got := coreTestRead(t, store, "full-byte-queue")
	if got.status.State != EndedIncomplete || got.status.DroppedChunks != 4 || got.status.DroppedBytes != uint64(len(body)+17) {
		t.Fatalf("terminal publication lost full-queue counters: %+v", got.status)
	}
	if len(got.body("request.body")) != 0 {
		t.Fatal("queue accepted data larger than its entire configured byte budget")
	}
	stats := store.Stats()
	if stats.DroppedChunks != 4 || stats.DroppedBytes != uint64(len(body)+17) || stats.Skipped == 0 || stats.Active != 0 {
		t.Fatalf("global counters lost queue/admission failure: %+v", stats)
	}
	coreTestMissing(t, store, "full-active-slots")
	coreTestAssertBudget(t, store, cfg.Root)
}

func TestStoreShortWriteAndENOSPCCountActualPrefixAndRemainIncomplete(t *testing.T) {
	for _, fault := range []string{"short-write", "enospc"} {
		t.Run(fault, func(t *testing.T) {
			cfg := coreTestConfig(t)
			gate := coreTestNewGate()
			var writes atomic.Int32
			var prefix atomic.Int64
			hooks := &faultHooks{beforeWorker: gate.beforeWorker}
			hooks.write = func(file *os.File, data []byte) (int, error) {
				// Magic and metadata are real successful writes; the first body
				// frame leaves a real, uncommitted prefix on disk.
				if writes.Add(1) != 3 {
					return file.Write(data)
				}
				n, err := file.Write(data[:min(7, len(data))])
				prefix.Store(int64(n))
				if err != nil {
					return n, err
				}
				if fault == "enospc" {
					return n, syscall.ENOSPC
				}
				return n, nil
			}
			store, closeStore := coreTestHookStore(t, cfg, hooks)
			t.Cleanup(gate.release)
			coreTestAwait(t, gate.entered, "write-fault worker barrier")
			h := coreTestBegin(t, store, "disk-write-fault", "alpha")
			h.Capture("request.body", bytes.Repeat([]byte{0, 255, 0x7f}, 50))
			h.End(true)
			gate.release()
			if err := closeStore(); err == nil {
				t.Fatal("Close did not report a disk write failure")
			}
			got := coreTestRead(t, store, "disk-write-fault")
			if prefix.Load() == 0 || got.status.State != EndedIncomplete || got.status.Failure == "" || got.status.DroppedChunks == 0 || got.status.DroppedBytes != 150 {
				t.Fatalf("write failure not reflected: prefix=%d status=%+v", prefix.Load(), got.status)
			}
			if len(got.body("request.body")) != 0 {
				t.Fatal("uncommitted partial frame was exposed to readers")
			}
			if stats := store.Stats(); !stats.Degraded || stats.Failures == 0 || stats.ReservedBytes != 0 {
				t.Fatalf("write failure not reported/released: %+v", stats)
			}
			coreTestAssertBudget(t, store, cfg.Root)
		})
	}
}

func TestStoreFinalizationFailuresAreIncompleteAndEvictWithoutRestart(t *testing.T) {
	for _, fault := range []string{"sync", "close", "rename"} {
		t.Run(fault, func(t *testing.T) {
			cfg := coreTestConfig(t)
			cfg.MaxRecords = 1
			gate := coreTestNewGate()
			hooks := &faultHooks{beforeWorker: gate.beforeWorker}
			var failed atomic.Bool
			injected := errors.New("synthetic finalization failure")
			switch fault {
			case "sync":
				hooks.sync = func(file *os.File) error {
					if err := file.Sync(); err != nil {
						return err
					}
					if failed.CompareAndSwap(false, true) {
						return injected
					}
					return nil
				}
			case "close":
				hooks.close = func(file *os.File) error {
					if err := file.Close(); err != nil {
						return err
					}
					if failed.CompareAndSwap(false, true) {
						return injected
					}
					return nil
				}
			case "rename":
				hooks.rename = func(root *os.Root, from, to string) error {
					if failed.CompareAndSwap(false, true) {
						return injected
					}
					return root.Rename(from, to)
				}
			}
			store, closeStore := coreTestHookStore(t, cfg, hooks)
			t.Cleanup(gate.release)
			coreTestAwait(t, gate.entered, "finalization-fault worker barrier")
			first := coreTestBegin(t, store, "ended-incomplete", "alpha")
			first.Capture("body", []byte("first body"))
			first.End(true)
			gate.advance(t, store)
			got := coreTestRead(t, store, "ended-incomplete")
			if !failed.Load() || got.status.State != EndedIncomplete || got.status.Failure != fault || got.status.Ended.IsZero() {
				t.Fatalf("finalization failure has wrong state: %+v", got.status)
			}
			coreTestAssertBudget(t, store, cfg.Root)
			second := coreTestBegin(t, store, "replacement", "beta")
			second.Capture("body", []byte("replacement body"))
			second.End(true)
			gate.advance(t, store)
			coreTestMissing(t, store, "ended-incomplete")
			kept := coreTestRead(t, store, "replacement")
			if kept.status.State != EndedComplete || string(kept.body("body")) != "replacement body" {
				t.Fatalf("failed retired record remained ineligible without restart: %+v", kept.status)
			}
			coreTestAssertBudget(t, store, cfg.Root)
			gate.release()
			if err := closeStore(); err == nil {
				t.Fatal("Close lost prior finalization failure")
			}
		})
	}
}

func TestStoreFinalizationStallDoesNotBlockEndOrIndexReaders(t *testing.T) {
	cfg := coreTestConfig(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var syncs atomic.Int32
	hooks := &faultHooks{sync: func(file *os.File) error {
		if syncs.Add(1) == 1 {
			close(entered)
			<-release
		}
		return file.Sync()
	}}
	store, closeStore := coreTestHookStore(t, cfg, hooks)
	t.Cleanup(unblock)
	first := coreTestBegin(t, store, "sync-stall", "alpha")
	returned := make(chan struct{})
	go func() {
		first.Capture("body", []byte("before stall"))
		first.End(true)
		close(returned)
	}()
	coreTestAwait(t, returned, "End before finalizer release")
	coreTestAwait(t, entered, "sync finalizer stall")
	other := coreTestBegin(t, store, "during-sync-stall", "beta")
	inferenceDone := make(chan struct{})
	go func() {
		other.Capture("body", []byte("during stall"))
		other.End(true)
		close(inferenceDone)
	}()
	coreTestAwait(t, inferenceDone, "inference during blocked sync")
	indexDone := make(chan struct{})
	var openErr error
	var reader *Reader
	go func() {
		_ = store.Stats()
		reader, openErr = store.Open("sync-stall")
		close(indexDone)
	}()
	coreTestAwait(t, indexDone, "Stats/Open while sync is stalled")
	if openErr != nil {
		t.Fatalf("Open during finalization: %v", openErr)
	}
	if reader.Status().State == Active {
		t.Fatal("actual request End was not published before finalization I/O")
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	unblock()
	if err := closeStore(); err != nil {
		t.Fatal(err)
	}
	if got := coreTestRead(t, store, "during-sync-stall"); got.status.State != EndedComplete {
		t.Fatalf("other request was blocked or lost: %+v", got.status)
	}
}

func TestStoreTinyCapIncludesEveryWriteAndActiveTerminalReserve(t *testing.T) {
	cfg := coreTestConfig(t)
	cfg.MaxBytes = 12 << 10
	gate := coreTestNewGate()
	store, closeStore := coreTestHookStore(t, cfg, &faultHooks{beforeWorker: gate.beforeWorker})
	t.Cleanup(gate.release)
	coreTestAwait(t, gate.entered, "quota worker barrier")
	h := coreTestBegin(t, store, "tiny-cap-active", "alpha")
	gate.advance(t, store)
	if stats := store.Stats(); stats.ReservedBytes < terminalReserve || stats.Active != 1 {
		t.Fatalf("active request lacks independently reserved terminal space: %+v", stats)
	}
	coreTestAssertBudget(t, store, cfg.Root)
	h.Capture("body", bytes.Repeat([]byte{0xa5}, 512))
	gate.advance(t, store)
	coreTestAssertBudget(t, store, cfg.Root)
	// This body cannot fit while retaining the terminal reservation. Its
	// rejection must not evict this still-active record or consume its reserve.
	h.Capture("body", bytes.Repeat([]byte{0xff}, maxChunkBytes))
	gate.advance(t, store)
	coreTestAssertBudget(t, store, cfg.Root)
	if stats := store.Stats(); stats.Records != 1 || stats.Active != 1 || stats.DroppedBytes < maxChunkBytes || stats.ReservedBytes < terminalReserve {
		t.Fatalf("quota pressure corrupted active reservation: %+v", stats)
	}
	h.End(true)
	gate.advance(t, store)
	got := coreTestRead(t, store, "tiny-cap-active")
	if got.status.State != EndedIncomplete || !bytes.Equal(got.body("body"), bytes.Repeat([]byte{0xa5}, 512)) {
		t.Fatalf("tiny cap lost committed bytes or terminal loss: %+v", got.status)
	}
	coreTestAssertBudget(t, store, cfg.Root)
	if stats := store.Stats(); stats.ReservedBytes != 0 {
		t.Fatalf("terminal reservation leaked after End: %+v", stats)
	}
	gate.release()
	if err := closeStore(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreFailedLoggerCannotBeEvictedUntilActualRequestEnd(t *testing.T) {
	cfg := coreTestConfig(t)
	cfg.MaxRecords = 1
	gate := coreTestNewGate()
	var writes atomic.Int32
	hooks := &faultHooks{beforeWorker: gate.beforeWorker, write: func(file *os.File, data []byte) (int, error) {
		if writes.Add(1) == 3 {
			return 0, syscall.ENOSPC
		}
		return file.Write(data)
	}}
	store, closeStore := coreTestHookStore(t, cfg, hooks)
	t.Cleanup(gate.release)
	coreTestAwait(t, gate.entered, "failed-active worker barrier")
	active := coreTestBegin(t, store, "failed-but-running", "alpha")
	active.Capture("body", []byte("disk error"))
	gate.advance(t, store)
	before := coreTestRead(t, store, "failed-but-running")
	if before.status.State != Active || before.status.Failure == "" {
		t.Fatalf("logger failure falsely ended inference: %+v", before.status)
	}
	challenger := coreTestBegin(t, store, "must-not-evict-active", "beta")
	challenger.End(true)
	gate.advance(t, store)
	kept := coreTestRead(t, store, "failed-but-running")
	if kept.status.State != Active || store.Stats().Active != 1 {
		t.Fatalf("capacity pressure retired a failed logger before actual End: %+v stats=%+v", kept.status, store.Stats())
	}
	coreTestMissing(t, store, "must-not-evict-active")
	coreTestAssertBudget(t, store, cfg.Root)
	active.End(true)
	gate.advance(t, store)
	replacement := coreTestBegin(t, store, "after-actual-end", "beta")
	replacement.End(true)
	gate.advance(t, store)
	coreTestMissing(t, store, "failed-but-running")
	if got := coreTestRead(t, store, "after-actual-end"); got.status.State != EndedComplete {
		t.Fatalf("failed logger was not eligible after actual End: %+v", got.status)
	}
	coreTestAssertBudget(t, store, cfg.Root)
	gate.release()
	if err := closeStore(); err == nil {
		t.Fatal("disk and capacity failures were not reported")
	}
}

func TestStoreEvictionUsesOldestActualEndAcrossUsers(t *testing.T) {
	cfg := coreTestConfig(t)
	cfg.MaxRecords = 2
	gate := coreTestNewGate()
	store, closeStore := coreTestHookStore(t, cfg, &faultHooks{beforeWorker: gate.beforeWorker})
	t.Cleanup(gate.release)
	coreTestAwait(t, gate.entered, "end-order worker barrier")
	base := time.Unix(1700000000, 0)
	startedFirst := coreTestBeginAt(t, store, "start-first-end-last", "alpha", base)
	gate.advance(t, store)
	startedLast := coreTestBeginAt(t, store, "start-last-end-first", "beta", base.Add(time.Hour))
	startedLast.End(true)
	startedLast.endedAt.Store(base.Add(2 * time.Hour).UnixNano())
	gate.advance(t, store)
	startedFirst.End(true)
	startedFirst.endedAt.Store(base.Add(3 * time.Hour).UnixNano())
	gate.advance(t, store)
	old := coreTestRead(t, store, "start-last-end-first")
	newer := coreTestRead(t, store, "start-first-end-last")
	if !old.status.Ended.Before(newer.status.Ended) {
		t.Fatalf("fixture needs ordered actual Ends: old=%v newer=%v", old.status.Ended, newer.status.Ended)
	}
	third := coreTestBegin(t, store, "other-user-replacement", "gamma")
	third.End(true)
	gate.advance(t, store)
	coreTestMissing(t, store, "start-last-end-first")
	if got := coreTestRead(t, store, "start-first-end-last"); got.status.State != EndedComplete {
		t.Fatalf("evicted by start/admission order rather than actual End: %+v", got.status)
	}
	coreTestRead(t, store, "other-user-replacement")
	coreTestAssertBudget(t, store, cfg.Root)
	gate.release()
	if err := closeStore(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreReaderPinPreventsEvictionUntilClose(t *testing.T) {
	cfg := coreTestConfig(t)
	cfg.MaxRecords = 1
	gate := coreTestNewGate()
	store, closeStore := coreTestHookStore(t, cfg, &faultHooks{beforeWorker: gate.beforeWorker})
	t.Cleanup(gate.release)
	coreTestAwait(t, gate.entered, "reader-pin worker barrier")
	first := coreTestBegin(t, store, "pinned", "alpha")
	first.Capture("body", []byte("pinned body"))
	first.End(true)
	gate.advance(t, store)
	reader, err := store.Open("pinned")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	before := store.Stats().LogicalBytes
	second := coreTestBegin(t, store, "blocked-by-pin", "beta")
	second.End(true)
	gate.advance(t, store)
	if store.Stats().LogicalBytes != before {
		t.Fatal("pinned record was unlinked or unaccounted under pressure")
	}
	coreTestMissing(t, store, "blocked-by-pin")
	if got := coreTestReadReader(t, reader); string(got.body("body")) != "pinned body" {
		t.Fatal("pinned reader lost its complete file")
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	third := coreTestBegin(t, store, "after-unpin", "beta")
	third.End(true)
	gate.advance(t, store)
	coreTestMissing(t, store, "pinned")
	coreTestRead(t, store, "after-unpin")
	coreTestAssertBudget(t, store, cfg.Root)
	gate.release()
	if err := closeStore(); err == nil {
		t.Fatal("pin-induced capacity failure not reported")
	}
}

func TestStoreUnlinkFailureRetainsPhysicalAccountingAndCanRetry(t *testing.T) {
	cfg := coreTestConfig(t)
	cfg.MaxRecords = 1
	gate := coreTestNewGate()
	var unlinks atomic.Int32
	hooks := &faultHooks{beforeWorker: gate.beforeWorker, unlink: func(root *os.Root, path string) error {
		if unlinks.Add(1) == 1 {
			return syscall.EACCES
		}
		return root.Remove(path)
	}}
	store, closeStore := coreTestHookStore(t, cfg, hooks)
	t.Cleanup(gate.release)
	coreTestAwait(t, gate.entered, "unlink-fault worker barrier")
	first := coreTestBegin(t, store, "unlink-failed", "alpha")
	first.Capture("body", []byte("must stay charged"))
	first.End(true)
	gate.advance(t, store)
	before := store.Stats().LogicalBytes
	second := coreTestBegin(t, store, "unlink-rejected", "beta")
	second.End(true)
	gate.advance(t, store)
	if unlinks.Load() != 1 || store.Stats().LogicalBytes != before || coreTestPhysicalBytes(t, cfg.Root) != before {
		t.Fatalf("failed unlink released accounting: calls=%d before=%d stats=%+v", unlinks.Load(), before, store.Stats())
	}
	coreTestRead(t, store, "unlink-failed")
	coreTestMissing(t, store, "unlink-rejected")
	third := coreTestBegin(t, store, "unlink-retry", "beta")
	third.End(true)
	gate.advance(t, store)
	coreTestMissing(t, store, "unlink-failed")
	coreTestRead(t, store, "unlink-retry")
	coreTestAssertBudget(t, store, cfg.Root)
	gate.release()
	if err := closeStore(); err == nil || !store.Stats().Degraded {
		t.Fatalf("unlink failure was not visible: err=%v stats=%+v", err, store.Stats())
	}
}

func TestStoreOpenDuringDeletionCannotPinRetiredSelection(t *testing.T) {
	cfg := coreTestConfig(t)
	cfg.MaxRecords = 1
	gate := coreTestNewGate()
	deleting := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	hooks := &faultHooks{beforeWorker: gate.beforeWorker, unlink: func(root *os.Root, path string) error {
		close(deleting)
		<-release
		return root.Remove(path)
	}}
	store, closeStore := coreTestHookStore(t, cfg, hooks)
	t.Cleanup(gate.release)
	t.Cleanup(unblock)
	coreTestAwait(t, gate.entered, "delete-race worker barrier")
	first := coreTestBegin(t, store, "selected-for-deletion", "alpha")
	first.Capture("body", []byte("old body"))
	first.End(true)
	gate.advance(t, store)
	before := store.Stats().LogicalBytes
	second := coreTestBegin(t, store, "deletion-replacement", "beta")
	second.End(true)
	gate.resume <- struct{}{}
	store.notify()
	coreTestAwait(t, deleting, "unlink hook after eviction selection")
	openDone := make(chan struct{})
	var reader *Reader
	var openErr error
	var stats Stats
	go func() {
		reader, openErr = store.Open("selected-for-deletion")
		stats = store.Stats()
		close(openDone)
	}()
	coreTestAwait(t, openDone, "Open/Stats while unlink I/O is blocked")
	if reader != nil {
		_ = reader.Close()
		t.Fatal("Open pinned a record already selected for deletion")
	}
	if !errors.Is(openErr, os.ErrNotExist) || stats.LogicalBytes != before || coreTestPhysicalBytes(t, cfg.Root) != before {
		t.Fatalf("deletion selection released accounting before actual unlink: err=%v before=%d stats=%+v", openErr, before, stats)
	}
	unblock()
	coreTestAwait(t, gate.entered, "worker after real unlink")
	coreTestMissing(t, store, "selected-for-deletion")
	coreTestRead(t, store, "deletion-replacement")
	coreTestAssertBudget(t, store, cfg.Root)
	gate.release()
	if err := closeStore(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreOpenAcrossSuccessfulRenameBeforePathPublication(t *testing.T) {
	cfg := coreTestConfig(t)
	renamed := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	hooks := &faultHooks{rename: func(root *os.Root, from, to string) error {
		if err := root.Rename(from, to); err != nil {
			return err
		}
		close(renamed)
		<-release
		return nil
	}}
	store, closeStore := coreTestHookStore(t, cfg, hooks)
	t.Cleanup(unblock)
	h := coreTestBegin(t, store, "rename-publication-race", "alpha")
	h.Capture("body", []byte("sealed bytes"))
	h.End(true)
	coreTestAwait(t, renamed, "real rename before indexed path publication")
	opened := make(chan struct{})
	var reader *Reader
	var openErr error
	go func() {
		reader, openErr = store.Open("rename-publication-race")
		close(opened)
	}()
	coreTestAwait(t, opened, "Open after rename syscall and before path publication")
	if openErr != nil {
		t.Fatalf("a successfully sealed, pinned record disappeared during path publication: %v", openErr)
	}
	t.Cleanup(func() { _ = reader.Close() })
	got := coreTestReadReader(t, reader)
	if string(got.body("body")) != "sealed bytes" || got.status.State == Active {
		t.Fatalf("rename race exposed wrong bytes/state: %+v", got.status)
	}
	unblock()
	if err := closeStore(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreReaderSnapshotDoesNotAcquireFutureFramesOrStatus(t *testing.T) {
	cfg := coreTestConfig(t)
	gate := coreTestNewGate()
	store, closeStore := coreTestHookStore(t, cfg, &faultHooks{beforeWorker: gate.beforeWorker})
	t.Cleanup(gate.release)
	coreTestAwait(t, gate.entered, "snapshot worker barrier")
	h := coreTestBegin(t, store, "reader-snapshot", "alpha")
	h.Capture("body", []byte("prefix"))
	gate.advance(t, store)
	reader, err := store.Open("reader-snapshot")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	h.Capture("body", []byte("suffix"))
	h.End(true)
	gate.advance(t, store)
	old := coreTestReadReader(t, reader)
	fresh := coreTestRead(t, store, "reader-snapshot")
	if string(old.body("body")) != "prefix" || old.status.State != Active {
		t.Fatalf("reader changed its pinned snapshot: body=%q status=%+v", old.body("body"), old.status)
	}
	if string(fresh.body("body")) != "prefixsuffix" || fresh.status.State != EndedComplete {
		t.Fatalf("reopen failed to observe newly committed bytes/status: body=%q status=%+v", fresh.body("body"), fresh.status)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Next(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Next after Close = %v, want os.ErrClosed", err)
	}
	gate.release()
	if err := closeStore(); err != nil {
		t.Fatal(err)
	}
}
