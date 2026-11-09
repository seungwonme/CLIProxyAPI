package core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func coreTestFiles(t *testing.T, root string) map[string]int64 {
	t.Helper()
	files := make(map[string]int64)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if entry.Type().IsRegular() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			files[path] = info.Size()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir(%q): %v", root, err)
	}
	return files
}

func coreTestOnlyFile(t *testing.T, root string) string {
	t.Helper()
	files := coreTestFiles(t, root)
	if len(files) != 1 {
		t.Fatalf("want exactly one generated record, got %v", files)
	}
	for path := range files {
		return path
	}
	panic("unreachable")
}

func coreTestPhysicalBytes(t *testing.T, root string) int64 {
	t.Helper()
	var bytes int64
	for _, size := range coreTestFiles(t, root) {
		bytes += size
	}
	return bytes
}

func coreTestAssertBudget(t *testing.T, store *Store, root string) {
	t.Helper()
	stats := store.Stats()
	physical := coreTestPhysicalBytes(t, root)
	if stats.LogicalBytes != physical {
		t.Fatalf("owned file st_size=%d differs from accounted bytes=%d; stats=%+v", physical, stats.LogicalBytes, stats)
	}
	if stats.ReservedBytes < 0 || physical+stats.ReservedBytes > stats.LimitBytes {
		t.Fatalf("actual writes plus terminal reservations exceed cap: physical=%d stats=%+v", physical, stats)
	}
}

func TestStoreRejectsRelativeAndSymlinkRoots(t *testing.T) {
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(base, "external")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(target, "unrelated.txt")
	if err := os.WriteFile(marker, []byte("external sentinel"), 0600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(base, "alias")
	if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{"relative-records", alias, filepath.Join(alias, "child")} {
		t.Run(root, func(t *testing.T) {
			store, err := New(Config{Root: root})
			if store != nil {
				if errClose := store.Close(); errClose != nil && err == nil {
					t.Errorf("Close: %v", errClose)
				}
			}
			if err == nil {
				t.Fatalf("unsafe root accepted: %q", root)
			}
		})
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "external sentinel" {
		t.Fatalf("unsafe root modified external target: data=%q err=%v", data, err)
	}
	if _, err := os.Stat(filepath.Join(target, "child")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsafe ancestor created a directory outside root: %v", err)
	}
}

func TestStoreGeneratedPathsPermissionsAndUnrelatedFile(t *testing.T) {
	cfg := coreTestConfig(t)
	if err := os.Mkdir(cfg.Root, 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(cfg.Root, "keep-this-unrelated-file")
	if err := os.WriteFile(marker, []byte("sentinel"), 0600); err != nil {
		t.Fatal(err)
	}
	store, closeStore := coreTestStore(t, cfg)
	h := coreTestBegin(t, store, "safe-native-id", "../../synthetic-user/..")
	h.Capture("request.body", []byte("opaque body"))
	h.End(true)
	closeStore()
	got := coreTestRead(t, store, "safe-native-id")
	if string(got.body("request.body")) != "opaque body" {
		t.Fatal("opaque metadata changed body")
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "sentinel" {
		t.Fatalf("unrelated file changed: %q %v", data, err)
	}
	owned := 0
	err := filepath.WalkDir(cfg.Root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == marker {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		want := fs.FileMode(0600)
		if entry.IsDir() {
			want = 0700
		} else {
			owned++
		}
		if info.Mode().Perm() != want {
			t.Errorf("%s mode=%o want=%o", path, info.Mode().Perm(), want)
		}
		if entry.Type()&os.ModeSymlink != 0 {
			t.Errorf("generated path is symlink: %s", path)
		}
		if strings.Contains(path, "synthetic-user") || strings.Contains(path, "synthetic-client-session") {
			t.Errorf("raw identity became a filesystem component: %s", path)
		}
		return nil
	})
	if err != nil || owned == 0 {
		t.Fatalf("generated tree: files=%d err=%v", owned, err)
	}
	coreTestMissing(t, store, "../keep-this-unrelated-file")
	coreTestMissing(t, store, marker)
}

func TestStoreRecoveryPreservesBodyAndRebuildsNativeIDIndex(t *testing.T) {
	cfg := coreTestConfig(t)
	store, closeStore := coreTestStore(t, cfg)
	h := coreTestBegin(t, store, "018f3a5b-1234-7abc-def0-1234aabbccdd", "alpha")
	body := []byte{0, 255, 10, 13, '[', ']', 0}
	h.Capture("request.body", body)
	h.End(true)
	closeStore()
	original := coreTestRead(t, store, "aabbccdd")
	recovered, closeRecovered := coreTestStore(t, cfg)
	closeRecovered()
	got := coreTestRead(t, recovered, "018f3a5b-1234-7abc-def0-1234aabbccdd")
	if !bytes.Equal(got.body("request.body"), body) || got.status.State != EndedComplete || got.name != original.name {
		t.Fatalf("recovery changed completed record: status=%+v body=%v name=%q", got.status, got.body("request.body"), got.name)
	}
	if short := coreTestRead(t, recovered, "aabbccdd"); short.metadata.RequestID != got.metadata.RequestID {
		t.Fatal("recovery did not rebuild short native request-ID index")
	}
	coreTestAssertBudget(t, recovered, cfg.Root)
}

func TestStoreRecoveryRejectsCorruptAndTruncatedMetadata(t *testing.T) {
	for _, corruption := range []string{"invalid-magic", "truncated-magic", "oversize-metadata-frame", "invalid-metadata-json", "truncated-metadata-payload", "metadata-wrong-namespace"} {
		t.Run(corruption, func(t *testing.T) {
			cfg := coreTestConfig(t)
			store, closeStore := coreTestStore(t, cfg)
			h := coreTestBegin(t, store, "corrupt-metadata", "alpha")
			h.Capture("request.body", []byte("body must not invent an index entry"))
			h.End(true)
			closeStore()
			path := coreTestOnlyFile(t, cfg.Root)
			data, err := os.ReadFile(path)
			if err != nil || len(data) < len(fileMagic)+4 {
				t.Fatalf("read generated frame: length=%d err=%v", len(data), err)
			}
			offset := len(fileMagic)
			metadataBytes := int(binary.BigEndian.Uint32(data[offset : offset+4]))
			if metadataBytes <= 0 || offset+4+metadataBytes > len(data) {
				t.Fatalf("invalid generated metadata length %d", metadataBytes)
			}
			switch corruption {
			case "invalid-magic":
				for i := 0; i < 8; i++ {
					data[i] = 0xff
				}
			case "truncated-magic":
				data = data[:3]
			case "oversize-metadata-frame":
				binary.BigEndian.PutUint32(data[offset:offset+4], uint32(maxFrameBytes+1))
			case "invalid-metadata-json":
				data[offset+4] = 0xff
			case "truncated-metadata-payload":
				data = data[:offset+4+metadataBytes/2]
			case "metadata-wrong-namespace":
				frame, _, err := decodeFrame(bytes.NewReader(data[offset:]))
				if err != nil || frame.Metadata == nil {
					t.Fatalf("decode generated metadata: %v", err)
				}
				frame.Metadata.UserID = "different-verified-user"
				encoded, err := encodeFrame(frame)
				if err != nil {
					t.Fatal(err)
				}
				tail := bytes.Clone(data[offset+4+metadataBytes:])
				data = append(append(bytes.Clone(fileMagic), encoded...), tail...)
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			recovered, closeRecovered := coreTestStore(t, cfg)
			closeRecovered()
			coreTestMissing(t, recovered, "corrupt-metadata")
			stats := recovered.Stats()
			if !stats.Degraded || stats.Failures == 0 {
				t.Fatalf("invalid persisted metadata was silently ignored: %+v", stats)
			}
			coreTestAssertBudget(t, recovered, cfg.Root)
		})
	}
}

func TestStoreRecoveryShrinksOverCapUsingActualEndAcrossUsers(t *testing.T) {
	cfg := coreTestConfig(t)
	gate := coreTestNewGate()
	store, closeStore := coreTestHookStore(t, cfg, &faultHooks{beforeWorker: gate.beforeWorker})
	t.Cleanup(gate.release)
	coreTestAwait(t, gate.entered, "recovery end-order worker barrier")
	base := time.Unix(1700000000, 0)
	late := coreTestBeginAt(t, store, "started-first-ended-last", "alpha", base)
	late.Capture("request.body", bytes.Repeat([]byte{0xa1}, 8192))
	early := coreTestBeginAt(t, store, "started-last-ended-first", "beta", base.Add(time.Hour))
	early.Capture("request.body", bytes.Repeat([]byte{0xb2}, 8192))
	early.End(true)
	early.endedAt.Store(base.Add(2 * time.Hour).UnixNano())
	late.End(true)
	late.endedAt.Store(base.Add(3 * time.Hour).UnixNano())
	gate.release()
	if err := closeStore(); err != nil {
		t.Fatal(err)
	}
	before := coreTestPhysicalBytes(t, cfg.Root)
	if before <= 2*terminalReserve {
		t.Fatalf("fixture too small to distinguish an over-cap recovery: %d", before)
	}
	// Either record fits by itself, but their existing aggregate does not.
	cfg.MaxBytes = before - 1
	recovered, closeRecovered := coreTestStore(t, cfg)
	closeRecovered()
	coreTestMissing(t, recovered, "started-last-ended-first")
	kept := coreTestRead(t, recovered, "started-first-ended-last")
	if kept.status.State != EndedComplete || !bytes.Equal(kept.body("request.body"), bytes.Repeat([]byte{0xa1}, 8192)) {
		t.Fatalf("recovery evicted by start time instead of actual end: %+v", kept.status)
	}
	coreTestAssertBudget(t, recovered, cfg.Root)
}

func TestStoreRecoveryDoesNotFollowGeneratedFileSymlink(t *testing.T) {
	cfg := coreTestConfig(t)
	store, closeStore := coreTestStore(t, cfg)
	h := coreTestBegin(t, store, "file-symlink", "alpha")
	h.Capture("request.body", []byte("external opaque transcript"))
	h.End(true)
	closeStore()
	path := coreTestOnlyFile(t, cfg.Root)
	externalDir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	external := filepath.Join(externalDir, "external.log")
	if err := os.Rename(path, external); err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(external)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, path); err != nil {
		t.Fatal(err)
	}
	recovered, err := New(cfg)
	if err == nil {
		if errClose := recovered.Close(); errClose != nil {
			t.Errorf("Close: %v", errClose)
		}
		coreTestMissing(t, recovered, "file-symlink")
	}
	got, err := os.ReadFile(external)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("recovery followed or unlinked external file: bytes=%d want=%d err=%v", len(got), len(want), err)
	}
}

func TestStoreRecoveryAndCaptureDoNotFollowGeneratedUserSymlink(t *testing.T) {
	cfg := coreTestConfig(t)
	store, closeStore := coreTestStore(t, cfg)
	h := coreTestBegin(t, store, "user-symlink-old", "alpha")
	h.Capture("request.body", []byte("external sentinel"))
	h.End(true)
	closeStore()
	path := coreTestOnlyFile(t, cfg.Root)
	userDir := filepath.Dir(path)
	if userDir == cfg.Root {
		t.Fatal("implementation did not generate a user directory")
	}
	externalDir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	external := filepath.Join(externalDir, "external-user")
	if err := os.Rename(userDir, external); err != nil {
		t.Fatal(err)
	}
	want := coreTestFiles(t, external)
	contents := make(map[string][]byte, len(want))
	for path := range want {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		contents[path] = data
	}
	if err := os.Symlink(external, userDir); err != nil {
		t.Fatal(err)
	}
	recovered, err := New(cfg)
	if err == nil {
		newHandle := coreTestBegin(t, recovered, "user-symlink-new", "alpha")
		newHandle.Capture("request.body", []byte("must not reach external directory"))
		newHandle.End(true)
		_ = recovered.Close()
		coreTestMissing(t, recovered, "user-symlink-old")
	}
	for path, data := range contents {
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("external user file changed: %s err=%v", path, err)
		}
	}
	if got := coreTestFiles(t, external); len(got) != len(want) {
		t.Fatalf("capture wrote through user-directory symlink: before=%v after=%v", want, got)
	}
}

// This process deliberately exits with an admitted request still running. It
// writes through the real worker and syncs only the captured prefix; it does
// not fabricate a completed file and relabel it as a crash.
func TestStoreCrashWriterProcess(t *testing.T) {
	if len(os.Args) < 3 || os.Args[len(os.Args)-2] != "core-crash-fixture" {
		return
	}
	root := os.Args[len(os.Args)-1]
	gate := coreTestNewGate()
	store, err := newStore(Config{Root: root, MaxBytes: 1 << 20, MaxActive: 2, QueueBytes: 1 << 20, MaxRecords: 8}, &faultHooks{beforeWorker: gate.beforeWorker})
	if err != nil {
		t.Fatal(err)
	}
	coreTestAwait(t, gate.entered, "crash writer initial barrier")
	h := coreTestBegin(t, store, "crash-native-12345678", "alpha")
	h.Capture("request.body", []byte{0, 255, 'c', 'r', 'a', 's', 'h', '\n'})
	gate.advance(t, store)
	store.mu.Lock()
	var file *os.File
	for _, record := range store.records {
		if record.handle == h {
			file = record.file
		}
	}
	store.mu.Unlock()
	if file == nil {
		t.Fatal("crash fixture has no real open record")
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	os.Exit(23)
}

func TestStoreRecoversRealCrashedOpenFromReadOnlyFile(t *testing.T) {
	cfg := coreTestConfig(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestStoreCrashWriterProcess$", "--", "core-crash-fixture", cfg.Root)
	output, err := command.CombinedOutput()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 23 {
		t.Fatalf("crash writer must exit abruptly after real writes: err=%v output=%s", err, output)
	}
	path := coreTestOnlyFile(t, cfg.Root)
	if filepath.Ext(path) != ".open" {
		t.Fatalf("crashed request unexpectedly sealed: %s", path)
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0400); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(path, 0600) }()
	store, closeStore := coreTestStore(t, cfg)
	closeStore()
	got := coreTestRead(t, store, "crash-native-12345678")
	if got.status.State != EndedIncomplete || !got.status.ApproximateEnd || got.status.Ended.IsZero() || !bytes.Equal(got.body("request.body"), []byte{0, 255, 'c', 'r', 'a', 's', 'h', '\n'}) {
		t.Fatalf("crash recovery misrepresented the active prefix: %+v body=%v", got.status, got.body("request.body"))
	}
	for _, frame := range got.frames {
		if frame.Kind == "status" && frame.Status != nil && frame.Status.State == EndedComplete {
			t.Fatal("crash recovery invented a complete terminal status")
		}
	}
	if !store.Stats().Degraded {
		t.Fatal("crash-lost finalization was not reported as degraded")
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, want) {
		t.Fatalf("read-only crash recovery mutated the existing prefix: err=%v", err)
	}
	coreTestAssertBudget(t, store, cfg.Root)
}

func TestStoreRecoveryNeverTrustsCompleteTerminalInUnsealedOpen(t *testing.T) {
	cfg := coreTestConfig(t)
	hooks := &faultHooks{rename: func(*os.Root, string, string) error {
		return errors.New("synthetic rename failure after sync and close")
	}}
	store, closeStore := coreTestHookStore(t, cfg, hooks)
	h := coreTestBegin(t, store, "unsealed-terminal", "alpha")
	h.Capture("body", []byte("durable prefix"))
	h.End(true)
	if err := closeStore(); err == nil {
		t.Fatal("rename fault did not leave an incomplete publication")
	}
	path := coreTestOnlyFile(t, cfg.Root)
	if filepath.Ext(path) != ".open" {
		t.Fatalf("rename failure did not retain .open file: %s", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	reader := bytes.NewReader(data[len(fileMagic):])
	terminal := false
	for reader.Len() != 0 {
		frame, _, err := decodeFrame(reader)
		if err != nil {
			t.Fatal(err)
		}
		if frame.Kind == "status" && frame.Status != nil {
			terminal = frame.Status.State == EndedComplete
		}
	}
	if !terminal {
		t.Fatal("fixture does not contain the would-be complete terminal before rename")
	}
	recovered, closeRecovered := coreTestStore(t, cfg)
	closeRecovered()
	got := coreTestRead(t, recovered, "unsealed-terminal")
	if got.status.State != EndedIncomplete || !got.status.ApproximateEnd || string(got.body("body")) != "durable prefix" {
		t.Fatalf("recovery trusted an unsealed complete-looking terminal: %+v", got.status)
	}
	for _, frame := range got.frames {
		if frame.Kind == "status" && (frame.Status == nil || frame.Status.State != EndedIncomplete) {
			t.Fatalf("reader exposed misleading terminal from .open: %+v", frame.Status)
		}
	}
	coreTestAssertBudget(t, recovered, cfg.Root)
}

func TestStoreRecoveryTruncatedTailPreservesOnlyCommittedPrefix(t *testing.T) {
	for _, tail := range []string{"partial-frame-length", "partial-frame-payload", "missing-terminal"} {
		t.Run(tail, func(t *testing.T) {
			cfg := coreTestConfig(t)
			store, closeStore := coreTestStore(t, cfg)
			h := coreTestBegin(t, store, "truncated-tail", "alpha")
			h.Capture("body", []byte("complete prefix"))
			h.End(true)
			closeStore()
			path := coreTestOnlyFile(t, cfg.Root)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			offset := len(fileMagic)
			for offset < len(data) {
				frame, n, err := decodeFrame(bytes.NewReader(data[offset:]))
				if err != nil {
					t.Fatal(err)
				}
				if frame.Kind == "status" {
					break
				}
				offset += int(n)
			}
			if offset == len(data) {
				t.Fatal("generated record has no terminal frame")
			}
			switch tail {
			case "partial-frame-length":
				data = data[:offset+2]
			case "partial-frame-payload":
				data = data[:len(data)-1]
			case "missing-terminal":
				data = data[:offset]
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			recovered, closeRecovered := coreTestStore(t, cfg)
			closeRecovered()
			got := coreTestRead(t, recovered, "truncated-tail")
			if got.status.State != EndedIncomplete || got.status.Failure == "" || !got.status.ApproximateEnd || string(got.body("body")) != "complete prefix" {
				t.Fatalf("invalid tail hid committed prefix or claimed complete: %+v body=%q", got.status, got.body("body"))
			}
			if stats := recovered.Stats(); !stats.Degraded || stats.Failures == 0 {
				t.Fatalf("tail corruption was not reported: %+v", stats)
			}
			coreTestAssertBudget(t, recovered, cfg.Root)
		})
	}
}
