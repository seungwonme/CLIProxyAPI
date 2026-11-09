package core

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestStoreExclusiveWriterAndCloseReleasesOwnership(t *testing.T) {
	cfg := coreTestConfig(t)
	first, closeFirst := coreTestStore(t, cfg)
	h := coreTestBegin(t, first, "exclusive-writer", "alpha")
	h.Capture("body", []byte("retained"))
	h.End(true)
	second, err := New(cfg)
	if err == nil {
		_ = second.Close()
		t.Fatal("second writer admitted with independent quota accounting")
	}
	closeFirst()
	third, closeThird := coreTestStore(t, cfg)
	closeThird()
	if got := coreTestRead(t, third, "exclusive-writer"); string(got.body("body")) != "retained" {
		t.Fatal("ownership release lost transcript")
	}
}

func TestStoreRejectsUnownedOverCapWithoutDeletingFiles(t *testing.T) {
	cfg := coreTestConfig(t)
	cfg.MaxBytes = 4096
	if err := os.MkdirAll(cfg.Root, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(cfg.Root, "unowned-backup")
	original := bytes.Repeat([]byte("x"), 8192)
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	s, err := New(cfg)
	if err == nil {
		_ = s.Close()
		t.Fatal("startup admitted storage already over cap")
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, original) {
		t.Fatal("unowned file removed or modified")
	}
}
