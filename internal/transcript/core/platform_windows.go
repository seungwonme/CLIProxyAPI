//go:build windows

package core

import (
	"errors"
	"os"
)

// Windows capture is rejected before opening a root; these flags are never
// used to weaken the secure Unix capture path into a Windows fallback.
const (
	openNoFollow  = 0
	openDirectory = 0
	openNonblock  = 0
)

func checkCapturePlatform() error {
	return errors.New("transcript: capture is unsupported on Windows; disable transcripts")
}

func acquireRootLock(_ *os.Root) (*os.File, error) {
	return nil, checkCapturePlatform()
}
