//go:build darwin || linux || freebsd

package core

import (
	"errors"
	"os"
	"syscall"
)

const (
	openNoFollow  = syscall.O_NOFOLLOW
	openDirectory = syscall.O_DIRECTORY
	openNonblock  = syscall.O_NONBLOCK
)

func checkCapturePlatform() error {
	return nil
}

func acquireRootLock(root *os.Root) (*os.File, error) {
	lock, err := root.OpenFile(".", os.O_RDONLY|openNoFollow|openDirectory, 0)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, errors.New("transcript: root already owned or cannot be locked")
	}
	return lock, nil
}
