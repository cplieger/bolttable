//go:build windows

package bolttable

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

// tryLock locks byte range [0, 1) of f without blocking, and reports whether
// another holder keeps it. The range is the lock file's own; bbolt's lock on
// the data file is a different file.
func tryLock(f *os.File, exclusive bool) (busy bool, err error) {
	flags := uint32(windows.LOCKFILE_FAIL_IMMEDIATELY)
	if exclusive {
		flags |= windows.LOCKFILE_EXCLUSIVE_LOCK
	}
	err = windows.LockFileEx(windows.Handle(f.Fd()), flags, 0, 1, 0, &windows.Overlapped{})
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return true, nil
	}
	return false, err
}

func unlockFile(f *os.File) error {
	return windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, &windows.Overlapped{})
}

func lockSupported() error { return nil }

// unlockBolt releases bbolt's lock on its data file f, byte range -1..0
// (https://github.com/etcd-io/bbolt/blob/v1.5.0/bolt_windows.go#L57).
func unlockBolt(f *os.File) error {
	return windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, &windows.Overlapped{Offset: ^uint32(0), OffsetHigh: ^uint32(0)})
}
