//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package bolttable

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// tryLock takes a flock(2) on f without blocking, and reports whether
// another holder keeps it.
func tryLock(f *os.File, exclusive bool) (busy bool, err error) {
	how := unix.LOCK_SH
	if exclusive {
		how = unix.LOCK_EX
	}
	err = unix.Flock(int(f.Fd()), how|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) {
		return true, nil
	}
	return false, err
}

func unlockFile(f *os.File) error {
	return unix.Flock(int(f.Fd()), unix.LOCK_UN)
}

func lockSupported() error { return nil }

// unlockBolt releases bbolt's flock on its data file f
// (https://github.com/etcd-io/bbolt/blob/v1.5.0/bolt_unix.go#L50).
func unlockBolt(f *os.File) error { return unlockFile(f) }
