//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package bolttable

import (
	"os"
	"syscall"
	"testing"
)

// assertDataUnlocked fails the test when a handle still holds bbolt's flock on
// path. It takes the lock itself rather than through bbolt, whose open would
// read the pages of an invalid file.
func assertDataUnlocked(t *testing.T, path string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	fd := int(f.Fd())
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Errorf("%s is still locked: %v", path, err)
		return
	}
	_ = syscall.Flock(fd, syscall.LOCK_UN)
}
