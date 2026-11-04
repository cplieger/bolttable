//go:build !(linux || darwin || dragonfly || freebsd || netbsd || openbsd)

package bolttable

import (
	"errors"
	"testing"
	"time"

	"go.etcd.io/bbolt"
	bbolterrors "go.etcd.io/bbolt/errors"
)

// assertDataUnlocked fails the test when a handle still holds bbolt's lock on
// path.
func assertDataUnlocked(t *testing.T, path string) {
	t.Helper()
	bdb, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: time.Nanosecond})
	if errors.Is(err, bbolterrors.ErrTimeout) {
		t.Errorf("%s is still locked", path)
	}
	if err == nil {
		_ = bdb.Close()
	}
}

// assertUnmapped is a no-op where this process's mappings cannot be listed.
func assertUnmapped(*testing.T, string) {}
