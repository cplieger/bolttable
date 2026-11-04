package bolttable

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// lockPoll is how often a contended lock is retried while its wait lasts.
const lockPoll = 50 * time.Millisecond

// Lock is the exclusive process lock on a database path, the file
// <path>.lock. The lock assumes a local filesystem (see LockPath).
type Lock struct {
	f        *os.File
	path     string
	mu       sync.Mutex
	inUse    bool
	released bool
}

// LockPath takes the exclusive lock on path, waiting up to wait: zero tries
// once, negative is an error, and another holder fails it with ErrLocked. It
// creates <path>.lock, mode 0600, and never removes or renames it. Pass the
// Lock as Options.Lock to Open path under it. The lock, flock(2) on Linux,
// the BSDs and Darwin and LockFileEx on Windows, excludes other processes
// and other handles in this one only on a local filesystem: NFS and, since
// Linux 5.5, CIFS emulate flock with per-process byte-range locks. Other
// platforms get an error wrapping errors.ErrUnsupported.
func LockPath(path string, wait time.Duration) (*Lock, error) {
	if wait < 0 {
		return nil, fmt.Errorf("bolttable: negative lock wait %v", wait)
	}
	name := lockName(path)
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o600) //nolint:gosec // G703: the caller names its own database file
	if err != nil {
		return nil, fmt.Errorf("bolttable: lock: %w", err)
	}
	if err := lockFile(f, name, true, wait); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Lock{f: f, path: name}, nil
}

func lockName(path string) string { return filepath.Clean(path) + ".lock" }

// readLock takes the shared lock on path when its lock file exists and
// returns what releases it. An absent lock file means no writer opened the
// path, so only bbolt's own shared lock on the data file applies; the lock
// file is opened read-only so a reader never creates it.
func readLock(path string, wait time.Duration) (func() error, error) {
	if err := lockSupported(); err != nil {
		return nil, fmt.Errorf("bolttable: lock: %w", err)
	}
	name := lockName(path)
	f, err := os.Open(name)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return func() error { return nil }, nil
	case err != nil:
		return nil, fmt.Errorf("bolttable: lock: %w", err)
	}
	if err := lockFile(f, name, false, wait); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() error {
		if err := errors.Join(unlockFile(f), f.Close()); err != nil {
			return fmt.Errorf("bolttable: release %s: %w", name, err)
		}
		return nil
	}, nil
}

// lockFile takes f's lock, retrying a contended attempt until wait has
// elapsed. Any other failure is returned at once: waiting cannot fix it.
func lockFile(f *os.File, name string, exclusive bool, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	var tick *time.Ticker
	for {
		busy, err := tryLock(f, exclusive)
		switch {
		case err != nil:
			stop(tick)
			return fmt.Errorf("bolttable: lock %s: %w", name, err)
		case !busy:
			stop(tick)
			return nil
		case !time.Now().Before(deadline):
			stop(tick)
			return fmt.Errorf("%w: %s", ErrLocked, name)
		}
		if tick == nil {
			tick = time.NewTicker(lockPoll)
		}
		<-tick.C
	}
}

func stop(t *time.Ticker) {
	if t != nil {
		t.Stop()
	}
}

// Release releases the lock. A second Release returns nil. While an open DB
// runs under the lock (Options.Lock), Release fails and the lock stays held:
// Close the DB first.
func (l *Lock) Release() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch {
	case l.released:
		return nil
	case l.inUse:
		return errors.New("bolttable: Release of a lock an open DB runs under")
	}
	l.released = true
	if err := errors.Join(unlockFile(l.f), l.f.Close()); err != nil {
		return fmt.Errorf("bolttable: release %s: %w", l.path, err)
	}
	return nil
}

// adopt marks l in use by an Open of path. A released lock or one for
// another path is the caller's bug; one in use backs another DB.
func (l *Lock) adopt(path string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch {
	case l.released:
		return errors.New("bolttable: Open under a released Lock")
	case l.path != lockName(path):
		return fmt.Errorf("bolttable: Open of %s under the lock %s", path, l.path)
	case l.inUse:
		return fmt.Errorf("%w: %s backs another open DB", ErrLocked, l.path)
	}
	l.inUse = true
	return nil
}

// free ends an adoption, leaving l held.
func (l *Lock) free() {
	l.mu.Lock()
	l.inUse = false
	l.mu.Unlock()
}
