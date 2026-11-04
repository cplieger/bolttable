package bolttable

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

func mustLock(t *testing.T, path string) *Lock {
	t.Helper()
	l, err := LockPath(path, 0)
	if err != nil {
		t.Fatalf("LockPath: %v", err)
	}
	t.Cleanup(func() { _ = l.Release() })
	return l
}

// assertLocked fails the test unless another holder keeps path's lock.
func assertLocked(t *testing.T, path string) {
	t.Helper()
	l, err := LockPath(path, 0)
	if err == nil {
		_ = l.Release()
		t.Errorf("%s.lock is free", path)
		return
	}
	if !errors.Is(err, ErrLocked) {
		t.Errorf("LockPath = %v, want ErrLocked", err)
	}
}

func TestLockPathExclusive(t *testing.T) {
	path := tempPath(t)
	first, err := LockPath(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := LockPath(path, 0); !errors.Is(err, ErrLocked) {
		t.Errorf("second LockPath = %v, want ErrLocked", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("a zero wait waited %v", d)
	}
	time.AfterFunc(50*time.Millisecond, func() { _ = first.Release() })
	second, err := LockPath(path, 2*time.Second)
	if err != nil {
		t.Fatalf("LockPath waiting for a release = %v", err)
	}
	if err := second.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestLockFileNeverRemoved(t *testing.T) {
	path := tempPath(t)
	l, err := LockPath(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path + ".lock")
	if err != nil {
		t.Fatalf("lock file: %v", err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("lock file mode %v, want 0600", fi.Mode().Perm())
	}
	if err := l.Release(); err != nil {
		t.Fatal(err)
	}
	db := mustOpen(t, path, newFixture(t).schema, Options{})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".lock"); err != nil {
		t.Errorf("lock file after Release and Close: %v", err)
	}
}

func TestOpenHoldsLockUntilClose(t *testing.T) {
	path := tempPath(t)
	db := mustOpen(t, path, newFixture(t).schema, Options{})
	assertLocked(t, path)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	mustLock(t, path)
}

func TestSecondOpenErrLocked(t *testing.T) {
	path := tempPath(t)
	mustOpen(t, path, newFixture(t).schema, Options{})
	for name, wait := range map[string]time.Duration{"no wait": 0, "200ms": 200 * time.Millisecond} {
		t.Run(name, func(t *testing.T) {
			start := time.Now()
			db, err := Open(path, newFixture(t).schema, Options{LockWait: wait})
			d := time.Since(start)
			if err == nil {
				_ = db.Close()
				t.Fatal("second Open succeeded")
			}
			if !errors.Is(err, ErrLocked) {
				t.Errorf("second Open = %v, want ErrLocked", err)
			}
			if d < wait || d > wait+time.Second {
				t.Errorf("second Open took %v, want about %v", d, wait)
			}
		})
	}
}

func TestLockAcrossProcesses(t *testing.T) {
	if childMode(t, "lock", func(path string) error {
		l, err := LockPath(path, 0)
		if err != nil {
			return err
		}
		fmt.Println("locked")
		_, _ = io.Copy(io.Discard, os.Stdin)
		return l.Release()
	}) {
		return
	}
	path := tempPath(t)
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestLockAcrossProcesses$", "-test.count=1")
	cmd.Env = append(os.Environ(), childEnv+"=lock:"+path)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if line != "locked\n" {
		_ = stdin.Close()
		_ = cmd.Wait()
		t.Fatalf("child said %q, %v", line, err)
	}
	if _, err := LockPath(path, 0); !errors.Is(err, ErrLocked) {
		t.Errorf("LockPath while another process holds it = %v, want ErrLocked", err)
	}
	if db, err := Open(path, newFixture(t).schema, Options{}); !errors.Is(err, ErrLocked) {
		if err == nil {
			_ = db.Close()
		}
		t.Errorf("Open while another process holds the lock = %v, want ErrLocked", err)
	}
	_ = stdin.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatalf("child: %v", err)
	}
	mustLock(t, path)
}

func TestReleaseTwice(t *testing.T) {
	l, err := LockPath(tempPath(t), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Release(); err != nil {
		t.Fatal(err)
	}
	if err := l.Release(); err != nil {
		t.Errorf("second Release = %v", err)
	}
}

func TestCloseTwice(t *testing.T) {
	t.Run("own lock", func(t *testing.T) {
		path := tempPath(t)
		db := mustOpen(t, path, newFixture(t).schema, Options{})
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Errorf("second Close = %v", err)
		}
		mustLock(t, path)
	})
	t.Run("adopted lock", func(t *testing.T) {
		path := tempPath(t)
		l := mustLock(t, path)
		db := mustOpen(t, path, newFixture(t).schema, Options{Lock: l})
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		next := mustOpen(t, path, newFixture(t).schema, Options{Lock: l})
		if err := db.Close(); err != nil {
			t.Errorf("second Close = %v", err)
		}
		assertLocked(t, path)
		if err := l.Release(); err == nil {
			t.Error("a second Close freed the lock another DB runs under")
		}
		if err := next.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestOpenFailureReleasesLock(t *testing.T) {
	path := storedFile(t)
	if _, err := Open(path, mismatchSchema(), Options{}); !errors.Is(err, ErrFormat) {
		t.Fatalf("Open = %v, want a *FormatError", err)
	}
	mustLock(t, path)
}

func TestOpenUnderAdoptedLock(t *testing.T) {
	path := storedFile(t)
	l, err := LockPath(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	db := mustOpen(t, path, newFixture(t).schema, Options{Lock: l})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Go(func() { assertLocked(t, path) })
	wg.Wait()
	f := newFixture(t)
	db = mustOpen(t, path, f.schema, Options{Lock: l, Reset: ResetPolicy{Force: []string{"a"}}})
	view(t, db, func(r *ReadTx) error {
		if _, found, _ := f.items.Get(r, 1); found {
			t.Error("the forced namespace kept its row")
		}
		if _, found, _ := f.other.Get(r, 1); !found {
			t.Error("namespace b lost its row")
		}
		return nil
	})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	assertLocked(t, path)
	if err := l.Release(); err != nil {
		t.Fatal(err)
	}
	mustLock(t, path)
}

func TestAdoptedLockFailedOpenKeepsLock(t *testing.T) {
	path := storedFile(t)
	l := mustLock(t, path)
	if _, err := Open(path, mismatchSchema(), Options{Lock: l}); !errors.Is(err, ErrFormat) {
		t.Fatalf("Open = %v, want a *FormatError", err)
	}
	assertLocked(t, path)
	db := mustOpen(t, path, newFixture(t).schema, Options{Lock: l})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAdoptedLockOneDB(t *testing.T) {
	path := tempPath(t)
	l := mustLock(t, path)
	mustOpen(t, path, newFixture(t).schema, Options{Lock: l})
	if db, err := Open(path, newFixture(t).schema, Options{Lock: l}); !errors.Is(err, ErrLocked) {
		if err == nil {
			_ = db.Close()
		}
		t.Errorf("second Open under one Lock = %v, want ErrLocked", err)
	}
	if err := l.Release(); err == nil {
		t.Error("the refused Open freed the lock the first DB runs under")
	}
}

func TestReleaseWhileInUse(t *testing.T) {
	path := tempPath(t)
	l := mustLock(t, path)
	db := mustOpen(t, path, newFixture(t).schema, Options{Lock: l})
	if err := l.Release(); err == nil {
		t.Error("Release succeeded while a DB runs under the lock")
	}
	assertLocked(t, path)
	update(t, db, func(*Tx) error { return nil })
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Release(); err != nil {
		t.Errorf("Release after Close = %v", err)
	}
	mustLock(t, path)
}

func TestAdoptedLockArguments(t *testing.T) {
	cases := map[string]func(path string) Options{
		"released": func(path string) Options {
			l, err := LockPath(path, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := l.Release(); err != nil {
				t.Fatal(err)
			}
			return Options{Lock: l}
		},
		"other path": func(string) Options { return Options{Lock: mustLock(t, tempPath(t))} },
		"with a wait": func(path string) Options {
			return Options{Lock: mustLock(t, path), LockWait: time.Second}
		},
	}
	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			path := tempPath(t)
			db, err := Open(path, newFixture(t).schema, opts(path))
			if err == nil {
				_ = db.Close()
				t.Fatal("Open accepted the lock")
			}
			if errors.Is(err, ErrLocked) {
				t.Errorf("Open = %v, want an argument error", err)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("a refused Open left a file: %v", err)
			}
		})
	}
	path := tempPath(t)
	if _, err := Open(path, newFixture(t).schema, Options{LockWait: -time.Second}); err == nil || errors.Is(err, ErrLocked) {
		t.Errorf("Open with a negative LockWait = %v, want an argument error", err)
	}
	if _, err := os.Stat(path + ".lock"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a refused Open took the lock: %v", err)
	}
}

func TestAdoptedLockPathIsCleaned(t *testing.T) {
	path := tempPath(t)
	l := mustLock(t, filepath.Dir(path)+string(filepath.Separator)+"."+string(filepath.Separator)+filepath.Base(path))
	db := mustOpen(t, path, newFixture(t).schema, Options{Lock: l})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBoltOpensTryOnce(t *testing.T) {
	type open struct {
		path    string
		timeout time.Duration
	}
	var (
		mu    sync.Mutex
		opens []open
	)
	boltOpen = func(p string, mode os.FileMode, o *bbolt.Options) (*bbolt.DB, error) {
		mu.Lock()
		opens = append(opens, open{p, o.Timeout})
		mu.Unlock()
		return bbolt.Open(p, mode, o)
	}
	t.Cleanup(func() { boltOpen = bbolt.Open })
	path := sparseFile(t)
	db := mustOpen(t, path, fixtureAt(t, 2).schema, Options{Reset: allowAll, Compact: compactAt})
	if !db.OpenReport().Compacted {
		t.Fatal("fixture: Open did not compact")
	}
	backup := filepath.Join(t.TempDir(), "backup.bolt")
	if err := db.Backup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	var paths []string
	for _, o := range opens {
		paths = append(paths, filepath.Base(o.path))
		if o.timeout != boltTryOnce {
			t.Errorf("bbolt open of %s with Timeout %v, want %v", o.path, o.timeout, boltTryOnce)
		}
	}
	for _, want := range []string{".backup.bolt.bolttable-tmp", "db.bolt.compact-tmp"} {
		if !slices.Contains(paths, want) {
			t.Errorf("no bbolt open of %s among %s", want, strings.Join(paths, ", "))
		}
	}
}
