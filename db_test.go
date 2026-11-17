package bolttable

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.etcd.io/bbolt"
	bbolterrors "go.etcd.io/bbolt/errors"
)

func TestBboltOptions(t *testing.T) {
	db := mustOpen(t, tempPath(t), newFixture(t).schema, Options{})
	if db.bolt.NoFreelistSync {
		t.Error("NoFreelistSync is on: a failed commit would walk the file")
	}
	if db.bolt.FreelistType != bbolt.FreelistMapType {
		t.Errorf("FreelistType = %q, want %q", db.bolt.FreelistType, bbolt.FreelistMapType)
	}
	o := boltOptions(false)
	if o.InitialMmapSize != 1<<30 {
		t.Errorf("InitialMmapSize = %d, want 1 GiB", o.InitialMmapSize)
	}
	if o.Timeout != time.Nanosecond {
		t.Errorf("Timeout = %v, want 1ns, which bbolt tries once", o.Timeout)
	}
	if ro := boltOptions(true); !ro.ReadOnly || ro.NoFreelistSync || ro.FreelistType != bbolt.FreelistMapType {
		t.Errorf("read-only options = %+v", ro)
	}
}

func TestEpochDiffersPerOpen(t *testing.T) {
	path := tempPath(t)
	f := newFixture(t)
	db, err := Open(path, f.schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	first := db.Epoch()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	second := mustOpen(t, path, f.schema, Options{}).Epoch()
	if first == ([16]byte{}) || second == ([16]byte{}) {
		t.Errorf("zero epoch: %x, %x", first, second)
	}
	if first == second {
		t.Errorf("two opens share epoch %x", first)
	}
}

func TestOpenInitializesEmptyFile(t *testing.T) {
	path := tempPath(t)
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t)
	db, err := Open(path, f.schema, Options{})
	if err != nil {
		t.Fatalf("Open of an empty file: %v", err)
	}
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "kept"}) })
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	f = newFixture(t)
	db = mustOpen(t, path, f.schema, Options{})
	view(t, db, func(r *ReadTx) error {
		got, found, err := f.items.Get(r, 1)
		if err != nil || !found || got.Name != "kept" {
			t.Errorf("Get after reopen = %+v, %v, %v; want kept", got, found, err)
		}
		return nil
	})
}

func TestOpenRefusesBadArguments(t *testing.T) {
	if _, err := Open(tempPath(t), nil, Options{}); err == nil {
		t.Error("Open accepted a nil schema")
	}
	if _, err := Open(tempPath(t), newFixture(t).schema, Options{Retention: -1}); err == nil {
		t.Error("Open accepted a negative Retention")
	}
	path := tempPath(t)
	if _, err := Open(path, newFixture(t).schema, Options{InvalidFile: InvalidFileMoveAside + 1}); err == nil {
		t.Error("Open accepted an unknown InvalidFile policy")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a refused Open left a file: %v", err)
	}
}

func TestBatchOptionsDefault(t *testing.T) {
	db := mustOpen(t, tempPath(t), newFixture(t).schema, Options{})
	if db.bolt.MaxBatchSize != 1000 || db.bolt.MaxBatchDelay != 10*time.Millisecond {
		t.Errorf("batch limits = %d, %v; want 1000 and 10ms", db.bolt.MaxBatchSize, db.bolt.MaxBatchDelay)
	}
}

func TestBatchOptionsApplied(t *testing.T) {
	db := mustOpen(t, tempPath(t), newFixture(t).schema, Options{MaxBatchSize: 4, MaxBatchDelay: time.Hour})
	if db.bolt.MaxBatchSize != 4 || db.bolt.MaxBatchDelay != time.Hour {
		t.Errorf("batch limits = %d, %v; want 4 and 1h", db.bolt.MaxBatchSize, db.bolt.MaxBatchDelay)
	}
}

func TestBatchOptionsNegativeRefused(t *testing.T) {
	for name, o := range map[string]Options{"size": {MaxBatchSize: -1}, "delay": {MaxBatchDelay: -time.Millisecond}} {
		t.Run(name, func(t *testing.T) {
			path := tempPath(t)
			if db, err := Open(path, newFixture(t).schema, o); err == nil {
				_ = db.Close()
				t.Fatal("Open accepted a negative batch limit")
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Errorf("a refused Open left a file: %v", err)
			}
		})
	}
}

// failingCalls is every DB call that answers an error, by name.
func failingCalls(t *testing.T, db *DB, f *fixture) map[string]func() error {
	t.Helper()
	put := func(tx *Tx) error { return f.items.Put(tx, 1, &item{}) }
	return map[string]func() error{
		"View":   func() error { return db.View(t.Context(), func(*ReadTx) error { return nil }) },
		"Update": func() error { return db.Update(t.Context(), put) },
		"Batch":  func() error { return db.Batch(t.Context(), put) },
		"Backup": func() error { return db.Backup(t.Context(), filepath.Join(t.TempDir(), "copy.bolt")) },
		"Verify": func() error { _, err := db.Verify(t.Context(), false); return err },
		"Repair": func() error { _, err := db.Verify(t.Context(), true); return err },
		"Stats":  func() error { _, err := db.Stats(t.Context()); return err },
		"Space":  func() error { _, err := db.Space(); return err },
	}
}

func assertClosedError(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrClosed) || !errors.Is(err, bbolterrors.ErrDatabaseNotOpen) {
		t.Errorf("err = %v, want ErrClosed and ErrDatabaseNotOpen", err)
	}
	if errors.Is(err, ErrCommit) {
		t.Errorf("err = %v wraps ErrCommit, which a write-health record counts as a fault", err)
	}
}

func TestClosedDBMatchesBothSentinels(t *testing.T) {
	f := newFixture(t)
	t.Run("after Close", func(t *testing.T) {
		db := mustOpen(t, tempPath(t), f.schema, Options{})
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		for name, call := range failingCalls(t, db, f) {
			t.Run(name, func(t *testing.T) { assertClosedError(t, call()) })
		}
	})
	// A read that passed the closed check before Close began meets bbolt's
	// own refusal to begin a transaction.
	t.Run("past the closed check", func(t *testing.T) {
		db := mustOpen(t, tempPath(t), f.schema, Options{})
		if err := db.bolt.Close(); err != nil {
			t.Fatal(err)
		}
		calls := failingCalls(t, db, f)
		delete(calls, "Space") // TestSpacePastTheCloseCheck: Space reads the cached path
		// Close waits for a write past its check: TestCloseWaitsForCommitCallbacks
		delete(calls, "Update")
		delete(calls, "Batch")
		delete(calls, "Repair")
		for name, call := range calls {
			t.Run(name, func(t *testing.T) { assertClosedError(t, call()) })
		}
	})
	t.Run("read-only", func(t *testing.T) {
		rdb, err := OpenReadOnly(storedFile(t), f.schema, ReadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := rdb.Close(); err != nil {
			t.Fatal(err)
		}
		calls := map[string]func() error{
			"View":   func() error { return rdb.View(t.Context(), func(*ReadTx) error { return nil }) },
			"Backup": func() error { return rdb.Backup(t.Context(), filepath.Join(t.TempDir(), "copy.bolt")) },
			"Verify": func() error { _, err := rdb.Verify(t.Context()); return err },
			"Stats":  func() error { _, err := rdb.Stats(t.Context()); return err },
			"Space":  func() error { _, err := rdb.Space(); return err },
		}
		for name, call := range calls {
			t.Run(name, func(t *testing.T) { assertClosedError(t, call()) })
		}
	})
}

// TestCloseWaitsForCommitCallbacks pins that Close returns only after the
// commit callbacks of a write in progress ran: bbolt runs them after it
// releases its locks, and a View inside one is allowed.
func TestCloseWaitsForCommitCallbacks(t *testing.T) {
	f := newFixture(t)
	put := func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "x"}) }
	cases := map[string]struct {
		opts  func(hold func()) Options
		write func(db *DB, hold func()) error
	}{
		"OnCommit": {
			opts:  func(hold func()) Options { return Options{OnCommit: func(Change) { hold() }} },
			write: func(db *DB, _ func()) error { return db.Update(t.Context(), put) },
		},
		"Update AfterCommit": {
			write: func(db *DB, hold func()) error {
				return db.Update(t.Context(), func(tx *Tx) error { tx.AfterCommit(hold); return put(tx) })
			},
		},
		"Batch AfterCommit": {
			write: func(db *DB, hold func()) error {
				return db.Batch(t.Context(), func(tx *Tx) error { tx.AfterCommit(hold); return put(tx) })
			},
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			entered, gate, viewed := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			var db *DB
			hold := func() {
				close(entered)
				<-gate
				viewed <- db.View(t.Context(), func(*ReadTx) error { return nil })
			}
			var o Options
			if c.opts != nil {
				o = c.opts(hold)
			}
			db = mustOpen(t, tempPath(t), f.schema, o)
			wrote := make(chan error, 1)
			go func() { wrote <- c.write(db, hold) }()
			await(t, entered, "the commit callback to start")
			release := sync.OnceFunc(func() { close(gate) })
			defer release()
			closed := make(chan error, 1)
			go func() { closed <- db.Close() }()
			for deadline := time.Now().Add(5 * time.Second); !db.closed.Load(); time.Sleep(time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatal("Close never began")
				}
			}
			// A write that starts while Close waits is refused at once.
			assertClosedError(t, answerNow(t, func() error { return db.Update(t.Context(), put) }))
			select {
			case err := <-closed:
				t.Errorf("Close returned (%v) while a commit callback runs", err)
				closed <- err
			case <-time.After(50 * time.Millisecond):
			}
			release()
			if err := await(t, viewed, "the callback's View"); err != nil {
				t.Errorf("View inside a commit callback while Close waits = %v, want nil", err)
			}
			if err := await(t, wrote, "the write"); err != nil {
				t.Errorf("the write in progress = %v, want nil", err)
			}
			if err := await(t, closed, "Close"); err != nil {
				t.Errorf("Close = %v", err)
			}
			assertClosedError(t, db.Update(t.Context(), put))
		})
	}
}

// TestCloseAfterPanickedWrite pins that a write whose function or callback
// panicked leaves nothing for Close to wait on.
func TestCloseAfterPanickedWrite(t *testing.T) {
	f := newFixture(t)
	cases := map[string]func(db *DB) error{
		"Update": func(db *DB) error {
			return db.Update(t.Context(), func(*Tx) error { panic(errBoom) })
		},
		"Batch": func(db *DB) error {
			return db.Batch(t.Context(), func(*Tx) error { panic(errBoom) })
		},
		"AfterCommit": func(db *DB) error {
			return db.Update(t.Context(), func(tx *Tx) error {
				tx.AfterCommit(func() { panic(errBoom) })
				return f.items.Put(tx, 1, &item{Name: "x"})
			})
		},
	}
	for name, write := range cases {
		t.Run(name, func(t *testing.T) {
			db := mustOpen(t, tempPath(t), f.schema, Options{})
			func() {
				defer func() {
					if p := recover(); p != errBoom {
						t.Errorf("recovered %v, want errBoom", p)
					}
				}()
				_ = write(db)
			}()
			if err := answerNow(t, db.Close); err != nil {
				t.Errorf("Close = %v", err)
			}
		})
	}
}
