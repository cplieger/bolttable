package bolttable

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var errBoom = errors.New("boom")

// await receives from ch, failing the test when nothing arrives in time.
func await[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		var zero T
		return zero
	}
}

func newLive(t *testing.T, path string, s *Schema, o Options) *Live {
	t.Helper()
	l, err := NewLive(path, s, o)
	if err != nil {
		t.Fatalf("NewLive: %v", err)
	}
	if l == nil {
		t.Fatal("NewLive returned nil")
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func openLive(t *testing.T, path string, s *Schema, o Options) *Live {
	t.Helper()
	l := newLive(t, path, s, o)
	if err := l.Open(); err != nil {
		t.Fatalf("Open: %v", err)
	}
	return l
}

func putName(ctx context.Context, l *Live, f *fixture, name string) error {
	return l.Update(ctx, func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: name}) })
}

func nameOf(ctx context.Context, l *Live, f *fixture) (string, error) {
	var name string
	err := l.View(ctx, func(r *ReadTx) error {
		v, _, err := f.items.Get(r, 1)
		name = v.Name
		return err
	})
	return name, err
}

func assertName(t *testing.T, l *Live, f *fixture, want string) {
	t.Helper()
	got, err := nameOf(t.Context(), l, f)
	if err != nil || got != want {
		t.Errorf("row 1 = %q, %v; want %q", got, err, want)
	}
}

// storedWith is a closed file whose row 1 is named name.
func storedWith(t *testing.T, f *fixture, name string) string {
	t.Helper()
	path := tempPath(t)
	db, err := Open(path, f.schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: name}) })
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// moveIn is a rename that puts the file at from at path.
func moveIn(from, path string) func() error {
	return func() error { return os.Rename(from, path) }
}

// liveCalls is every Live call that admits through the handle, by name.
func liveCalls(t *testing.T, l *Live, f *fixture) map[string]func() error {
	t.Helper()
	put := func(tx *Tx) error { return f.items.Put(tx, 2, &item{}) }
	return map[string]func() error{
		"View":   func() error { return l.View(t.Context(), func(*ReadTx) error { return nil }) },
		"Update": func() error { return l.Update(t.Context(), put) },
		"Batch":  func() error { return l.Batch(t.Context(), put) },
		"Backup": func() error { return l.Backup(t.Context(), filepath.Join(t.TempDir(), "copy.bolt")) },
		"Pin":    func() error { _, err := l.Pin(); return err },
		"Space":  func() error { _, err := l.Space(); return err },
	}
}

// answerNow runs call and fails the test when it does not return at once.
func answerNow(t *testing.T, call func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- call() }()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("the call waited")
		return nil
	}
}

// waitRefusing polls until a call on l answers ErrClosed.
func waitRefusing(t *testing.T, l *Live) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := l.View(t.Context(), func(*ReadTx) error { return nil })
		if errors.Is(err, ErrClosed) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the handle still admits calls: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
}

// waitInFlight polls until n calls run through l.
func waitInFlight(t *testing.T, l *Live, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		l.mu.Lock()
		got := l.inflight
		l.mu.Unlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d calls in flight, want %d", got, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// holdView starts a View on l that stays in flight until release is closed,
// and returns once it is running.
func holdView(t *testing.T, view func(context.Context, func(*ReadTx) error) error) (release func(), done <-chan error) {
	t.Helper()
	entered, gate, out := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		out <- view(t.Context(), func(*ReadTx) error {
			close(entered)
			<-gate
			return nil
		})
	}()
	await(t, entered, "the held View to start")
	return sync.OnceFunc(func() { close(gate) }), out
}

func TestLiveSwapAnswersErrClosed(t *testing.T) {
	f := newFixture(t)
	path := tempPath(t)
	// A Batch call waits for its group's delay before bbolt begins its
	// transaction, so only the handle's own wait keeps it from a closed file.
	l := openLive(t, path, f.schema, Options{MaxBatchDelay: 200 * time.Millisecond})
	if err := putName(t.Context(), l, f, "old"); err != nil {
		t.Fatal(err)
	}
	before := l.Epoch()
	next := storedWith(t, f, "new")
	release, held := holdView(t, l.View)
	defer release()
	batched := make(chan error, 1)
	go func() {
		batched <- l.Batch(t.Context(), func(tx *Tx) error { return f.items.Put(tx, 3, &item{}) })
	}()
	waitInFlight(t, l, 2)
	var renames atomic.Int32
	swapped := make(chan error, 1)
	go func() {
		swapped <- l.Swap(t.Context(), func() error { renames.Add(1); return os.Rename(next, path) })
	}()
	waitRefusing(t, l)
	for name, call := range liveCalls(t, l, f) {
		t.Run(name, func(t *testing.T) {
			if err := answerNow(t, call); !errors.Is(err, ErrClosed) {
				t.Errorf("%s during a swap = %v, want ErrClosed", name, err)
			}
		})
	}
	if err := l.Err(); err != nil {
		t.Errorf("Err during a swap = %v, want nil", err)
	}
	if n := renames.Load(); n != 0 {
		t.Errorf("rename ran %d times with a call in flight", n)
	}
	release()
	if err := await(t, held, "the held View"); err != nil {
		t.Errorf("the call in flight = %v, want nil", err)
	}
	if err := await(t, batched, "the Batch in flight"); err != nil {
		t.Errorf("the Batch in flight = %v, want nil", err)
	}
	if err := await(t, swapped, "Swap"); err != nil {
		t.Fatalf("Swap = %v", err)
	}
	if n := renames.Load(); n != 1 {
		t.Errorf("rename ran %d times, want 1", n)
	}
	assertName(t, l, f, "new")
	if after := l.Epoch(); after == before || after == ([16]byte{}) {
		t.Errorf("Epoch after a swap = %x, before %x: want a new non-zero epoch", after, before)
	}
}

func TestEpochHeldThroughSwapWait(t *testing.T) {
	f := newFixture(t)
	path := tempPath(t)
	l := openLive(t, path, f.schema, Options{})
	before := l.Epoch()
	next := storedWith(t, f, "new")
	entered, gate := make(chan struct{}), make(chan struct{})
	inside, held := make(chan [16]byte, 1), make(chan error, 1)
	go func() {
		held <- l.View(t.Context(), func(*ReadTx) error {
			close(entered)
			<-gate
			inside <- l.Epoch()
			return nil
		})
	}()
	await(t, entered, "the held View to start")
	release := sync.OnceFunc(func() { close(gate) })
	defer release()
	swapped := make(chan error, 1)
	go func() {
		swapped <- l.Swap(t.Context(), func() error {
			if e := l.Epoch(); e != ([16]byte{}) {
				t.Errorf("Epoch during the rename, the old DB closed = %x, want zero", e)
			}
			return os.Rename(next, path)
		})
	}()
	waitRefusing(t, l)
	if got := l.Epoch(); got != before {
		t.Errorf("Epoch outside any call while the swap waits = %x, want the open DB's %x", got, before)
	}
	release()
	if got := await(t, inside, "the held View's Epoch"); got != before {
		t.Errorf("Epoch inside a call admitted before the swap = %x, want %x", got, before)
	}
	if err := await(t, held, "the held View"); err != nil {
		t.Errorf("the call in flight = %v", err)
	}
	if err := await(t, swapped, "Swap"); err != nil {
		t.Fatalf("Swap = %v", err)
	}
	if after := l.Epoch(); after == before || after == ([16]byte{}) {
		t.Errorf("Epoch after the swap = %x, before %x: want a new non-zero epoch", after, before)
	}
}

// TestEpochHeldWhileDBCloseWaits pins that a transition keeps the old DB's
// epoch visible until that DB's close returns: the close waits for the
// transactions admitted before it, and they still run on that DB.
func TestEpochHeldWhileDBCloseWaits(t *testing.T) {
	f := newFixture(t)
	t.Run("Close", func(t *testing.T) {
		l := openLive(t, tempPath(t), f.schema, Options{})
		before := l.Epoch()
		entered, gate := make(chan struct{}), make(chan struct{})
		inside, held := make(chan [16]byte, 1), make(chan error, 1)
		go func() {
			held <- l.Update(t.Context(), func(tx *Tx) error {
				close(entered)
				<-gate
				inside <- l.Epoch()
				return f.items.Put(tx, 1, &item{Name: "late"})
			})
		}()
		await(t, entered, "the held Update to start")
		release := sync.OnceFunc(func() { close(gate) })
		defer release()
		closed := make(chan error, 1)
		go func() { closed <- l.Close() }()
		waitRefusing(t, l)
		if got := l.Epoch(); got != before {
			t.Errorf("Epoch while Close waits for a call = %x, want the open DB's %x", got, before)
		}
		release()
		if got := await(t, inside, "the held Update's Epoch"); got != before {
			t.Errorf("Epoch inside a call admitted before Close = %x, want %x", got, before)
		}
		if err := await(t, held, "the held Update"); err != nil {
			t.Errorf("the call in flight = %v, want nil", err)
		}
		if err := await(t, closed, "Close"); err != nil {
			t.Fatalf("Close = %v", err)
		}
		if got := l.Epoch(); got != ([16]byte{}) {
			t.Errorf("Epoch after Close = %x, want zero", got)
		}
	})
	t.Run("Swap", func(t *testing.T) {
		path := tempPath(t)
		l := openLive(t, path, f.schema, Options{})
		before := l.Epoch()
		db, err := l.Pin()
		if err != nil {
			t.Fatalf("Pin = %v", err)
		}
		release, held := holdView(t, db.View)
		defer release()
		next := storedWith(t, f, "new")
		swapped := make(chan error, 1)
		go func() { swapped <- l.Swap(t.Context(), moveIn(next, path)) }()
		waitRefusing(t, l)
		// No call is in flight, so the swap goes straight to the close that
		// the pinned View holds.
		for end := time.Now().Add(50 * time.Millisecond); time.Now().Before(end); time.Sleep(time.Millisecond) {
			if got := l.Epoch(); got != before {
				t.Fatalf("Epoch while the swap's close waits for a pinned transaction = %x, want the open DB's %x", got, before)
			}
		}
		release()
		if err := await(t, held, "the pinned View"); err != nil {
			t.Errorf("the pinned call in flight = %v", err)
		}
		if err := await(t, swapped, "Swap"); err != nil {
			t.Fatalf("Swap = %v", err)
		}
		if after := l.Epoch(); after == before || after == ([16]byte{}) {
			t.Errorf("Epoch after the swap = %x, before %x: want a new non-zero epoch", after, before)
		}
	})
}

// holdCallback starts write with an AfterCommit that blocks until release
// and then reads l's epoch, and returns once the callback runs.
func holdCallback(t *testing.T, f *fixture, l *Live, write func(context.Context, func(*Tx) error) error) (release func(), inside <-chan [16]byte, wrote <-chan error) {
	t.Helper()
	entered, gate := make(chan struct{}), make(chan struct{})
	epoch, out := make(chan [16]byte, 1), make(chan error, 1)
	go func() {
		out <- write(t.Context(), func(tx *Tx) error {
			tx.AfterCommit(func() {
				close(entered)
				<-gate
				epoch <- l.Epoch()
			})
			return f.items.Put(tx, 1, &item{Name: "late"})
		})
	}()
	await(t, entered, "the commit callback to start")
	return sync.OnceFunc(func() { close(gate) }), epoch, out
}

// TestEpochHeldThroughCommitCallbacks pins that a transition waits for the
// commit callbacks of a write admitted on the old DB, which bbolt runs after
// it releases its locks, and that they read that DB's epoch.
func TestEpochHeldThroughCommitCallbacks(t *testing.T) {
	f := newFixture(t)
	t.Run("Close", func(t *testing.T) {
		l := openLive(t, tempPath(t), f.schema, Options{})
		before := l.Epoch()
		release, inside, wrote := holdCallback(t, f, l, l.Update)
		defer release()
		closed := make(chan error, 1)
		go func() { closed <- l.Close() }()
		waitRefusing(t, l)
		select {
		case err := <-closed:
			t.Errorf("Close returned (%v) while a commit callback of a call admitted before it runs", err)
			closed <- err
		case <-time.After(50 * time.Millisecond):
		}
		release()
		if got := await(t, inside, "the callback's Epoch"); got != before {
			t.Errorf("Epoch inside a commit callback while Close waits = %x, want the open DB's %x", got, before)
		}
		if err := await(t, wrote, "the held Update"); err != nil {
			t.Errorf("the call in flight = %v, want nil", err)
		}
		if err := await(t, closed, "Close"); err != nil {
			t.Fatalf("Close = %v", err)
		}
		if got := l.Epoch(); got != ([16]byte{}) {
			t.Errorf("Epoch after Close = %x, want zero", got)
		}
	})
	t.Run("Swap", func(t *testing.T) {
		path := tempPath(t)
		l := openLive(t, path, f.schema, Options{})
		before := l.Epoch()
		db, err := l.Pin()
		if err != nil {
			t.Fatalf("Pin = %v", err)
		}
		release, inside, wrote := holdCallback(t, f, l, db.Update)
		defer release()
		next := storedWith(t, f, "new")
		renamed, swapped := make(chan struct{}), make(chan error, 1)
		go func() {
			swapped <- l.Swap(t.Context(), func() error {
				close(renamed)
				return os.Rename(next, path)
			})
		}()
		waitRefusing(t, l)
		select {
		case <-renamed:
			t.Error("Swap ran its rename while a pinned commit's callback runs")
		case <-time.After(50 * time.Millisecond):
		}
		release()
		if got := await(t, inside, "the callback's Epoch"); got != before {
			t.Errorf("Epoch inside a pinned commit's callback while Swap waits = %x, want the old DB's %x", got, before)
		}
		if err := await(t, wrote, "the pinned Update"); err != nil {
			t.Errorf("the pinned write = %v, want nil", err)
		}
		if err := await(t, swapped, "Swap"); err != nil {
			t.Fatalf("Swap = %v", err)
		}
		if after := l.Epoch(); after == before || after == ([16]byte{}) {
			t.Errorf("Epoch after the swap = %x, before %x: want a new non-zero epoch", after, before)
		}
		assertName(t, l, f, "new")
	})
}

// TestTransitionsWaitForOnOpen pins that Close and Swap wait for the open an
// OnOpen hook runs in.
func TestTransitionsWaitForOnOpen(t *testing.T) {
	f := newFixture(t)
	transitions := map[string]func(l *Live, path string) error{
		"Close": func(l *Live, _ string) error { return l.Close() },
		"Swap":  func(l *Live, path string) error { return l.Swap(t.Context(), moveIn(storedWith(t, f, "new"), path)) },
	}
	for name, transition := range transitions {
		t.Run(name, func(t *testing.T) {
			path := tempPath(t)
			l := newLive(t, path, f.schema, Options{})
			entered, gate := make(chan struct{}), make(chan struct{})
			var once sync.Once
			l.OnOpen(func(*DB) error {
				once.Do(func() {
					close(entered)
					<-gate
				})
				return nil
			})
			opened := make(chan error, 1)
			go func() { opened <- l.Open() }()
			await(t, entered, "the OnOpen hook to start")
			release := sync.OnceFunc(func() { close(gate) })
			defer release()
			done := make(chan error, 1)
			go func() { done <- transition(l, path) }()
			select {
			case err := <-done:
				t.Errorf("%s returned (%v) while an OnOpen hook runs", name, err)
				done <- err
			case <-time.After(50 * time.Millisecond):
			}
			release()
			if err := await(t, opened, "Open"); err != nil {
				t.Errorf("Open = %v", err)
			}
			if err := await(t, done, name); err != nil {
				t.Errorf("%s = %v", name, err)
			}
		})
	}
}

func TestLiveBindsItsLock(t *testing.T) {
	f := newFixture(t)
	t.Run("held through the rename", func(t *testing.T) {
		path := tempPath(t)
		lk := mustLock(t, path)
		l := openLive(t, path, f.schema, Options{Lock: lk})
		next := storedWith(t, f, "new")
		if err := l.Swap(t.Context(), func() error {
			if err := lk.Release(); err == nil {
				t.Error("Release of the handle's lock during the rename = nil")
			}
			assertLocked(t, path)
			return os.Rename(next, path)
		}); err != nil {
			t.Fatalf("Swap = %v", err)
		}
		assertName(t, l, f, "new")
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		if err := lk.Release(); err != nil {
			t.Errorf("Release of the adopted lock after Close = %v", err)
		}
	})
	t.Run("one handle per lock", func(t *testing.T) {
		path := tempPath(t)
		lk := mustLock(t, path)
		l := newLive(t, path, f.schema, Options{Lock: lk})
		if second, err := NewLive(path, f.schema, Options{Lock: lk}); !errors.Is(err, ErrLocked) {
			if second != nil {
				_ = second.Close()
			}
			t.Errorf("NewLive on a lock another handle holds = %v, want ErrLocked", err)
		}
		if db, err := Open(path, f.schema, Options{Lock: lk}); !errors.Is(err, ErrLocked) {
			if db != nil {
				_ = db.Close()
			}
			t.Errorf("Open on a lock a handle holds = %v, want ErrLocked", err)
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		again := newLive(t, path, f.schema, Options{Lock: lk})
		if err := again.Open(); err != nil {
			t.Errorf("Open of a handle on the lock after the first one closed = %v", err)
		}
	})
}

func TestInspectPanicLeavesHandleClosed(t *testing.T) {
	f := newFixture(t)
	// panicky's Inspect panics on its nth run, counting from 1.
	panicky := func(n int32) Options {
		var runs atomic.Int32
		return Options{Inspect: func(*ReadTx) error {
			if runs.Add(1) == n {
				panic(testPanic("inspect"))
			}
			return nil
		}}
	}
	assertPanicked := func(t *testing.T, l *Live, v any) {
		t.Helper()
		if v != testPanic("inspect") {
			t.Errorf("recovered %v, want Inspect's panic", v)
		}
		if err := l.Err(); !errors.Is(err, errPanicked) {
			t.Errorf("Err after a panicking open = %v, want errPanicked", err)
		}
		if err := answerNow(t, func() error { return putName(t.Context(), l, f, "x") }); !errors.Is(err, ErrClosed) || !errors.Is(err, errPanicked) {
			t.Errorf("Update after a panicking open = %v, want ErrClosed wrapping errPanicked", err)
		}
	}
	t.Run("Open", func(t *testing.T) {
		l := newLive(t, storedWith(t, f, "old"), f.schema, panicky(1))
		v := func() (v any) {
			defer func() { v = recover() }()
			_ = l.Open()
			return nil
		}()
		assertPanicked(t, l, v)
		if err := l.Open(); err != nil {
			t.Fatalf("Open after the panic = %v", err)
		}
		assertName(t, l, f, "old")
	})
	t.Run("Swap", func(t *testing.T) {
		l := openLive(t, storedWith(t, f, "old"), f.schema, panicky(2))
		v := func() (v any) {
			defer func() { v = recover() }()
			_ = l.Swap(t.Context(), func() error { return nil })
			return nil
		}()
		assertPanicked(t, l, v)
		if err := l.Swap(t.Context(), func() error { return nil }); err != nil {
			t.Fatalf("Swap after the panic = %v", err)
		}
		assertName(t, l, f, "old")
	})
}

func TestPinnedDBClosedAfterSwap(t *testing.T) {
	f := newFixture(t)
	path := tempPath(t)
	l := openLive(t, path, f.schema, Options{})
	db, err := l.Pin()
	if err != nil || db == nil {
		t.Fatalf("Pin = %v, %v", db, err)
	}
	if db.Epoch() != l.Epoch() {
		t.Errorf("pinned epoch %x, handle epoch %x", db.Epoch(), l.Epoch())
	}
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "old"}) })
	// A pinned DB's transaction in flight holds the swap's close.
	release, held := holdView(t, db.View)
	defer release()
	next := storedWith(t, f, "new")
	var renames atomic.Int32
	swapped := make(chan error, 1)
	go func() {
		swapped <- l.Swap(t.Context(), func() error { renames.Add(1); return os.Rename(next, path) })
	}()
	waitRefusing(t, l)
	time.Sleep(20 * time.Millisecond) // give a close that does not wait the time to run
	if n := renames.Load(); n != 0 {
		t.Errorf("rename ran with a pinned transaction in flight")
	}
	release()
	if err := await(t, held, "the pinned View"); err != nil {
		t.Errorf("the pinned call in flight = %v", err)
	}
	if err := await(t, swapped, "Swap"); err != nil {
		t.Fatalf("Swap = %v", err)
	}
	for name, call := range failingCalls(t, db, f) {
		t.Run(name, func(t *testing.T) { assertClosedError(t, call()) })
	}
	assertName(t, l, f, "new")
	again, err := l.Pin()
	if err != nil || again == db {
		t.Errorf("Pin after a swap = %p, %v; want the new DB, not %p", again, err, db)
	}
}

func TestOnOpenRunsBeforeCalls(t *testing.T) {
	f := newFixture(t)
	path := tempPath(t)
	l := newLive(t, path, f.schema, Options{})
	var order []string
	l.OnOpen(func(db *DB) error {
		order = append(order, "first")
		if err := l.View(t.Context(), func(*ReadTx) error { return nil }); !errors.Is(err, ErrClosed) {
			return fmt.Errorf("a handle call inside a hook = %v, want ErrClosed", err)
		}
		if _, err := l.Pin(); !errors.Is(err, ErrClosed) {
			return fmt.Errorf("Pin inside a hook = %v, want ErrClosed", err)
		}
		if _, err := l.Space(); !errors.Is(err, ErrClosed) {
			return fmt.Errorf("Space inside a hook = %v, want ErrClosed", err)
		}
		return db.Update(t.Context(), func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "hook"}) })
	})
	l.OnOpen(func(*DB) error {
		order = append(order, "second")
		return nil
	})
	if err := l.Open(); err != nil {
		t.Fatalf("Open = %v", err)
	}
	if want := []string{"first", "second"}; !slices.Equal(order, want) {
		t.Errorf("hooks at the first open ran %v, want %v", order, want)
	}
	assertName(t, l, f, "hook")
	if err := l.Swap(t.Context(), moveIn(storedWith(t, f, "new"), path)); err != nil {
		t.Fatalf("Swap = %v", err)
	}
	if want := []string{"first", "second", "first", "second"}; !slices.Equal(order, want) {
		t.Errorf("hooks after a swap ran %v, want %v", order, want)
	}
	assertName(t, l, f, "hook")
}

func TestSwapOnClosedHandleOpens(t *testing.T) {
	f := newFixture(t)
	t.Run("never opened", func(t *testing.T) {
		path := tempPath(t)
		l := newLive(t, path, f.schema, Options{})
		if err := l.Err(); err == nil {
			t.Error("Err of a handle never opened = nil")
		}
		if err := putName(t.Context(), l, f, "x"); !errors.Is(err, ErrClosed) {
			t.Errorf("Update before Open = %v, want ErrClosed", err)
		}
		if err := l.Swap(t.Context(), moveIn(storedWith(t, f, "new"), path)); err != nil {
			t.Fatalf("Swap = %v", err)
		}
		if err := l.Err(); err != nil {
			t.Errorf("Err after the swap = %v", err)
		}
		assertName(t, l, f, "new")
	})
	t.Run("failed open", func(t *testing.T) {
		path := tempPath(t)
		if err := os.WriteFile(path, make([]byte, 64<<10), 0o600); err != nil {
			t.Fatal(err)
		}
		l := newLive(t, path, f.schema, Options{})
		if err := l.Open(); !errors.Is(err, ErrInvalidFile) {
			t.Fatalf("Open of a zeroed file = %v, want ErrInvalidFile", err)
		}
		if err := l.Err(); !errors.Is(err, ErrInvalidFile) {
			t.Errorf("Err = %v, want the open's ErrInvalidFile", err)
		}
		err := putName(t.Context(), l, f, "x")
		if !errors.Is(err, ErrClosed) || !errors.Is(err, ErrInvalidFile) {
			t.Errorf("Update on the closed handle = %v, want ErrClosed wrapping the open's error", err)
		}
		if err := l.Swap(t.Context(), moveIn(storedWith(t, f, "repaired"), path)); err != nil {
			t.Fatalf("Swap = %v", err)
		}
		assertName(t, l, f, "repaired")
	})
}

func TestSwapKeepsAdoptedLock(t *testing.T) {
	f := newFixture(t)
	t.Run("adopted", func(t *testing.T) {
		path := tempPath(t)
		lk := mustLock(t, path)
		l := openLive(t, path, f.schema, Options{Lock: lk})
		next := storedWith(t, f, "new")
		if err := l.Swap(t.Context(), func() error {
			assertLocked(t, path)
			return os.Rename(next, path)
		}); err != nil {
			t.Fatalf("Swap = %v", err)
		}
		assertName(t, l, f, "new")
		assertLocked(t, path)
		// The reopened DB runs under the adopted lock, which Release refuses.
		if err := lk.Release(); err == nil {
			t.Fatal("Release of the adopted lock while the swapped-in DB runs under it = nil")
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		assertLocked(t, path)
		if err := lk.Release(); err != nil {
			t.Errorf("Release of the adopted lock after Close = %v", err)
		}
	})
	t.Run("taken", func(t *testing.T) {
		path := tempPath(t)
		l := newLive(t, path, f.schema, Options{})
		assertLocked(t, path)
		if err := l.Open(); err != nil {
			t.Fatal(err)
		}
		next := storedWith(t, f, "new")
		if err := l.Swap(t.Context(), func() error {
			assertLocked(t, path)
			return os.Rename(next, path)
		}); err != nil {
			t.Fatalf("Swap = %v", err)
		}
		assertLocked(t, path)
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		lk, err := LockPath(path, 0)
		if err != nil {
			t.Fatalf("LockPath after Close = %v, want the lock released", err)
		}
		_ = lk.Release()
	})
}

func TestSwapContextEndKeepsHandleOpen(t *testing.T) {
	f := newFixture(t)
	path := tempPath(t)
	l := openLive(t, path, f.schema, Options{})
	if err := putName(t.Context(), l, f, "old"); err != nil {
		t.Fatal(err)
	}
	epoch := l.Epoch()
	release, held := holdView(t, l.View)
	defer release()
	ctx, cancel := context.WithCancel(t.Context())
	swapped := make(chan error, 1)
	renamed := false
	go func() { swapped <- l.Swap(ctx, func() error { renamed = true; return nil }) }()
	waitRefusing(t, l)
	cancel()
	if err := await(t, swapped, "Swap"); !errors.Is(err, context.Canceled) {
		t.Errorf("Swap whose context ended = %v, want context.Canceled", err)
	}
	if renamed {
		t.Error("rename ran after the context ended")
	}
	assertName(t, l, f, "old")
	if l.Epoch() != epoch {
		t.Error("the handle reopened after a swap that gave up")
	}
	release()
	if err := await(t, held, "the held View"); err != nil {
		t.Errorf("the call in flight = %v", err)
	}
}

func TestSwapRenameFailureLeavesHandleClosed(t *testing.T) {
	f := newFixture(t)
	path := tempPath(t)
	l := openLive(t, path, f.schema, Options{})
	if err := putName(t.Context(), l, f, "old"); err != nil {
		t.Fatal(err)
	}
	if err := l.Swap(t.Context(), func() error { return errBoom }); !errors.Is(err, errBoom) {
		t.Fatalf("Swap with a failing rename = %v, want errBoom", err)
	}
	if err := l.Err(); !errors.Is(err, errBoom) {
		t.Errorf("Err = %v, want errBoom", err)
	}
	for name, call := range liveCalls(t, l, f) {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, ErrClosed) || !errors.Is(err, errBoom) {
				t.Errorf("%s = %v, want ErrClosed wrapping errBoom", name, err)
			}
		})
	}
	if e := l.Epoch(); e != ([16]byte{}) {
		t.Errorf("Epoch of a closed handle = %x, want zero", e)
	}
	if err := l.Swap(t.Context(), func() error { return nil }); err != nil {
		t.Fatalf("a second Swap = %v", err)
	}
	assertName(t, l, f, "old")
}

func TestOnOpenErrorClosesHandle(t *testing.T) {
	f := newFixture(t)
	t.Run("at Open", func(t *testing.T) {
		l := newLive(t, tempPath(t), f.schema, Options{})
		var got *DB
		l.OnOpen(func(db *DB) error { got = db; return errBoom })
		if err := l.Open(); !errors.Is(err, errBoom) {
			t.Fatalf("Open = %v, want the hook's error", err)
		}
		if err := l.Err(); !errors.Is(err, errBoom) {
			t.Errorf("Err = %v, want the hook's error", err)
		}
		if err := putName(t.Context(), l, f, "x"); !errors.Is(err, ErrClosed) || !errors.Is(err, errBoom) {
			t.Errorf("Update = %v, want ErrClosed wrapping the hook's error", err)
		}
		if got == nil {
			t.Fatal("the hook never ran")
		}
		assertClosedError(t, got.View(t.Context(), func(*ReadTx) error { return nil }))
	})
	t.Run("at a swap", func(t *testing.T) {
		path := tempPath(t)
		l := newLive(t, path, f.schema, Options{})
		var opens atomic.Int32
		l.OnOpen(func(*DB) error {
			if opens.Add(1) > 1 {
				return errBoom
			}
			return nil
		})
		if err := l.Open(); err != nil {
			t.Fatal(err)
		}
		if err := l.Swap(t.Context(), func() error { return nil }); !errors.Is(err, errBoom) {
			t.Fatalf("Swap = %v, want the hook's error", err)
		}
		if err := l.Err(); !errors.Is(err, errBoom) {
			t.Errorf("Err = %v, want the hook's error", err)
		}
		if _, err := l.Pin(); !errors.Is(err, ErrClosed) {
			t.Errorf("Pin = %v, want ErrClosed", err)
		}
	})
}

func TestLiveCloseIsFinal(t *testing.T) {
	f := newFixture(t)
	l := openLive(t, tempPath(t), f.schema, Options{})
	db, err := l.Pin()
	if err != nil || db == nil {
		t.Fatalf("Pin = %v, %v", db, err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	assertClosedError(t, db.View(t.Context(), func(*ReadTx) error { return nil }))
	if err := l.Err(); !errors.Is(err, ErrClosed) {
		t.Errorf("Err after Close = %v, want ErrClosed", err)
	}
	if err := l.Open(); !errors.Is(err, ErrClosed) {
		t.Errorf("Open after Close = %v, want ErrClosed", err)
	}
	renamed := false
	if err := l.Swap(t.Context(), func() error { renamed = true; return nil }); !errors.Is(err, ErrClosed) {
		t.Errorf("Swap after Close = %v, want ErrClosed", err)
	}
	if renamed {
		t.Error("Swap after Close ran rename")
	}
	for name, call := range liveCalls(t, l, f) {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, ErrClosed) {
				t.Errorf("%s after Close = %v, want ErrClosed", name, err)
			}
		})
	}
	if err := l.Close(); err != nil {
		t.Errorf("a second Close = %v, want nil", err)
	}
}

func TestOnOpenAfterOpenPanics(t *testing.T) {
	f := newFixture(t)
	l := openLive(t, tempPath(t), f.schema, Options{})
	defer func() {
		if recover() == nil {
			t.Error("OnOpen after Open did not panic")
		}
	}()
	l.OnOpen(func(*DB) error { return nil })
}

func TestOpenOfOpenLiveRefused(t *testing.T) {
	f := newFixture(t)
	l := openLive(t, tempPath(t), f.schema, Options{})
	epoch := l.Epoch()
	if err := l.Open(); err == nil {
		t.Error("a second Open = nil, want an error")
	}
	if l.Epoch() != epoch {
		t.Error("a second Open reopened the file")
	}
}

func TestSwapRefusesNilRename(t *testing.T) {
	f := newFixture(t)
	l := openLive(t, tempPath(t), f.schema, Options{})
	epoch := l.Epoch()
	if err := l.Swap(t.Context(), nil); err == nil {
		t.Error("Swap with a nil rename = nil, want an error")
	}
	if l.Epoch() != epoch || l.Err() != nil {
		t.Error("Swap with a nil rename changed the handle")
	}
}

func TestNewLiveChecksOptions(t *testing.T) {
	f := newFixture(t)
	path := tempPath(t)
	other := mustLock(t, tempPath(t))
	released, err := LockPath(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := released.Release(); err != nil {
		t.Fatal(err)
	}
	cases := map[string]struct {
		s *Schema
		o Options
	}{
		"nil schema":         {nil, Options{}},
		"negative retention": {f.schema, Options{Retention: -1}},
		"negative batch":     {f.schema, Options{MaxBatchSize: -1}},
		"lock and wait":      {f.schema, Options{Lock: other, LockWait: time.Second}},
		"another path lock":  {f.schema, Options{Lock: other}},
		"released lock":      {f.schema, Options{Lock: released}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if l, err := NewLive(path, c.s, c.o); err == nil {
				_ = l.Close()
				t.Error("NewLive = nil error")
			}
		})
	}
	t.Run("opens nothing", func(t *testing.T) {
		newLive(t, path, f.schema, Options{})
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("stat after NewLive = %v, want no file", err)
		}
	})
	t.Run("lock held", func(t *testing.T) {
		p := tempPath(t)
		holder := mustLock(t, p)
		if _, err := NewLive(p, f.schema, Options{}); !errors.Is(err, ErrLocked) {
			t.Errorf("NewLive on a held path = %v, want ErrLocked", err)
		}
		_ = holder
	})
}

func TestSwapReopensWithSameOptions(t *testing.T) {
	f := newFixture(t)
	path := tempPath(t)
	var rec recorder
	l := openLive(t, path, f.schema, Options{OnCommit: rec.on, Retention: 8})
	if err := l.Swap(t.Context(), moveIn(storedWith(t, f, "new"), path)); err != nil {
		t.Fatal(err)
	}
	if err := putName(t.Context(), l, f, "after"); err != nil {
		t.Fatal(err)
	}
	if got := rec.all(); len(got) != 1 || !hasSubject(got[0], f.items.Subject(1)) {
		t.Errorf("OnCommit after a swap received %v, want the one write", got)
	}
	db, err := l.Pin()
	if err != nil || db == nil {
		t.Fatalf("Pin = %v, %v", db, err)
	}
	if db.retention != 8 {
		t.Errorf("Retention after a swap = %d, want 8", db.retention)
	}
}

func TestLiveBackup(t *testing.T) {
	f := newFixture(t)
	l := openLive(t, tempPath(t), f.schema, Options{})
	if err := putName(t.Context(), l, f, "kept"); err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(t.TempDir(), "copy.bolt")
	if err := l.Backup(t.Context(), copyPath); err != nil {
		t.Fatalf("Backup = %v", err)
	}
	cp := openLive(t, copyPath, f.schema, Options{})
	assertName(t, cp, f, "kept")
}

func assertLiveSpace(t *testing.T, l *Live, path string) Space {
	t.Helper()
	sp, err := l.Space()
	if err != nil {
		t.Fatalf("Space = %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if sp.FileBytes != fi.Size() {
		t.Errorf("Space().FileBytes = %d, want the file's %d bytes", sp.FileBytes, fi.Size())
	}
	db, err := l.Pin()
	if err != nil {
		t.Fatalf("Pin = %v", err)
	}
	want, err := db.Space()
	if err != nil || sp != want {
		t.Errorf("Space() = %+v, the open DB's Space = %+v, %v", sp, want, err)
	}
	return sp
}

func TestLiveSpace(t *testing.T) {
	f := newFixture(t)
	path := tempPath(t)
	l := openLive(t, path, f.schema, Options{})
	before := assertLiveSpace(t, l, path)
	next := tempPath(t)
	db := mustOpen(t, next, f.schema, Options{})
	fillAndThin(t, db, f)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Swap(t.Context(), moveIn(next, path)); err != nil {
		t.Fatalf("Swap = %v", err)
	}
	if after := assertLiveSpace(t, l, path); after.InUseBytes <= before.InUseBytes {
		t.Errorf("Space after swapping in a fuller file = %+v, before %+v: want the new file's", after, before)
	}
}

// Calls racing repeated swaps either run on an open DB or answer ErrClosed.
func TestLiveSwapConcurrent(t *testing.T) {
	f := newFixture(t)
	l := openLive(t, tempPath(t), f.schema, Options{})
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var ran atomic.Int64
	for i := range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				var err error
				if i%2 == 0 {
					err = putName(t.Context(), l, f, "w")
				} else {
					_, err = nameOf(t.Context(), l, f)
				}
				switch {
				case err == nil:
					ran.Add(1)
				case !errors.Is(err, ErrClosed):
					t.Errorf("call during swaps = %v, want nil or ErrClosed", err)
					return
				}
			}
		})
	}
	deadline := time.Now().Add(5 * time.Second)
	swaps := 0
	for ; (swaps < 10 || ran.Load() == 0) && time.Now().Before(deadline); swaps++ {
		if err := l.Swap(t.Context(), func() error { return nil }); err != nil {
			t.Errorf("Swap = %v", err)
		}
	}
	close(stop)
	wg.Wait()
	if ran.Load() == 0 {
		t.Errorf("no call ran during %d swaps", swaps)
	}
}

func TestRefuseSetsErrOnClosedHandle(t *testing.T) {
	f := newFixture(t)
	t.Run("until Swap", func(t *testing.T) {
		path := tempPath(t)
		l := newLive(t, path, f.schema, Options{})
		l.Refuse(errBoom)
		if err := l.Err(); !errors.Is(err, errBoom) {
			t.Errorf("Err after Refuse = %v, want errBoom", err)
		}
		for name, call := range liveCalls(t, l, f) {
			t.Run(name, func(t *testing.T) {
				if err := call(); !errors.Is(err, ErrClosed) || !errors.Is(err, errBoom) {
					t.Errorf("%s = %v, want ErrClosed wrapping errBoom", name, err)
				}
			})
		}
		if err := l.Swap(t.Context(), moveIn(storedWith(t, f, "new"), path)); err != nil {
			t.Fatalf("Swap = %v", err)
		}
		if err := l.Err(); err != nil {
			t.Errorf("Err after the swap = %v, want nil", err)
		}
		assertName(t, l, f, "new")
	})
	t.Run("until Open", func(t *testing.T) {
		l := newLive(t, tempPath(t), f.schema, Options{})
		l.Refuse(errBoom)
		if err := l.Open(); err != nil {
			t.Fatalf("Open = %v", err)
		}
		if err := l.Err(); err != nil {
			t.Errorf("Err after Open = %v, want nil", err)
		}
	})
	t.Run("after Close", func(t *testing.T) {
		l := newLive(t, tempPath(t), f.schema, Options{})
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		l.Refuse(errBoom)
		if err := l.Err(); !errors.Is(err, ErrClosed) || errors.Is(err, errBoom) {
			t.Errorf("Err after Close and Refuse = %v, want ErrClosed alone", err)
		}
	})
}

func TestRefuseOnOpenHandleDoesNothing(t *testing.T) {
	f := newFixture(t)
	l := openLive(t, tempPath(t), f.schema, Options{})
	epoch := l.Epoch()
	l.Refuse(errBoom)
	if err := l.Err(); err != nil {
		t.Errorf("Err after Refuse on an open handle = %v, want nil", err)
	}
	if err := putName(t.Context(), l, f, "still"); err != nil {
		t.Errorf("Update after Refuse on an open handle = %v", err)
	}
	assertName(t, l, f, "still")
	if l.Epoch() != epoch {
		t.Error("Refuse on an open handle reopened it")
	}
}
