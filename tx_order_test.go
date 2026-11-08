package bolttable

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bbolterrors "go.etcd.io/bbolt/errors"
)

// events is a goroutine-safe log of callback marks.
type events struct {
	got []string
	mu  sync.Mutex
}

func (e *events) add(s string) {
	e.mu.Lock()
	e.got = append(e.got, s)
	e.mu.Unlock()
}

func (e *events) all() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.got...)
}

func setBeforeDeliver(t *testing.T, fn func(txid uint64)) {
	t.Helper()
	beforeDeliver = fn
	t.Cleanup(func() { beforeDeliver = nil })
}

func setMetaWithoutHandler(t *testing.T, fn func(txid uint64) bool) {
	t.Helper()
	metaWithoutHandler = fn
	t.Cleanup(func() { metaWithoutHandler = nil })
}

// within runs fn on another goroutine and fails the test when it has not
// returned by d, or returned an error.
func within(t *testing.T, d time.Duration, what string, fn func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	case <-timer.C:
		t.Fatalf("%s did not return within %v", what, d)
	}
}

func wait(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	within(t, 10*time.Second, what, func() error { <-ch; return nil })
}

// nextTxID is the bbolt transaction id the next write will take.
func nextTxID(t *testing.T, db *DB) uint64 {
	t.Helper()
	var id uint64
	view(t, db, func(r *ReadTx) error { id = txID(r.btx) + 1; return nil })
	return id
}

// markedPut commits one row and an AfterCommit that logs name; a non-silent
// write also marks its change with name.
func markedPut(ctx context.Context, db *DB, f *fixture, key uint64, name string, silent bool, log *events) error {
	return db.Update(ctx, func(tx *Tx) error {
		tx.AfterCommit(func() { log.add(name) })
		if silent {
			return f.quiet.Put(tx, key, &item{Name: name})
		}
		tx.Mark(name)
		return f.items.Put(tx, key, &item{Name: name})
	})
}

// heldPair is two commits, A then B, whose deliveries overlap: A's is held at
// beforeDeliver until B's has begun, then a moment longer, so a build that
// lets B's callbacks overtake A's has the time to.
type heldPair struct {
	log            events
	revs           recorder
	aSeen, bSeen   bool
	bReturnedEarly bool
}

func runHeldPair(t *testing.T, silent bool) *heldPair {
	t.Helper()
	f := newFixture(t)
	p := &heldPair{}
	db := mustOpen(t, tempPath(t), f.schema, Options{OnCommit: p.revs.on, Retention: 10})
	held, release, bEntered := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	setBeforeDeliver(t, func(uint64) {
		switch calls.Add(1) {
		case 1:
			close(held)
			<-release
		case 2:
			close(bEntered)
		}
	})
	aDone, bDone := make(chan error, 1), make(chan error, 1)
	go func() {
		err := markedPut(t.Context(), db, f, 1, "A", silent, &p.log)
		p.aSeen = slices.Contains(p.log.all(), "A")
		aDone <- err
	}()
	wait(t, held, "A's delivery")
	go func() {
		err := markedPut(t.Context(), db, f, 2, "B", silent, &p.log)
		p.bSeen = slices.Contains(p.log.all(), "B")
		bDone <- err
	}()
	wait(t, bEntered, "B's delivery")
	select {
	case err := <-bDone:
		p.bReturnedEarly = true
		bDone <- err
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	within(t, 10*time.Second, "A's Update", func() error { return <-aDone })
	within(t, 10*time.Second, "B's Update", func() error { return <-bDone })
	return p
}

func TestCallbacksRunInCommitOrder(t *testing.T) {
	p := runHeldPair(t, false)
	if got := p.log.all(); fmt.Sprint(got) != "[A B]" {
		t.Errorf("AfterCommit order = %v, want [A B]", got)
	}
	revs := p.revs.all()
	if len(revs) != 2 || fmt.Sprint(revs[0].Rev, revs[0].Marks, revs[1].Rev, revs[1].Marks) != "1 [A] 2 [B]" {
		t.Errorf("OnCommit saw %+v, want A's revision 1 then B's 2", revs)
	}
}

func TestSilentCommitCallbacksAreOrdered(t *testing.T) {
	p := runHeldPair(t, true)
	if got := p.log.all(); fmt.Sprint(got) != "[A B]" {
		t.Errorf("AfterCommit order = %v, want [A B]", got)
	}
	if n := len(p.revs.all()); n != 0 {
		t.Errorf("silent writes delivered %d changes", n)
	}
}

func TestUpdateReturnsAfterItsCallbacks(t *testing.T) {
	p := runHeldPair(t, false)
	if !p.aSeen || !p.bSeen {
		t.Errorf("at return: A's callback ran %v, B's %v; want both true", p.aSeen, p.bSeen)
	}
	if p.bReturnedEarly {
		t.Error("B's Update returned while A's delivery was held")
	}
}

func TestEmptyCommitKeepsSequence(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	var log events
	within(t, 10*time.Second, "three writes", func() error {
		if err := markedPut(t.Context(), db, f, 1, "1", false, &log); err != nil {
			return err
		}
		if err := db.Update(t.Context(), func(*Tx) error { return nil }); err != nil {
			return err
		}
		return markedPut(t.Context(), db, f, 3, "3", false, &log)
	})
	if got := log.all(); fmt.Sprint(got) != "[1 3]" {
		t.Errorf("callbacks = %v, want [1 3]", got)
	}
}

func TestVerifyRepairKeepsSequence(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	var log events
	within(t, 10*time.Second, "write, repair, write", func() error {
		if err := markedPut(t.Context(), db, f, 1, "1", false, &log); err != nil {
			return err
		}
		if _, err := db.Verify(t.Context(), true); err != nil {
			return err
		}
		return markedPut(t.Context(), db, f, 2, "2", false, &log)
	})
	if got := log.all(); fmt.Sprint(got) != "[1 2]" {
		t.Errorf("callbacks = %v, want [1 2]", got)
	}
}

func TestPanickingCallbackDoesNotWedge(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	func() {
		defer func() {
			if r := recover(); r != "callback" {
				t.Errorf("recovered %v, want the callback's panic", r)
			}
		}()
		_ = db.Update(t.Context(), func(tx *Tx) error {
			tx.AfterCommit(func() { panic("callback") })
			return f.items.Put(tx, 1, &item{})
		})
	}()
	var log events
	within(t, 10*time.Second, "the next write", func() error { return markedPut(t.Context(), db, f, 2, "2", false, &log) })
	if got := log.all(); fmt.Sprint(got) != "[2]" {
		t.Errorf("callbacks = %v, want [2]", got)
	}
}

func TestCommitAfterSyncFailureDelivers(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	var log events
	if err := markedPut(t.Context(), db, f, 1, "A", false, &log); err != nil {
		t.Fatal(err)
	}
	target := nextTxID(t, db)
	setMetaWithoutHandler(t, func(txid uint64) bool { return txid == target })
	if err := markedPut(t.Context(), db, f, 2, "T", false, &log); !errors.Is(err, ErrCommit) {
		t.Fatalf("T's Update = %v, want ErrCommit", err)
	}
	if got := nextTxID(t, db); got != target+1 {
		t.Fatalf("next txid after T = %d, want %d: T's meta should be in the file", got, target+1)
	}
	within(t, 10*time.Second, "the write after T", func() error { return markedPut(t.Context(), db, f, 3, "C", false, &log) })
	if got := log.all(); fmt.Sprint(got) != "[A C]" {
		t.Errorf("callbacks = %v, want [A C]", got)
	}
}

func TestAbandonedSlotOrders(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	var log events
	if err := markedPut(t.Context(), db, f, 1, "A", false, &log); err != nil {
		t.Fatal(err)
	}
	before := nextTxID(t, db)
	restore := failNextCommit(db)
	err := db.Update(t.Context(), func(tx *Tx) error {
		tx.AfterCommit(func() { log.add("F") })
		return f.other.Put(tx, 2, &item{Name: strings.Repeat("b", 4<<20)})
	})
	restore()
	if !errors.Is(err, ErrCommit) {
		t.Fatalf("failed Update = %v, want ErrCommit", err)
	}
	if got := nextTxID(t, db); got != before {
		t.Fatalf("next txid = %d, want %d reused after a rollback", got, before)
	}
	within(t, 10*time.Second, "the write after the failure", func() error { return markedPut(t.Context(), db, f, 3, "C", false, &log) })
	if got := log.all(); fmt.Sprint(got) != "[A C]" {
		t.Errorf("callbacks = %v, want [A C]", got)
	}
}

// oneGroup makes every Batch group wait for four calls.
func oneGroup(db *DB) {
	db.bolt.MaxBatchSize = 4
	db.bolt.MaxBatchDelay = time.Hour
}

// batch4 runs four concurrent Batch calls of fn and returns their errors.
func batch4(ctx context.Context, db *DB, fn func(i int, tx *Tx) error) []error {
	errs := make([]error, 4)
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() { errs[i] = db.Batch(ctx, func(tx *Tx) error { return fn(i, tx) }) })
	}
	wg.Wait()
	return errs
}

func TestBatchGroupFailureAbandonsOnce(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	oneGroup(db)
	target := nextTxID(t, db)
	setMetaWithoutHandler(t, func(txid uint64) bool { return txid == target })
	var errs []error
	within(t, 10*time.Second, "the failing group", func() error {
		errs = batch4(t.Context(), db, func(i int, tx *Tx) error { return f.items.Put(tx, uint64(i+1), &item{}) })
		return nil
	})
	for i, err := range errs {
		if !errors.Is(err, ErrCommit) {
			t.Errorf("call %d = %v, want ErrCommit", i, err)
		}
	}
	var log events
	within(t, 10*time.Second, "the next write", func() error { return markedPut(t.Context(), db, f, 9, "9", false, &log) })
	if got := log.all(); fmt.Sprint(got) != "[9]" {
		t.Errorf("callbacks = %v, want [9]", got)
	}
}

func TestWriteAfterCloseReturnsError(t *testing.T) {
	f := newFixture(t)
	path := tempPath(t)
	db, err := Open(path, f.schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	var errs []error
	within(t, 10*time.Second, "writes after Close", func() error {
		put := func(tx *Tx) error { return f.items.Put(tx, 1, &item{}) }
		errs = append(errs, db.Update(t.Context(), put))
		errs = append(errs, batch4(t.Context(), db, func(_ int, tx *Tx) error { return put(tx) })...)
		return nil
	})
	for i, err := range errs {
		if !errors.Is(err, ErrCommit) || !errors.Is(err, bbolterrors.ErrDatabaseNotOpen) {
			t.Errorf("write %d = %v, want ErrCommit and ErrDatabaseNotOpen", i, err)
		}
	}
	db2 := mustOpen(t, path, f.schema, Options{})
	var log events
	within(t, 10*time.Second, "writes after reopen", func() error {
		if err := markedPut(t.Context(), db2, f, 1, "1", false, &log); err != nil {
			return err
		}
		return markedPut(t.Context(), db2, f, 2, "2", false, &log)
	})
	if got := log.all(); fmt.Sprint(got) != "[1 2]" {
		t.Errorf("callbacks after reopen = %v, want [1 2]", got)
	}
}

func TestFailedCallWaitsForEarlierCallbacks(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	held, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	setBeforeDeliver(t, func(uint64) { once.Do(func() { close(held); <-release }) })
	order := make(chan string, 2)
	aDone := make(chan error, 1)
	go func() {
		aDone <- db.Update(t.Context(), func(tx *Tx) error {
			tx.AfterCommit(func() { order <- "A" })
			return f.items.Put(tx, 1, &item{})
		})
	}()
	wait(t, held, "A's delivery")
	refused := errors.New("refused")
	failed := make(chan error, 1)
	go func() {
		err := db.Update(t.Context(), func(tx *Tx) error {
			_ = f.items.Put(tx, 2, &item{})
			return refused
		})
		order <- "failed"
		failed <- err
	}()
	select {
	case err := <-failed:
		t.Error("the failed call returned while A's delivery was held")
		failed <- err
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	within(t, 10*time.Second, "A's Update", func() error { return <-aDone })
	within(t, 10*time.Second, "the failed call", func() error {
		if err := <-failed; err != refused {
			return fmt.Errorf("got %v, want the function's error as is", err)
		}
		return nil
	})
	if first, second := <-order, <-order; first != "A" || second != "failed" {
		t.Errorf("order = %s, %s; want A's callback before the failed call's return", first, second)
	}
}

func TestBatchSoloRerunEitherOrder(t *testing.T) {
	f := newFixture(t)
	var revs recorder
	db := mustOpen(t, tempPath(t), f.schema, Options{OnCommit: revs.on, Retention: 1000})
	oneGroup(db)
	for run := range 50 {
		var log events
		var flaky atomic.Int32
		var errs []error
		within(t, 10*time.Second, "a group with a solo rerun", func() error {
			errs = batch4(t.Context(), db, func(i int, tx *Tx) error {
				name := fmt.Sprint(i)
				tx.AfterCommit(func() { log.add(name) })
				if err := f.items.Put(tx, uint64(run*10+i), &item{N: 1}); err != nil {
					return err
				}
				if i == 0 && flaky.Add(1) == 1 {
					return errors.New("first run fails")
				}
				return nil
			})
			return nil
		})
		for i, err := range errs {
			if err != nil {
				t.Fatalf("run %d call %d = %v", run, i, err)
			}
		}
		seen := map[string]int{}
		for _, m := range log.all() {
			seen[m]++
		}
		if fmt.Sprint(seen) != "map[0:1 1:1 2:1 3:1]" {
			t.Fatalf("run %d: callbacks per call %v, want each call once", run, seen)
		}
	}
	all := revs.all()
	if len(all) != 200 {
		t.Fatalf("%d deliveries, want 200", len(all))
	}
	for i, c := range all {
		if c.Rev != uint64(i+1) {
			t.Fatalf("delivery %d has revision %d, want %d", i, c.Rev, i+1)
		}
	}
}

func TestBatchGroupCallbacksInRunOrder(t *testing.T) {
	f := newFixture(t)
	var log events
	db := mustOpen(t, tempPath(t), f.schema, Options{Retention: 10, OnCommit: func(c Change) {
		log.add(fmt.Sprintf("on %d %s", c.Rev, c.Marks[0]))
	}})
	oneGroup(db)
	within(t, 10*time.Second, "one group", func() error {
		return errors.Join(batch4(t.Context(), db, func(i int, tx *Tx) error {
			name := fmt.Sprint("call", i)
			tx.Mark(name)
			tx.AfterCommit(func() { log.add("after " + name) })
			return nil
		})...)
	})
	got := log.all()
	if len(got) != 8 {
		t.Fatalf("events = %v, want 8", got)
	}
	for i := 0; i < 8; i += 2 {
		var rev int
		var name string
		if _, err := fmt.Sscanf(got[i], "on %d %s", &rev, &name); err != nil {
			t.Fatalf("event %d = %q, want an OnCommit", i, got[i])
		}
		if rev != i/2+1 || got[i+1] != "after "+name {
			t.Errorf("events %d and %d = %q, %q; want revision %d then its own AfterCommit", i, i+1, got[i], got[i+1], i/2+1)
		}
	}
}
