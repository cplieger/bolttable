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
)

func assertOrdered(t *testing.T, got []Change, n int) {
	t.Helper()
	if len(got) != n {
		t.Fatalf("OnCommit ran %d times, want %d", len(got), n)
	}
	for i, c := range got {
		if c.Rev != uint64(i+1) {
			t.Fatalf("delivery %d has revision %d, want %d", i, c.Rev, i+1)
		}
	}
}

func TestOnCommitOrder(t *testing.T) {
	for _, mode := range []string{"update", "batch"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			var rec recorder
			db := mustOpen(t, tempPath(t), f.schema, Options{OnCommit: rec.on, Retention: 100})
			write := db.Update
			if mode == "batch" {
				write = db.Batch
			}
			var wg sync.WaitGroup
			for i := range uint64(16) {
				wg.Go(func() {
					if err := write(t.Context(), func(tx *Tx) error { return f.items.Put(tx, i+1, &item{N: 1}) }); err != nil {
						t.Error(err)
					}
				})
			}
			wg.Wait()
			assertOrdered(t, rec.all(), 16)
			view(t, db, func(r *ReadTx) error {
				changes, full, err := r.Since(0)
				if err != nil || full || len(changes) != 16 {
					t.Errorf("Since(0) = %d changes, %v, %v", len(changes), full, err)
				}
				return nil
			})
		})
	}
}

func TestSequencerOrdersOutOfOrderHandlers(t *testing.T) {
	var got []uint64
	s := newSequencer(4, func(c Change) { got = append(got, c.Rev) })
	for _, rev := range []uint64{7, 5, 6, 8} {
		s.publish(Change{Rev: rev})
	}
	if !slices.Equal(got, []uint64{5, 6, 7, 8}) {
		t.Errorf("delivered %v, want 5 to 8 in order", got)
	}
}

func TestSilentTableReservesNothing(t *testing.T) {
	f := newFixture(t)
	var rec recorder
	db := mustOpen(t, tempPath(t), f.schema, Options{OnCommit: rec.on, Retention: 10})
	ran := 0
	update(t, db, func(tx *Tx) error {
		tx.AfterCommit(func() { ran++ })
		if err := f.quiet.Put(tx, 1, &item{Name: "q"}); err != nil {
			return err
		}
		_, err := f.quiet.Delete(tx, 1)
		return err
	})
	if rev := headRev(t, db); rev != 0 || len(rec.all()) != 0 {
		t.Errorf("silent writes reserved revision %d and %d deliveries", rev, len(rec.all()))
	}
	if ran != 1 {
		t.Errorf("AfterCommit ran %d times after a silent write, want 1", ran)
	}
}

func TestNoSubjectNoRevision(t *testing.T) {
	f := newFixture(t)
	var rec recorder
	db := mustOpen(t, tempPath(t), f.schema, Options{OnCommit: rec.on, Retention: 10})
	update(t, db, func(*Tx) error { return nil })
	if rev := headRev(t, db); rev != 0 {
		t.Errorf("an empty Update reserved revision %d", rev)
	}
	update(t, db, func(tx *Tx) error { tx.Mark("m"); tx.Mark("m"); return nil })
	got := rec.all()
	if len(got) != 1 || got[0].Rev != 1 || !slices.Equal(got[0].Marks, []string{"m"}) || len(got[0].Subjects) != 0 {
		t.Errorf("a mark-only Update delivered %+v, want revision 1 with mark m once", got)
	}
}

func TestBeforeCommitSeesPendingAndChoosesSubjects(t *testing.T) {
	f := newFixture(t)
	var rec recorder
	var seen Pending
	db := mustOpen(t, tempPath(t), f.schema, Options{
		OnCommit:  rec.on,
		Retention: 10,
		BeforeCommit: func(tx *Tx, p *Pending) ([]Subject, error) {
			seen = *p
			tx.Mark("added")
			if err := f.quiet.Put(tx, 1, &item{Name: "derived"}); err != nil {
				return nil, err
			}
			return []Subject{Subject("root")}, nil
		},
	})
	update(t, db, func(tx *Tx) error {
		tx.Mark("m")
		return f.items.Put(tx, 1, &item{Name: "x"})
	})
	if seen.Rev != 1 || len(seen.Touched) != 1 || !slices.Equal(seen.Marks, []string{"m"}) {
		t.Errorf("Pending = %+v", seen)
	}
	got := rec.all()
	if len(got) != 1 || !slices.EqualFunc(got[0].Subjects, []Subject{Subject("root")}, slices.Equal) || !slices.Equal(got[0].Marks, []string{"m", "added"}) {
		t.Errorf("delivered %+v, want BeforeCommit's subjects and both marks", got)
	}
	view(t, db, func(r *ReadTx) error {
		if _, found, _ := f.quiet.Get(r, 1); !found {
			t.Error("BeforeCommit's silent write was lost")
		}
		return nil
	})
}

func TestBeforeCommitNonSilentWriteFails(t *testing.T) {
	cases := map[string]func(f *fixture, tx *Tx){
		"put":    func(f *fixture, tx *Tx) { _ = f.items.Put(tx, 2, &item{Name: "late"}) },
		"delete": func(f *fixture, tx *Tx) { _, _ = f.items.Delete(tx, 1) },
		"touch":  func(_ *fixture, tx *Tx) { tx.Touch(Subject("late")) },
	}
	for name, bad := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			path := tempPath(t)
			db := mustOpen(t, path, f.schema, Options{Retention: 10})
			update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "x"}) })
			db.opts.BeforeCommit = func(tx *Tx, p *Pending) ([]Subject, error) {
				bad(f, tx)
				return p.Touched, nil
			}
			err := db.Update(t.Context(), func(tx *Tx) error { return f.items.Put(tx, 3, &item{Name: "y"}) })
			if err == nil || !strings.Contains(err.Error(), "BeforeCommit") {
				t.Fatalf("Update = %v, want a BeforeCommit refusal", err)
			}
			if rev := headRev(t, db); rev != 1 {
				t.Errorf("rev = %d, want 1", rev)
			}
			view(t, db, func(r *ReadTx) error {
				if _, found, _ := f.items.Get(r, 3); found {
					t.Error("the refused transaction's row was stored")
				}
				return nil
			})
		})
	}
}

func TestBeforeCommitErrorRollsBack(t *testing.T) {
	f := newFixture(t)
	boom := errors.New("boom")
	var rec recorder
	db := mustOpen(t, tempPath(t), f.schema, Options{
		OnCommit:     rec.on,
		BeforeCommit: func(*Tx, *Pending) ([]Subject, error) { return nil, boom },
	})
	ran := false
	err := db.Update(t.Context(), func(tx *Tx) error {
		tx.AfterCommit(func() { ran = true })
		return f.items.Put(tx, 1, &item{Name: "x"})
	})
	if !errors.Is(err, boom) || errors.Is(err, ErrCommit) {
		t.Fatalf("Update = %v, want boom as is", err)
	}
	if ran || len(rec.all()) != 0 || headRev(t, db) != 0 {
		t.Error("a BeforeCommit error committed something")
	}
	view(t, db, func(r *ReadTx) error {
		if _, found, _ := f.items.Get(r, 1); found {
			t.Error("the row survived")
		}
		return nil
	})
}

// failNextCommit makes the next write's page allocation fail: bbolt refuses
// to grow the file past MaxSize, in a commit.
func failNextCommit(db *DB) func() {
	db.bolt.MaxSize = 1
	return func() { db.bolt.MaxSize = 0 }
}

func TestCommitFailureWrapsErrCommit(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{Retention: 10})
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "x"}) })
	restore := failNextCommit(db)
	big := strings.Repeat("b", 4<<20)
	err := db.Update(t.Context(), func(tx *Tx) error { return f.other.Put(tx, 2, &item{Name: big}) })
	restore()
	if !errors.Is(err, ErrCommit) {
		t.Fatalf("Update = %v, want ErrCommit", err)
	}
	mine := errors.New("mine")
	if err := db.Update(t.Context(), func(*Tx) error { return mine }); err != mine {
		t.Errorf("fn's error = %v, want it as is", err)
	}
	if rev := headRev(t, db); rev != 1 {
		t.Errorf("rev = %d, want 1", rev)
	}
}

func TestContextDoneBeforeTx(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ran := false
	run := func(*Tx) error { ran = true; return nil }
	for name, call := range map[string]func() error{
		"update": func() error { return db.Update(ctx, run) },
		"batch":  func() error { return db.Batch(ctx, run) },
		"view":   func() error { return db.View(ctx, func(*ReadTx) error { ran = true; return nil }) },
	} {
		if err := call(); !errors.Is(err, context.Canceled) {
			t.Errorf("%s = %v, want context.Canceled", name, err)
		}
	}
	if ran {
		t.Error("a function ran under a done context")
	}
}

func TestAfterCommitRunsOnlyOnCommit(t *testing.T) {
	f := newFixture(t)
	failBC := false
	db := mustOpen(t, tempPath(t), f.schema, Options{BeforeCommit: func(_ *Tx, p *Pending) ([]Subject, error) {
		if failBC {
			return nil, errors.New("refused")
		}
		return p.Touched, nil
	}})
	var runs atomic.Int32
	write := func(id uint64, fail error) error {
		return db.Update(t.Context(), func(tx *Tx) error {
			tx.AfterCommit(func() { runs.Add(1) })
			if err := f.other.Put(tx, id, &item{Name: strings.Repeat("x", int(id%2)<<22)}); err != nil {
				return err
			}
			return fail
		})
	}
	if err := write(1, nil); err != nil || runs.Load() != 1 {
		t.Fatalf("commit: err %v, %d runs; want 1", err, runs.Load())
	}
	if err := write(2, errors.New("no")); err == nil || runs.Load() != 1 {
		t.Errorf("fn error: %d runs, want none more", runs.Load())
	}
	failBC = true
	if err := write(4, nil); err == nil || runs.Load() != 1 {
		t.Errorf("BeforeCommit error: %d runs, want none more", runs.Load())
	}
	failBC = false
	restore := failNextCommit(db)
	err := write(3, nil)
	restore()
	if !errors.Is(err, ErrCommit) || runs.Load() != 1 {
		t.Errorf("ErrCommit (%v): %d runs, want none more", err, runs.Load())
	}
	restore = failNextCommit(db)
	err = db.Update(t.Context(), func(tx *Tx) error {
		tx.AfterCommit(func() { runs.Add(1) })
		return f.quiet.Put(tx, 5, &item{Name: strings.Repeat("q", 4<<20)})
	})
	restore()
	if !errors.Is(err, ErrCommit) || runs.Load() != 1 {
		t.Errorf("silent-only ErrCommit (%v): %d runs, want none more", err, runs.Load())
	}
}

func TestBatchRerunKeepsLastRun(t *testing.T) {
	f := newFixture(t)
	var rec recorder
	db := mustOpen(t, tempPath(t), f.schema, Options{OnCommit: rec.on, Retention: 100})
	// One group of four calls, so a failure discards the others' runs too.
	db.bolt.MaxBatchSize = 4
	db.bolt.MaxBatchDelay = 10 * time.Second
	var flakyRuns, totalRuns, afters atomic.Int32
	failing := errors.New("always fails")
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range 4 {
		wg.Go(func() {
			errs[i] = db.Batch(t.Context(), func(tx *Tx) error {
				totalRuns.Add(1)
				tx.AfterCommit(func() { afters.Add(1) })
				tx.Mark(fmt.Sprint("call", i))
				switch i {
				case 0:
					n := flakyRuns.Add(1)
					if err := f.items.Put(tx, uint64(100+n), &item{N: 1}); err != nil {
						return err
					}
					if n == 1 {
						return errors.New("first run fails")
					}
					return nil
				case 1:
					_ = f.items.Put(tx, 200, &item{N: 1})
					return failing
				default:
					return f.items.Put(tx, uint64(i), &item{N: 1})
				}
			})
		})
	}
	wg.Wait()
	if errs[0] != nil || errs[2] != nil || errs[3] != nil || errs[1] != failing {
		t.Fatalf("errors = %v, want only call 1's, as is", errs)
	}
	if totalRuns.Load() <= 4 {
		t.Errorf("%d runs: nothing was rerun", totalRuns.Load())
	}
	if afters.Load() != 3 {
		t.Errorf("AfterCommit ran %d times, want once per surviving call", afters.Load())
	}
	assertOrdered(t, rec.all(), 3)
	marks := map[string]int{}
	for _, c := range rec.all() {
		for _, m := range c.Marks {
			marks[m]++
		}
	}
	if marks["call0"] != 1 || marks["call1"] != 0 || marks["call2"] != 1 || marks["call3"] != 1 {
		t.Errorf("marks per change = %v, want each surviving call once", marks)
	}
	view(t, db, func(r *ReadTx) error {
		if _, found, _ := f.items.Get(r, 101); found {
			t.Error("the flaky call's first run survived")
		}
		if _, found, _ := f.items.Get(r, uint64(100+flakyRuns.Load())); !found {
			t.Error("the flaky call's last run is missing")
		}
		if _, found, _ := f.items.Get(r, 200); found {
			t.Error("the failing call's write survived")
		}
		if n, _ := f.total.Value(r); n != 3 {
			t.Errorf("total = %d, want 3", n)
		}
		return nil
	})
}

func TestTxUsedAfterReturnPanics(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	var kept *Tx
	update(t, db, func(tx *Tx) error { kept = tx; return nil })
	defer func() {
		if r := recover(); r != errClosed {
			t.Errorf("recovered %v, want errClosed", r)
		}
	}()
	kept.Mark("late")
	t.Error("Mark on an ended Tx returned")
}

// sliceError is uncomparable: == between two of its values panics.
type sliceError []string

func (e sliceError) Error() string { return strings.Join(e, "; ") }

func TestUncomparableRunErrorReturnsAsIs(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{Retention: 10})
	calls := map[string]func(context.Context, func(*Tx) error) error{"update": db.Update, "batch": db.Batch}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			err := call(t.Context(), func(tx *Tx) error {
				if err := f.items.Put(tx, 1, &item{Name: name}); err != nil {
					return err
				}
				return sliceError{"refused", name}
			})
			got, ok := errors.AsType[sliceError](err)
			if !ok || !slices.Equal(got, sliceError{"refused", name}) || errors.Is(err, ErrCommit) {
				t.Fatalf("%s = %v, want the run's sliceError as is", name, err)
			}
			view(t, db, func(r *ReadTx) error {
				if _, found, _ := f.items.Get(r, 1); found {
					t.Error("the refused run's row was stored")
				}
				return nil
			})
		})
	}
}

func TestBeforeCommitNextID(t *testing.T) {
	cases := map[string]struct {
		table   func(*fixture) *Table[uint64, item]
		refused bool
	}{
		"non-silent table refused": {func(f *fixture) *Table[uint64, item] { return f.items }, true},
		"silent table allowed":     {func(f *fixture) *Table[uint64, item] { return f.quiet }, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			db := mustOpen(t, tempPath(t), f.schema, Options{Retention: 10})
			table := tc.table(f)
			var id uint64
			var idErr error
			db.opts.BeforeCommit = func(tx *Tx, p *Pending) ([]Subject, error) {
				id, idErr = table.NextID(tx)
				return p.Touched, nil
			}
			err := db.Update(t.Context(), func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "x"}) })
			db.opts.BeforeCommit = nil
			var next uint64
			update(t, db, func(tx *Tx) error {
				var err error
				next, err = table.NextID(tx)
				return err
			})
			if tc.refused {
				if err == nil || idErr == nil || !strings.Contains(err.Error(), "BeforeCommit") {
					t.Errorf("Update = %v, NextID = %d, %v; want a BeforeCommit refusal", err, id, idErr)
				}
				if next != 1 || headRev(t, db) != 0 {
					t.Errorf("after the refusal: next id %d, rev %d; want 1 and 0", next, headRev(t, db))
				}
				return
			}
			if err != nil || idErr != nil || id != 1 || next != 2 {
				t.Errorf("Update = %v, NextID = %d, %v, next %d; want ids 1 then 2", err, id, idErr, next)
			}
		})
	}
}
