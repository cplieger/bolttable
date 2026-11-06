package bolttable

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A write a test runs through one of the DB's writers; viaRepair ignores fn.
type write func(ctx context.Context, db *DB, fn func(*Tx) error) error

func viaUpdate(ctx context.Context, db *DB, fn func(*Tx) error) error { return db.Update(ctx, fn) }

func viaBatch(ctx context.Context, db *DB, fn func(*Tx) error) error { return db.Batch(ctx, fn) }

func viaRepair(ctx context.Context, db *DB, _ func(*Tx) error) error {
	_, err := db.Verify(ctx, true)
	return err
}

// holdWriter starts a write through w that holds bbolt's writer until
// release is called, and returns once it holds it.
func holdWriter(t *testing.T, db *DB, w write) (release func()) {
	t.Helper()
	held, stop, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- w(t.Context(), db, func(*Tx) error {
			close(held)
			<-stop
			return nil
		})
	}()
	wait(t, held, "the held writer")
	return func() {
		close(stop)
		if err := <-done; err != nil {
			t.Errorf("held writer: %v", err)
		}
	}
}

func TestWritersLeaveTheQueueWhenTheirContextEnds(t *testing.T) {
	cases := []struct {
		name       string
		hold, wait write
	}{
		{"Update_behind_Update", viaUpdate, viaUpdate},
		{"Batch_behind_Update", viaUpdate, viaBatch},
		{"Verify_repair_behind_Update", viaUpdate, viaRepair},
		{"Update_behind_Batch", viaBatch, viaUpdate},
		{"Verify_repair_behind_Batch", viaBatch, viaRepair},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFixture(t)
			db := mustOpen(t, tempPath(t), f.schema, Options{})
			release := holdWriter(t, db, c.hold)
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			ran := make(chan struct{}, 1)
			got := make(chan error, 1)
			go func() {
				got <- c.wait(ctx, db, func(tx *Tx) error {
					select {
					case ran <- struct{}{}:
					default:
					}
					return f.items.Put(tx, 1, &item{Name: "late"})
				})
			}()
			select {
			case err := <-got:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("queued write = %v, want context.DeadlineExceeded", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("the queued write still waited for the writer after its deadline")
			}
			release()
			// A write that stayed queued would have committed before this one.
			update(t, db, func(*Tx) error { return nil })
			select {
			case <-ran:
				t.Error("fn ran for a write that gave up while queued")
			default:
			}
			view(t, db, func(r *ReadTx) error {
				if _, ok, err := f.items.Get(r, 1); err != nil || ok {
					t.Errorf("Get(1) = ok %v, %v; want nothing written", ok, err)
				}
				return nil
			})
		})
	}
}

func TestUpdateContextDoneBeforeCommitRollsBack(t *testing.T) {
	f := newFixture(t)
	var rec recorder
	db := mustOpen(t, tempPath(t), f.schema, Options{OnCommit: rec.on})
	ctx, cancel := context.WithCancel(t.Context())
	after := false
	err := db.Update(ctx, func(tx *Tx) error {
		if err := f.items.Put(tx, 1, &item{Name: "x"}); err != nil {
			return err
		}
		tx.AfterCommit(func() { after = true })
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Update = %v, want context.Canceled", err)
	}
	if after || len(rec.all()) != 0 {
		t.Errorf("a rolled-back call ran its callbacks: AfterCommit %v, OnCommit %d", after, len(rec.all()))
	}
	if rev := headRev(t, db); rev != 0 {
		t.Errorf("rev = %d, want 0", rev)
	}
	view(t, db, func(r *ReadTx) error {
		if _, ok, err := f.items.Get(r, 1); err != nil || ok {
			t.Errorf("Get(1) = ok %v, %v; want the write rolled back", ok, err)
		}
		return nil
	})
}

func TestUpdateContextDoneInBeforeCommitRollsBack(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	db := mustOpen(t, tempPath(t), f.schema, Options{BeforeCommit: func(_ *Tx, p *Pending) ([]Subject, error) {
		cancel()
		return p.Touched, nil
	}})
	err := db.Update(ctx, func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "x"}) })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Update = %v, want context.Canceled", err)
	}
	if rev := headRev(t, db); rev != 0 {
		t.Errorf("rev = %d, want 0", rev)
	}
}

func TestUpdateCommitIgnoresALaterCancel(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	db := mustOpen(t, tempPath(t), f.schema, Options{OnCommit: func(Change) { cancel() }})
	if err := db.Update(ctx, func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "x"}) }); err != nil {
		t.Fatalf("Update = %v, want nil for a commit that landed", err)
	}
	if rev := headRev(t, db); rev != 1 {
		t.Errorf("rev = %d, want 1", rev)
	}
}

func TestBatchContextDoneBeforeCommitRollsBackOnlyItsCall(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{MaxBatchDelay: 50 * time.Millisecond})
	ctx, cancel := context.WithCancel(t.Context())
	errs := make(chan error, 2)
	go func() {
		errs <- db.Batch(ctx, func(tx *Tx) error {
			if err := f.items.Put(tx, 1, &item{Name: "cancelled"}); err != nil {
				return err
			}
			cancel()
			return nil
		})
	}()
	go func() {
		errs <- db.Batch(t.Context(), func(tx *Tx) error { return f.items.Put(tx, 2, &item{Name: "kept"}) })
	}()
	var canceled, ok int
	for range 2 {
		switch err := <-errs; {
		case errors.Is(err, context.Canceled):
			canceled++
		case err == nil:
			ok++
		default:
			t.Errorf("Batch = %v", err)
		}
	}
	if canceled != 1 || ok != 1 {
		t.Fatalf("got %d cancelled and %d committed, want 1 and 1", canceled, ok)
	}
	view(t, db, func(r *ReadTx) error {
		if _, has, err := f.items.Get(r, 1); err != nil || has {
			t.Errorf("Get(1) = ok %v, %v; want the cancelled call rolled back", has, err)
		}
		if _, has, err := f.items.Get(r, 2); err != nil || !has {
			t.Errorf("Get(2) = ok %v, %v; want the other call committed", has, err)
		}
		return nil
	})
}

func TestUpdatePanicRollsBackInTheCaller(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	func() {
		defer func() {
			if r := recover(); r != "boom" {
				t.Errorf("recover() = %v, want the panic value", r)
			}
		}()
		_ = db.Update(t.Context(), func(tx *Tx) error {
			if err := f.items.Put(tx, 1, &item{Name: "x"}); err != nil {
				return err
			}
			panic("boom")
		})
	}()
	update(t, db, func(*Tx) error { return nil })
	view(t, db, func(r *ReadTx) error {
		if _, ok, err := f.items.Get(r, 1); err != nil || ok {
			t.Errorf("Get(1) = ok %v, %v; want the panicking write rolled back", ok, err)
		}
		return nil
	})
}
