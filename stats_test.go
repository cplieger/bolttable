package bolttable

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func stats(t *testing.T, db *DB) Stats {
	t.Helper()
	st, err := db.Stats(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestStatsCounts(t *testing.T) {
	f := newFixture(t)
	failBC := false
	db := mustOpen(t, tempPath(t), f.schema, Options{Retention: 3, BeforeCommit: func(_ *Tx, p *Pending) ([]Subject, error) {
		if failBC {
			return nil, errors.New("refused")
		}
		return p.Touched, nil
	}})
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "x"}) })
	steps := []struct {
		name    string
		write   func() error
		commits uint64
		rev     uint64
	}{
		{"silent-only", func() error {
			return db.Update(t.Context(), func(tx *Tx) error { return f.quiet.Put(tx, 1, &item{}) })
		}, 1, 0},
		{"byte-equal", func() error {
			return db.Update(t.Context(), func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "x"}) })
		}, 1, 0},
		{"empty", func() error { return db.Update(t.Context(), func(*Tx) error { return nil }) }, 1, 0},
		{"non-silent", func() error {
			return db.Update(t.Context(), func(tx *Tx) error { return f.items.Put(tx, 2, &item{Name: "y"}) })
		}, 1, 1},
		{"fn error", func() error {
			return db.Update(t.Context(), func(tx *Tx) error {
				_ = f.items.Put(tx, 3, &item{Name: "z"})
				return errors.New("no")
			})
		}, 0, 0},
		{"BeforeCommit error", func() error {
			failBC = true
			defer func() { failBC = false }()
			return db.Update(t.Context(), func(tx *Tx) error { return f.items.Put(tx, 3, &item{Name: "z"}) })
		}, 0, 0},
		{"ErrCommit", func() error {
			defer failNextCommit(db)()
			return db.Update(t.Context(), func(tx *Tx) error { return f.other.Put(tx, 3, &item{Name: strings.Repeat("z", 4<<20)}) })
		}, 0, 0},
		{"context done", func() error {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			return db.Update(ctx, func(tx *Tx) error { return f.items.Put(tx, 3, &item{Name: "z"}) })
		}, 0, 0},
		{"batch group", func() error {
			db.bolt.MaxBatchSize, db.bolt.MaxBatchDelay = 3, 10*time.Second
			var wg sync.WaitGroup
			for i := range uint64(3) {
				wg.Go(func() {
					if err := db.Batch(t.Context(), func(tx *Tx) error { return f.items.Put(tx, 10+i, &item{}) }); err != nil {
						t.Error(err)
					}
				})
			}
			wg.Wait()
			return nil
		}, 1, 3},
	}
	for _, s := range steps {
		before := stats(t, db)
		_ = s.write()
		after := stats(t, db)
		if got := after.Commits - before.Commits; got != s.commits {
			t.Errorf("%s: Commits rose by %d, want %d", s.name, got, s.commits)
		}
		if got := after.Rev - before.Rev; got != s.rev {
			t.Errorf("%s: Rev rose by %d, want %d", s.name, got, s.rev)
		}
	}
	st := stats(t, db)
	view(t, db, func(r *ReadTx) error {
		if st.Rev != r.Rev() {
			t.Errorf("Rev = %d, ReadTx.Rev = %d", st.Rev, r.Rev())
		}
		if _, full, _ := r.Since(st.Oldest - 1); full {
			t.Errorf("Since(Oldest-1 = %d) answered full", st.Oldest-1)
		}
		if _, full, _ := r.Since(st.Oldest - 2); !full {
			t.Errorf("Since(Oldest-2 = %d) answered from a trimmed log", st.Oldest-2)
		}
		return nil
	})
	if st.Oldest != st.Rev-2 {
		t.Errorf("Oldest = %d with Rev %d and Retention 3", st.Oldest, st.Rev)
	}
}

func TestStatsSizes(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	update(t, db, func(tx *Tx) error {
		for i := range uint64(2000) {
			if err := f.items.Put(tx, i, &item{Name: fmt.Sprintf("%0100d", i)}); err != nil {
				return err
			}
		}
		return f.other.Put(tx, 1, &item{})
	})
	update(t, db, func(tx *Tx) error {
		for i := range uint64(1000) {
			if _, err := f.items.Delete(tx, i); err != nil {
				return err
			}
		}
		return nil
	})
	st := stats(t, db)
	if st.FreeBytes == 0 || st.FileBytes <= st.FreeBytes {
		t.Errorf("FileBytes %d, FreeBytes %d after deleting 1,000 rows", st.FileBytes, st.FreeBytes)
	}
	rows := map[string]int{}
	for _, ns := range st.Namespaces {
		if ns.Format != db.schema.digests()[ns.Name] {
			t.Errorf("%s: Format = %x, want its digest", ns.Name, ns.Format)
		}
		for _, tb := range ns.Tables {
			rows[ns.Name+"."+tb.Name] = tb.Rows
			if tb.Rows > 0 && tb.Bytes == 0 {
				t.Errorf("%s.%s: %d rows in 0 bytes", ns.Name, tb.Name, tb.Rows)
			}
		}
	}
	scanned := 0
	view(t, db, func(r *ReadTx) error {
		return f.items.Scan(r, Range{}, func(uint64, *item) (bool, error) { scanned++; return true, nil })
	})
	if rows["a.items"] != scanned || rows["a.items"] != 1000 || rows["b.other"] != 1 || rows["a.quiet"] != 0 {
		t.Errorf("rows = %v, scanned %d", rows, scanned)
	}
}
