package bolttable

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	bbolterrors "go.etcd.io/bbolt/errors"
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
		if rev, err := r.Rev(); st.Rev != rev || err != nil {
			t.Errorf("Rev = %d, ReadTx.Rev = %d, %v", st.Rev, rev, err)
		}
		if page, _ := r.Since(st.Oldest-1, 100); page.Full {
			t.Errorf("Since(Oldest-1 = %d) answered full", st.Oldest-1)
		}
		if page, _ := r.Since(st.Oldest-2, 100); !page.Full {
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
	if st.Space.FreeBytes == 0 || st.Space.FileBytes <= st.Space.FreeBytes {
		t.Errorf("FileBytes %d, FreeBytes %d after deleting 1,000 rows", st.Space.FileBytes, st.Space.FreeBytes)
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

// fillAndThin writes 2,000 rows and deletes the first 1,000.
func fillAndThin(t *testing.T, db *DB, f *fixture) {
	t.Helper()
	update(t, db, func(tx *Tx) error {
		for i := range uint64(2000) {
			if err := f.items.Put(tx, i, &item{Name: fmt.Sprintf("%0100d", i)}); err != nil {
				return err
			}
		}
		return nil
	})
	update(t, db, func(tx *Tx) error {
		for i := range uint64(1000) {
			if _, err := f.items.Delete(tx, i); err != nil {
				return err
			}
		}
		return nil
	})
}

func TestSpaceMatchesStats(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	fillAndThin(t, db, f)
	sp, err := db.Space()
	if err != nil {
		t.Fatal(err)
	}
	if st := stats(t, db); st.Space != sp {
		t.Errorf("Space() = %+v, Stats().Space = %+v", sp, st.Space)
	}
}

func TestSpaceFreeAfterDeletes(t *testing.T) {
	f := newFixture(t)
	path := tempPath(t)
	db := mustOpen(t, path, f.schema, Options{})
	before, err := db.Space()
	if err != nil {
		t.Fatal(err)
	}
	fillAndThin(t, db, f)
	after, err := db.Space()
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.FreeBytes <= before.FreeBytes || after.FileBytes != fi.Size() || after.FreeBytes%int64(os.Getpagesize()) != 0 {
		t.Errorf("Space before %+v, after %+v, file %d bytes; want free bytes risen, in whole pages, and the file's size", before, after, fi.Size())
	}
}

func TestSpaceNeedsNoTransaction(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	viewing, writing, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	done := make(chan error, 3)
	go func() {
		done <- db.View(t.Context(), func(*ReadTx) error { close(viewing); <-release; return nil })
	}()
	go func() {
		done <- db.Update(t.Context(), func(*Tx) error { close(writing); <-release; return nil })
	}()
	wait(t, viewing, "the long View")
	wait(t, writing, "the held write")
	go func() { done <- db.Update(t.Context(), func(*Tx) error { return nil }) }()
	within(t, 10*time.Second, "Space under a held View and write", func() error {
		_, err := db.Space()
		return err
	})
	close(release)
	for range 3 {
		within(t, 10*time.Second, "the held transactions", func() error { return <-done })
	}
}

func TestSpaceStatFailure(t *testing.T) {
	f := newFixture(t)
	path := tempPath(t)
	db := mustOpen(t, path, f.schema, Options{})
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if sp, err := db.Space(); err == nil || !strings.Contains(err.Error(), path) {
		t.Errorf("Space of a removed file = %+v, %v; want a stat error naming the path", sp, err)
	}
}

func TestSpaceAfterClose(t *testing.T) {
	db, err := Open(tempPath(t), newFixture(t).schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if sp, err := db.Space(); !errors.Is(err, bbolterrors.ErrDatabaseNotOpen) {
		t.Errorf("Space after Close = %+v, %v; want ErrDatabaseNotOpen", sp, err)
	}
}

func TestSpaceRacesClose(t *testing.T) {
	for range 20 {
		db, err := Open(tempPath(t), newFixture(t).schema, Options{})
		if err != nil {
			t.Fatal(err)
		}
		stop := make(chan struct{})
		results := make(chan error, 1)
		go func() {
			for {
				select {
				case <-stop:
					results <- nil
					return
				default:
				}
				if sp, err := db.Space(); err != nil && !errors.Is(err, bbolterrors.ErrDatabaseNotOpen) {
					results <- fmt.Errorf("Space = %+v, %w", sp, err)
					return
				}
			}
		}()
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		close(stop)
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

// TestSpacePastTheCloseCheck closes bbolt under a DB whose closed flag is
// still clear: the state a Space call racing Close meets after its check.
func TestSpacePastTheCloseCheck(t *testing.T) {
	path := tempPath(t)
	db, err := Open(path, newFixture(t).schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.bolt.Close(); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if sp, err := db.Space(); err != nil || sp.FileBytes != fi.Size() {
		t.Errorf("Space after bbolt closed = %+v, %v; want the file's size from the cached path", sp, err)
	}
}
