package bolttable

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func benchDB(b *testing.B, rows int) (*DB, *fixture) {
	b.Helper()
	s := NewSchema("bench")
	a := Must(s.Namespace("a"))
	f := &fixture{schema: s}
	f.items = Must(Define(a, 0x10, "items", Uint64Key{}, JSON[item](1), TableOptions{}))
	f.byName = Must(DefineIndex(f.items, "by_name", IndexOptions{Unique: true, Version: 1}, func(_ uint64, v *item) []Entry {
		return []Entry{{Key: []byte(v.Name)}}
	}))
	db, err := Open(filepath.Join(b.TempDir(), "bench.bolt"), s, Options{Retention: 20000})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	if err := db.Update(b.Context(), func(tx *Tx) error {
		for i := range rows {
			if err := f.items.Put(tx, uint64(i), &item{Name: fmt.Sprintf("row-%08d", i), N: 1}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	return db, f
}

// BenchmarkCommitPersistedFreelist is one single-row, revision-reserving
// commit, which writes the freelist page every time.
func BenchmarkCommitPersistedFreelist(b *testing.B) {
	db, f := benchDB(b, 63000)
	b.ReportAllocs()
	n := uint64(0)
	for b.Loop() {
		n++
		if err := db.Update(b.Context(), func(tx *Tx) error {
			return f.items.Put(tx, n%63000, &item{Name: fmt.Sprintf("row-%08d", n%63000), N: int64(n)})
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkViewDuringBackup measures one point read while a backup holds its
// source read transaction open and a writer commits. The backup is held
// after its walk, inside its source read transaction, for the whole
// measurement, and timing starts only after the writer's first commit.
func BenchmarkViewDuringBackup(b *testing.B) {
	db, f := benchDB(b, 63000)
	walked, release := make(chan struct{}), make(chan struct{})
	backupWalk = func(string) { close(walked); <-release }
	b.Cleanup(func() { backupWalk = func(string) {} })
	ctx, cancel := context.WithCancel(b.Context())
	var wg sync.WaitGroup
	stop := sync.OnceFunc(func() {
		cancel()
		close(release)
		wg.Wait()
	})
	defer stop()

	backupErr := make(chan error, 1)
	dst := filepath.Join(b.TempDir(), "copy")
	wg.Go(func() { backupErr <- db.Backup(ctx, dst) })
	select {
	case <-walked:
	case err := <-backupErr:
		b.Fatalf("backup ended before finishing its walk: %v", err)
	}

	var writes atomic.Int64
	wrote, writeErr := make(chan struct{}), make(chan error, 1)
	wg.Go(func() {
		for i := uint64(0); ; i++ {
			err := db.Update(ctx, func(tx *Tx) error {
				return f.items.Put(tx, i%63000, &item{Name: fmt.Sprintf("row-%08d", i%63000), N: int64(i)})
			})
			if ctx.Err() != nil {
				writeErr <- nil
				return
			}
			if err != nil {
				writeErr <- err
				return
			}
			if writes.Add(1) == 1 {
				close(wrote)
			}
		}
	})
	select {
	case <-wrote:
	case err := <-writeErr:
		b.Fatalf("writer stopped before its first commit: %v", err)
	}

	before := writes.Load()
	b.ReportAllocs()
	n := uint64(0)
	for b.Loop() {
		n++
		if err := db.View(b.Context(), func(r *ReadTx) error {
			_, _, err := f.items.Get(r, n%63000)
			return err
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	during := writes.Load() - before
	stop()
	if err := <-backupErr; err != nil {
		b.Fatalf("backup: %v", err)
	}
	if err := <-writeErr; err != nil {
		b.Fatalf("writer: %v", err)
	}
	b.ReportMetric(float64(during), "writes")
}
