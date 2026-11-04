package bolttable

import (
	"errors"
	"fmt"
	"testing"

	"go.etcd.io/bbolt"
)

func TestSinceRetentionBoundaries(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{Retention: 5})
	for i := range uint64(8) {
		update(t, db, func(tx *Tx) error { return f.items.Put(tx, i, &item{N: 1}) })
	}
	// head 8, retention 5: revisions 4 to 8 are kept.
	const oldest, head = 4, 8
	cases := []struct {
		name  string
		after uint64
		full  bool
		first uint64
		n     int
	}{
		{"oldest-2", oldest - 2, true, 0, 0},
		{"oldest-1", oldest - 1, false, 4, 5},
		{"oldest", oldest, false, 5, 4},
		{"head", head, false, 0, 0},
		{"head+1", head + 1, true, 0, 0},
	}
	view(t, db, func(r *ReadTx) error {
		if rev, err := r.Rev(); rev != head || err != nil {
			t.Fatalf("Rev = %d, %v; want %d", rev, err, head)
		}
		for _, tc := range cases {
			page, err := r.Since(tc.after, 100)
			changes, full := page.Changes, page.Full
			if err != nil || full != tc.full || len(changes) != tc.n || page.More {
				t.Errorf("%s: Since(%d) = %d changes, full %v, more %v, %v; want %d, %v, false", tc.name, tc.after, len(changes), full, page.More, err, tc.n, tc.full)
				continue
			}
			if full && changes != nil {
				t.Errorf("%s: a full page carries changes %v", tc.name, changes)
			}
			for i, c := range changes {
				if c.Rev != tc.first+uint64(i) {
					t.Errorf("%s: change %d has revision %d, want %d", tc.name, i, c.Rev, tc.first+uint64(i))
				}
			}
			if !full && changes == nil {
				t.Errorf("%s: an answerable Since returned nil, not an empty slice", tc.name)
			}
		}
		return nil
	})
	view(t, db, func(r *ReadTx) error {
		_, log, err := r.feed()
		if err != nil {
			return err
		}
		if n := log.Stats().KeyN; n != 5 {
			t.Errorf("the change log holds %d entries, want 5", n)
		}
		return nil
	})
}

func TestZeroRetentionKeepsOnlyTheHead(t *testing.T) {
	path := tempPath(t)
	f := newFixture(t)
	db, err := Open(path, f.schema, Options{Retention: 5})
	if err != nil {
		t.Fatal(err)
	}
	for i := range uint64(3) {
		update(t, db, func(tx *Tx) error { return f.items.Put(tx, i, &item{}) })
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	f = newFixture(t)
	db = mustOpen(t, path, f.schema, Options{})
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 10, &item{}) })
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 11, &item{}) })
	view(t, db, func(r *ReadTx) error {
		if page, _ := r.Since(4, 100); !page.Full || page.Changes != nil || page.More {
			t.Errorf("Since below the head = %+v, want full with no changes", page)
		}
		if page, _ := r.Since(5, 100); page.Full || len(page.Changes) != 0 || page.More {
			t.Errorf("Since(head) = %+v; want empty", page)
		}
		_, log, err := r.feed()
		if err != nil {
			return err
		}
		if n := log.Stats().KeyN; n != 0 {
			t.Errorf("the change log holds %d entries from the earlier retention, want 0", n)
		}
		return nil
	})
}

func TestRevSurvivesReopen(t *testing.T) {
	path := tempPath(t)
	f := newFixture(t)
	db, err := Open(path, f.schema, Options{Retention: 10})
	if err != nil {
		t.Fatal(err)
	}
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{}) })
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	f = newFixture(t)
	var rec recorder
	db = mustOpen(t, path, f.schema, Options{OnCommit: rec.on, Retention: 10})
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 2, &item{}) })
	if got := rec.all(); len(got) != 1 || got[0].Rev != 2 {
		t.Errorf("after reopen delivered %+v, want revision 2", got)
	}
}

func TestCorruptChangeLogEntry(t *testing.T) {
	path := tempPath(t)
	f := newFixture(t)
	db, err := Open(path, f.schema, Options{Retention: 10})
	if err != nil {
		t.Fatal(err)
	}
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{}) })
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	rawUpdate(t, path, func(btx *bbolt.Tx) error {
		// A subject count far beyond the entry's bytes.
		return btx.Bucket([]byte(libBucket)).Bucket(keyChanges).Put(be64(1), []byte{0xFF, 0xFF, 0x03})
	})
	db = mustOpen(t, path, newFixture(t).schema, Options{Retention: 10})
	view(t, db, func(r *ReadTx) error {
		if _, err := r.Since(0, 100); err == nil {
			t.Error("Since decoded a corrupt change")
		}
		return nil
	})
}

func TestRevReportsCorruptHead(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{Retention: 10})
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{}) })
	undo := errors.New("undo")
	err := db.Update(t.Context(), func(tx *Tx) error {
		if err := tx.read.btx.Bucket([]byte(libBucket)).Put(keyHead, []byte{0, 0, 1}); err != nil {
			return err
		}
		if rev, err := tx.Read().Rev(); !errors.Is(err, ErrCorrupt) {
			t.Errorf("Rev over a 3-byte head = %d, %v; want ErrCorrupt", rev, err)
		}
		return undo
	})
	if err != undo {
		t.Fatalf("Update = %v, want the rollback error", err)
	}
	if rev := headRev(t, db); rev != 1 {
		t.Errorf("after the rollback Rev = %d, want 1", rev)
	}
}

func TestRevReportsMissingLibrary(t *testing.T) {
	cases := map[string]func(*bbolt.Tx) error{
		"no library bucket": func(btx *bbolt.Tx) error { _, err := btx.CreateBucket([]byte("other")); return err },
		"no change log": func(btx *bbolt.Tx) error {
			lib, err := btx.CreateBucket([]byte(libBucket))
			if err != nil {
				return err
			}
			return lib.Put(keyHead, be64(3))
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			path := tempPath(t)
			rawUpdate(t, path, build)
			bdb, err := bbolt.Open(path, 0o600, &bbolt.Options{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = bdb.Close() }()
			if err := bdb.View(func(btx *bbolt.Tx) error {
				if rev, err := (&ReadTx{btx: btx}).Rev(); !errors.Is(err, ErrCorrupt) {
					t.Errorf("Rev = %d, %v; want ErrCorrupt", rev, err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRevAfterTransactionFails(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{Retention: 10})
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{}) })
	var kept *ReadTx
	view(t, db, func(r *ReadTx) error { kept = r; return nil })
	if rev, err := kept.Rev(); !errors.Is(err, errTxClosed) {
		t.Errorf("Rev after View returned = %d, %v; want the transaction-ended error", rev, err)
	}
}

func commitN(t *testing.T, db *DB, f *fixture, n int) {
	t.Helper()
	for i := range uint64(n) {
		update(t, db, func(tx *Tx) error { return f.items.Put(tx, i+1, &item{N: 1}) })
	}
}

func TestSincePages(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{Retention: 100})
	commitN(t, db, f, 10)
	view(t, db, func(r *ReadTx) error {
		var revs []uint64
		var mores []bool
		after := uint64(0)
		for range 5 {
			page, err := r.Since(after, 3)
			if err != nil || page.Full {
				t.Fatalf("Since(%d, 3) = %+v, %v", after, page, err)
			}
			if len(page.Changes) > 3 {
				t.Fatalf("Since(%d, 3) returned %d changes", after, len(page.Changes))
			}
			for _, c := range page.Changes {
				revs = append(revs, c.Rev)
			}
			mores = append(mores, page.More)
			if !page.More {
				break
			}
			after = page.Changes[len(page.Changes)-1].Rev
		}
		if fmt.Sprint(revs) != "[1 2 3 4 5 6 7 8 9 10]" || fmt.Sprint(mores) != "[true true true false]" {
			t.Errorf("pages gave revisions %v with More %v; want 1 to 10 over pages of 3, 3, 3, 1", revs, mores)
		}
		return nil
	})
}

func TestSinceLimitExact(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{Retention: 100})
	commitN(t, db, f, 4)
	view(t, db, func(r *ReadTx) error {
		page, err := r.Since(1, 3)
		if err != nil || len(page.Changes) != 3 || page.More || page.Full {
			t.Errorf("Since(1, 3) over 3 remaining = %+v, %v; want 3 changes, More false", page, err)
		}
		return nil
	})
}

func TestSinceRejectsLimitBelowOne(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{Retention: 100})
	commitN(t, db, f, 2)
	view(t, db, func(r *ReadTx) error {
		for _, limit := range []int{0, -1} {
			if page, err := r.Since(0, limit); err == nil {
				t.Errorf("Since(0, %d) = %+v, want an error", limit, page)
			}
		}
		return nil
	})
}

func TestSinceEmptyIsNotNil(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{Retention: 100})
	commitN(t, db, f, 2)
	view(t, db, func(r *ReadTx) error {
		page, err := r.Since(2, 5)
		if err != nil || page.Changes == nil || len(page.Changes) != 0 || page.More || page.Full {
			t.Errorf("Since(head, 5) = %#v, %v; want an empty non-nil page", page, err)
		}
		return nil
	})
}
