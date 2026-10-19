package bolttable

import (
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
		if r.Rev() != head {
			t.Fatalf("Rev = %d, want %d", r.Rev(), head)
		}
		for _, tc := range cases {
			changes, full, err := r.Since(tc.after)
			if err != nil || full != tc.full || len(changes) != tc.n {
				t.Errorf("%s: Since(%d) = %d changes, full %v, %v; want %d, %v", tc.name, tc.after, len(changes), full, err, tc.n, tc.full)
				continue
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
		if _, full, _ := r.Since(4); !full {
			t.Error("Since below the head answered without a log")
		}
		if changes, full, _ := r.Since(5); full || len(changes) != 0 {
			t.Errorf("Since(head) = %v, %v; want empty", changes, full)
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
		if _, _, err := r.Since(0); err == nil {
			t.Error("Since decoded a corrupt change")
		}
		return nil
	})
}
