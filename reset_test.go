package bolttable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io/fs"
	"slices"
	"testing"

	"go.etcd.io/bbolt"
)

type resetTables map[string]byte // table name to prefix, in namespace a

// resetSchema declares namespace a with the given tables and namespace b
// with one table at 0x20.
func resetSchema(a resetTables) (*Schema, map[string]*Table[uint64, item], *Table[uint64, item]) {
	s := NewSchema("reset")
	nsA := Must(s.Namespace("a"))
	nsB := Must(s.Namespace("b"))
	tables := map[string]*Table[uint64, item]{}
	for name, prefix := range a {
		tables[name] = Must(Define(nsA, prefix, name, Uint64Key{}, JSON[item](1), TableOptions{}))
	}
	return s, tables, Must(Define(nsB, 0x20, "other", Uint64Key{}, JSON[item](1), TableOptions{}))
}

func issueIDs(t *testing.T, db *DB, tb *Table[uint64, item], n int) {
	t.Helper()
	update(t, db, func(tx *Tx) error {
		for range n {
			if _, err := tb.NextID(tx); err != nil {
				return err
			}
		}
		return nil
	})
}

func nextID(t *testing.T, db *DB, tb *Table[uint64, item]) uint64 {
	t.Helper()
	var id uint64
	update(t, db, func(tx *Tx) error {
		var err error
		id, err = tb.NextID(tx)
		return err
	})
	return id
}

// resetAndOpen opens path under s, which must fail with a *FormatError,
// resets every name it lists and opens again.
func resetAndOpen(t *testing.T, path string, s *Schema) *DB {
	t.Helper()
	_, err := Open(path, s, Options{})
	var fe *FormatError
	if !errors.As(err, &fe) {
		t.Fatalf("Open = %v, want a *FormatError", err)
	}
	names := make([]string, 0, len(fe.Namespaces))
	for _, n := range fe.Namespaces {
		names = append(names, n.Name)
	}
	if err := ResetNamespaces(path, s, names); err != nil {
		t.Fatalf("ResetNamespaces(%v): %v", names, err)
	}
	return mustOpen(t, path, s, Options{})
}

func TestResetSeedsSequencesAcrossLayoutChanges(t *testing.T) {
	path := tempPath(t)
	s1, t1, other := resetSchema(resetTables{"x": 0x10, "y": 0x11})
	db, err := Open(path, s1, Options{})
	if err != nil {
		t.Fatal(err)
	}
	issueIDs(t, db, t1["x"], 500)
	issueIDs(t, db, t1["y"], 900)
	issueIDs(t, db, other, 42)
	update(t, db, func(tx *Tx) error { return other.Put(tx, 7, &item{Name: "kept"}) })
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The second schema moves x to 0x12 and renames y to z.
	s2, t2, other2 := resetSchema(resetTables{"x": 0x12, "z": 0x11})
	db = resetAndOpen(t, path, s2)
	for _, name := range []string{"x", "z"} {
		if id := nextID(t, db, t2[name]); id <= 900 {
			t.Errorf("schema 2: %s's next id = %d, want above 900", name, id)
		}
	}
	if id := nextID(t, db, other2); id != 43 {
		t.Errorf("namespace b's next id = %d, want 43", id)
	}
	view(t, db, func(r *ReadTx) error {
		if v, found, _ := other2.Get(r, 7); !found || v.Name != "kept" {
			t.Errorf("namespace b's row = %+v, %v", v, found)
		}
		return nil
	})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The third schema removes the renamed table.
	s3, t3, _ := resetSchema(resetTables{"x": 0x12})
	db = resetAndOpen(t, path, s3)
	if id := nextID(t, db, t3["x"]); id <= 901 {
		t.Errorf("schema 3: x's next id = %d, want above every id issued", id)
	}
}

func TestResetReplacesOtherLayoutLibraryBucket(t *testing.T) {
	f := newFixture(t)
	path := tempPath(t)
	db, err := Open(path, f.schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	update(t, db, func(tx *Tx) error {
		if err := f.items.Put(tx, 1, &item{Name: "a"}); err != nil {
			return err
		}
		return f.other.Put(tx, 1, &item{Name: "b"})
	})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	layoutTo(t, path, 2)
	db = resetAndOpen(t, path, newFixture(t).schema)
	view(t, db, func(r *ReadTx) error {
		lib := r.btx.Bucket([]byte(libBucket))
		if got := binary.BigEndian.Uint32(lib.Get(keyLayout)); got != layoutNumber {
			t.Errorf("stored layout number = %d, want %d", got, layoutNumber)
		}
		return nil
	})
	if _, err := NewSchema("s").Namespace(libBucket); err == nil {
		t.Error("a schema declared the namespace bolttable")
	}
}

func TestResetRemovedNamespace(t *testing.T) {
	path := tempPath(t)
	f := newFixture(t)
	db, err := Open(path, f.schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "a"}) })
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s := NewSchema("test")
	a := Must(s.Namespace("a"))
	items := Must(Define(a, 0x10, "items", Uint64Key{}, JSON[item](1), TableOptions{}))
	Must(DefineIndex(items, "by_name", IndexOptions{Unique: true, Version: 1}, func(uint64, *item) []Entry { return nil }))
	Must(DefineIndex(items, "by_tag", IndexOptions{Version: 1}, func(uint64, *item) []Entry { return nil }))
	Must(DefineCounter(items, "total", 1, func(uint64, *item) int64 { return 0 }))
	Must(Define(a, 0x11, "quiet", Uint64Key{}, JSON[item](1), TableOptions{Silent: true}))
	db = resetAndOpen(t, path, s)
	view(t, db, func(r *ReadTx) error {
		if r.btx.Bucket([]byte("b")) != nil {
			t.Error("the removed namespace b survived its reset")
		}
		if _, found, _ := items.Get(r, 1); !found {
			t.Error("namespace a lost its row")
		}
		return nil
	})
}

func TestResetUnknownNameErrors(t *testing.T) {
	path := closedFile(t, newFixture(t).schema)
	before := readFile(t, path)
	if err := ResetNamespaces(path, newFixture(t).schema, []string{"a", "nowhere"}); err == nil {
		t.Error("ResetNamespaces accepted a name neither stored nor declared")
	}
	if !bytes.Equal(before, readFile(t, path)) {
		t.Error("a refused reset changed the file")
	}
	if err := ResetNamespaces(tempPath(t), newFixture(t).schema, []string{"a"}); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("reset of an absent file = %v, want fs.ErrNotExist", err)
	}
}

func TestResetDeclaredNamespaceKeepsTheOther(t *testing.T) {
	path := tempPath(t)
	f := newFixture(t)
	db, err := Open(path, f.schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	update(t, db, func(tx *Tx) error {
		if err := f.items.Put(tx, 1, &item{Name: "a", N: 4}); err != nil {
			return err
		}
		return f.other.Put(tx, 1, &item{Name: "b"})
	})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ResetNamespaces(path, newFixture(t).schema, []string{"a"}); err != nil {
		t.Fatal(err)
	}
	f = newFixture(t)
	db = mustOpen(t, path, f.schema, Options{})
	view(t, db, func(r *ReadTx) error {
		if _, found, _ := f.items.Get(r, 1); found {
			t.Error("the reset namespace kept its row")
		}
		if n, _ := f.total.Value(r); n != 0 {
			t.Errorf("the reset namespace's counter = %d", n)
		}
		if _, found, _ := f.other.Get(r, 1); !found {
			t.Error("namespace b lost its row")
		}
		return nil
	})
	rawNames := []string{}
	view(t, db, func(r *ReadTx) error {
		return r.btx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
			rawNames = append(rawNames, string(name))
			return nil
		})
	})
	if !slices.Equal(rawNames, []string{"a", "b", libBucket}) {
		t.Errorf("top-level buckets = %v", rawNames)
	}
}
