package bolttable

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

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

// resetAndOpen opens path under s, whose format must differ, allowing the
// reset of every name it lists.
func resetAndOpen(t *testing.T, path string, s *Schema) *DB {
	t.Helper()
	db := mustOpen(t, path, s, Options{Reset: allowAll})
	if len(db.OpenReport().Reset) == 0 {
		t.Fatal("Open reset nothing")
	}
	return db
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
	jobs := Must(Define(a, 0x12, "jobs", Uint64Key{}, JSON[item](1), TableOptions{Silent: true}))
	Must(DefineIndex(jobs, "by_state", IndexOptions{Version: 1}, func(uint64, *item) []Entry { return nil }))
	Must(DefineCounter(jobs, "job_weight", 1, func(uint64, *item) int64 { return 0 }))
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

// keptCopies lists the .old copies of path, oldest first.
func keptCopies(t *testing.T, path string) []string {
	t.Helper()
	m, err := filepath.Glob(path + ".*.*.old")
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(m)
	return m
}

func storedDigestOf(t *testing.T, path, name string) [32]byte {
	t.Helper()
	bdb, err := bbolt.Open(path, 0o600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bdb.Close() }()
	var d [32]byte
	_ = bdb.View(func(btx *bbolt.Tx) error {
		copy(d[:], btx.Bucket([]byte(name)).Get(keyDigest))
		return nil
	})
	return d
}

func TestOpenResetAllowedKeepsCopy(t *testing.T) {
	path := storedFile(t)
	storedA := storedDigestOf(t, path, "a")
	storedB := storedDigestOf(t, path, "b")
	before := readFile(t, path)
	f2 := fixtureAt(t, 2)
	db := mustOpen(t, path, f2.schema, Options{Reset: ResetPolicy{Allow: func(NamespaceDigest) bool { return true }, Keep: 2}})
	rep := db.OpenReport()
	want := []NamespaceDigest{{Name: "a", Stored: storedA, Want: f2.schema.digests()["a"]}}
	if !slices.Equal(rep.Reset, want) {
		t.Errorf("Reset = %+v, want %+v", rep.Reset, want)
	}
	if storedB != f2.schema.digests()["b"] {
		t.Fatal("fixture: namespace b's digest changed with a's")
	}
	view(t, db, func(r *ReadTx) error {
		if _, found, _ := f2.items.Get(r, 1); found {
			t.Error("the reset namespace kept its row")
		}
		if v, found, _ := f2.other.Get(r, 1); !found || v.Name != "b" {
			t.Errorf("namespace b's row = %+v, %v", v, found)
		}
		return nil
	})
	prefix := path + "." + hex.EncodeToString(storedA[:4]) + "."
	stamp, ok := strings.CutPrefix(rep.Kept, prefix)
	stamp, ok2 := strings.CutSuffix(stamp, ".old")
	if !ok || !ok2 {
		t.Fatalf("Kept = %q, want %s<stamp>.old", rep.Kept, prefix)
	}
	if _, err := time.Parse("20060102T150405.000000000Z", stamp); err != nil {
		t.Errorf("Kept's stamp %q: %v", stamp, err)
	}
	if !bytes.Equal(before, readFile(t, rep.Kept)) {
		t.Error("the kept copy is not the file before the reset")
	}
	if _, err := os.Stat(path + ".copy-tmp"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("copy-tmp left: %v", err)
	}
	f1 := newFixture(t)
	old := mustOpen(t, rep.Kept, f1.schema, Options{})
	view(t, old, func(r *ReadTx) error {
		if v, found, _ := f1.items.Get(r, 1); !found || v.Name != "a" {
			t.Errorf("the kept copy's row = %+v, %v", v, found)
		}
		return nil
	})
}

func TestOpenResetKeepsNewestCopies(t *testing.T) {
	path := storedFile(t)
	dir := filepath.Dir(path)
	unrelated := []string{
		filepath.Join(dir, "x.old"), path + ".zz.old", path + ".0000000g.20000101T000000.000000000Z.old",
		path + ".0000000A.20000101T000000.000000000Z.old", path + ".00000000.20000101T000000Z.old",
		filepath.Join(dir, "other.bolt.00000000.20000101T000000.000000000Z.old"),
	}
	for _, u := range unrelated {
		if err := os.WriteFile(u, []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var kept []string
	for _, v := range []uint32{2, 1, 2} {
		db := mustOpen(t, path, fixtureAt(t, v).schema, Options{Reset: ResetPolicy{Allow: func(NamespaceDigest) bool { return true }, Keep: 2}})
		kept = append(kept, db.OpenReport().Kept)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	got := slices.DeleteFunc(keptCopies(t, path), func(n string) bool { return slices.Contains(unrelated, n) })
	if !slices.Equal(got, kept[1:]) {
		t.Errorf(".old copies = %v, want the newest two %v", got, kept[1:])
	}
	for _, u := range unrelated {
		if string(readFile(t, u)) != "keep" {
			t.Errorf("%s changed", u)
		}
	}
}

func TestOpenResetNoCopy(t *testing.T) {
	path := storedFile(t)
	db := mustOpen(t, path, fixtureAt(t, 2).schema, Options{Reset: allowAll})
	if rep := db.OpenReport(); len(rep.Reset) != 1 || rep.Kept != "" {
		t.Errorf("report = %+v, want one reset and no copy", rep)
	}
	if m := keptCopies(t, path); len(m) != 0 {
		t.Errorf(".old copies %v", m)
	}
	if _, err := os.Stat(path + ".copy-tmp"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("copy-tmp: %v", err)
	}
}

func TestOpenResetRefusedWritesNothing(t *testing.T) {
	var asked []string
	cases := map[string]ResetPolicy{
		"one refused": {Keep: 2, Force: []string{"a"}, Allow: func(n NamespaceDigest) bool {
			asked = append(asked, n.Name)
			return n.Name != "a"
		}},
		"nil Allow": {Keep: 2, Force: []string{"a"}},
	}
	for name, policy := range cases {
		t.Run(name, func(t *testing.T) {
			asked = nil
			path := storedFile(t)
			before := readFile(t, path)
			_, err := Open(path, mismatchSchema(), Options{Reset: policy})
			if _, ok := errors.AsType[*FormatError](err); !ok {
				t.Fatalf("Open = %v, want a *FormatError", err)
			}
			if policy.Allow != nil && !slices.Equal(asked, []string{"a", "b"}) {
				t.Errorf("Allow asked about %v, want a then b", asked)
			}
			if !bytes.Equal(before, readFile(t, path)) {
				t.Error("a refused reset changed the file")
			}
			if m := keptCopies(t, path); len(m) != 0 {
				t.Errorf(".old copies %v", m)
			}
		})
	}
}

func TestOpenForceResetsMatchingNamespace(t *testing.T) {
	path := storedFile(t)
	storedA := storedDigestOf(t, path, "a")
	f := newFixture(t)
	db := mustOpen(t, path, f.schema, Options{Reset: ResetPolicy{Force: []string{"a", "a"}, Keep: 1}})
	rep := db.OpenReport()
	if want := []NamespaceDigest{{Name: "a", Stored: storedA, Want: storedA}}; !slices.Equal(rep.Reset, want) {
		t.Errorf("Reset = %+v, want %+v", rep.Reset, want)
	}
	if rep.Kept == "" {
		t.Error("no copy kept")
	}
	view(t, db, func(r *ReadTx) error {
		if _, found, _ := f.items.Get(r, 1); found {
			t.Error("the forced namespace kept its row")
		}
		if _, found, _ := f.other.Get(r, 1); !found {
			t.Error("namespace b lost its row")
		}
		return nil
	})
}

func TestOpenForceWithFormatErrorResetsOnce(t *testing.T) {
	path := storedFile(t)
	f2 := fixtureAt(t, 2)
	db := mustOpen(t, path, f2.schema, Options{Reset: ResetPolicy{Allow: func(NamespaceDigest) bool { return true }, Force: []string{"b", "a", libBucket}}})
	var names []string
	for _, n := range db.OpenReport().Reset {
		names = append(names, n.Name)
	}
	if !slices.Equal(names, []string{"a", "b", libBucket}) {
		t.Errorf("reset %v, want a, b and bolttable once each", names)
	}
	if rev := headRev(t, db); rev != 0 {
		t.Errorf("head after the library reset = %d, want 0", rev)
	}
}

func TestOpenForceUnknownName(t *testing.T) {
	path := storedFile(t)
	before := readFile(t, path)
	o := Options{Reset: ResetPolicy{Force: []string{"a", "nowhere"}, Keep: 1}}
	if _, err := Open(path, newFixture(t).schema, o); err == nil || errors.Is(err, ErrFormat) || !strings.Contains(err.Error(), "nowhere") {
		t.Errorf("Open = %v, want an error naming nowhere", err)
	}
	if !bytes.Equal(before, readFile(t, path)) {
		t.Error("a refused Force changed the file")
	}
	if m := keptCopies(t, path); len(m) != 0 {
		t.Errorf(".old copies %v", m)
	}
	t.Run("new file", func(t *testing.T) {
		if _, err := Open(tempPath(t), newFixture(t).schema, o); err == nil || !strings.Contains(err.Error(), "nowhere") {
			t.Errorf("Open of a new file = %v, want an error naming nowhere", err)
		}
		db := mustOpen(t, tempPath(t), newFixture(t).schema, Options{Reset: ResetPolicy{Force: []string{"a", libBucket}, Keep: 1}})
		if rep := db.OpenReport(); len(rep.Reset) != 0 || rep.Kept != "" {
			t.Errorf("a new file's report = %+v, want nothing reset", rep)
		}
	})
}

// failNthSync fails the nth directory sync with err: the copy's is the
// first, the prune's the second.
func failNthSync(n int, err error) func(string) error {
	synced := 0
	return func(dir string) error {
		if synced++; synced == n {
			return err
		}
		return syncDirectory(dir)
	}
}

func TestOpenResetFailureLeavesFileUnreset(t *testing.T) {
	boom := errors.New("boom")
	cases := map[string]func(){
		"copy": func() {
			copyFile = func(_, dst string) error {
				_ = os.WriteFile(dst, nil, 0o600)
				return boom
			}
		},
		"link":           func() { linkFile = func(string, string) error { return boom } },
		"dir sync":       func() { syncDir = failNthSync(1, boom) },
		"prune dir sync": func() { syncDir = failNthSync(2, boom) },
		"prune": func() {
			removeFile = func(name string) error {
				if strings.HasSuffix(name, ".old") {
					return boom
				}
				return os.Remove(name)
			}
		},
	}
	for name, inject := range cases {
		t.Run(name, func(t *testing.T) {
			path := storedFile(t)
			stale := path + ".00000000.20000101T000000.000000000Z.old"
			if err := os.WriteFile(stale, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			before := readFile(t, path)
			inject()
			t.Cleanup(func() { copyFile, linkFile, syncDir, removeFile = copyFileSync, os.Link, syncDirectory, os.Remove })
			_, err := Open(path, fixtureAt(t, 2).schema, Options{Reset: ResetPolicy{Allow: func(NamespaceDigest) bool { return true }, Keep: 1}})
			if !errors.Is(err, boom) {
				t.Errorf("Open = %v, want the injected failure", err)
			}
			if !bytes.Equal(before, readFile(t, path)) {
				t.Error("the live file changed")
			}
			if _, err := os.Stat(path + ".copy-tmp"); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("copy-tmp left: %v", err)
			}
			assertUnlocked(t, path)
		})
	}
}
