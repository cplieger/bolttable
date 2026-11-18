package bolttable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/cplieger/go/httpserver/v2"
	"go.etcd.io/bbolt"
)

// errNewer is an app's refusal of a file a later build stamped.
var errNewer = fmt.Errorf("store version 3 is above this build's 2: %w", ErrNewerBuild)

func refuseNewer(*ReadTx) error { return errNewer }

func TestOpenFailureClasses(t *testing.T) {
	errAppCorrupt := fmt.Errorf("%w: settings document", ErrCorrupt)
	is := func(err error) func(*testing.T) error { return func(*testing.T) error { return err } }
	cases := []struct {
		err   func(t *testing.T) error
		name  string
		class string
		ok    bool
	}{
		{name: "ErrInvalidFile", err: is(ErrInvalidFile), class: "damaged", ok: true},
		{name: "an invalid file", class: "damaged", ok: true, err: func(t *testing.T) error {
			_, err := Open(filled(t, 2*os.Getpagesize()), newFixture(t).schema, Options{})
			return err
		}},
		{name: "ErrCorrupt", err: is(ErrCorrupt), class: "damaged", ok: true},
		{name: "an app sentinel wrapping ErrCorrupt", err: is(fmt.Errorf("store: %w", errAppCorrupt)), class: "damaged", ok: true},
		{name: "ErrFormat", err: is(ErrFormat), class: "format", ok: true},
		{name: "a format change", class: "format", ok: true, err: func(t *testing.T) error {
			_, err := Open(storedFile(t), mismatchSchema(), Options{})
			return err
		}},
		{name: "ErrNewerBuild", err: is(ErrNewerBuild), class: "newer", ok: true},
		{name: "an Inspect refusal wrapping ErrNewerBuild", class: "newer", ok: true, err: func(t *testing.T) error {
			_, err := Open(storedFile(t), newFixture(t).schema, Options{Inspect: refuseNewer})
			return err
		}},
		{name: "newer over damaged", err: is(errors.Join(errAppCorrupt, errNewer)), class: "newer", ok: true},
		{name: "newer over format", err: is(errors.Join(&FormatError{}, errNewer)), class: "newer", ok: true},
		{name: "ErrLocked", err: is(ErrLocked)},
		{name: "a held lock", err: func(t *testing.T) error {
			path := storedFile(t)
			l, err := LockPath(path, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = l.Release() }()
			_, err = Open(path, newFixture(t).schema, Options{})
			return err
		}},
		{name: "a held lock over newer", err: is(errors.Join(ErrLocked, errNewer))},
		{name: "any other error", err: is(errors.New("permission denied")), class: "unreadable", ok: true},
		{name: "a path that is not a file", class: "unreadable", ok: true, err: func(t *testing.T) error {
			_, err := Open(t.TempDir(), newFixture(t).schema, Options{})
			return err
		}},
		{name: "ErrReset", err: is(fmt.Errorf("%w: disk full", ErrReset)), class: "unwritable", ok: true},
		{name: "a reset whose write failed", class: "unwritable", ok: true, err: func(t *testing.T) error {
			path := storedFile(t)
			before := readFile(t, path)
			refuseWrites(t)
			_, err := Open(path, fixtureAt(t, 2).schema, Options{Reset: allowAll})
			if !errors.Is(err, ErrReset) {
				t.Errorf("Open = %v, want ErrReset", err)
			}
			if !bytes.Equal(before, readFile(t, path)) {
				t.Error("the file changed")
			}
			assertUnlocked(t, path)
			return err
		}},
		{name: "ErrCommit", err: is(fmt.Errorf("%w: disk full", ErrCommit)), class: "unwritable", ok: true},
		{name: "an open hook whose commit failed", class: "unwritable", ok: true, err: func(t *testing.T) error {
			f := newFixture(t)
			path := storedFile(t)
			refuseWrites(t)
			l := newLive(t, path, f.schema, Options{})
			l.OnOpen(func(db *DB) error {
				return db.Update(t.Context(), func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "x"}) })
			})
			err := l.Open()
			if !errors.Is(err, ErrCommit) {
				t.Errorf("Open = %v, want ErrCommit", err)
			}
			return err
		}},
		{name: "damaged over a failed commit", err: is(errors.Join(ErrCommit, errAppCorrupt)), class: "damaged", ok: true},
		{name: "format over a failed commit", err: is(errors.Join(ErrCommit, &FormatError{})), class: "format", ok: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := c.err(t)
			if err == nil {
				t.Fatal("the case built no error")
			}
			class, ok := OpenFailure(err)
			if class != c.class || ok != c.ok {
				t.Errorf("OpenFailure(%v) = %q, %v; want %q, %v", err, class, ok, c.class, c.ok)
			}
		})
	}
	t.Run("nil", func(t *testing.T) {
		if class, ok := OpenFailure(nil); class != "" || ok {
			t.Errorf("OpenFailure(nil) = %q, %v; want \"\", false", class, ok)
		}
	})
	t.Run("through a Live", func(t *testing.T) {
		f := newFixture(t)
		l := newLive(t, storedFile(t), f.schema, Options{Inspect: refuseNewer})
		if class, _ := OpenFailure(l.Open()); class != "newer" {
			t.Errorf("Open's error class = %q, want newer", class)
		}
		if class, _ := OpenFailure(l.Err()); class != "newer" {
			t.Errorf("Err's class = %q, want newer", class)
		}
		err := l.View(t.Context(), func(*ReadTx) error { return nil })
		if class, _ := OpenFailure(err); class != "newer" || !errors.Is(err, ErrClosed) {
			t.Errorf("View = %v, class %q; want ErrClosed of class newer", err, class)
		}
	})
}

// An app hands OpenFailure's class to httpserver.OutagePage, which refuses
// any spelling outside its OutageClass constants.
func TestOpenFailureSpellingsAreOutageClasses(t *testing.T) {
	cases := []struct {
		err  error
		name string
		want httpserver.OutageClass
	}{
		{name: "ErrInvalidFile", err: ErrInvalidFile, want: httpserver.OutageDamaged},
		{name: "ErrCorrupt", err: ErrCorrupt, want: httpserver.OutageDamaged},
		{name: "ErrFormat", err: ErrFormat, want: httpserver.OutageFormat},
		{name: "ErrNewerBuild", err: ErrNewerBuild, want: httpserver.OutageNewer},
		{name: "ErrReset", err: ErrReset, want: httpserver.OutageUnwritable},
		{name: "ErrCommit", err: ErrCommit, want: httpserver.OutageUnwritable},
		{name: "any other error", err: errors.New("permission denied"), want: httpserver.OutageUnreadable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			class, ok := OpenFailure(c.err)
			if !ok || httpserver.OutageClass(class) != c.want {
				t.Errorf("OpenFailure(%v) = %q, %v; want %q, true", c.err, class, ok, c.want)
			}
		})
	}
}

// changingOptions makes an Open of storedFile under changingSchema reset
// namespace a, force-reset b keeping a copy, compact, and create namespace c.
func changingOptions(allow func(NamespaceDigest) bool) Options {
	return Options{
		Reset:       ResetPolicy{Allow: allow, Force: []string{"b"}, Keep: 2},
		Compact:     CompactPolicy{MinFreeRatio: 1e-9},
		InvalidFile: InvalidFileMoveAside,
	}
}

func changingSchema(t *testing.T) *Schema {
	t.Helper()
	f := fixtureAt(t, 2)
	c := Must(f.schema.Namespace("c"))
	Must(Define(c, 0x30, "fresh", Uint64Key{}, JSON[item](1), TableOptions{}))
	return f.schema
}

func TestInspectRunsBeforeReset(t *testing.T) {
	path := storedFile(t)
	f := fixtureAt(t, 2)
	var steps []string
	o := Options{
		Inspect: func(r *ReadTx) error {
			v, found, err := f.items.Get(r, 1)
			steps = append(steps, fmt.Sprintf("inspect %q %v", v.Name, found))
			return err
		},
		Reset: ResetPolicy{Allow: func(n NamespaceDigest) bool {
			steps = append(steps, "allow "+n.Name)
			return true
		}},
	}
	db := mustOpen(t, path, f.schema, o)
	if want := []string{`inspect "a" true`, "allow a"}; !slices.Equal(steps, want) {
		t.Errorf("steps = %q, want %q", steps, want)
	}
	if len(db.OpenReport().Reset) == 0 {
		t.Error("Open reset nothing")
	}
	view(t, db, func(r *ReadTx) error {
		if _, found, err := f.items.Get(r, 1); err != nil || found {
			t.Errorf("row 1 after the reset: found %v, %v", found, err)
		}
		return nil
	})
}

func TestInspectRefusalLeavesFileUnchanged(t *testing.T) {
	t.Run("control", func(t *testing.T) {
		db := mustOpen(t, storedFile(t), changingSchema(t), changingOptions(allowAll.Allow))
		r := db.OpenReport()
		if len(r.Reset) == 0 || r.Kept == "" || !r.Compacted {
			t.Errorf("OpenReport = %+v, want a reset, a kept copy and a compaction", r)
		}
	})
	refused := func(t *testing.T, path string, lock *Lock) {
		t.Helper()
		before := readFile(t, path)
		asked := 0
		o := changingOptions(func(NamespaceDigest) bool { asked++; return true })
		o.Inspect, o.Lock = refuseNewer, lock
		db, err := Open(path, changingSchema(t), o)
		if err != errNewer { //nolint:errorlint // Inspect's error is returned as is
			t.Errorf("Open = %v, want Inspect's error as is", err)
		}
		if db != nil {
			_ = db.Close()
		}
		if asked != 0 {
			t.Errorf("Allow asked %d times", asked)
		}
		if !bytes.Equal(before, readFile(t, path)) {
			t.Error("a refused open changed the file")
		}
		if m := keptCopies(t, path); len(m) != 0 {
			t.Errorf(".old copies %v", m)
		}
		if m := invalidFiles(t, path); len(m) != 0 {
			t.Errorf("moved aside as %v", m)
		}
	}
	t.Run("taken lock", func(t *testing.T) {
		path := storedFile(t)
		refused(t, path, nil)
		assertUnlocked(t, path)
	})
	t.Run("adopted lock", func(t *testing.T) {
		path := storedFile(t)
		l, err := LockPath(path, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = l.Release() })
		refused(t, path, l)
		assertLocked(t, path)
		db, err := Open(path, newFixture(t).schema, Options{Lock: l})
		if err != nil {
			t.Fatalf("Open under the lock after the refusal = %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if err := l.Release(); err != nil {
			t.Errorf("Release after the refusal = %v", err)
		}
	})
}

func TestInspectSkipsNewFile(t *testing.T) {
	for name, build := range map[string]func(t *testing.T) string{
		"absent": tempPath,
		"empty": func(t *testing.T) string {
			path := tempPath(t)
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			return path
		},
	} {
		t.Run(name, func(t *testing.T) {
			ran := false
			mustOpen(t, build(t), newFixture(t).schema, Options{Inspect: func(*ReadTx) error { ran = true; return errNewer }})
			if ran {
				t.Error("Inspect ran on a file holding no database")
			}
		})
	}
}

func TestInspectRefusalWinsOverFormat(t *testing.T) {
	path := storedFile(t)
	_, err := Open(path, mismatchSchema(), Options{Inspect: refuseNewer})
	if !errors.Is(err, ErrNewerBuild) || errors.Is(err, ErrFormat) {
		t.Errorf("Open = %v, want Inspect's refusal, not the format result", err)
	}
	assertUnlocked(t, path)
}

func TestInspectPanicPropagates(t *testing.T) {
	path := storedFile(t)
	before := readFile(t, path)
	v := openPanic(t, path, changingSchema(t), Options{Inspect: func(*ReadTx) error { panic(testPanic("inspect")) }})
	if v != testPanic("inspect") {
		t.Errorf("recovered %v (%T), want Inspect's panic", v, v)
	}
	if !bytes.Equal(before, readFile(t, path)) {
		t.Error("the file changed")
	}
	assertUnlocked(t, path)
}

func TestInspectReadsAbsentAsEmpty(t *testing.T) {
	t.Run("namespace absent", func(t *testing.T) {
		path := storedFile(t)
		var stored uint64
		func() {
			db := mustOpen(t, path, newFixture(t).schema, Options{})
			stored = headRev(t, db)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}()
		f := newFixture(t)
		c := Must(f.schema.Namespace("c"))
		fresh := Must(Define(c, 0x30, "fresh", Uint64Key{}, JSON[item](1), TableOptions{}))
		byName := Must(DefineIndex(fresh, "by_name", IndexOptions{Unique: true, Version: 1}, func(_ uint64, v *item) []Entry {
			return []Entry{{Key: []byte(v.Name)}}
		}))
		total := Must(DefineCounter(fresh, "total", 1, func(_ uint64, v *item) int64 { return v.N }))
		ran := false
		mustOpen(t, path, f.schema, Options{Inspect: func(r *ReadTx) error {
			ran = true
			assertEmptyReads(t, r, fresh, byName, total)
			if v, found, err := f.items.Get(r, 1); err != nil || !found || v.Name != "a" {
				t.Errorf("stored row 1 = %+v, %v, %v; want a", v, found, err)
			}
			if rev, err := r.Rev(); err != nil || rev != stored {
				t.Errorf("Rev = %d, %v; want %d", rev, err, stored)
			}
			return nil
		}})
		if !ran {
			t.Error("Inspect did not run")
		}
	})
	t.Run("tables and log absent", func(t *testing.T) {
		path := storedFile(t)
		rawUpdate(t, path, func(btx *bbolt.Tx) error {
			a := btx.Bucket([]byte("a"))
			for _, name := range [][]byte{{0x10}, []byte("index:by_name"), []byte("index:by_tag")} {
				if err := a.DeleteBucket(name); err != nil {
					return err
				}
			}
			return btx.DeleteBucket([]byte(libBucket))
		})
		f := newFixture(t)
		ran := false
		db, err := Open(path, f.schema, Options{Retention: 10, Inspect: func(r *ReadTx) error {
			ran = true
			assertEmptyReads(t, r, f.items, f.byName, f.total)
			if rev, err := r.Rev(); err != nil || rev != 0 {
				t.Errorf("Rev = %d, %v; want 0", rev, err)
			}
			for after, full := range map[uint64]bool{0: false, 1: true} {
				page, err := r.Since(after, 10)
				if err != nil || page.Full != full || len(page.Changes) != 0 {
					t.Errorf("Since(%d) = %+v, %v; want no changes, Full %v", after, page, err, full)
				}
			}
			return nil
		}})
		if err == nil {
			_ = db.Close()
		}
		if !ran {
			t.Error("Inspect did not run")
		}
	})
}

// assertEmptyReads checks that tb, its unique index ix and its counter c
// read as empty in r.
func assertEmptyReads(t *testing.T, r *ReadTx, tb *Table[uint64, item], ix *Index[uint64, item], c *Counter[uint64, item]) {
	t.Helper()
	if _, found, err := tb.Get(r, 1); err != nil || found {
		t.Errorf("Get = found %v, %v; want absent", found, err)
	}
	rows := 0
	if err := tb.Scan(r, Range{}, func(uint64, *item) (bool, error) { rows++; return true, nil }); err != nil || rows != 0 {
		t.Errorf("Scan = %d rows, %v; want none", rows, err)
	}
	if _, _, found, err := ix.Lookup(r, []byte("a")); err != nil || found {
		t.Errorf("Lookup = found %v, %v; want absent", found, err)
	}
	entries := 0
	if err := ix.Scan(r, Range{}, func([]byte, uint64, []byte) (bool, error) { entries++; return true, nil }); err != nil || entries != 0 {
		t.Errorf("index Scan = %d entries, %v; want none", entries, err)
	}
	if n, err := c.Value(r); err != nil || n != 0 {
		t.Errorf("counter = %d, %v; want 0", n, err)
	}
}

// damagedUnder is a closed fixture file whose bucket at path[0]/path[1]/...
// has its root page's flags overwritten. Its 300 rows per 4 KiB of page make
// the items table, its unique index and the change log too large to be
// inline whatever the OS page size (16 KiB on darwin/arm64), and the open's
// own inspection reads none of their pages.
func damagedUnder(t *testing.T, f *fixture, path ...[]byte) string {
	t.Helper()
	file := tempPath(t)
	db := mustOpen(t, file, f.schema, Options{Retention: 10})
	rows := uint64(300 * db.pageSize / 4096)
	update(t, db, func(tx *Tx) error {
		for i := range rows {
			if err := f.items.Put(tx, i, &item{Name: fmt.Sprintf("%s%05d", strings.Repeat("n", 40), i)}); err != nil {
				return err
			}
		}
		return nil
	})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := bbolt.Open(file, 0o600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	var root uint64
	err = raw.View(func(btx *bbolt.Tx) error {
		b := btx.Bucket(path[0])
		for _, name := range path[1:] {
			b = b.Bucket(name)
		}
		root = uint64(b.Root())
		return nil
	})
	if cerr := raw.Close(); err != nil || cerr != nil {
		t.Fatal(err, cerr)
	}
	if root == 0 {
		t.Fatalf("fixture: bucket %q is inline", path)
	}
	setPageFlags(t, file, root, 0xFFFF)
	return file
}

func TestInspectOnDamagedPageIsInvalidFile(t *testing.T) {
	f := newFixture(t)
	rows := [][]byte{[]byte("a"), {0x10}}
	byName := [][]byte{[]byte("a"), []byte("index:by_name")}
	changes := [][]byte{[]byte(libBucket), keyChanges}
	cases := []struct {
		read func(r *ReadTx) error
		name string
		path [][]byte
	}{
		{name: "Table.Get", path: rows, read: func(r *ReadTx) error { _, _, err := f.items.Get(r, 1); return err }},
		{name: "Table.Scan", path: rows, read: func(r *ReadTx) error {
			return f.items.Scan(r, Range{}, func(uint64, *item) (bool, error) { return true, nil })
		}},
		{name: "Index.Lookup", path: byName, read: func(r *ReadTx) error {
			_, _, _, err := f.byName.Lookup(r, []byte("absent"))
			return err
		}},
		{name: "Index.Scan", path: byName, read: func(r *ReadTx) error {
			return f.byName.Scan(r, Range{}, func([]byte, uint64, []byte) (bool, error) { return true, nil })
		}},
		{name: "ReadTx.Since", path: changes, read: func(r *ReadTx) error { _, err := r.Since(0, 10); return err }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := damagedUnder(t, f, c.path...)
			before := readFile(t, path)
			reached := false
			inspect := func(r *ReadTx) error { reached = true; return c.read(r) }
			defer func() {
				if v := recover(); v != nil {
					t.Fatalf("Open panicked: %v", v)
				}
			}()
			db, err := Open(path, f.schema, Options{Retention: 10, Inspect: inspect})
			if err == nil {
				_ = db.Close()
			}
			if !reached {
				t.Fatal("Inspect did not run")
			}
			if !errors.Is(err, ErrInvalidFile) || !strings.Contains(err.Error(), stepInspect) {
				t.Errorf("Open = %v, want ErrInvalidFile naming the inspect step", err)
			}
			if err != nil && strings.Contains(err.Error(), "ffff") {
				t.Errorf("the error carries bbolt's panic text: %v", err)
			}
			if class, ok := OpenFailure(err); class != "damaged" || !ok {
				t.Errorf("OpenFailure = %q, %v; want damaged, true", class, ok)
			}
			if !bytes.Equal(before, readFile(t, path)) {
				t.Error("the file changed")
			}
			assertUnlocked(t, path)
		})
	}
	t.Run("moved aside", func(t *testing.T) {
		path := damagedUnder(t, f, rows...)
		db, err := Open(path, f.schema, Options{InvalidFile: InvalidFileMoveAside, Inspect: cases[0].read})
		if err != nil {
			t.Fatalf("Open = %v, want a new file", err)
		}
		defer func() { _ = db.Close() }()
		if rep := db.OpenReport(); !errors.Is(rep.Invalid, ErrInvalidFile) || rep.MovedAside == "" {
			t.Errorf("OpenReport = %+v, want the damaged file moved aside", rep)
		}
	})
}
