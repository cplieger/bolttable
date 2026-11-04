package bolttable

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
	"time"

	"go.etcd.io/bbolt"
	bbolterrors "go.etcd.io/bbolt/errors"
)

func mustOpenReadOnly(t *testing.T, path string, s *Schema, o ReadOptions) *ReadDB {
	t.Helper()
	rdb, err := OpenReadOnly(path, s, o)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// dirState backdates every entry of dir, so any later write moves its
// modification time, and returns each entry's size and time.
func dirState(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	past := time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)
	m := map[string]string{}
	for _, e := range entries {
		if err := os.Chtimes(filepath.Join(dir, e.Name()), past, past); err != nil {
			t.Fatal(err)
		}
		m[e.Name()] = entryState(t, filepath.Join(dir, e.Name()))
	}
	return m
}

func entryState(t *testing.T, path string) string {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("%d bytes, modified %s", fi.Size(), fi.ModTime().UTC().Format(time.RFC3339Nano))
}

func assertDirUnchanged(t *testing.T, dir string, before map[string]string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(before) {
		t.Errorf("directory holds %d entries, want %v", len(entries), before)
	}
	for name, st := range before {
		if got := entryState(t, filepath.Join(dir, name)); got != st {
			t.Errorf("%s: %s, want %s", name, got, st)
		}
	}
}

// copyTo copies the closed file at path to a new path in a fresh directory,
// without its lock file.
func copyTo(t *testing.T, path string) string {
	t.Helper()
	dst := tempPath(t)
	if err := os.WriteFile(dst, readFile(t, path), 0o600); err != nil {
		t.Fatal(err)
	}
	return dst
}

func TestOpenReadOnlyWritesNothing(t *testing.T) {
	path := storedFile(t)
	dir := filepath.Dir(path)
	before := readFile(t, path)
	state := dirState(t, dir)
	f := newFixture(t)
	rdb, err := OpenReadOnly(path, f.schema, ReadOptions{Retention: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.View(t.Context(), func(r *ReadTx) error {
		_, _, err := f.items.Get(r, 1)
		return err
	}); err != nil {
		t.Errorf("View: %v", err)
	}
	if _, err := rdb.Stats(t.Context()); err != nil {
		t.Errorf("Stats: %v", err)
	}
	if _, err := rdb.Space(); err != nil {
		t.Errorf("Space: %v", err)
	}
	if rep, err := rdb.Verify(t.Context()); err != nil || len(rep.Mismatches) != 0 {
		t.Errorf("Verify = %+v, %v", rep, err)
	}
	if err := rdb.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, readFile(t, path)) {
		t.Error("the file's bytes changed")
	}
	assertDirUnchanged(t, dir, state)
}

// refusal opens path read-only under s, which must fail with a
// *FormatError, and asserts the open changed nothing in path's directory.
func refusal(t *testing.T, path string, s *Schema) *FormatError {
	t.Helper()
	dir := filepath.Dir(path)
	state := dirState(t, dir)
	before := readFile(t, path)
	rdb, err := OpenReadOnly(path, s, ReadOptions{})
	if err == nil {
		_ = rdb.Close()
		t.Fatal("OpenReadOnly accepted the file")
	}
	fe, ok := errors.AsType[*FormatError](err)
	if !ok {
		t.Fatalf("OpenReadOnly = %v, want a *FormatError", err)
	}
	if !errors.Is(err, ErrFormat) {
		t.Errorf("%v does not match ErrFormat", err)
	}
	if !bytes.Equal(before, readFile(t, path)) {
		t.Error("the file changed")
	}
	assertDirUnchanged(t, dir, state)
	assertUnlocked(t, path)
	return fe
}

func names(ns []NamespaceDigest) []string {
	out := make([]string, 0, len(ns))
	for _, n := range ns {
		out = append(out, n.Name)
	}
	return out
}

// withNamespaceC is the fixture's schema plus a namespace c with one table.
func withNamespaceC(f *fixture) *Schema {
	c := Must(f.schema.Namespace("c"))
	Must(Define(c, 0x30, "extra", Uint64Key{}, JSON[item](1), TableOptions{}))
	return f.schema
}

func TestOpenReadOnlyRefusesMismatch(t *testing.T) {
	t.Run("digest change", func(t *testing.T) {
		path := storedFile(t)
		want := fixtureAt(t, 2).schema.digests()["a"]
		fe := refusal(t, path, fixtureAt(t, 2).schema)
		if len(fe.Namespaces) != 1 || fe.Namespaces[0].Name != "a" {
			t.Fatalf("Namespaces = %v, want [a]", names(fe.Namespaces))
		}
		if n := fe.Namespaces[0]; n.Stored != storedDigestOf(t, path, "a") || n.Want != want {
			t.Errorf("a = %+v, want the stored and the v2 digest", n)
		}
		if fe.Missing != nil {
			t.Errorf("Missing = %v, want nil", fe.Missing)
		}
	})
	t.Run("declared namespace absent", func(t *testing.T) {
		fe := refusal(t, storedFile(t), withNamespaceC(newFixture(t)))
		if len(fe.Namespaces) != 0 {
			t.Errorf("Namespaces = %v, want none", names(fe.Namespaces))
		}
		if !slices.Equal(fe.Missing, []string{"c"}) {
			t.Errorf("Missing = %v, want [c]", fe.Missing)
		}
	})
	t.Run("stored namespace without digest", func(t *testing.T) {
		path := storedFile(t)
		rawUpdate(t, path, func(btx *bbolt.Tx) error { return btx.Bucket([]byte("b")).Delete(keyDigest) })
		fe := refusal(t, path, newFixture(t).schema)
		if len(fe.Namespaces) != 1 || fe.Namespaces[0].Name != "b" || fe.Namespaces[0].Stored != ([32]byte{}) {
			t.Errorf("Namespaces = %+v, want b with a zero Stored", fe.Namespaces)
		}
		if len(fe.Missing) != 0 {
			t.Errorf("Missing = %v, want none", fe.Missing)
		}
	})
	t.Run("file without buckets", func(t *testing.T) {
		path := tempPath(t)
		raw, err := bbolt.Open(path, 0o600, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := raw.Close(); err != nil {
			t.Fatal(err)
		}
		fe := refusal(t, path, newFixture(t).schema)
		if len(fe.Namespaces) != 0 {
			t.Errorf("Namespaces = %v, want none", names(fe.Namespaces))
		}
		if !slices.Equal(fe.Missing, []string{"a", "b", "bolttable"}) {
			t.Errorf("Missing = %v, want [a b bolttable]", fe.Missing)
		}
		if got, want := fe.Error(), ErrFormat.Error()+"; missing a, b, bolttable"; got != want {
			t.Errorf("Error() = %q, want %q", got, want)
		}
	})
	t.Run("library without change log", func(t *testing.T) {
		path := storedFile(t)
		rawUpdate(t, path, func(btx *bbolt.Tx) error { return btx.Bucket([]byte(libBucket)).DeleteBucket(keyChanges) })
		fe := refusal(t, path, newFixture(t).schema)
		if len(fe.Namespaces) != 0 || !slices.Equal(fe.Missing, []string{"bolttable"}) {
			t.Errorf("Namespaces %v, Missing %v; want none and [bolttable]", names(fe.Namespaces), fe.Missing)
		}
	})
	t.Run("other-layout library without change log", func(t *testing.T) {
		path := storedFile(t)
		rawUpdate(t, path, func(btx *bbolt.Tx) error {
			lib := btx.Bucket([]byte(libBucket))
			if err := lib.Put(keyLayout, []byte{0, 0, 0, 99}); err != nil {
				return err
			}
			return lib.DeleteBucket(keyChanges)
		})
		fe := refusal(t, path, newFixture(t).schema)
		if !slices.Equal(names(fe.Namespaces), []string{"a", "b", "bolttable"}) || len(fe.Missing) != 0 {
			t.Errorf("Namespaces %v, Missing %v; want every bucket and none", names(fe.Namespaces), fe.Missing)
		}
	})
	t.Run("mismatched and incomplete", func(t *testing.T) {
		path := storedFile(t)
		rawUpdate(t, path, func(btx *bbolt.Tx) error { return btx.Bucket([]byte("a")).DeleteBucket([]byte{0x10}) })
		fe := refusal(t, path, fixtureAt(t, 2).schema)
		if !slices.Equal(names(fe.Namespaces), []string{"a"}) || len(fe.Missing) != 0 {
			t.Errorf("Namespaces %v, Missing %v; want [a] and none", names(fe.Namespaces), fe.Missing)
		}
	})
	t.Run("both lists", func(t *testing.T) {
		fe := refusal(t, storedFile(t), withNamespaceC(fixtureAt(t, 2)))
		if !slices.Equal(names(fe.Namespaces), []string{"a"}) || !slices.Equal(fe.Missing, []string{"c"}) {
			t.Errorf("Namespaces %v, Missing %v; want [a] and [c]", names(fe.Namespaces), fe.Missing)
		}
		msg := fe.Error()
		if !strings.Contains(msg, "a (stored ") || !strings.HasSuffix(msg, "; missing c") {
			t.Errorf("Error() = %q, want a's digests and \"; missing c\"", msg)
		}
	})
}

func TestOpenReadOnlyIncompleteNamespace(t *testing.T) {
	cases := map[string]func(*bbolt.Bucket) error{
		"table bucket": func(b *bbolt.Bucket) error { return b.DeleteBucket([]byte{0x10}) },
		"index bucket": func(b *bbolt.Bucket) error { return b.DeleteBucket([]byte("index:by_name")) },
	}
	for name, del := range cases {
		t.Run(name, func(t *testing.T) {
			path := storedFile(t)
			rawUpdate(t, path, func(btx *bbolt.Tx) error { return del(btx.Bucket([]byte("a"))) })
			before := readFile(t, path)
			rdb, err := OpenReadOnly(path, newFixture(t).schema, ReadOptions{})
			if err == nil {
				_ = rdb.Close()
				t.Fatal("OpenReadOnly accepted an incomplete namespace")
			}
			if !errors.Is(err, ErrCorrupt) || errors.Is(err, ErrFormat) || !strings.Contains(err.Error(), "namespace a") {
				t.Errorf("OpenReadOnly = %v, want ErrCorrupt naming namespace a", err)
			}
			if !bytes.Equal(before, readFile(t, path)) {
				t.Error("the file changed")
			}
		})
	}
}

// assertReadOnlyInvalid opens path read-only, which must fail with
// ErrInvalidFile saying want, the file unchanged and both locks free.
func assertReadOnlyInvalid(t *testing.T, path, want string) {
	t.Helper()
	before := readFile(t, path)
	// A GC would run the finalizer of a descriptor the open leaked.
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	rdb, err := OpenReadOnly(path, newFixture(t).schema, ReadOptions{})
	if err == nil {
		_ = rdb.Close()
		t.Fatal("OpenReadOnly accepted an invalid file")
	}
	if !errors.Is(err, ErrInvalidFile) || !strings.Contains(err.Error(), want) {
		t.Errorf("OpenReadOnly = %v, want ErrInvalidFile saying %q", err, want)
	}
	if !bytes.Equal(before, readFile(t, path)) {
		t.Error("the file changed")
	}
	if m := invalidFiles(t, path); len(m) != 0 {
		t.Errorf("moved aside as %v", m)
	}
	assertUnlocked(t, path)
}

func TestOpenReadOnlyTruncated(t *testing.T) {
	t.Run("freelist past the end", func(t *testing.T) {
		path := bigFile(t)
		m := readMeta(t, path)
		if m.freelist < 3 {
			t.Fatalf("fixture: freelist page %d lies inside three pages", m.freelist)
		}
		truncate(t, path, int64(3*m.pageSize))
		assertReadOnlyInvalid(t, path, "truncated")
	})
	t.Run("freelist inside", func(t *testing.T) {
		path := bigFile(t)
		m := readMeta(t, path)
		truncate(t, path, int64(m.pgid-1)*int64(m.pageSize))
		assertReadOnlyInvalid(t, path, "truncated")
	})
}

func TestOpenReadOnlyCorruptFile(t *testing.T) {
	t.Run("garbage", func(t *testing.T) {
		assertReadOnlyInvalid(t, filled(t, 2*os.Getpagesize()), "")
	})
	t.Run("corrupt freelist page", func(t *testing.T) {
		path := storedFile(t)
		setPageFlags(t, path, readMeta(t, path).freelist, 0x02)
		assertReadOnlyInvalid(t, path, "freelist")
	})
}

func TestReadDBAfterClose(t *testing.T) {
	path := storedFile(t)
	rdb, err := OpenReadOnly(path, newFixture(t).schema, ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := rdb.Space(); !errors.Is(err, bbolterrors.ErrDatabaseNotOpen) {
		t.Errorf("Space after Close = %v", err)
	}
	if err := rdb.View(t.Context(), func(*ReadTx) error { return nil }); !errors.Is(err, bbolterrors.ErrDatabaseNotOpen) {
		t.Errorf("View after Close = %v", err)
	}
	if _, err := rdb.Verify(t.Context()); !errors.Is(err, bbolterrors.ErrDatabaseNotOpen) {
		t.Errorf("Verify after Close = %v", err)
	}
	if _, err := rdb.Stats(t.Context()); !errors.Is(err, bbolterrors.ErrDatabaseNotOpen) {
		t.Errorf("Stats after Close = %v", err)
	}
	if err := rdb.Backup(t.Context(), filepath.Join(t.TempDir(), "copy.bolt")); !errors.Is(err, bbolterrors.ErrDatabaseNotOpen) {
		t.Errorf("Backup after Close = %v", err)
	}
	if err := rdb.Close(); err != nil {
		t.Errorf("second Close = %v", err)
	}
	assertUnlocked(t, path)
}

func TestOpenReadOnlyAbsentOrEmpty(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		path := tempPath(t)
		_, err := OpenReadOnly(path, newFixture(t).schema, ReadOptions{})
		if !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("OpenReadOnly = %v, want fs.ErrNotExist", err)
		}
		if entries, _ := os.ReadDir(filepath.Dir(path)); len(entries) != 0 {
			t.Errorf("created %v", entries)
		}
	})
	t.Run("empty", func(t *testing.T) {
		path := filled(t, 0)
		_, err := OpenReadOnly(path, newFixture(t).schema, ReadOptions{})
		if !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("OpenReadOnly = %v, want fs.ErrNotExist", err)
		}
		if fi, serr := os.Stat(path); serr != nil || fi.Size() != 0 {
			t.Errorf("the file became %v, %v", fi, serr)
		}
		if _, serr := os.Stat(path + ".lock"); !errors.Is(serr, fs.ErrNotExist) {
			t.Errorf("lock file: %v", serr)
		}
	})
}

func TestOpenReadOnlyRejectsBadOptions(t *testing.T) {
	path := tempPath(t)
	cases := []struct {
		s    *Schema
		name string
		o    ReadOptions
	}{
		{name: "nil schema"},
		{name: "negative Retention", s: newFixture(t).schema, o: ReadOptions{Retention: -1}},
		{name: "negative LockWait", s: newFixture(t).schema, o: ReadOptions{LockWait: -time.Second}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := OpenReadOnly(path, c.s, c.o)
			if err == nil || errors.Is(err, fs.ErrNotExist) {
				t.Errorf("OpenReadOnly = %v, want an argument error before the file is read", err)
			}
		})
	}
}

func TestOpenReadOnlyWhileWriterOpen(t *testing.T) {
	path := tempPath(t)
	f := newFixture(t)
	mustOpen(t, path, f.schema, Options{})
	if _, err := OpenReadOnly(path, f.schema, ReadOptions{}); !errors.Is(err, ErrLocked) {
		t.Errorf("OpenReadOnly = %v, want ErrLocked", err)
	}
	start := time.Now()
	_, err := OpenReadOnly(path, f.schema, ReadOptions{LockWait: 200 * time.Millisecond})
	if waited := time.Since(start); !errors.Is(err, ErrLocked) || waited < 200*time.Millisecond {
		t.Errorf("OpenReadOnly with a 200 ms wait = %v after %v, want ErrLocked after the wait", err, waited)
	}
}

func TestWriterWaitsForReader(t *testing.T) {
	path := storedFile(t)
	f := newFixture(t)
	rdb, err := OpenReadOnly(path, f.schema, ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if db, err := Open(path, f.schema, Options{}); !errors.Is(err, ErrLocked) {
		if err == nil {
			_ = db.Close()
		}
		t.Errorf("Open while a reader is open = %v, want ErrLocked", err)
	}
	if err := rdb.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path, f.schema, Options{})
	if err != nil {
		t.Fatalf("Open after the reader closed: %v", err)
	}
	_ = db.Close()
}

func TestTwoReadersShare(t *testing.T) {
	path := storedFile(t)
	f := newFixture(t)
	first := mustOpenReadOnly(t, path, f.schema, ReadOptions{})
	second := mustOpenReadOnly(t, path, f.schema, ReadOptions{})
	for i, rdb := range []*ReadDB{first, second} {
		if err := rdb.View(t.Context(), func(r *ReadTx) error {
			v, found, err := f.items.Get(r, 1)
			if err != nil || !found || v.Name != "a" {
				t.Errorf("reader %d: Get = %+v, %v, %v", i, v, found, err)
			}
			return nil
		}); err != nil {
			t.Errorf("reader %d: View: %v", i, err)
		}
	}
	assertLocked(t, path)
}

func TestOpenReadOnlyWithoutLockFile(t *testing.T) {
	path := copyTo(t, storedFile(t))
	f := newFixture(t)
	rdb, err := OpenReadOnly(path, f.schema, ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.View(t.Context(), func(r *ReadTx) error {
		v, found, err := f.items.Get(r, 1)
		if err != nil || !found || v.Name != "a" {
			t.Errorf("Get = %+v, %v, %v", v, found, err)
		}
		return nil
	}); err != nil {
		t.Errorf("View: %v", err)
	}
	if err := rdb.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".lock"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("lock file: %v", err)
	}
}

func TestReadDBReadsFeedAndTables(t *testing.T) {
	path := tempPath(t)
	f := newFixture(t)
	db, err := Open(path, f.schema, Options{Retention: 10})
	if err != nil {
		t.Fatal(err)
	}
	for i := range uint64(3) {
		update(t, db, func(tx *Tx) error { return f.items.Put(tx, i+1, &item{Name: string(rune('x' + i)), N: 5}) })
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	rdb := mustOpenReadOnly(t, path, f.schema, ReadOptions{Retention: 10})
	if err := rdb.View(t.Context(), func(r *ReadTx) error {
		if rev, err := r.Rev(); err != nil || rev != 3 {
			t.Errorf("Rev = %d, %v, want 3", rev, err)
		}
		page, err := r.Since(1, 10)
		if err != nil || page.Full || len(page.Changes) != 2 || page.Changes[0].Rev != 2 {
			t.Errorf("Since(1) = %+v, %v, want revisions 2 and 3", page, err)
		}
		if v, found, err := f.items.Get(r, 2); err != nil || !found || v.Name != "y" {
			t.Errorf("Get(2) = %+v, %v, %v", v, found, err)
		}
		var keys []uint64
		if err := f.items.Scan(r, Range{}, func(k uint64, _ *item) (bool, error) { keys = append(keys, k); return true, nil }); err != nil || !slices.Equal(keys, []uint64{1, 2, 3}) {
			t.Errorf("Scan = %v, %v", keys, err)
		}
		if k, _, found, err := f.byName.Lookup(r, []byte("z")); err != nil || !found || k != 3 {
			t.Errorf("Lookup(z) = %d, %v, %v", k, found, err)
		}
		if n, err := f.total.Value(r); err != nil || n != 15 {
			t.Errorf("Value = %d, %v, want 15", n, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	st, err := rdb.Stats(t.Context())
	if err != nil || st.Rev != 3 || st.Commits != 0 {
		t.Errorf("Stats = %+v, %v, want Rev 3 and no commits", st, err)
	}
	if rdb.Epoch() == ([16]byte{}) {
		t.Error("Epoch is zero")
	}
}

func TestReadDBSpaceReportsFree(t *testing.T) {
	path := tempPath(t)
	f := newFixture(t)
	db, err := Open(path, f.schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	fillAndThin(t, db, f)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	rdb := mustOpenReadOnly(t, path, f.schema, ReadOptions{})
	sp, err := rdb.Space()
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if sp.FreeBytes == 0 || sp.FileBytes != fi.Size() {
		t.Errorf("Space = %+v, file %d bytes; want free bytes and the file's size", sp, fi.Size())
	}
}

func TestReadDBBackup(t *testing.T) {
	path := storedFile(t)
	f := newFixture(t)
	before := readFile(t, path)
	rdb := mustOpenReadOnly(t, path, f.schema, ReadOptions{})
	dst := filepath.Join(t.TempDir(), "copy.bolt")
	if err := rdb.Backup(t.Context(), dst); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, readFile(t, path)) {
		t.Error("Backup changed the source")
	}
	copyDB := mustOpenReadOnly(t, dst, f.schema, ReadOptions{})
	if err := copyDB.View(t.Context(), func(r *ReadTx) error {
		v, found, err := f.items.Get(r, 1)
		if err != nil || !found || v.Name != "a" {
			t.Errorf("Get from the backup = %+v, %v, %v", v, found, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
