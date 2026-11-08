package bolttable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

// meta is the newer meta page of a closed file, read at bbolt v1.5.0's
// offsets (internal/common/meta.go): the page header is 16 bytes.
type meta struct {
	pageSize             int
	root, freelist, pgid uint64
}

func readMeta(t *testing.T, path string) meta {
	t.Helper()
	data := readFile(t, path)
	ps := int(binary.LittleEndian.Uint32(data[24:28]))
	m := data[:ps]
	if binary.LittleEndian.Uint64(data[ps+64:ps+72]) > binary.LittleEndian.Uint64(data[64:72]) {
		m = data[ps : 2*ps]
	}
	return meta{
		pageSize: ps,
		root:     binary.LittleEndian.Uint64(m[32:40]),
		freelist: binary.LittleEndian.Uint64(m[48:56]),
		pgid:     binary.LittleEndian.Uint64(m[56:64]),
	}
}

// patchFile rewrites the closed file at path through fn.
func patchFile(t *testing.T, path string, fn func(data []byte) []byte) {
	t.Helper()
	if err := os.WriteFile(path, fn(readFile(t, path)), 0o600); err != nil {
		t.Fatal(err)
	}
}

// setPageFlags overwrites the flags of page id, bytes 8 to 10 of its header.
func setPageFlags(t *testing.T, path string, id uint64, flags uint16) {
	t.Helper()
	ps := readMeta(t, path).pageSize
	patchFile(t, path, func(data []byte) []byte {
		binary.LittleEndian.PutUint16(data[int(id)*ps+8:], flags)
		return data
	})
}

func filled(t *testing.T, n int) string {
	t.Helper()
	path := tempPath(t)
	if err := os.WriteFile(path, bytes.Repeat([]byte{0xAB}, n), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// storedFile is a closed fixture file holding a row in each namespace.
func storedFile(t *testing.T) string {
	t.Helper()
	path := tempPath(t)
	f := newFixture(t)
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
	return path
}

// bigFile is a closed fixture file of about 12 MiB, then two small deletes,
// which move the freelist page into a page the bulk write freed.
func bigFile(t *testing.T) string {
	t.Helper()
	path := tempPath(t)
	f := newFixture(t)
	db, err := Open(path, f.schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	name := strings.Repeat("v", 4000)
	update(t, db, func(tx *Tx) error {
		for i := range uint64(3000) {
			if err := f.quiet.Put(tx, i, &item{Name: name}); err != nil {
				return err
			}
		}
		return nil
	})
	for _, k := range []uint64{10, 20} {
		update(t, db, func(tx *Tx) error { _, err := f.quiet.Delete(tx, k); return err })
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func truncate(t *testing.T, path string, size int64) {
	t.Helper()
	if err := os.Truncate(path, size); err != nil {
		t.Fatal(err)
	}
}

// mismatchSchema differs from the fixture's in every namespace's digest.
func mismatchSchema() *Schema {
	s, _, _ := resetSchema(resetTables{"x": 0x10})
	return s
}

var allowAll = ResetPolicy{Allow: func(NamespaceDigest) bool { return true }}

func invalidFiles(t *testing.T, path string) []string {
	t.Helper()
	m, err := filepath.Glob(path + ".*.invalid")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// openPanic runs Open, which must panic, and returns the recovered value.
func openPanic(t *testing.T, path string, s *Schema, o Options) (v any) {
	t.Helper()
	defer func() { v = recover() }()
	db, err := Open(path, s, o)
	if err == nil {
		_ = db.Close()
	}
	t.Errorf("Open returned %v instead of panicking", err)
	return nil
}

// panicAt makes stepHook panic with v at each named step.
func panicAt(t *testing.T, at map[string]any) {
	t.Helper()
	stepHook = func(step string) {
		if v, ok := at[step]; ok {
			panic(v)
		}
	}
	t.Cleanup(func() { stepHook = nil })
}

type testPanic string

// assertUnlocked fails the test when a handle still holds the lock file or
// bbolt's lock on the data file at path.
func assertUnlocked(t *testing.T, path string) {
	t.Helper()
	assertDataUnlocked(t, path)
	assertLockFree(t, path)
	assertUnmapped(t, path)
}

// assertLockFree fails the test when a handle still holds path's lock file.
func assertLockFree(t *testing.T, path string) {
	t.Helper()
	l, err := LockPath(path, 0)
	if err != nil {
		t.Errorf("LockPath after Open = %v", err)
		return
	}
	_ = l.Release()
}

func TestOpenClassifiesInvalidFile(t *testing.T) {
	cases := []struct {
		build     func(t *testing.T) string
		name      string
		panicText string
		wantText  string
	}{
		{name: "garbage of two host pages", build: func(t *testing.T) string { return filled(t, 2*os.Getpagesize()) }},
		{name: "garbage of 100 bytes", build: func(t *testing.T) string { return filled(t, 100) }},
		{name: "both meta checksums flipped", build: func(t *testing.T) string {
			path := storedFile(t)
			ps := readMeta(t, path).pageSize
			patchFile(t, path, func(data []byte) []byte {
				data[72] ^= 0xFF
				data[ps+72] ^= 0xFF
				return data
			})
			return path
		}},
		{name: "both meta versions changed", build: func(t *testing.T) string {
			path := storedFile(t)
			ps := readMeta(t, path).pageSize
			patchFile(t, path, func(data []byte) []byte {
				binary.LittleEndian.PutUint32(data[20:], 9)
				binary.LittleEndian.PutUint32(data[ps+20:], 9)
				return data
			})
			return path
		}},
		{name: "truncated with the freelist past the end", wantText: "truncated", build: func(t *testing.T) string {
			path := bigFile(t)
			m := readMeta(t, path)
			if m.freelist < 3 {
				t.Fatalf("fixture: freelist page %d lies inside three pages", m.freelist)
			}
			truncate(t, path, int64(3*m.pageSize))
			return path
		}},
		{name: "truncated with the freelist inside", wantText: "truncated", build: func(t *testing.T) string {
			path := bigFile(t)
			m := readMeta(t, path)
			if m.freelist+1 >= m.pgid-1 {
				t.Fatalf("fixture: freelist page %d is not below the last page %d", m.freelist, m.pgid-1)
			}
			truncate(t, path, int64(m.pgid-1)*int64(m.pageSize))
			return path
		}},
		{name: "corrupt root page", panicText: "unexpected type", build: func(t *testing.T) string {
			path := storedFile(t)
			setPageFlags(t, path, readMeta(t, path).root, 0xFFFF)
			return path
		}},
		{name: "corrupt freelist page", panicText: "invalid freelist page", wantText: "freelist", build: func(t *testing.T) string {
			path := storedFile(t)
			setPageFlags(t, path, readMeta(t, path).freelist, 0x02)
			return path
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := c.build(t)
			before := readFile(t, path)
			// A GC would run the finalizer of a descriptor Open leaked,
			// which closes it and frees its flock.
			defer debug.SetGCPercent(debug.SetGCPercent(-1))
			db, err := Open(path, newFixture(t).schema, Options{})
			if err == nil {
				_ = db.Close()
				t.Fatal("Open accepted an invalid file")
			}
			if !errors.Is(err, ErrInvalidFile) {
				t.Errorf("Open = %v, want ErrInvalidFile", err)
			}
			if !strings.Contains(err.Error(), c.wantText) {
				t.Errorf("Open = %v, want it to say %q", err, c.wantText)
			}
			if c.panicText != "" && strings.Contains(err.Error(), c.panicText) {
				t.Errorf("the error carries bbolt's panic text: %v", err)
			}
			if !bytes.Equal(before, readFile(t, path)) {
				t.Error("Open changed an invalid file")
			}
			assertUnlocked(t, path)
		})
	}
}

func TestValidSmallPageFileIsNotInvalid(t *testing.T) {
	path := tempPath(t)
	raw, err := bbolt.Open(path, 0o600, &bbolt.Options{PageSize: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() >= int64(2*os.Getpagesize()) {
		t.Fatalf("fixture: %d bytes is not below two host pages", fi.Size())
	}
	f := newFixture(t)
	db, err := Open(path, f.schema, Options{InvalidFile: InvalidFileMoveAside})
	if err != nil {
		t.Fatalf("Open of a valid 1 KiB-page file: %v", err)
	}
	if got := db.OpenReport().MovedAside; got != "" {
		t.Errorf("MovedAside = %q", got)
	}
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "kept"}) })
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if m := invalidFiles(t, path); len(m) != 0 {
		t.Errorf("invalid files %v", m)
	}
	f = newFixture(t)
	db = mustOpen(t, path, f.schema, Options{})
	view(t, db, func(r *ReadTx) error {
		if v, found, err := f.items.Get(r, 1); err != nil || !found || v.Name != "kept" {
			t.Errorf("Get after reopen = %+v, %v, %v", v, found, err)
		}
		return nil
	})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err = bbolt.Open(path, 0o600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	if ps := raw.Info().PageSize; ps != 1024 {
		t.Errorf("page size = %d, want 1024", ps)
	}
}

func TestTooSmallStaysUnclassified(t *testing.T) {
	path := filled(t, 2*os.Getpagesize()-1)
	before := readFile(t, path)
	db, err := Open(path, newFixture(t).schema, Options{InvalidFile: InvalidFileMoveAside})
	if err == nil {
		_ = db.Close()
		t.Fatal("Open accepted a file shorter than two pages")
	}
	if errors.Is(err, ErrInvalidFile) {
		t.Errorf("Open = %v, want an unclassified error", err)
	}
	if !bytes.Equal(before, readFile(t, path)) {
		t.Error("the file changed")
	}
	if m := invalidFiles(t, path); len(m) != 0 {
		t.Errorf("moved aside as %v", m)
	}
}

func TestOpenPathNotAFile(t *testing.T) {
	dir := tempPath(t)
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	file := filled(t, 10)
	for name, path := range map[string]string{"a directory": dir, "under a regular file": filepath.Join(file, "db")} {
		t.Run(name, func(t *testing.T) {
			db, err := Open(path, newFixture(t).schema, Options{InvalidFile: InvalidFileMoveAside})
			if err == nil {
				_ = db.Close()
				t.Fatal("Open succeeded")
			}
			if errors.Is(err, ErrInvalidFile) {
				t.Errorf("Open = %v, want an unclassified error", err)
			}
			if m := invalidFiles(t, path); len(m) != 0 {
				t.Errorf("moved aside as %v", m)
			}
		})
	}
	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		t.Errorf("the directory = %v, %v", fi, err)
	}
}

func TestForeignBboltHolderIsErrLocked(t *testing.T) {
	path := storedFile(t)
	raw, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	start := time.Now()
	db, err := Open(path, newFixture(t).schema, Options{})
	if err == nil {
		_ = db.Close()
		t.Fatal("Open succeeded while bbolt held the file")
	}
	if !errors.Is(err, ErrLocked) {
		t.Errorf("Open = %v, want ErrLocked", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("Open waited %v", d)
	}
}

func TestGuardDoesNotCoverAllow(t *testing.T) {
	path := storedFile(t)
	v := openPanic(t, path, mismatchSchema(), Options{Reset: ResetPolicy{Allow: func(NamespaceDigest) bool { panic(testPanic("allow")) }}})
	if v != testPanic("allow") {
		t.Errorf("recovered %v, want Allow's panic", v)
	}
	assertUnlocked(t, path)
}

func TestPanicInOpenPropagates(t *testing.T) {
	for name, c := range map[string]struct {
		schema func() *Schema
		step   string
	}{
		"reset":        {mismatchSchema, "reset"},
		"start":        {func() *Schema { return newFixture(t).schema }, "start"},
		"reset ensure": {mismatchSchema, "reset-ensure"},
	} {
		t.Run(name, func(t *testing.T) {
			path := storedFile(t)
			before := readFile(t, path)
			panicAt(t, map[string]any{c.step: testPanic(c.step)})
			v := openPanic(t, path, c.schema(), Options{InvalidFile: InvalidFileMoveAside, Reset: allowAll})
			if v != testPanic(c.step) {
				t.Errorf("recovered %v (%T), want the hook's panic", v, v)
			}
			if m := invalidFiles(t, path); len(m) != 0 {
				t.Errorf("moved aside as %v", m)
			}
			if !bytes.Equal(before, readFile(t, path)) {
				t.Error("the file changed")
			}
			assertUnlocked(t, path)
		})
	}
}

func TestMarkClearedAfterRecoveredPanic(t *testing.T) {
	path := storedFile(t)
	before := readFile(t, path)
	panicAt(t, map[string]any{"reset-walk": testPanic("walk"), "start": testPanic("start")})
	v := openPanic(t, path, mismatchSchema(), Options{InvalidFile: InvalidFileMoveAside, Reset: allowAll})
	if v != testPanic("start") {
		t.Errorf("recovered %v (%T), want the start panic", v, v)
	}
	m := invalidFiles(t, path)
	if len(m) != 1 {
		t.Fatalf("invalid files %v, want one", m)
	}
	if !bytes.Equal(before, readFile(t, m[0])) {
		t.Error("the moved file is not the original")
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("the new file: %v", err)
	}
	assertUnlocked(t, path)
	assertUnlocked(t, m[0])
}

// corruptTableRoot is a stored file whose items table, too large to be
// inline, has its root page's flags overwritten: a page the inspection never
// reads and the reset's subtree walk does.
func corruptTableRoot(t *testing.T) string {
	t.Helper()
	path := tempPath(t)
	f := newFixture(t)
	db, err := Open(path, f.schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	update(t, db, func(tx *Tx) error {
		for i := range uint64(300) {
			if err := f.items.Put(tx, i, &item{Name: strings.Repeat("n", 40) + string(rune('a'+i%26)) + string(rune('a'+i/26))}); err != nil {
				return err
			}
		}
		return nil
	})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	corruptItemsRoot(t, path)
	return path
}

// corruptItemsRoot overwrites the flags of the root page of the closed
// fixture file's items table, which must not be inline.
func corruptItemsRoot(t *testing.T, path string) {
	t.Helper()
	raw, err := bbolt.Open(path, 0o600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	var root uint64
	err = raw.View(func(btx *bbolt.Tx) error {
		root = uint64(btx.Bucket([]byte("a")).Bucket([]byte{0x10}).Root())
		return nil
	})
	if cerr := raw.Close(); err != nil || cerr != nil {
		t.Fatal(err, cerr)
	}
	if root == 0 {
		t.Fatal("fixture: the items table is inline")
	}
	setPageFlags(t, path, root, 0xFFFF)
}

// assertResetInvalid opens path, whose reset meets a corrupt page or a
// classified panic, under both policies' expectations.
func assertResetInvalid(t *testing.T, path string, policy InvalidFilePolicy) {
	t.Helper()
	before := readFile(t, path)
	db, err := Open(path, mismatchSchema(), Options{InvalidFile: policy, Reset: allowAll})
	switch policy {
	case InvalidFileFail:
		if err == nil {
			_ = db.Close()
			t.Fatal("Open succeeded")
		}
		if !errors.Is(err, ErrInvalidFile) || !strings.Contains(err.Error(), "step 7") {
			t.Errorf("Open = %v, want ErrInvalidFile naming step 7", err)
		}
		if strings.Contains(err.Error(), "unexpected type") || strings.Contains(err.Error(), "ffff") {
			t.Errorf("the error carries bbolt's panic text: %v", err)
		}
		if !bytes.Equal(before, readFile(t, path)) {
			t.Error("the file changed")
		}
		if m := invalidFiles(t, path); len(m) != 0 {
			t.Errorf("moved aside as %v", m)
		}
	case InvalidFileMoveAside:
		if err != nil {
			t.Fatalf("Open = %v, want a new file", err)
		}
		rep := db.OpenReport()
		m := invalidFiles(t, path)
		if len(m) != 1 || rep.MovedAside != m[0] {
			t.Errorf("invalid files %v, MovedAside %q; want one, named", m, rep.MovedAside)
		}
		if !errors.Is(rep.Invalid, ErrInvalidFile) {
			t.Errorf("Invalid = %v", rep.Invalid)
		}
		if len(m) == 1 && !bytes.Equal(before, readFile(t, m[0])) {
			t.Error("the moved file is not the original")
		}
		if err := db.Update(t.Context(), func(*Tx) error { return nil }); err != nil {
			t.Errorf("Update on the new file: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		for _, p := range m {
			assertUnlocked(t, p)
		}
	}
	assertUnlocked(t, path)
}

var policies = map[string]InvalidFilePolicy{"fail": InvalidFileFail, "move aside": InvalidFileMoveAside}

func TestCorruptPageInResetIsInvalid(t *testing.T) {
	for name, policy := range policies {
		t.Run(name, func(t *testing.T) { assertResetInvalid(t, corruptTableRoot(t), policy) })
	}
}

func TestPanicInResetWalkIsInvalid(t *testing.T) {
	for _, step := range []string{"reset-walk", "reset-commit"} {
		for name, policy := range policies {
			t.Run(step+" "+name, func(t *testing.T) {
				panicAt(t, map[string]any{step: testPanic(step)})
				assertResetInvalid(t, storedFile(t), policy)
			})
		}
	}
}

// A panic in the reset's deferred Rollback leaves the transaction open, and
// bbolt's Close would wait for it forever: a broken Open hangs the binary.
func TestRollbackPanicDoesNotHang(t *testing.T) {
	for name, policy := range policies {
		t.Run(name, func(t *testing.T) {
			panicAt(t, map[string]any{"reset-commit": testPanic("commit"), "reset-rollback": testPanic("rollback")})
			assertResetInvalid(t, storedFile(t), policy)
		})
	}
}

// A rollback that panics again when retried leaves bbolt's Close nothing
// it can wait for, so the lock must still be released.
func TestRetriedRollbackPanicReleasesLock(t *testing.T) {
	path := storedFile(t)
	before := readFile(t, path)
	panicAt(t, map[string]any{"reset-commit": testPanic("commit"), "reset-rollback": testPanic("rollback"), "rollback-retry": testPanic("retry")})
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	db, err := Open(path, mismatchSchema(), Options{Reset: allowAll})
	if err == nil {
		_ = db.Close()
		t.Fatal("Open succeeded")
	}
	if !errors.Is(err, ErrInvalidFile) || !strings.Contains(err.Error(), "step 7") {
		t.Errorf("Open = %v, want ErrInvalidFile naming step 7", err)
	}
	if !bytes.Equal(before, readFile(t, path)) {
		t.Error("the file changed")
	}
	assertDataUnlocked(t, path)
	assertLockFree(t, path)
	stepHook = nil
	mustOpen(t, path, mismatchSchema(), Options{Reset: allowAll})
}

// A panic in the deferred Rollback of start's namespace creation leaves its
// write transaction open, which bbolt's Close would wait for forever. A
// retry that succeeds closes the file; one that panics again releases it.
func TestStartRollbackPanicReleasesFile(t *testing.T) {
	released := func(t *testing.T, path string) {
		t.Helper()
		assertDataUnlocked(t, path)
		assertLockFree(t, path)
	}
	for name, c := range map[string]struct {
		at    map[string]any
		check func(*testing.T, string)
	}{
		"retry succeeds": {map[string]any{"start-commit": testPanic("commit"), "start-rollback": testPanic("rollback")}, assertUnlocked},
		"retry panics":   {map[string]any{"start-commit": testPanic("commit"), "start-rollback": testPanic("rollback"), "rollback-retry": testPanic("retry")}, released},
	} {
		t.Run(name, func(t *testing.T) {
			path := storedFile(t)
			before := readFile(t, path)
			panicAt(t, c.at)
			defer debug.SetGCPercent(debug.SetGCPercent(-1))
			v := openPanic(t, path, withNamespaceC(newFixture(t)), Options{})
			if v != testPanic("rollback") {
				t.Errorf("recovered %v (%T), want the rollback's panic", v, v)
			}
			if !bytes.Equal(before, readFile(t, path)) {
				t.Error("the file changed")
			}
			c.check(t, path)
			stepHook = nil
			mustOpen(t, path, withNamespaceC(newFixture(t)), Options{})
		})
	}
}

// A panic bbolt raises inside its open, after it mapped and locked the
// file, returns no handle to close; the lock must be released anyway.
func TestPanicInBoltOpenReleasesLock(t *testing.T) {
	path := storedFile(t)
	boltOpen = func(p string, mode os.FileMode, o *bbolt.Options) (*bbolt.DB, error) {
		bdb, err := bbolt.Open(p, mode, o)
		if err == nil {
			t.Cleanup(func() { _ = bdb.Close() })
		}
		panic(testPanic("inside bbolt's open"))
	}
	t.Cleanup(func() { boltOpen = bbolt.Open })
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	db, err := Open(path, newFixture(t).schema, Options{})
	if err == nil {
		_ = db.Close()
		t.Fatal("Open succeeded")
	}
	if !errors.Is(err, ErrInvalidFile) {
		t.Errorf("Open = %v, want ErrInvalidFile", err)
	}
	assertDataUnlocked(t, path)
	assertLockFree(t, path)
	boltOpen = bbolt.Open
	mustOpen(t, path, newFixture(t).schema, Options{})
}

// A file whose Open failed on a corrupt freelist opens again in the same
// process once its bytes are repaired.
func TestFailedOpenFileReopens(t *testing.T) {
	path := storedFile(t)
	good := readFile(t, path)
	setPageFlags(t, path, readMeta(t, path).freelist, 0x02)
	defer debug.SetGCPercent(debug.SetGCPercent(-1))
	f := newFixture(t)
	if db, err := Open(path, f.schema, Options{}); !errors.Is(err, ErrInvalidFile) {
		if err == nil {
			_ = db.Close()
		}
		t.Fatalf("Open = %v, want ErrInvalidFile", err)
	}
	if rdb, err := OpenReadOnly(path, f.schema, ReadOptions{}); !errors.Is(err, ErrInvalidFile) {
		if err == nil {
			_ = rdb.Close()
		}
		t.Fatalf("OpenReadOnly = %v, want ErrInvalidFile", err)
	}
	if err := os.WriteFile(path, good, 0o600); err != nil {
		t.Fatal(err)
	}
	rdb, err := OpenReadOnly(path, f.schema, ReadOptions{})
	if err != nil {
		t.Fatalf("OpenReadOnly of the repaired file: %v", err)
	}
	if err := rdb.Close(); err != nil {
		t.Fatal(err)
	}
	db := mustOpen(t, path, f.schema, Options{})
	view(t, db, func(r *ReadTx) error {
		if v, found, err := f.items.Get(r, 1); err != nil || !found || v.Name != "a" {
			t.Errorf("Get after reopen = %+v, %v, %v", v, found, err)
		}
		return nil
	})
}

func TestOpenMovesAsideOnce(t *testing.T) {
	path := filled(t, 2*os.Getpagesize())
	before := readFile(t, path)
	boltOpen = func(p string, mode os.FileMode, o *bbolt.Options) (*bbolt.DB, error) {
		if !o.ReadOnly {
			panic(testPanic("step 9"))
		}
		return bbolt.Open(p, mode, o)
	}
	t.Cleanup(func() { boltOpen = bbolt.Open })
	db, err := Open(path, newFixture(t).schema, Options{InvalidFile: InvalidFileMoveAside})
	if err == nil {
		_ = db.Close()
		t.Fatal("Open succeeded")
	}
	if !errors.Is(err, ErrInvalidFile) {
		t.Errorf("Open = %v, want ErrInvalidFile", err)
	}
	m := invalidFiles(t, path)
	if len(m) != 1 {
		t.Fatalf("invalid files %v, want one", m)
	}
	if !bytes.Equal(before, readFile(t, m[0])) {
		t.Error("the moved file is not the original")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the new path = %v, want it left absent", err)
	}
}

func TestOpenRemovesStrandedTemps(t *testing.T) {
	path := storedFile(t)
	before := readFile(t, path)
	temps := []string{path + ".compact-tmp", path + ".copy-tmp"}
	for _, tmp := range temps {
		if err := os.WriteFile(tmp, []byte("stranded"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	db := mustOpen(t, path, newFixture(t).schema, Options{})
	for _, tmp := range temps {
		if _, err := os.Stat(tmp); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s: %v, want it removed", tmp, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, readFile(t, path)) {
		t.Error("the data file changed")
	}
	t.Run("removal fails", func(t *testing.T) {
		path := storedFile(t)
		if err := os.WriteFile(path+".copy-tmp", nil, 0o600); err != nil {
			t.Fatal(err)
		}
		boom := errors.New("boom")
		removeFile = func(string) error { return boom }
		t.Cleanup(func() { removeFile = os.Remove })
		if _, err := Open(path, newFixture(t).schema, Options{}); !errors.Is(err, boom) {
			t.Errorf("Open = %v, want the removal failure", err)
		}
		removeFile = os.Remove
		assertUnlocked(t, path)
	})
}

func TestOpenInvalidMoveAside(t *testing.T) {
	path := filled(t, 2*os.Getpagesize())
	before := readFile(t, path)
	f := newFixture(t)
	db := mustOpen(t, path, f.schema, Options{InvalidFile: InvalidFileMoveAside})
	rep := db.OpenReport()
	if !errors.Is(rep.Invalid, ErrInvalidFile) {
		t.Errorf("Invalid = %v, want ErrInvalidFile", rep.Invalid)
	}
	stamp, ok := strings.CutPrefix(rep.MovedAside, path+".")
	stamp, ok2 := strings.CutSuffix(stamp, ".invalid")
	if !ok || !ok2 {
		t.Fatalf("MovedAside = %q, want %s.<stamp>.invalid", rep.MovedAside, path)
	}
	if _, err := time.Parse("20060102T150405.000000000Z", stamp); err != nil {
		t.Errorf("MovedAside's stamp %q: %v", stamp, err)
	}
	if !bytes.Equal(before, readFile(t, rep.MovedAside)) {
		t.Error("the moved file is not the original")
	}
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "new"}) })
	if rev := headRev(t, db); rev != 1 {
		t.Errorf("the new file's head = %d, want 1", rev)
	}
}

// assertMovedAsideError checks an Open error raised after the file at path
// was moved aside: it names the moved file, wraps ErrInvalidFile and want,
// the moved file holds before unless before is nil, and no new file or lock
// is left behind.
func assertMovedAsideError(t *testing.T, path string, before []byte, err, want error) {
	t.Helper()
	if err == nil {
		t.Fatal("Open succeeded")
	}
	m := invalidFiles(t, path)
	if len(m) != 1 {
		t.Fatalf("invalid files %v, want one", m)
	}
	if !strings.Contains(err.Error(), m[0]) {
		t.Errorf("Open = %v, want it to name %s", err, m[0])
	}
	if !errors.Is(err, ErrInvalidFile) {
		t.Errorf("Open = %v, want ErrInvalidFile", err)
	}
	if want != nil && !errors.Is(err, want) {
		t.Errorf("Open = %v, want %v", err, want)
	}
	if before != nil && !bytes.Equal(before, readFile(t, m[0])) {
		t.Error("the moved file is not the original")
	}
	if _, serr := os.Stat(path); !errors.Is(serr, os.ErrNotExist) {
		t.Errorf("the original path = %v, want it absent", serr)
	}
	assertDataUnlocked(t, m[0])
	assertLockFree(t, path)
}

func TestOpenErrorAfterMoveAsideNamesMovedFile(t *testing.T) {
	t.Run("Force names nothing", func(t *testing.T) {
		path := filled(t, 2*os.Getpagesize())
		before := readFile(t, path)
		db, err := Open(path, newFixture(t).schema, Options{InvalidFile: InvalidFileMoveAside, Reset: ResetPolicy{Force: []string{"not-a-namespace"}}})
		if err == nil {
			_ = db.Close()
		}
		assertMovedAsideError(t, path, before, err, nil)
	})
	t.Run("the new file's open fails", func(t *testing.T) {
		path := filled(t, 2*os.Getpagesize())
		before := readFile(t, path)
		boom := errors.New("create failed")
		boltOpen = func(p string, mode os.FileMode, o *bbolt.Options) (*bbolt.DB, error) {
			if !o.ReadOnly {
				return nil, boom
			}
			return bbolt.Open(p, mode, o)
		}
		t.Cleanup(func() { boltOpen = bbolt.Open })
		db, err := Open(path, newFixture(t).schema, Options{InvalidFile: InvalidFileMoveAside})
		if err == nil {
			_ = db.Close()
		}
		assertMovedAsideError(t, path, before, err, boom)
	})
}

func TestOpenMoveAsideDirSyncFails(t *testing.T) {
	path := filled(t, 2*os.Getpagesize())
	before := readFile(t, path)
	boom := errors.New("dir sync failed")
	syncDir = func(string) error { return boom }
	t.Cleanup(func() { syncDir = syncDirectory })
	db, err := Open(path, newFixture(t).schema, Options{InvalidFile: InvalidFileMoveAside})
	if err == nil {
		_ = db.Close()
	}
	assertMovedAsideError(t, path, before, err, boom)
}

func TestOpenRejectsBadOptions(t *testing.T) {
	for name, o := range map[string]Options{
		"negative Keep":          {Reset: ResetPolicy{Keep: -1}},
		"ratio below zero":       {Compact: CompactPolicy{MinFreeRatio: -0.1}},
		"ratio above one":        {Compact: CompactPolicy{MinFreeRatio: 1.1}},
		"NaN ratio":              {Compact: CompactPolicy{MinFreeRatio: math.NaN()}},
		"negative MinFreeBytes":  {Compact: CompactPolicy{MinFreeBytes: -1}},
		"unknown InvalidFile":    {InvalidFile: InvalidFileMoveAside + 1},
		"negative LockWait":      {LockWait: -time.Second},
		"negative Retention":     {Retention: -1},
		"negative MaxBatchSize":  {MaxBatchSize: -1},
		"negative MaxBatchDelay": {MaxBatchDelay: -time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			path := tempPath(t)
			if db, err := Open(path, newFixture(t).schema, o); err == nil {
				_ = db.Close()
				t.Fatal("Open accepted the options")
			}
			for _, p := range []string{path, path + ".lock"} {
				if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("a refused Open left %s: %v", p, err)
				}
			}
		})
	}
}

func TestOpenReportIsACopy(t *testing.T) {
	db := mustOpen(t, storedFile(t), fixtureAt(t, 2).schema, Options{Reset: allowAll})
	first := db.OpenReport()
	if len(first.Reset) != 1 {
		t.Fatalf("Reset = %+v", first.Reset)
	}
	first.Reset[0].Name = "changed"
	if got := db.OpenReport().Reset[0].Name; got != "a" {
		t.Errorf("a caller's write reached the report: %q", got)
	}
}

func TestOpenForceAfterMoveAside(t *testing.T) {
	path := corruptTableRoot(t)
	s := NewSchema("test")
	a := Must(s.Namespace("a"))
	Must(Define(a, 0x10, "items", Uint64Key{}, JSON[item](2), TableOptions{}))
	o := Options{InvalidFile: InvalidFileMoveAside, Reset: ResetPolicy{Allow: func(NamespaceDigest) bool { return true }, Force: []string{"b"}}}
	db, err := Open(path, s, o)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open forced a namespace the new file neither stores nor declares")
	}
	if !strings.Contains(err.Error(), `"b"`) {
		t.Errorf("Open = %v, want an error naming b", err)
	}
	if m := invalidFiles(t, path); len(m) != 1 {
		t.Errorf("invalid files %v, want the corrupt file moved aside once", m)
	}
}
