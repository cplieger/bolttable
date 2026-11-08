package bolttable

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	bbolt "go.etcd.io/bbolt"
)

// namesNoFile are paths that name no file, so a lock or temporary name
// derived from one lands in the working directory or beside a directory.
var namesNoFile = []struct{ name, path string }{
	{"empty", ""},
	{"dot", "."},
	{"dotdot", ".."},
	{"trailing separator", "sub/"},
	{"directory dot", "sub/."},
	{"directory dotdot", "sub/.."},
}

// emptyWorkdir changes into a fresh directory holding only sub/ and returns
// a check that nothing else appeared in it or in sub/.
func emptyWorkdir(t *testing.T) func(label string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	return func(label string) {
		t.Helper()
		for _, d := range []string{dir, filepath.Join(dir, "sub")} {
			entries, err := os.ReadDir(d)
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range entries {
				if e.Name() != "sub" {
					t.Errorf("%s left %s in %s", label, e.Name(), d)
				}
			}
		}
	}
}

func TestPathNamingNoFileRefused(t *testing.T) {
	for _, c := range namesNoFile {
		t.Run(c.name, func(t *testing.T) {
			p := c.path
			check := emptyWorkdir(t)
			if l, err := LockPath(p, 0); err == nil {
				_ = l.Release()
				t.Errorf("LockPath(%q) succeeded", p)
			}
			check("LockPath")
			if db, err := Open(p, newFixture(t).schema, Options{}); err == nil {
				_ = db.Close()
				t.Errorf("Open(%q) succeeded", p)
			}
			check("Open")
			rdb, err := OpenReadOnly(p, newFixture(t).schema, ReadOptions{})
			if err == nil {
				_ = rdb.Close()
			}
			if err == nil || errors.Is(err, fs.ErrNotExist) {
				t.Errorf("OpenReadOnly(%q) = %v, want the argument error", p, err)
			}
			check("OpenReadOnly")
		})
	}
}

func TestBackupToPathNamingNoFileWritesNothing(t *testing.T) {
	db := mustOpen(t, tempPath(t), newFixture(t).schema, Options{})
	t.Cleanup(func() { _ = db.Close() })
	opened := 0
	boltOpen = func(p string, mode os.FileMode, o *bbolt.Options) (*bbolt.DB, error) {
		opened++
		return bbolt.Open(p, mode, o)
	}
	t.Cleanup(func() { boltOpen = bbolt.Open })
	for _, c := range namesNoFile {
		t.Run(c.name, func(t *testing.T) {
			p := c.path
			check := emptyWorkdir(t)
			opened = 0
			if err := db.Backup(t.Context(), p); err == nil {
				t.Errorf("Backup(%q) succeeded", p)
			}
			if opened != 0 {
				t.Errorf("Backup(%q) opened %d temporary files", p, opened)
			}
			check("Backup")
		})
	}
}

// An adopted lock skips LockPath, so Open must refuse "sub/" itself: under
// sub's lock it would otherwise clear stranded names inside the directory.
func TestOpenUnderAdoptedLockRefusesPathNamingNoFile(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o700); err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(sub, ".compact-tmp")
	if err := os.WriteFile(foreign, []byte("not bolttable's"), 0o600); err != nil {
		t.Fatal(err)
	}
	l := mustLock(t, sub)
	if db, err := Open(sub+"/", newFixture(t).schema, Options{Lock: l}); err == nil {
		_ = db.Close()
		t.Error("Open(sub/) under sub's lock succeeded")
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Errorf("Open removed %s: %v", foreign, err)
	}
}
