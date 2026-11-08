//go:build unix

package bolttable

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// sink keeps the faulting load: a discarded read compiles to a bounds check
// alone.
var sink byte

// faultAt makes stepHook read past the end of a mapped one-page file the
// first time it reaches step, which raises a real SIGBUS.
func faultAt(t *testing.T, step string) {
	t.Helper()
	ps := os.Getpagesize()
	path := filepath.Join(t.TempDir(), "page")
	if err := os.WriteFile(path, make([]byte, ps), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	m, err := syscall.Mmap(int(f.Fd()), 0, 2*ps, syscall.PROT_READ, syscall.MAP_SHARED)
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Munmap(m) })
	fired := false
	stepHook = func(s string) {
		if s == step && !fired {
			fired = true
			sink = m[ps]
		}
	}
	t.Cleanup(func() { stepHook = nil })
}

func TestFaultInResetIsInvalid(t *testing.T) {
	for name, policy := range policies {
		t.Run(name, func(t *testing.T) {
			faultAt(t, "reset")
			assertResetInvalid(t, storedFile(t), policy)
		})
	}
}

func TestFaultInStartIsInvalid(t *testing.T) {
	t.Run("fail", func(t *testing.T) {
		path := storedFile(t)
		before := readFile(t, path)
		faultAt(t, "start")
		db, err := Open(path, newFixture(t).schema, Options{})
		if err == nil {
			_ = db.Close()
			t.Fatal("Open succeeded")
		}
		if !errors.Is(err, ErrInvalidFile) || !strings.Contains(err.Error(), "step 9") {
			t.Errorf("Open = %v, want ErrInvalidFile naming step 9", err)
		}
		if !bytes.Equal(before, readFile(t, path)) {
			t.Error("the file changed")
		}
		if m := invalidFiles(t, path); len(m) != 0 {
			t.Errorf("moved aside as %v", m)
		}
		assertUnlocked(t, path)
	})
	t.Run("move aside", func(t *testing.T) {
		path := storedFile(t)
		before := readFile(t, path)
		faultAt(t, "start")
		f := newFixture(t)
		db, err := Open(path, f.schema, Options{InvalidFile: InvalidFileMoveAside})
		if err != nil {
			t.Fatalf("Open = %v, want a new file", err)
		}
		rep := db.OpenReport()
		m := invalidFiles(t, path)
		if len(m) != 1 || rep.MovedAside != m[0] {
			t.Fatalf("invalid files %v, MovedAside %q; want one, named", m, rep.MovedAside)
		}
		if !errors.Is(rep.Invalid, ErrInvalidFile) || !strings.Contains(rep.Invalid.Error(), "step 9") {
			t.Errorf("Invalid = %v, want ErrInvalidFile naming step 9", rep.Invalid)
		}
		if !bytes.Equal(before, readFile(t, m[0])) {
			t.Error("the moved file is not the original")
		}
		update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "new"}) })
		if rev := headRev(t, db); rev != 1 {
			t.Errorf("the new file's head = %d, want 1", rev)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		assertUnlocked(t, m[0])
		assertUnlocked(t, path)
	})
	t.Run("move aside rechecks Force", func(t *testing.T) {
		f := newFixture(t)
		path := closedFile(t, withNamespaceC(fixtureAt(t, 1)))
		faultAt(t, "start")
		o := Options{InvalidFile: InvalidFileMoveAside, Reset: ResetPolicy{Allow: allowAll.Allow, Force: []string{"c"}}}
		db, err := Open(path, f.schema, o)
		if err == nil {
			_ = db.Close()
		}
		assertMovedAsideError(t, path, nil, err, nil)
		if err != nil && !strings.Contains(err.Error(), `"c"`) {
			t.Errorf("Open = %v, want it to name c", err)
		}
	})
}
