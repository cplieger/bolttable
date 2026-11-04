package bolttable

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"

	"go.etcd.io/bbolt"
)

// denseFile writes 4,000 rows and closes the file, leaving few pages free.
func denseFile(t *testing.T) string {
	t.Helper()
	path := tempPath(t)
	f := newFixture(t)
	db, err := Open(path, f.schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	update(t, db, func(tx *Tx) error {
		for i := range uint64(4000) {
			if err := f.items.Put(tx, i, &item{Name: fmt.Sprintf("%0200d", i), N: 1}); err != nil {
				return err
			}
		}
		return nil
	})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// sparseFile writes 4,000 rows, deletes 3,000 and closes the file, leaving
// most of its pages free.
func sparseFile(t *testing.T) string {
	t.Helper()
	path := tempPath(t)
	f := newFixture(t)
	db, err := Open(path, f.schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	update(t, db, func(tx *Tx) error {
		for i := range uint64(4000) {
			if err := f.items.Put(tx, i, &item{Name: fmt.Sprintf("%0200d", i), N: 1}); err != nil {
				return err
			}
		}
		return nil
	})
	update(t, db, func(tx *Tx) error {
		for i := range uint64(3000) {
			if _, err := f.items.Delete(tx, i); err != nil {
				return err
			}
		}
		return nil
	})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertRows(t *testing.T, path string, want int64) {
	t.Helper()
	f := newFixture(t)
	db := mustOpen(t, path, f.schema, Options{})
	view(t, db, func(r *ReadTx) error {
		if n, err := f.total.Value(r); n != want || err != nil {
			t.Errorf("total = %d, %v; want %d", n, err, want)
		}
		return nil
	})
	if rep, err := db.Verify(t.Context(), false); err != nil || len(rep.Mismatches) != 0 {
		t.Errorf("Verify = %+v, %v", rep, err)
	}
}

var compactAt = CompactPolicy{MinFreeRatio: 0.2, MinFreeBytes: 4096}

func size(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

func TestOpenCompactsFreeFile(t *testing.T) {
	path := sparseFile(t)
	before := size(t, path)
	db := mustOpen(t, path, newFixture(t).schema, Options{Compact: compactAt})
	if rep := db.OpenReport(); !rep.Compacted || rep.CompactErr != nil {
		t.Errorf("report = %+v, want a compaction", rep)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if after := size(t, path); after >= before {
		t.Errorf("size %d after compaction, %d before", after, before)
	}
	if _, err := os.Stat(path + ".compact-tmp"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("temporary file left: %v", err)
	}
	assertRows(t, path, 1000)
}

func TestOpenCompactThresholds(t *testing.T) {
	path := sparseFile(t)
	before := size(t, path)
	for name, c := range map[string]CompactPolicy{
		"zero policy":                 {},
		"bytes above free":            {MinFreeBytes: before},
		"ratio above free":            {MinFreeRatio: 1},
		"both above free":             {MinFreeRatio: 1, MinFreeBytes: before},
		"ratio met, bytes above free": {MinFreeRatio: 0.2, MinFreeBytes: before},
	} {
		t.Run(name, func(t *testing.T) {
			db := mustOpen(t, path, newFixture(t).schema, Options{Compact: c})
			if rep := db.OpenReport(); rep.Compacted || rep.CompactErr != nil {
				t.Errorf("report = %+v, want no compaction", rep)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if after := size(t, path); after != before {
				t.Errorf("size %d, want %d", after, before)
			}
		})
	}
	t.Run("ratio alone", func(t *testing.T) {
		db := mustOpen(t, path, newFixture(t).schema, Options{Compact: CompactPolicy{MinFreeRatio: 0.2}})
		if !db.OpenReport().Compacted {
			t.Error("a ratio-only policy did not compact")
		}
	})
}

func TestOpenCompactsAfterReset(t *testing.T) {
	path := denseFile(t)
	before := size(t, path)
	db := mustOpen(t, path, newFixture(t).schema, Options{Compact: compactAt})
	if db.OpenReport().Compacted {
		t.Fatal("fixture: the dense file compacted before any reset")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = mustOpen(t, path, fixtureAt(t, 2).schema, Options{Reset: allowAll, Compact: compactAt})
	rep := db.OpenReport()
	if len(rep.Reset) == 0 || !rep.Compacted || rep.CompactErr != nil {
		t.Errorf("report = %+v, want a reset then a compaction", rep)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if after := size(t, path); after >= before {
		t.Errorf("size %d after the reset's compaction, %d before", after, before)
	}
}

func TestOpenResetBelowThresholdDoesNotCompact(t *testing.T) {
	path := storedFile(t)
	db := mustOpen(t, path, fixtureAt(t, 2).schema, Options{Reset: allowAll, Compact: CompactPolicy{MinFreeBytes: 1 << 40}})
	if rep := db.OpenReport(); len(rep.Reset) == 0 || rep.Compacted || rep.CompactErr != nil {
		t.Errorf("report = %+v, want a reset and no compaction", rep)
	}
}

func TestOpenCompactionFailureContinues(t *testing.T) {
	path := sparseFile(t)
	original := readFile(t, path)
	crash := errors.New("crash")
	beforeSwap = func(string) error { return crash }
	t.Cleanup(func() { beforeSwap = func(string) error { return nil } })
	db := mustOpen(t, path, newFixture(t).schema, Options{Compact: compactAt})
	if rep := db.OpenReport(); rep.Compacted || !errors.Is(rep.CompactErr, crash) {
		t.Errorf("report = %+v, want CompactErr and no compaction", rep)
	}
	if _, err := os.Stat(path + ".compact-tmp"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("temporary file left: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(original, readFile(t, path)) {
		t.Error("a failed compaction changed the file")
	}
	assertRows(t, path, 1000)
}

func TestOpenCompactionDirSyncFailure(t *testing.T) {
	path := sparseFile(t)
	boom := errors.New("boom")
	syncDir = func(string) error { return boom }
	t.Cleanup(func() { syncDir = syncDirectory })
	db := mustOpen(t, path, newFixture(t).schema, Options{Compact: compactAt})
	if rep := db.OpenReport(); !rep.Compacted || !errors.Is(rep.CompactErr, boom) {
		t.Errorf("report = %+v, want Compacted and CompactErr", rep)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	syncDir = syncDirectory
	assertRows(t, path, 1000)
}

func TestOpenReusesInspectionMeasure(t *testing.T) {
	var readOnly int
	boltOpen = func(p string, mode os.FileMode, o *bbolt.Options) (*bbolt.DB, error) {
		if o.ReadOnly {
			readOnly++
		}
		return bbolt.Open(p, mode, o)
	}
	t.Cleanup(func() { boltOpen = bbolt.Open })
	path := sparseFile(t)
	db := mustOpen(t, path, newFixture(t).schema, Options{Compact: CompactPolicy{MinFreeRatio: 1}})
	if db.OpenReport().Compacted {
		t.Fatal("fixture: the policy compacted")
	}
	if readOnly != 2 {
		t.Errorf("%d read-only opens, want the inspection's two", readOnly)
	}
}

// heldFiles lists the paths of this process's open descriptors that contain
// sub, and nil where /proc/self/fd is absent.
func heldFiles(t *testing.T, sub string) []string {
	t.Helper()
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return nil
	}
	var held []string
	for _, e := range ents {
		if target, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name())); err == nil && strings.Contains(target, sub) {
			held = append(held, target)
		}
	}
	return held
}

// A corrupt page that only compaction reads: the items table's root page,
// which the inspection and the freelist load never read.
func TestOpenCompactionMeetsCorruptPage(t *testing.T) {
	for name, policy := range policies {
		t.Run(name, func(t *testing.T) {
			path := sparseFile(t)
			corruptItemsRoot(t, path)
			before := readFile(t, path)
			// A GC would run the finalizer of a descriptor Open leaked.
			defer debug.SetGCPercent(debug.SetGCPercent(-1))
			db, err := Open(path, newFixture(t).schema, Options{InvalidFile: policy, Compact: compactAt})
			if held := heldFiles(t, ".compact-tmp"); len(held) != 0 {
				t.Errorf("descriptors still open on %v", held)
			}
			if _, serr := os.Stat(path + ".compact-tmp"); !errors.Is(serr, fs.ErrNotExist) {
				t.Errorf("compact-tmp = %v, want it removed", serr)
			}
			switch policy {
			case InvalidFileFail:
				if err == nil {
					_ = db.Close()
					t.Fatal("Open succeeded")
				}
				if !errors.Is(err, ErrInvalidFile) || !strings.Contains(err.Error(), "step 8") {
					t.Errorf("Open = %v, want ErrInvalidFile naming step 8", err)
				}
				if strings.Contains(err.Error(), "invalid page type") || strings.Contains(err.Error(), "ffff") {
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
					t.Fatalf("invalid files %v, MovedAside %q; want one, named", m, rep.MovedAside)
				}
				if !errors.Is(rep.Invalid, ErrInvalidFile) || !strings.Contains(rep.Invalid.Error(), "step 8") {
					t.Errorf("Invalid = %v, want ErrInvalidFile naming step 8", rep.Invalid)
				}
				if rep.CompactErr != nil || rep.Compacted {
					t.Errorf("CompactErr %v, Compacted %v; want neither", rep.CompactErr, rep.Compacted)
				}
				if !bytes.Equal(before, readFile(t, m[0])) {
					t.Error("the moved file is not the original")
				}
				if err := db.Update(t.Context(), func(*Tx) error { return nil }); err != nil {
					t.Errorf("Update on the new file: %v", err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				assertUnlocked(t, m[0])
			}
			assertUnlocked(t, path)
		})
	}
}
