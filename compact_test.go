package bolttable

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"testing"
)

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

func TestCompactIfFreeAbsent(t *testing.T) {
	done, err := CompactIfFree(tempPath(t), 0.5, 1)
	if done || err != nil {
		t.Errorf("CompactIfFree on an absent path = %v, %v", done, err)
	}
}

func TestCompactIfFreeThresholds(t *testing.T) {
	path := sparseFile(t)
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	for name, th := range map[string]struct {
		ratio float64
		bytes int64
	}{"bytes above free": {0, before.Size()}, "ratio above free": {1, 0}} {
		if done, err := CompactIfFree(path, th.ratio, th.bytes); done || err != nil {
			t.Errorf("%s: CompactIfFree = %v, %v; want no compaction", name, done, err)
		}
	}
	if done, err := CompactIfFree(path, 0.2, 4096); !done || err != nil {
		t.Fatalf("CompactIfFree = %v, %v; want a compaction", done, err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() >= before.Size() {
		t.Errorf("size %d after compaction, %d before", after.Size(), before.Size())
	}
	if _, err := os.Stat(path + ".compact-tmp"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("temporary file left: %v", err)
	}
	assertRows(t, path, 1000)
	if _, err := CompactIfFree(path, -0.1, 0); err == nil {
		t.Error("CompactIfFree accepted a negative ratio")
	}
}

func TestCompactIfFreeCrashBeforeRename(t *testing.T) {
	path := sparseFile(t)
	original := readFile(t, path)
	crash := errors.New("crash")
	beforeSwap = func(string) error { return crash }
	t.Cleanup(func() { beforeSwap = func(string) error { return nil } })
	if _, err := CompactIfFree(path, 0.2, 4096); !errors.Is(err, crash) {
		t.Fatalf("CompactIfFree = %v, want the crash", err)
	}
	if string(readFile(t, path)) != string(original) {
		t.Error("the original changed before the rename")
	}
	if _, err := os.Stat(path + ".compact-tmp"); err != nil {
		t.Fatalf("the crash left no temporary file: %v", err)
	}
	beforeSwap = func(string) error { return nil }
	if done, err := CompactIfFree(path, 0.2, 4096); !done || err != nil {
		t.Fatalf("CompactIfFree after the crash = %v, %v", done, err)
	}
	assertRows(t, path, 1000)
}
