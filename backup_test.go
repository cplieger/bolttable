package bolttable

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"go.etcd.io/bbolt"
)

// dump lists every bucket path, sequence, key and value of a closed file.
func dump(t *testing.T, path string) []string {
	t.Helper()
	bdb, err := bbolt.Open(path, 0o600, &bbolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bdb.Close() }()
	var out []string
	var walk func(prefix string, b *bbolt.Bucket) error
	walk = func(prefix string, b *bbolt.Bucket) error {
		out = append(out, fmt.Sprintf("bucket %q seq %d", prefix, b.Sequence()))
		return b.ForEach(func(k, v []byte) error {
			if v == nil {
				if child := b.Bucket(k); child != nil {
					return walk(prefix+"/"+string(k), child)
				}
			}
			out = append(out, fmt.Sprintf("%q %q=%q", prefix, k, v))
			return nil
		})
	}
	if err := bdb.View(func(btx *bbolt.Tx) error {
		return btx.ForEach(func(name []byte, b *bbolt.Bucket) error { return walk(string(name), b) })
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func backupSource(t *testing.T) (*DB, string) {
	t.Helper()
	path := tempPath(t)
	f := newFixture(t)
	db := mustOpen(t, path, f.schema, Options{Retention: 10})
	update(t, db, func(tx *Tx) error {
		for i := range uint64(3000) {
			if _, err := f.items.NextID(tx); err != nil {
				return err
			}
			if err := f.items.Put(tx, i, &item{Name: fmt.Sprint("n", i), Tags: []string{"t"}, N: 1}); err != nil {
				return err
			}
		}
		if _, err := f.other.NextID(tx); err != nil {
			return err
		}
		return f.quiet.Put(tx, 1, &item{Name: "q"})
	})
	return db, path
}

func TestBackupEqualsSource(t *testing.T) {
	db, src := backupSource(t)
	dst := filepath.Join(t.TempDir(), "copy.bolt")
	if err := db.Backup(t.Context(), dst); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	want, got := dump(t, src), dump(t, dst)
	if len(want) != len(got) {
		t.Fatalf("backup has %d lines, source %d", len(got), len(want))
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("line %d: backup %s, source %s", i, got[i], want[i])
		}
	}
	if fi, err := os.Stat(dst); err != nil || fi.Mode().Perm() != 0o600 {
		t.Errorf("backup mode = %v, %v; want 0600", fi.Mode(), err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dst), ".copy.bolt.bolttable-tmp")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("temporary file left: %v", err)
	}
	mustOpen(t, dst, newFixture(t).schema, Options{})
}

func TestBackupRefusesExistingPath(t *testing.T) {
	db, _ := backupSource(t)
	dst := filepath.Join(t.TempDir(), "copy.bolt")
	if err := os.WriteFile(dst, []byte("mine"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := db.Backup(t.Context(), dst); !errors.Is(err, fs.ErrExist) {
		t.Errorf("Backup = %v, want fs.ErrExist", err)
	}
	if !bytes.Equal(readFile(t, dst), []byte("mine")) {
		t.Error("Backup overwrote an existing file")
	}
}

func TestBackupKeepsAStrangerTemporaryFile(t *testing.T) {
	db, _ := backupSource(t)
	dir := t.TempDir()
	tmp := filepath.Join(dir, ".copy.bolt.bolttable-tmp")
	if err := os.WriteFile(tmp, []byte("another backup"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := db.Backup(t.Context(), filepath.Join(dir, "copy.bolt")); err == nil {
		t.Fatal("Backup reused an existing temporary file")
	}
	if !bytes.Equal(readFile(t, tmp), []byte("another backup")) {
		t.Error("Backup removed or changed a temporary file it did not create")
	}
}

func TestBackupCompetingCreateSurvives(t *testing.T) {
	db, _ := backupSource(t)
	dst := filepath.Join(t.TempDir(), "copy.bolt")
	backupWalk = func(string) {
		if err := os.WriteFile(dst, []byte("raced"), 0o600); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(func() { backupWalk = func(string) {} })
	if err := db.Backup(t.Context(), dst); !errors.Is(err, fs.ErrExist) {
		t.Errorf("Backup = %v, want fs.ErrExist from the link", err)
	}
	if !bytes.Equal(readFile(t, dst), []byte("raced")) {
		t.Error("the competing file lost its bytes")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dst), ".copy.bolt.bolttable-tmp")); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("temporary file left: %v", err)
	}
}

// cancelAfter is a context whose Err reports Canceled from its nth call on.
type cancelAfter struct {
	context.Context //nolint:containedctx // a test context wrapper
	calls           atomic.Int32
	n               int32
}

func (c *cancelAfter) Err() error {
	if c.calls.Add(1) >= c.n {
		return context.Canceled
	}
	return nil
}

func TestBackupCancelledLeavesNoFile(t *testing.T) {
	db, _ := backupSource(t)
	dir := t.TempDir()
	dst := filepath.Join(dir, "copy.bolt")
	// The first call is Backup's own check; the second falls mid-walk.
	ctx := &cancelAfter{Context: t.Context(), n: 2}
	if err := db.Backup(ctx, dst); !errors.Is(err, context.Canceled) {
		t.Fatalf("Backup = %v, want context.Canceled", err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("a cancelled backup left %v", entries)
	}
}

func TestBackupPostLinkFailureIsErrPublished(t *testing.T) {
	boom := errors.New("boom")
	cases := map[string]func(){
		"remove":   func() { removeFile = func(string) error { return boom } },
		"dir sync": func() { syncDir = func(string) error { return boom } },
	}
	for name, inject := range cases {
		t.Run(name, func(t *testing.T) {
			db, _ := backupSource(t)
			dst := filepath.Join(t.TempDir(), "copy.bolt")
			inject()
			t.Cleanup(func() { removeFile, syncDir = os.Remove, syncDirectory })
			err := db.Backup(t.Context(), dst)
			if !errors.Is(err, ErrPublished) || !errors.Is(err, boom) {
				t.Fatalf("Backup = %v, want ErrPublished wrapping the cause", err)
			}
			removeFile, syncDir = os.Remove, syncDirectory
			mustOpen(t, dst, newFixture(t).schema, Options{})
		})
	}
}
