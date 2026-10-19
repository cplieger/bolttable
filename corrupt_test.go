package bolttable

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"go.etcd.io/bbolt"
)

// secretKey is a key whose bytes must never appear in an error.
var secretKey = []byte("sk-0123456789-secret-key-bytes")

func corruptBytesRow(t *testing.T, path string, key, val []byte) {
	t.Helper()
	rawUpdate(t, path, func(btx *bbolt.Tx) error {
		return btx.Bucket([]byte("a")).Bucket([]byte{0x01}).Put(key, val)
	})
}

func assertNoKeyBytes(t *testing.T, err error) {
	t.Helper()
	for _, s := range []string{err.Error(), fmt.Sprintf("%+v", err), fmt.Sprintf("%#v", err)} {
		if strings.Contains(s, string(secretKey)) || strings.Contains(s, fmt.Sprintf("%x", secretKey)) {
			t.Errorf("error text %q holds the key bytes", s)
		}
	}
	for e := err; e != nil; e = errors.Unwrap(e) {
		if strings.Contains(e.Error(), string(secretKey)) {
			t.Errorf("wrapped error %q holds the key bytes", e)
		}
	}
}

func TestCorruptRowNamesTableAndFingerprint(t *testing.T) {
	path := tempPath(t)
	f := newBytesFixture()
	db, err := Open(path, f.schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	corruptBytesRow(t, path, secretKey, []byte(`{"name": "`+string(secretKey)))
	f = newBytesFixture()
	db = mustOpen(t, path, f.schema, Options{})
	view(t, db, func(r *ReadTx) error {
		_, found, err := f.kv.Get(r, secretKey)
		if !errors.Is(err, ErrCorrupt) || found {
			t.Fatalf("Get = %v, %v; want ErrCorrupt", found, err)
		}
		want := "table a.kv, key " + fingerprint(secretKey)
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Get error %q does not name %q", err, want)
		}
		if len(fingerprint(secretKey)) != 16 {
			t.Errorf("fingerprint %q is not 8 bytes in hex", fingerprint(secretKey))
		}
		assertNoKeyBytes(t, err)
		return nil
	})
}

func TestCorruptValueSurfacesFromScan(t *testing.T) {
	path := tempPath(t)
	db, err := Open(path, newBytesFixture().schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	corruptBytesRow(t, path, secretKey, []byte("not json"))
	f := newBytesFixture()
	db = mustOpen(t, path, f.schema, Options{})
	visited := 0
	err = db.View(t.Context(), func(r *ReadTx) error {
		return f.kv.Scan(r, Range{}, func([]byte, *item) (bool, error) { visited++; return true, nil })
	})
	if !errors.Is(err, ErrCorrupt) || visited != 0 {
		t.Fatalf("Scan = %v after %d rows; want ErrCorrupt before any", err, visited)
	}
	assertNoKeyBytes(t, err)
}

func TestCorruptKeySurfacesFromScan(t *testing.T) {
	path := tempPath(t)
	f := newFixture(t)
	db, err := Open(path, f.schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// A 3-byte key fails Uint64Key.ParseKey.
	rawUpdate(t, path, func(btx *bbolt.Tx) error {
		return btx.Bucket([]byte("a")).Bucket([]byte{0x10}).Put([]byte{1, 2, 3}, []byte(`{"name":"x"}`))
	})
	f = newFixture(t)
	db = mustOpen(t, path, f.schema, Options{})
	err = db.View(t.Context(), func(r *ReadTx) error {
		return f.items.Scan(r, Range{}, func(uint64, *item) (bool, error) { return true, nil })
	})
	if !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "table a.items, key "+fingerprint([]byte{1, 2, 3})) {
		t.Errorf("Scan = %v, want ErrCorrupt naming a.items and the key's fingerprint", err)
	}
}

func TestPutOverCorruptRowFails(t *testing.T) {
	path := tempPath(t)
	db, err := Open(path, newBytesFixture().schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	corruptBytesRow(t, path, secretKey, []byte("not json"))
	f := newBytesFixture()
	db = mustOpen(t, path, f.schema, Options{})
	err = db.Update(t.Context(), func(tx *Tx) error { return f.kv.Put(tx, secretKey, &item{Name: "new"}) })
	if !errors.Is(err, ErrCorrupt) {
		t.Errorf("Put over an undecodable row = %v, want ErrCorrupt (its old index entries are unknown)", err)
	}
	assertNoKeyBytes(t, err)
}
