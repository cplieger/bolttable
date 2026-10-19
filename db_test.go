package bolttable

import (
	"os"
	"strings"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

func TestBboltOptions(t *testing.T) {
	db := mustOpen(t, tempPath(t), newFixture(t).schema, Options{})
	if db.bolt.NoFreelistSync {
		t.Error("NoFreelistSync is on: a failed commit would walk the file")
	}
	if db.bolt.FreelistType != bbolt.FreelistMapType {
		t.Errorf("FreelistType = %q, want %q", db.bolt.FreelistType, bbolt.FreelistMapType)
	}
	o := boltOptions(false)
	if o.InitialMmapSize != 1<<30 {
		t.Errorf("InitialMmapSize = %d, want 1 GiB", o.InitialMmapSize)
	}
	if o.Timeout != 5*time.Second {
		t.Errorf("Timeout = %v, want 5s", o.Timeout)
	}
	if ro := boltOptions(true); !ro.ReadOnly || ro.NoFreelistSync || ro.FreelistType != bbolt.FreelistMapType {
		t.Errorf("read-only options = %+v", ro)
	}
}

func TestEpochDiffersPerOpen(t *testing.T) {
	path := tempPath(t)
	f := newFixture(t)
	db, err := Open(path, f.schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	first := db.Epoch()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	second := mustOpen(t, path, f.schema, Options{}).Epoch()
	if first == ([16]byte{}) || second == ([16]byte{}) {
		t.Errorf("zero epoch: %x, %x", first, second)
	}
	if first == second {
		t.Errorf("two opens share epoch %x", first)
	}
}

func TestSecondOpenTimesOut(t *testing.T) {
	tuneBolt = func(o *bbolt.Options) { o.Timeout = 50 * time.Millisecond }
	t.Cleanup(func() { tuneBolt = nil })
	path := tempPath(t)
	mustOpen(t, path, newFixture(t).schema, Options{})
	if db, err := Open(path, newFixture(t).schema, Options{}); err == nil || !strings.Contains(err.Error(), "timeout") {
		if db != nil {
			_ = db.Close()
		}
		t.Errorf("second Open = %v, want a timeout", err)
	}
}

func TestOpenInitializesEmptyFile(t *testing.T) {
	path := tempPath(t)
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t)
	db, err := Open(path, f.schema, Options{})
	if err != nil {
		t.Fatalf("Open of an empty file: %v", err)
	}
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "kept"}) })
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	f = newFixture(t)
	db = mustOpen(t, path, f.schema, Options{})
	view(t, db, func(r *ReadTx) error {
		got, found, err := f.items.Get(r, 1)
		if err != nil || !found || got.Name != "kept" {
			t.Errorf("Get after reopen = %+v, %v, %v; want kept", got, found, err)
		}
		return nil
	})
}

func TestOpenRefusesBadArguments(t *testing.T) {
	if _, err := Open(tempPath(t), nil, Options{}); err == nil {
		t.Error("Open accepted a nil schema")
	}
	if _, err := Open(tempPath(t), newFixture(t).schema, Options{Retention: -1}); err == nil {
		t.Error("Open accepted a negative Retention")
	}
}
