package bolttable

import (
	"errors"
	"os"
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
	if o.Timeout != time.Nanosecond {
		t.Errorf("Timeout = %v, want 1ns, which bbolt tries once", o.Timeout)
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
	path := tempPath(t)
	if _, err := Open(path, newFixture(t).schema, Options{InvalidFile: InvalidFileMoveAside + 1}); err == nil {
		t.Error("Open accepted an unknown InvalidFile policy")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a refused Open left a file: %v", err)
	}
}

func TestBatchOptionsDefault(t *testing.T) {
	db := mustOpen(t, tempPath(t), newFixture(t).schema, Options{})
	if db.bolt.MaxBatchSize != 1000 || db.bolt.MaxBatchDelay != 10*time.Millisecond {
		t.Errorf("batch limits = %d, %v; want 1000 and 10ms", db.bolt.MaxBatchSize, db.bolt.MaxBatchDelay)
	}
}

func TestBatchOptionsApplied(t *testing.T) {
	db := mustOpen(t, tempPath(t), newFixture(t).schema, Options{MaxBatchSize: 4, MaxBatchDelay: time.Hour})
	if db.bolt.MaxBatchSize != 4 || db.bolt.MaxBatchDelay != time.Hour {
		t.Errorf("batch limits = %d, %v; want 4 and 1h", db.bolt.MaxBatchSize, db.bolt.MaxBatchDelay)
	}
}

func TestBatchOptionsNegativeRefused(t *testing.T) {
	for name, o := range map[string]Options{"size": {MaxBatchSize: -1}, "delay": {MaxBatchDelay: -time.Millisecond}} {
		t.Run(name, func(t *testing.T) {
			path := tempPath(t)
			if db, err := Open(path, newFixture(t).schema, o); err == nil {
				_ = db.Close()
				t.Fatal("Open accepted a negative batch limit")
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Errorf("a refused Open left a file: %v", err)
			}
		})
	}
}
