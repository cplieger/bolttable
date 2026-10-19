package bolttable

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

	"go.etcd.io/bbolt"
)

// openTimeout bounds the wait for bbolt's file lock, so a second opener
// fails instead of hanging.
const openTimeout = 5 * time.Second

// tuneBolt is a test seam over bbolt's options; nil outside tests.
var tuneBolt func(*bbolt.Options)

// boltOptions are the library's fixed bbolt choices. The persisted freelist
// (NoFreelistSync false) is read at open and after a failed commit; without
// it bbolt rebuilds the freelist by walking the file, where a bad page
// panics in a goroutine no recover reaches (https://github.com/etcd-io/bbolt/blob/v1.5.0/db.go#L1293).
// The hashmap freelist allocates in O(1). A 1 GiB initial mapping avoids
// remaps while the file stays below it; growth past it still remaps, which
// stalls new readers (https://github.com/etcd-io/bbolt/blob/v1.5.0/db.go#L801).
func boltOptions(readOnly bool) *bbolt.Options {
	o := &bbolt.Options{
		Timeout:         openTimeout,
		FreelistType:    bbolt.FreelistMapType,
		NoFreelistSync:  false,
		InitialMmapSize: 1 << 30,
		ReadOnly:        readOnly,
	}
	if tuneBolt != nil {
		tuneBolt(o)
	}
	return o
}

// Options configures an open DB. The zero value records Touched as each
// change's subjects, delivers nothing and retains no change, so every Since
// below the head answers full.
type Options struct {
	// BeforeCommit runs once per revision-reserving transaction, before bbolt
	// commits, and returns the subjects to record. It may write silent tables
	// and add marks; a non-silent write or Touch fails the transaction, and
	// its error rolls everything back.
	BeforeCommit func(*Tx, *Pending) ([]Subject, error)
	// OnCommit receives each committed change in revision order. It must not
	// block: it runs under the lock that orders deliveries.
	OnCommit func(Change)
	// Retention is how many of the newest changes Since can return; it must
	// not be negative.
	Retention int
}

// Change is one committed revision: the subjects BeforeCommit returned (or
// the touched ones) and every mark.
type Change struct {
	Subjects []Subject
	Marks    []string
	Rev      uint64
}

// Pending is the revision a transaction is about to commit, handed to
// BeforeCommit. Its slices are copies.
type Pending struct {
	Touched []Subject
	Marks   []string
	Rev     uint64
}

// DB is an open bolttable file.
type DB struct {
	bolt   *bbolt.DB
	schema *Schema
	seq    *sequencer
	opts   Options
	// baseTxID is bbolt's transaction id when Open returned; bbolt advances
	// it by one per committed write (https://github.com/etcd-io/bbolt/blob/v1.5.0/tx.go#L63), so the
	// difference counts commits.
	baseTxID  uint64
	retention uint64
	epoch     [16]byte
}

// Open opens or creates the file at path, mode 0600, under s, sealing s. A
// file whose stored format differs from s fails with a *FormatError and is
// left byte-for-byte unchanged; only when nothing mismatches does Open
// create the namespaces the file lacks. Open fails when another process
// holds the file for 5 s.
func Open(path string, s *Schema, o Options) (*DB, error) {
	if s == nil {
		return nil, errors.New("bolttable: Open with a nil schema")
	}
	retention, err := retentionOf(o.Retention)
	if err != nil {
		return nil, err
	}
	s.seal()
	want := s.digests()
	if cerr := checkReadOnly(path, s, want); cerr != nil {
		return nil, cerr
	}
	bdb, err := bbolt.Open(path, 0o600, boltOptions(false))
	if err != nil {
		return nil, fmt.Errorf("bolttable: open %s: %w", path, err)
	}
	db, err := start(bdb, s, o, want)
	if err != nil {
		_ = bdb.Close()
		return nil, err
	}
	db.retention = retention
	return db, nil
}

// checkReadOnly inspects a stored file through a read-only open, which
// writes nothing: a read-write bbolt open may commit, persisting a missing
// freelist (https://github.com/etcd-io/bbolt/blob/v1.5.0/db.go#L313), and a refused file must stay
// unchanged. An absent or empty file stores nothing: bbolt's read-write open
// initializes it as new and its read-only open cannot
// (https://github.com/etcd-io/bbolt/blob/v1.5.0/db.go#L272).
func checkReadOnly(path string, s *Schema, want map[string][32]byte) error {
	fi, err := os.Stat(path) //nolint:gosec // G703: the caller names its own database file
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("bolttable: %w", err)
	case fi.Size() == 0:
		return nil
	}
	ro, err := bbolt.Open(path, 0o600, boltOptions(true))
	if err != nil {
		return fmt.Errorf("bolttable: open %s: %w", path, err)
	}
	defer func() { _ = ro.Close() }()
	return ro.View(func(btx *bbolt.Tx) error {
		_, fe, err := inspect(btx, s, want)
		if fe != nil {
			return fe
		}
		return err
	})
}

func start(bdb *bbolt.DB, s *Schema, o Options, want map[string][32]byte) (*DB, error) {
	var plan openPlan
	if err := bdb.View(func(btx *bbolt.Tx) error {
		p, fe, err := inspect(btx, s, want)
		if fe != nil {
			return fe
		}
		plan = p
		return err
	}); err != nil {
		return nil, err
	}
	if plan.lib || len(plan.ensure) > 0 {
		if err := bdb.Update(func(btx *bbolt.Tx) error { return plan.apply(btx, want) }); err != nil {
			return nil, fmt.Errorf("bolttable: create namespaces: %w", err)
		}
	}
	db := &DB{bolt: bdb, schema: s, opts: o}
	if err := bdb.View(func(btx *bbolt.Tx) error {
		head, err := readHead(btx.Bucket([]byte(libBucket)))
		db.seq = newSequencer(head, o.OnCommit)
		db.baseTxID = txID(btx)
		return err
	}); err != nil {
		return nil, err
	}
	// crypto/rand.Read never returns an error (Go 1.24+).
	_, _ = rand.Read(db.epoch[:])
	return db, nil
}

func retentionOf(n int) (uint64, error) {
	if n < 0 {
		return 0, fmt.Errorf("bolttable: negative Retention %d", n)
	}
	return uint64(n), nil
}

// txID is bbolt's transaction id, which counts up from 0 and is never
// negative.
func txID(btx *bbolt.Tx) uint64 {
	return uint64(btx.ID()) //nolint:gosec // bbolt's Txid is a uint64 it hands out as int
}

// Close closes the file, waiting for open transactions.
func (db *DB) Close() error { return db.bolt.Close() }

// Epoch returns the random identity of this open: a reader holding a
// revision from another epoch must resync.
func (db *DB) Epoch() [16]byte { return db.epoch }
