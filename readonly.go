package bolttable

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"time"

	"go.etcd.io/bbolt"
)

// ReadOptions configures OpenReadOnly.
type ReadOptions struct {
	// Retention is how many of the newest changes Since can return, as
	// Options.Retention.
	Retention int
	// LockWait is how long OpenReadOnly waits for a writer holding
	// <path>.lock, as Options.LockWait.
	LockWait time.Duration
}

// ReadDB is a file opened by OpenReadOnly. It has no write method.
type ReadDB struct{ db *DB }

// OpenReadOnly opens the file at path for reading only under s, sealing s:
// it creates, resets, compacts and writes nothing. It holds the shared lock
// on an existing <path>.lock until Close, so a writer's Open fails with
// ErrLocked meanwhile, and fails with ErrLocked when a writer keeps it past
// o.LockWait. An absent or empty file wraps fs.ErrNotExist, an unreadable
// one ErrInvalidFile; a file Open would reset or complete fails with a
// *FormatError whose Missing names what Open would create. The lock has
// LockPath's filesystem and platform limits.
func OpenReadOnly(path string, s *Schema, o ReadOptions) (rdb *ReadDB, err error) {
	if s == nil {
		return nil, errors.New("bolttable: OpenReadOnly with a nil schema")
	}
	retention, err := retentionOf(o.Retention)
	if err != nil {
		return nil, err
	}
	if o.LockWait < 0 {
		return nil, fmt.Errorf("bolttable: negative LockWait %v", o.LockWait)
	}
	s.seal()
	unlock, err := readLock(path, o.LockWait)
	if err != nil {
		return nil, err
	}
	defer func() {
		if rdb == nil {
			_ = unlock()
		}
	}()
	size, err := storedSize(path)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return nil, fmt.Errorf("bolttable: open %s: holds no database: %w", path, fs.ErrNotExist)
	}
	want := s.digests()
	err = readOnly(path, stepReadInspect, false, func(bdb *bbolt.DB) error {
		return bdb.View(func(btx *bbolt.Tx) error {
			if serr := checkSize(btx, path, stepReadInspect, size); serr != nil {
				return serr
			}
			return inspectReadOnly(btx, s, want)
		})
	})
	if err != nil {
		return nil, err
	}
	db, err := openReader(path, s)
	if err != nil {
		return nil, err
	}
	db.unlock, db.retention = unlock, retention
	return &ReadDB{db: db}, nil
}

// openReader opens the ReadDB's own handle. PreLoadFreelist makes bbolt
// read the freelist, without which a read-only handle reports no free pages
// (https://github.com/etcd-io/bbolt/blob/v1.5.0/db.go#L303).
func openReader(path string, s *Schema) (*DB, error) {
	o := boltOptions(true)
	o.PreLoadFreelist = true
	bdb, _, err := openBolt(path, stepReadOpen, o)
	if err != nil {
		return nil, err
	}
	db := &DB{bolt: bdb, schema: s, path: bdb.Path(), pageSize: bdb.Info().PageSize}
	if err := bdb.View(func(btx *bbolt.Tx) error {
		db.baseTxID = txID(btx)
		return nil
	}); err != nil {
		_ = bdb.Close()
		return nil, fmt.Errorf("bolttable: open %s: %w", path, err)
	}
	// crypto/rand.Read never returns an error (Go 1.24+).
	_, _ = rand.Read(db.epoch[:])
	return db, nil
}

// inspectReadOnly refuses a file Open would reset or create anything in. A
// namespace whose digest matches but which lacks a table or index bucket is
// ErrCorrupt: its digest says it is complete, and only a raw bbolt deletion
// leaves it so.
func inspectReadOnly(btx *bbolt.Tx, s *Schema, want map[string][32]byte) error {
	_, fe, err := inspect(btx, s, want)
	if err != nil {
		return err
	}
	listed := map[string]bool{}
	if fe != nil {
		for _, n := range fe.Namespaces {
			listed[n.Name] = true
		}
	}
	var absent []string
	if lib := btx.Bucket([]byte(libBucket)); lib == nil || (!listed[libBucket] && lib.Bucket(keyChanges) == nil) {
		absent = append(absent, libBucket)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ns := range s.namespaces {
		switch nsb := btx.Bucket([]byte(ns.name)); {
		case nsb == nil:
			absent = append(absent, ns.name)
		case !listed[ns.name] && !namespaceComplete(nsb, ns):
			return fmt.Errorf("%w: namespace %s lacks a declared table or index bucket", ErrCorrupt, ns.name)
		}
	}
	if fe == nil && len(absent) == 0 {
		return nil
	}
	if fe == nil {
		fe = &FormatError{}
	}
	slices.Sort(absent)
	fe.Missing = absent
	return fe
}

// View runs fn in one read-only transaction. A context already done runs
// nothing.
func (db *ReadDB) View(ctx context.Context, fn func(*ReadTx) error) error {
	return db.db.View(ctx, fn)
}

// Verify rescans every table and recomputes every index entry and counter,
// as DB.Verify does; it never repairs.
func (db *ReadDB) Verify(ctx context.Context) (VerifyReport, error) {
	return db.db.Verify(ctx, false)
}

// Stats reads the file's statistics as DB.Stats does; Commits is 0.
func (db *ReadDB) Stats(ctx context.Context) (Stats, error) { return db.db.Stats(ctx) }

// Space reads the file's size and free bytes as DB.Space does.
func (db *ReadDB) Space() (Space, error) { return db.db.Space() }

// Backup writes a compacted copy of the file to path as DB.Backup does.
func (db *ReadDB) Backup(ctx context.Context, path string) error { return db.db.Backup(ctx, path) }

// Epoch returns the random identity of this open.
func (db *ReadDB) Epoch() [16]byte { return db.db.Epoch() }

// Close closes the file, waiting for open transactions, and releases the
// shared lock. A second Close returns nil.
func (db *ReadDB) Close() error { return db.db.Close() }
