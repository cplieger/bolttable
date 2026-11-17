package bolttable

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/bbolt"
	"golang.org/x/sync/semaphore"
)

// boltTryOnce makes bbolt try its flock once: its retry loop gives up when
// the wait exceeds Timeout minus 50 ms, and a zero Timeout waits forever
// (https://github.com/etcd-io/bbolt/blob/v1.5.0/bolt_unix.go#L36).
const boltTryOnce = time.Nanosecond

// The Batch limits a zero Options field means: bbolt v1.5.0's defaults,
// pinned so a bbolt upgrade cannot change them.
const (
	defaultMaxBatchSize  = 1000
	defaultMaxBatchDelay = 10 * time.Millisecond
)

// boltOptions are the library's fixed bbolt choices. The persisted freelist
// (NoFreelistSync false) is read at open and after a failed commit; without
// it bbolt rebuilds the freelist by walking the file, where a bad page
// panics in a goroutine no recover reaches (https://github.com/etcd-io/bbolt/blob/v1.5.0/db.go#L1293).
// The hashmap freelist allocates in O(1). A 1 GiB initial mapping avoids
// remaps while the file stays below it; growth past it still remaps, which
// stalls new readers (https://github.com/etcd-io/bbolt/blob/v1.5.0/db.go#L801).
func boltOptions(readOnly bool) *bbolt.Options {
	return &bbolt.Options{
		Timeout:         boltTryOnce,
		FreelistType:    bbolt.FreelistMapType,
		NoFreelistSync:  false,
		InitialMmapSize: 1 << 30,
		ReadOnly:        readOnly,
	}
}

// Options configures an open DB. The zero value records Touched as each
// change's subjects, delivers nothing and retains no change, so every Since
// below the head answers full.
type Options struct {
	// BeforeCommit runs once per revision-reserving transaction, before bbolt
	// commits, and returns the subjects to record. It may write silent tables
	// and add marks; a non-silent write or Touch fails the transaction, and
	// its error rolls everything back. It may return subjects naming no row,
	// built with Synthetic, which reach Since and OnCommit unchanged; a
	// subject starting with neither a declared prefix nor SyntheticPrefix
	// fails the transaction.
	BeforeCommit func(*Tx, *Pending) ([]Subject, error)
	// OnCommit receives each committed change. OnCommit and every AfterCommit
	// function run in commit order across the DB, silent-only and empty
	// commits included: each commit's callbacks return before the next
	// commit's start. They run one at a time, outside every bbolt lock, and
	// must not block, start a write, close the DB, or call Close, Swap, Open
	// or Refuse on a Live serving it: blocking, a write, a close, Close and
	// Swap deadlock, and Open and Refuse deadlock while a Close or Swap of
	// that Live runs. A View is allowed. Close waits for them.
	OnCommit func(Change)
	// CommitFault, for tests of a caller's write-fault handling, is asked
	// once per run of every Update and Batch call whose context is not done
	// after fn, BeforeCommit and the change record, with that context;
	// Verify's repair never asks it. A non-nil error fails the run as a
	// failed bbolt commit would: it rolls back, runs no callback, and the
	// call returns an error wrapping ErrCommit and the hook's error. In a
	// Batch group it fails only the calls it selects, on every rerun, where a
	// real commit failure fails the whole group. Production leaves it nil.
	CommitFault func(ctx context.Context) error
	// Inspect runs once on a stored file, in a read-only transaction after
	// the format comparison and before Open resets, compacts or creates
	// anything. Its error refuses the open, returned as is, with the file and
	// the lock as they were. A table, index, counter, namespace or change log
	// the file lacks reads as empty; an absent or empty file is not inspected.
	// A memory fault or a bbolt panic reading a page is ErrInvalidFile, which
	// InvalidFile decides on; a panic in fn's own code reaches the caller.
	Inspect func(*ReadTx) error
	// Lock is a lock the caller took with LockPath(path, …) and still holds.
	// Open runs under it instead of taking its own, and Close leaves it
	// held; one a Live holds fails Open with ErrLocked. nil has Open take the
	// lock and Close release it. Setting it with a non-zero LockWait is an
	// error.
	Lock *Lock
	// Reset decides what Open resets.
	Reset ResetPolicy
	// Compact decides when Open compacts the file.
	Compact CompactPolicy
	// Retention is how many of the newest changes Since can return; it must
	// not be negative.
	Retention int
	// MaxBatchSize and MaxBatchDelay bound a Batch group: it runs when it
	// holds MaxBatchSize calls or MaxBatchDelay after its first call,
	// whichever comes first. Zero means 1000 calls and 10 ms; negative is an
	// error.
	MaxBatchSize  int
	MaxBatchDelay time.Duration
	// LockWait is how long Open waits for another holder of <path>.lock,
	// polling every 50 ms. Zero tries once; negative is an error.
	LockWait time.Duration
	// InvalidFile is what Open does with a file wrapping ErrInvalidFile.
	InvalidFile InvalidFilePolicy
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
	// cur is the newest write transaction's slot, touched only inside bbolt
	// write transaction bodies.
	cur *commitSlot
	// writers admits write transactions (DB.commit): Update and Verify's
	// repair take all of it, a Batch call one unit so concurrent calls group.
	writers *semaphore.Weighted
	// unlock releases the lock Open took, or frees an adopted one.
	unlock func() error
	// idle closes when the last commit call in progress, callbacks
	// included, returns during Close, which waits for it: bbolt runs a
	// commit's callbacks after it releases the locks its own Close waits on
	// (https://github.com/etcd-io/bbolt/blob/v1.5.0/tx.go#L275). calls counts
	// them under callsMu.
	idle   chan struct{}
	report OpenReport
	// path and pageSize are cached at open for Space, which must work
	// after bbolt's Close.
	path     string
	opts     Options
	pageSize int
	// baseTxID is bbolt's transaction id when Open returned; bbolt advances
	// it by one per committed write (https://github.com/etcd-io/bbolt/blob/v1.5.0/tx.go#L63), so the
	// difference counts commits.
	baseTxID  uint64
	retention uint64
	calls     int
	callsMu   sync.Mutex
	epoch     [16]byte
	closed    atomic.Bool
}

// start inspects and completes the file bdb opened and starts the DB over
// it, leaving in *wtx the write transaction it began, for closeBolt.
func start(bdb *bbolt.DB, s *Schema, o *Options, want map[string][32]byte, wtx **bbolt.Tx) (*DB, error) {
	hook("start")
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
		if err := createNamespaces(bdb, &plan, want, wtx); err != nil {
			return nil, fmt.Errorf("bolttable: create namespaces: %w", err)
		}
	}
	db := &DB{bolt: bdb, schema: s, opts: *o, path: bdb.Path(), pageSize: bdb.Info().PageSize, writers: semaphore.NewWeighted(exclusive)}
	if err := bdb.View(func(btx *bbolt.Tx) error {
		db.baseTxID = txID(btx)
		db.cur = closedSlot(db.baseTxID)
		_, err := readHead(btx.Bucket([]byte(libBucket)))
		return err
	}); err != nil {
		return nil, err
	}
	// crypto/rand.Read never returns an error (Go 1.24+).
	_, _ = rand.Read(db.epoch[:])
	return db, nil
}

// createNamespaces applies plan in one write transaction, left in *wtx. It
// is unmanaged, as the reset's is, because bbolt refuses Rollback on a
// managed Update transaction that a panicking rollback left open.
func createNamespaces(bdb *bbolt.DB, plan *openPlan, want map[string][32]byte, wtx **bbolt.Tx) error {
	btx, err := bdb.Begin(true)
	if err != nil {
		return err
	}
	*wtx = btx
	defer func() {
		hook("start-rollback")
		_ = btx.Rollback()
	}()
	if err := plan.apply(btx, want); err != nil {
		return err
	}
	hook("start-commit")
	return btx.Commit()
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

// Close refuses new writes, waits for the writes in progress, their
// OnCommit and AfterCommit callbacks included, and for open transactions,
// closes the file, then releases the lock Open took or frees an adopted one,
// which stays held. A second Close returns nil. Every later call that returns
// an error answers one matching both ErrClosed and bbolt's
// ErrDatabaseNotOpen. Close from inside a transaction or a commit callback of
// the same DB deadlocks.
func (db *DB) Close() error {
	if !db.closed.CompareAndSwap(false, true) {
		return nil
	}
	var idle chan struct{}
	db.callsMu.Lock()
	if db.calls > 0 {
		idle = make(chan struct{})
		db.idle = idle
	}
	db.callsMu.Unlock()
	if idle != nil {
		<-idle
	}
	return errors.Join(db.bolt.Close(), db.unlock())
}

// enterCall admits a commit call unless Close has begun. It reads closed
// under callsMu, so Close either refuses the call or counts it.
func (db *DB) enterCall() bool {
	db.callsMu.Lock()
	defer db.callsMu.Unlock()
	if db.closed.Load() {
		return false
	}
	db.calls++
	return true
}

// exitCall ends a commit call, waking Close at the last one.
func (db *DB) exitCall() {
	db.callsMu.Lock()
	defer db.callsMu.Unlock()
	db.calls--
	if db.calls == 0 && db.idle != nil {
		close(db.idle)
		db.idle = nil
	}
}

// Epoch returns the random identity of this open: a reader holding a
// revision from another epoch must resync.
func (db *DB) Epoch() [16]byte { return db.epoch }
