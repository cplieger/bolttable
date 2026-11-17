package bolttable

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type liveState uint8

const (
	liveClosed liveState = iota // Err says why
	liveOpen
	liveSwapping
	liveFinal // after Close
)

var (
	// errNotOpened is Err of a handle no Open or Swap has opened yet.
	errNotOpened = errors.New("bolttable: handle not opened")
	errPanicked  = errors.New("bolttable: the handle's open or Swap's rename panicked")
)

// Live is the one handle to a live file. Every store of the file holds it,
// never a *DB, so a Swap replaces the file under all of them and nothing
// keeps writing to the file it replaced.
type Live struct {
	schema *Schema
	// ops admits one Open, Swap or Close at a time; a Swap's wait for it
	// ends with its context.
	ops chan struct{}
	// db is the open DB, kept until its close returns, so Epoch answers it
	// to the work that close waits for. Nil while none is open.
	db    *DB
	err   error
	path  string
	hooks []func(*DB) error
	// drained is closed when the last call in flight returns during a swap.
	drained  chan struct{}
	opts     Options
	mu       sync.Mutex
	inflight int
	state    liveState
	// opened is set by the first Open or Swap; OnOpen panics after it.
	opened bool
	// owned is a lock NewLive took, which Close releases.
	owned bool
}

// NewLive checks o as Open does and holds path's lock for the handle's
// life: o.Lock when set, which the handle adopts and never releases, else
// one it takes with LockPath(path, o.LockWait), which Close releases. Until
// Close the lock's Release fails, and an Open or NewLive under it answers
// ErrLocked. It opens nothing: the handle is closed until Open or Swap
// opens it.
func NewLive(path string, s *Schema, o Options) (*Live, error) { //nolint:gocritic // hugeParam: callers build Options inline
	if _, err := checkOpen(path, s, &o); err != nil {
		return nil, err
	}
	l := &Live{path: path, schema: s, opts: o, err: errNotOpened, ops: make(chan struct{}, 1)}
	if o.Lock != nil {
		if err := o.Lock.bind(path, l); err != nil {
			return nil, err
		}
		return l, nil
	}
	lk, err := LockPath(path, o.LockWait)
	if err != nil {
		return nil, err
	}
	lk.live = l
	l.opts.Lock, l.opts.LockWait, l.owned = lk, 0, true
	return l, nil
}

// OnOpen registers fn to run at every open of the file, the first and each
// swap's, in registration order and before any call is admitted. fn works
// on db. View, Update, Batch, Backup, Space and Pin on the handle answer
// ErrClosed inside it; Open, Swap, Close and Refuse wait for the open it
// runs in, so a hook never calls them. Its error closes the handle with that
// error, and the open answers it. OnOpen after the handle's first Open or
// Swap panics, since that hook would miss an open.
func (l *Live) OnOpen(fn func(db *DB) error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.opened {
		panic("bolttable: OnOpen after the handle's first open")
	}
	l.hooks = append(l.hooks, fn)
}

// Open opens the file under the held lock and runs the OnOpen hooks. A
// failure leaves the handle closed with Err set and answers it. Open of an
// open handle is an error; after Close it answers ErrClosed.
func (l *Live) Open() error {
	l.ops <- struct{}{}
	defer func() { <-l.ops }()
	l.mu.Lock()
	switch l.state {
	case liveFinal:
		defer l.mu.Unlock()
		return l.closedErr()
	case liveOpen:
		l.mu.Unlock()
		return errors.New("bolttable: Open of an open handle")
	}
	l.opened = true
	l.mu.Unlock()
	return l.open()
}

// open opens the file, runs the hooks and admits calls. A failure, a panic
// included, closes what it opened and leaves the handle closed with the
// error. The caller holds ops.
func (l *Live) open() error {
	var db *DB
	// failed stays errPanicked only when the open or a hook panics.
	failed := errPanicked
	defer func() {
		if failed == nil {
			return
		}
		if db != nil {
			_ = db.Close()
		}
		l.fail(failed)
	}()
	db, err := openBy(l.path, l.schema, l.opts, l)
	if err == nil {
		err = l.runHooks(db)
	}
	failed = err
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.db, l.state, l.err = db, liveOpen, nil
	l.mu.Unlock()
	return nil
}

func (l *Live) runHooks(db *DB) error {
	for _, fn := range l.hooks {
		if err := fn(db); err != nil {
			return err
		}
	}
	return nil
}

// fail closes the handle with err.
func (l *Live) fail(err error) {
	l.mu.Lock()
	l.db, l.state, l.err = nil, liveClosed, err
	l.mu.Unlock()
}

// Err is nil while the handle is open or swapping; otherwise why it is
// closed: the error of the open, rename, hook or DB close that closed it,
// ErrClosed after Close, or an error saying it was never opened.
func (l *Live) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state == liveOpen || l.state == liveSwapping {
		return nil
	}
	return l.err
}

// closedErr is a refused call's error, ErrClosed wrapping Err when it is
// set. The caller holds mu.
func (l *Live) closedErr() error {
	if l.state != liveClosed || l.err == nil {
		return ErrClosed
	}
	return fmt.Errorf("%w: %w", ErrClosed, l.err)
}

// call runs fn on the open DB as one call in flight, or answers ErrClosed
// at once while the handle is closed or swapping.
func (l *Live) call(fn func(*DB) error) error {
	l.mu.Lock()
	if l.state != liveOpen {
		defer l.mu.Unlock()
		return l.closedErr()
	}
	l.inflight++
	db := l.db
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		l.inflight--
		if l.inflight == 0 && l.drained != nil {
			close(l.drained)
			l.drained = nil
		}
		l.mu.Unlock()
	}()
	return fn(db)
}

// View is (*DB).View on the open DB; a closed or swapping handle answers
// ErrClosed.
func (l *Live) View(ctx context.Context, fn func(*ReadTx) error) error {
	return l.call(func(db *DB) error { return db.View(ctx, fn) })
}

// Update is (*DB).Update on the open DB; a closed or swapping handle
// answers ErrClosed.
func (l *Live) Update(ctx context.Context, fn func(*Tx) error) error {
	return l.call(func(db *DB) error { return db.Update(ctx, fn) })
}

// Batch is (*DB).Batch on the open DB; a closed or swapping handle answers
// ErrClosed.
func (l *Live) Batch(ctx context.Context, fn func(*Tx) error) error {
	return l.call(func(db *DB) error { return db.Batch(ctx, fn) })
}

// Backup is (*DB).Backup on the open DB; a closed or swapping handle
// answers ErrClosed.
func (l *Live) Backup(ctx context.Context, path string) error {
	return l.call(func(db *DB) error { return db.Backup(ctx, path) })
}

// Space is (*DB).Space on the open DB; a closed or swapping handle answers
// ErrClosed.
func (l *Live) Space() (Space, error) {
	var sp Space
	err := l.call(func(db *DB) error {
		var err error
		sp, err = db.Space()
		return err
	})
	return sp, err
}

// Epoch is the open DB's epoch, kept until that DB's close returns, which
// waits for its transactions and the callbacks of its commits, and zero
// while no DB is open.
func (l *Live) Epoch() [16]byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.db == nil {
		return [16]byte{}
	}
	return l.db.Epoch()
}

// Pin returns the open DB, for work spanning transactions that must not
// cross a swap: after a Swap that DB is closed, so its next call answers
// ErrClosed. Drop it when the work ends. A closed or swapping handle answers
// ErrClosed.
func (l *Live) Pin() (*DB, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state != liveOpen {
		return nil, l.closedErr()
	}
	return l.db, nil
}

// Swap replaces the file: it refuses new calls with ErrClosed, waits for the
// calls in flight until ctx ends (then admits calls again, closes nothing and
// answers ctx's error), closes the DB as (*DB).Close does, calls rename,
// reopens the file with the same schema and options, runs the OnOpen hooks
// and admits calls. The wait is cooperative: no bbolt transaction reads ctx
// inside its function. A failed close, rename, open or hook leaves the
// handle closed with Err set and answers the error. On a closed handle Swap
// skips the close; after Close it answers ErrClosed.
func (l *Live) Swap(ctx context.Context, rename func() error) error {
	if rename == nil {
		return errors.New("bolttable: Swap with a nil rename")
	}
	select {
	case l.ops <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-l.ops }()
	// A panicking rename leaves the handle closed, not swapping forever.
	defer func() {
		l.mu.Lock()
		if l.state == liveSwapping {
			l.db, l.state, l.err = nil, liveClosed, errPanicked
		}
		l.mu.Unlock()
	}()
	l.mu.Lock()
	switch l.state {
	case liveFinal:
		defer l.mu.Unlock()
		return l.closedErr()
	case liveOpen:
		db := l.db
		var drained chan struct{}
		l.state = liveSwapping
		if l.inflight > 0 {
			drained = make(chan struct{})
			l.drained = drained
		}
		l.mu.Unlock()
		if err := l.drain(ctx, drained); err != nil {
			return err
		}
		if err := db.Close(); err != nil {
			l.fail(err)
			return err
		}
		l.mu.Lock()
		l.db = nil
		l.mu.Unlock()
	default:
		l.opened = true
		l.state = liveSwapping
		l.mu.Unlock()
	}
	if err := rename(); err != nil {
		l.fail(err)
		return err
	}
	return l.open()
}

// drain waits for drained, nil when no call is in flight, until ctx ends,
// when it admits calls again.
func (l *Live) drain(ctx context.Context, drained chan struct{}) error {
	if drained == nil {
		return nil
	}
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		l.mu.Lock()
		l.state, l.drained = liveOpen, nil
		l.mu.Unlock()
		return ctx.Err()
	}
}

// Close refuses new calls, closes the DB as (*DB).Close does, unbinds the
// lock and releases one NewLive took, never an adopted one. It is final:
// every later View, Update, Batch, Backup, Space, Pin, Open and Swap
// answers ErrClosed, Err answers ErrClosed and Epoch zero. A second Close
// returns nil.
func (l *Live) Close() error {
	l.ops <- struct{}{}
	defer func() { <-l.ops }()
	l.mu.Lock()
	if l.state == liveFinal {
		l.mu.Unlock()
		return nil
	}
	db := l.db
	l.state, l.err = liveFinal, ErrClosed
	l.mu.Unlock()
	var err error
	if db != nil {
		err = db.Close()
	}
	l.mu.Lock()
	l.db = nil
	l.mu.Unlock()
	l.opts.Lock.unbind()
	if l.owned {
		err = errors.Join(err, l.opts.Lock.Release())
	}
	return err
}

// Refuse sets Err to err on a handle that is not open, such as a blocked
// start's, until an Open or Swap opens it; every call answers ErrClosed
// wrapping err. On an open handle, after Close, or with a nil err it does
// nothing. It waits for an Open, Swap or Close in progress.
func (l *Live) Refuse(err error) {
	if err == nil {
		return
	}
	l.ops <- struct{}{}
	defer func() { <-l.ops }()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state == liveClosed {
		l.err = err
	}
}
