package bolttable

import (
	"errors"
	"sync"

	"go.etcd.io/bbolt"
)

// Test seams, nil in production. beforeDeliver runs at the top of each
// slot's commit handler. metaWithoutHandler models bbolt's fdatasync failure
// after the meta write for the txids it selects: the handler delivers
// nothing and the call returns errSyncInjected, while the next writer
// begins past that txid.
var (
	beforeDeliver      func(txid uint64)
	metaWithoutHandler func(txid uint64) bool
)

var errSyncInjected = errors.New("bolttable: injected sync failure after the meta write")

// commitSlot orders one bbolt write transaction's callbacks after those of
// the transaction before it. Slots chain by pointer, never by txid
// arithmetic: a transaction whose meta reached the file before fdatasync
// failed moves the next txid on without running a handler
// (https://github.com/etcd-io/bbolt/blob/v1.5.0/tx.go#L267).
type commitSlot struct {
	btx   *bbolt.Tx
	prev  *commitSlot
	done  chan struct{}
	items []slotItem
	once  sync.Once
	txid  uint64
}

// slotItem is one successful run's delivery: its change, nil for a commit
// that reserved no revision, and its AfterCommit functions.
type slotItem struct {
	change *Change
	after  []func()
}

func closedSlot(txid uint64) *commitSlot {
	s := &commitSlot{txid: txid, done: make(chan struct{})}
	close(s.done)
	return s
}

// slot returns btx's slot, making it at btx's first run. db.cur is touched
// only inside bbolt write transaction bodies, which bbolt's writer lock
// serializes. A current slot with btx's own txid rolled back before its meta
// write, so it never delivers and is skipped.
func (db *DB) slot(btx *bbolt.Tx) *commitSlot {
	if db.cur.btx == btx {
		return db.cur
	}
	id := txID(btx)
	prev := db.cur
	if prev.txid == id {
		prev = prev.prev
	}
	s := &commitSlot{btx: btx, prev: prev, done: make(chan struct{}), txid: id}
	btx.OnCommit(func() {
		if metaWithoutHandler != nil && metaWithoutHandler(id) {
			return
		}
		db.deliver(s)
	})
	db.cur = s
	return s
}

// deliver runs s's callbacks once every earlier slot's have run. done closes
// even when a callback panics, so one panic cannot wedge later commits.
func (db *DB) deliver(s *commitSlot) {
	if beforeDeliver != nil {
		beforeDeliver(s.txid)
	}
	defer s.once.Do(func() { close(s.done) })
	<-s.prev.done
	s.prev = nil
	for _, it := range s.items {
		if it.change != nil && db.opts.OnCommit != nil {
			db.opts.OnCommit(*it.change)
		}
		runAll(it.after)
	}
}

// abandon closes the slot of a call that failed, after every earlier slot's
// callbacks ran. A nil slot is a call whose transaction never began.
func (db *DB) abandon(s *commitSlot) {
	if s == nil {
		return
	}
	<-s.prev.done
	s.once.Do(func() { close(s.done) })
}
