package bolttable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	"go.etcd.io/bbolt"
)

// Since returns every retained change with Rev above after, in order. full
// reports that the log cannot answer: a change after after has been trimmed
// (after+1 is below the oldest retained revision), or after is above the
// head; changes is then nil.
func (r *ReadTx) Since(after uint64) ([]Change, bool, error) {
	if err := r.usable(); err != nil {
		return nil, false, err
	}
	lib, log, err := r.feed()
	if err != nil {
		return nil, false, err
	}
	head, err := readHead(lib)
	if err != nil {
		return nil, false, err
	}
	oldest, err := oldestRetained(log, head, r.db.retention)
	if err != nil {
		return nil, false, err
	}
	if after > head || after+1 < oldest {
		return nil, true, nil
	}
	changes := []Change{}
	c := log.Cursor()
	for k, v := c.Seek(be64(after + 1)); k != nil; k, v = c.Next() {
		ch, err := decodeChange(k, v)
		if err != nil {
			return nil, false, err
		}
		changes = append(changes, ch)
	}
	return changes, false, nil
}

// Rev returns the head revision: the newest committed one, 0 before any.
func (r *ReadTx) Rev() uint64 {
	if r.usable() != nil {
		return 0
	}
	lib, _, err := r.feed()
	if err != nil {
		return 0
	}
	head, _ := readHead(lib)
	return head
}

func (r *ReadTx) feed() (lib, log *bbolt.Bucket, err error) {
	lib = r.btx.Bucket([]byte(libBucket))
	if lib == nil {
		return nil, nil, fmt.Errorf("%w: no library bucket", ErrCorrupt)
	}
	log = lib.Bucket(keyChanges)
	if log == nil {
		return nil, nil, fmt.Errorf("%w: no change log", ErrCorrupt)
	}
	return lib, log, nil
}

func be64(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }

func readHead(lib *bbolt.Bucket) (uint64, error) {
	if lib == nil {
		return 0, nil
	}
	raw := lib.Get(keyHead)
	switch len(raw) {
	case 0:
		return 0, nil
	case 8:
		return binary.BigEndian.Uint64(raw), nil
	}
	return 0, fmt.Errorf("%w: revision head", ErrCorrupt)
}

// oldestRetained is the lowest revision Since may return: the first stored
// one within the newest retention revisions, or head+1 when none is.
func oldestRetained(log *bbolt.Bucket, head, retention uint64) (uint64, error) {
	if retention == 0 {
		return head + 1, nil
	}
	lo := uint64(1)
	if head >= retention {
		lo = head - retention + 1
	}
	k, _ := log.Cursor().Seek(be64(lo))
	if k == nil {
		return head + 1, nil
	}
	if len(k) != 8 {
		return 0, fmt.Errorf("%w: change log key", ErrCorrupt)
	}
	return binary.BigEndian.Uint64(k), nil
}

// record advances the head to c.Rev, logs c when retention keeps any change,
// and trims every logged revision beyond retention, including entries an
// open with a larger retention left behind.
func record(lib *bbolt.Bucket, c Change, retention uint64) error {
	if err := lib.Put(keyHead, be64(c.Rev)); err != nil {
		return fmt.Errorf("bolttable: revision head: %w", err)
	}
	log := lib.Bucket(keyChanges)
	if retention > 0 {
		if err := log.Put(be64(c.Rev), encodeChange(c)); err != nil {
			return fmt.Errorf("bolttable: change log: %w", err)
		}
	}
	if c.Rev < retention {
		return nil
	}
	cut := be64(c.Rev - retention + 1)
	var stale [][]byte
	cur := log.Cursor()
	for k, _ := cur.First(); k != nil && bytes.Compare(k, cut) < 0; k, _ = cur.Next() {
		stale = append(stale, bytes.Clone(k))
	}
	for _, k := range stale {
		if err := log.Delete(k); err != nil {
			return fmt.Errorf("bolttable: trim change log: %w", err)
		}
	}
	return nil
}

func encodeChange(c Change) []byte {
	b := binary.AppendUvarint(nil, uint64(len(c.Subjects)))
	for _, s := range c.Subjects {
		b = binary.AppendUvarint(b, uint64(len(s)))
		b = append(b, s...)
	}
	b = binary.AppendUvarint(b, uint64(len(c.Marks)))
	for _, m := range c.Marks {
		b = binary.AppendUvarint(b, uint64(len(m)))
		b = append(b, m...)
	}
	return b
}

func decodeChange(k, v []byte) (Change, error) {
	bad := fmt.Errorf("%w: change log entry", ErrCorrupt)
	if len(k) != 8 {
		return Change{}, bad
	}
	c := Change{Rev: binary.BigEndian.Uint64(k)}
	d := decoder{b: v}
	n := d.uvarint()
	for range n {
		c.Subjects = append(c.Subjects, Subject(d.field()))
	}
	n = d.uvarint()
	for range n {
		c.Marks = append(c.Marks, string(d.field()))
	}
	if d.bad || len(d.b) != 0 {
		return Change{}, bad
	}
	return c, nil
}

// decoder reads uvarint-delimited fields, latching bad on the first
// malformed one so a corrupt count cannot loop or allocate unbounded.
type decoder struct {
	b   []byte
	bad bool
}

func (d *decoder) uvarint() uint64 {
	if d.bad {
		return 0
	}
	n, w := binary.Uvarint(d.b)
	if w <= 0 || n > uint64(len(d.b)) {
		d.bad = true
		return 0
	}
	d.b = d.b[w:]
	return n
}

func (d *decoder) field() []byte {
	n := d.uvarint()
	if d.bad || n > uint64(len(d.b)) {
		d.bad = true
		return nil
	}
	f := bytes.Clone(d.b[:n])
	d.b = d.b[n:]
	return f
}

// sequencer delivers OnCommit in revision order: bbolt runs commit handlers
// after releasing its writer lock, so two commits' handlers may run in
// either order (https://github.com/etcd-io/bbolt/blob/v1.5.0/tx.go#L278).
type sequencer struct {
	on      func(Change)
	pending map[uint64]Change
	mu      sync.Mutex
	next    uint64
}

func newSequencer(head uint64, on func(Change)) *sequencer {
	return &sequencer{on: on, pending: map[uint64]Change{}, next: head + 1}
}

func (s *sequencer) publish(c Change) {
	if s.on == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending[c.Rev] = c
	for {
		n, ok := s.pending[s.next]
		if !ok {
			return
		}
		delete(s.pending, s.next)
		s.next++
		s.on(n)
	}
}
