package bolttable

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"go.etcd.io/bbolt"
)

type txPhase uint8

const (
	phaseRun txPhase = iota
	phaseBeforeCommit
	phaseDone
)

// Tx is a read-write transaction. It is valid only inside the function
// Update or Batch passed it to.
type Tx struct {
	db       *DB
	read     *ReadTx
	err      error
	seen     map[string]struct{}
	markSeen map[string]struct{}
	subjects []Subject
	marks    []string
	after    []func()
	phase    txPhase
}

// ReadTx reads one consistent snapshot. It is valid only inside the
// function View passed it to, or while its Tx is.
type ReadTx struct {
	db  *DB
	btx *bbolt.Tx
}

// Update runs fn in one bbolt read-write transaction. fn's error, or one from
// BeforeCommit, rolls it back and is returned as is; a failure of bbolt
// itself wraps ErrCommit. A context already done runs nothing.
func (db *DB) Update(ctx context.Context, fn func(*Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return commit(db.bolt.Update, func(btx *bbolt.Tx) error { return db.run(btx, fn) })
}

// Batch is Update through bbolt's grouped commit: concurrent calls share one
// transaction and fsync. fn may run more than once (bbolt reruns a group
// after one call fails, and the failed call alone:
// https://github.com/etcd-io/bbolt/blob/v1.5.0/db.go#L1024); only its last
// run's writes, change and AfterCommit functions survive. Each call keeps its
// own subjects, marks and revision.
func (db *DB) Batch(ctx context.Context, fn func(*Tx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return commit(db.bolt.Batch, func(btx *bbolt.Tx) error { return db.run(btx, fn) })
}

// View runs fn in one read-only transaction. A context already done runs
// nothing.
func (db *DB) View(ctx context.Context, fn func(*ReadTx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return db.bolt.View(func(btx *bbolt.Tx) error { return fn(&ReadTx{db: db, btx: btx}) })
}

// runError carries the error a run returned through bbolt, which hands it
// back as is; any other error is bbolt's own failure to begin or commit.
type runError struct{ err error }

func (e *runError) Error() string { return e.err.Error() }

// commit runs fn through bbolt's Update or Batch and returns fn's error as
// is, or bbolt's own failure wrapped in ErrCommit. The run's error is told
// apart by its type, never compared: a caller's error may be uncomparable,
// and a Batch call's earlier failed run may leave an error that its solo
// rerun, failing to begin, never returns.
func commit(through func(func(*bbolt.Tx) error) error, fn func(*bbolt.Tx) error) error {
	err := through(func(btx *bbolt.Tx) error {
		if err := fn(btx); err != nil {
			return &runError{err: err}
		}
		return nil
	})
	if re, ok := errors.AsType[*runError](err); ok {
		return re.err
	}
	if err != nil {
		return fmt.Errorf("%w: %w", ErrCommit, err)
	}
	return nil
}

func (db *DB) run(btx *bbolt.Tx, fn func(*Tx) error) error {
	tx := &Tx{db: db, read: &ReadTx{db: db, btx: btx}, seen: map[string]struct{}{}, markSeen: map[string]struct{}{}, phase: phaseRun}
	defer func() { tx.phase = phaseDone }()
	if err := fn(tx); err != nil {
		return err
	}
	if tx.err != nil {
		return tx.err
	}
	return tx.finish(btx)
}

// finish reserves the next revision when the transaction recorded a subject
// or mark, runs BeforeCommit, records the change and trims the log, all
// before bbolt commits; it registers the change's delivery after commit.
func (tx *Tx) finish(btx *bbolt.Tx) error {
	after := tx.after
	if len(tx.subjects) == 0 && len(tx.marks) == 0 {
		if len(after) > 0 {
			btx.OnCommit(func() { runAll(after) })
		}
		return nil
	}
	lib := btx.Bucket([]byte(libBucket))
	head, err := readHead(lib)
	if err != nil {
		return err
	}
	rev := head + 1
	subjects := tx.subjects
	if bc := tx.db.opts.BeforeCommit; bc != nil {
		tx.phase = phaseBeforeCommit
		got, err := bc(tx, &Pending{Rev: rev, Touched: cloneSubjects(tx.subjects), Marks: slices.Clone(tx.marks)})
		if err != nil {
			return err
		}
		if tx.err != nil {
			return tx.err
		}
		subjects = cloneSubjects(got)
		after = tx.after
	}
	change := Change{Rev: rev, Subjects: subjects, Marks: slices.Clone(tx.marks)}
	if err := record(lib, change, tx.db.retention); err != nil {
		return err
	}
	btx.OnCommit(func() {
		tx.db.seq.publish(change)
		runAll(after)
	})
	return nil
}

func runAll(fns []func()) {
	for _, fn := range fns {
		fn()
	}
}

func cloneSubjects(in []Subject) []Subject {
	out := make([]Subject, len(in))
	for i, s := range in {
		out[i] = Subject(slices.Clone([]byte(s)))
	}
	return out
}

// Read returns a ReadTx over this transaction, seeing its own writes.
func (tx *Tx) Read() *ReadTx { return tx.read }

// Mark records a mark in this transaction's change; a transaction with a
// mark reserves a revision even when it wrote nothing.
func (tx *Tx) Mark(m string) {
	tx.mustRun()
	if _, dup := tx.markSeen[m]; !dup {
		tx.markSeen[m] = struct{}{}
		tx.marks = append(tx.marks, m)
	}
}

// Touch records s as a subject of this transaction's change without a write.
// Inside BeforeCommit it fails the transaction.
func (tx *Tx) Touch(s Subject) {
	tx.mustRun()
	if tx.phase == phaseBeforeCommit {
		_ = tx.fail(errors.New("bolttable: Touch inside BeforeCommit"))
		return
	}
	tx.addSubject(Subject(slices.Clone([]byte(s))))
}

// AfterCommit registers fn to run once after this transaction commits, never
// after a rollback or a discarded Batch run; for signals about silent writes.
func (tx *Tx) AfterCommit(fn func()) {
	tx.mustRun()
	tx.after = append(tx.after, fn)
}

// mustRun panics on a Tx used after its function returned: the bbolt
// transaction under it is gone, and a silently dropped mark or subject
// would lose a change.
func (tx *Tx) mustRun() {
	if tx.phase == phaseDone {
		panic(errClosed)
	}
}

func (tx *Tx) addSubject(s Subject) {
	if _, dup := tx.seen[string(s)]; !dup {
		tx.seen[string(s)] = struct{}{}
		tx.subjects = append(tx.subjects, s)
	}
}

func (tx *Tx) subject(td *tableDef, kb []byte) {
	if !td.silent {
		tx.addSubject(append(Subject{td.prefix}, kb...))
	}
}

// fail makes err the transaction's outcome: it rolls back however fn
// returns.
func (tx *Tx) fail(err error) error {
	if tx.err == nil {
		tx.err = err
	}
	return err
}

func (tx *Tx) usable() error {
	switch {
	case tx.phase == phaseDone:
		return errClosed
	case tx.err != nil:
		return tx.err
	}
	return nil
}

// writeCheck admits a write to td's rows: a non-silent write inside
// BeforeCommit would miss the change being recorded, so it fails the
// transaction.
func (tx *Tx) writeCheck(td *tableDef) error {
	if err := tx.usable(); err != nil {
		return err
	}
	if tx.phase == phaseBeforeCommit && !td.silent {
		return tx.fail(fmt.Errorf("bolttable: write to non-silent table %s inside BeforeCommit", td.qname))
	}
	return nil
}

func (r *ReadTx) usable() error {
	if r.btx.DB() == nil {
		return errTxClosed
	}
	return nil
}

func (r *ReadTx) namespace(td *tableDef) (*bbolt.Bucket, error) {
	if err := r.usable(); err != nil {
		return nil, err
	}
	if td.ns.schema != r.db.schema {
		return nil, fmt.Errorf("bolttable: table %s is not in this file's schema", td.qname)
	}
	nsb := r.btx.Bucket([]byte(td.ns.name))
	if nsb == nil {
		return nil, fmt.Errorf("%w: namespace %s has no bucket", ErrCorrupt, td.ns.name)
	}
	return nsb, nil
}

func (r *ReadTx) buckets(td *tableDef) (ns, rows *bbolt.Bucket, err error) {
	if ns, err = r.namespace(td); err != nil {
		return nil, nil, err
	}
	if rows = ns.Bucket([]byte{td.prefix}); rows == nil {
		return nil, nil, fmt.Errorf("%w: table %s has no bucket", ErrCorrupt, td.qname)
	}
	return ns, rows, nil
}

func (r *ReadTx) rows(td *tableDef) (*bbolt.Bucket, error) {
	_, rows, err := r.buckets(td)
	return rows, err
}

func (r *ReadTx) index(d *indexDef) (*bbolt.Bucket, error) {
	ns, err := r.namespace(d.table)
	if err != nil {
		return nil, err
	}
	ib := ns.Bucket(d.bucket)
	if ib == nil {
		return nil, fmt.Errorf("%w: index %s has no bucket", ErrCorrupt, d.qname)
	}
	return ib, nil
}
