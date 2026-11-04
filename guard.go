package bolttable

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"

	"go.etcd.io/bbolt"
	bbolterrors "go.etcd.io/bbolt/errors"
)

// Test seams: boltOpen is every bbolt open the package makes, and stepHook,
// nil outside tests, is called at named points of the reset, of start and of
// closeBolt's rollback retry.
var (
	boltOpen = bbolt.Open
	stepHook func(step string)
)

func hook(step string) {
	if stepHook != nil {
		stepHook(step)
	}
}

// The steps of Open's and OpenReadOnly's procedures, named in their errors.
const (
	stepInspect = "step 5 (inspect)"
	stepReset   = "step 7 (reset)"
	stepCompact = "step 8 (compact)"
	stepOpen    = "step 9 (open)"

	stepReadOpen = "read-only open"
)

// faultOp marks, for guardFault, a call into bbolt that walks pages the
// inspection never read. The guard, the marked call and its panic run on one
// goroutine, so the field needs no lock.
type faultOp struct{ inBbolt bool }

// guardRead runs fn, which reads the file through bbolt, inspects it or
// compacts it into a new file, and turns any panic or memory fault into
// ErrInvalidFile: the inspection never panics on stored bytes
// (FuzzStoredBytes_noPanic), so a panic there is the file's.
func guardRead(path, step string, fn func() error) (err error) {
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	defer func() {
		if v := recover(); v != nil {
			err = invalidPage(path, step, v)
		}
	}()
	return fn()
}

// guardFault runs fn, bolttable code that writes, and turns a memory fault,
// or any panic raised while op marks a bbolt walk, into ErrInvalidFile. It
// re-raises any other panic before the stack unwinds, so a bolttable bug is
// never read as a corrupt file.
func guardFault(path, step string, op *faultOp, fn func() error) (err error) {
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	defer func() {
		v := recover()
		if v == nil {
			return
		}
		// A panic skips the marked call's own clear; a later panic under op
		// is judged by its own call site.
		inBbolt := op.inBbolt
		op.inBbolt = false
		if !inBbolt && !isFault(v) {
			panic(v)
		}
		err = invalidPage(path, step, v)
	}()
	return fn()
}

// isFault reports whether v is the runtime's error for a memory fault under
// SetPanicOnFault, which alone carries the faulting address
// (https://pkg.go.dev/runtime/debug#SetPanicOnFault).
func isFault(v any) bool {
	_, ok := v.(interface {
		runtime.Error
		Addr() uintptr
	})
	return ok
}

// invalidPage drops v's text: some bbolt panics format stored bytes
// (https://github.com/etcd-io/bbolt/blob/v1.5.0/bucket.go#L776).
func invalidPage(path, step string, v any) error {
	what := "bbolt panicked reading a page"
	if isFault(v) {
		what = "memory fault reading a page"
	}
	return fmt.Errorf("%w: %s: %s: %s", ErrInvalidFile, path, step, what)
}

// openBolt opens path through bbolt inside guardRead, after checkPages, and
// classifies its error. It records the *os.File bbolt opens and releases it
// after a panic, since bbolt then returned no handle to close.
func openBolt(path, step string, o *bbolt.Options) (*bbolt.DB, *os.File, error) {
	if err := checkPages(path, step); err != nil {
		return nil, nil, err
	}
	var f *os.File
	o.OpenFile = func(name string, flag int, perm os.FileMode) (*os.File, error) {
		var err error
		f, err = os.OpenFile(name, flag, perm)
		return f, err
	}
	var bdb *bbolt.DB
	err := guardRead(path, step, func() error {
		var err error
		bdb, err = boltOpen(path, 0o600, o)
		return err
	})
	if err == nil {
		return bdb, f, nil
	}
	if f != nil {
		releaseFile(f)
	}
	return nil, nil, classify(path, step, err)
}

// releaseFile releases bbolt's lock on f and closes f, for a handle bbolt
// cannot close. The lock goes first: on Linux bbolt's mapping keeps f's open
// file description, and with it the flock, alive after f closes. The
// mapping itself stays, since bbolt exposes no way to unmap it.
func releaseFile(f *os.File) {
	unlockBolt(f)
	_ = f.Close()
}

// closeBolt closes bdb, which f backs. tx is bdb's last write transaction,
// nil when it had none: one a panic left open makes bbolt's Close wait for
// it forever, so closeBolt rolls it back once more, and releases f instead
// when it stays open.
func closeBolt(path string, bdb *bbolt.DB, f *os.File, tx *bbolt.Tx) error {
	if tx != nil && tx.DB() != nil {
		retryRollback(tx)
		if tx.DB() != nil {
			releaseFile(f)
			return nil
		}
	}
	if err := bdb.Close(); err != nil {
		return fmt.Errorf("bolttable: close %s: %w", path, err)
	}
	return nil
}

// retryRollback rolls tx back, dropping any panic: a rollback that panics
// again leaves tx open, which closeBolt checks.
func retryRollback(tx *bbolt.Tx) {
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	defer func() { _ = recover() }()
	hook("rollback-retry")
	_ = tx.Rollback()
}

// classify maps an error of a bbolt open: a meta bbolt rejects is an invalid
// file, and its flock timeout is another holder.
func classify(path, step string, err error) error {
	switch {
	case errors.Is(err, ErrInvalidFile):
		return err
	case errors.Is(err, bbolterrors.ErrInvalid), errors.Is(err, bbolterrors.ErrChecksum), errors.Is(err, bbolterrors.ErrVersionMismatch):
		return fmt.Errorf("%w: %s: %s: %w", ErrInvalidFile, path, step, err)
	case errors.Is(err, bbolterrors.ErrTimeout):
		return fmt.Errorf("%w: %s", ErrLocked, path)
	}
	return fmt.Errorf("bolttable: open %s: %w", path, err)
}
