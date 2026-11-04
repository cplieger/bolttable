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
// nil outside tests, is called at named points of the reset and of start.
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

	stepReadInspect = "read-only open (inspect)"
	stepReadOpen    = "read-only open"
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

// openBolt opens path through bbolt inside guardRead and classifies its
// error. It records the *os.File bbolt opens and closes it after a panic,
// since bbolt then returned no handle to close; that releases bbolt's flock.
func openBolt(path, step string, o *bbolt.Options) (*bbolt.DB, *os.File, error) {
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
		_ = f.Close()
	}
	return nil, nil, classify(path, step, err)
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
