package bolttable

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	bbolterrors "go.etcd.io/bbolt/errors"
)

var (
	// ErrFormat matches a *FormatError: the file's stored format differs from
	// the schema.
	ErrFormat = errors.New("bolttable: stored format differs from the schema")
	// ErrConflict matches a *ConflictError: a Put would give a unique index
	// key a second row.
	ErrConflict = errors.New("bolttable: unique index conflict")
	// ErrCorrupt reports stored bytes that do not decode. Its message names
	// the table or index and a fingerprint of the stored key, never key or
	// row bytes, and it drops the codec's own error, whose text may hold
	// them.
	ErrCorrupt = errors.New("bolttable: row does not decode")
	// ErrCommit wraps a failure of bbolt itself to begin or commit a
	// transaction, so a caller tells a storage fault from its own refusal. A
	// failed page write keeps its *os.PathError and a failed fdatasync its
	// syscall.Errno, but bbolt flattens a failure to grow the file to text
	// (https://github.com/etcd-io/bbolt/blob/v1.5.0/db.go#L1245), so a caller
	// classing causes by errno falls back to "other" when none matches. A
	// call on a closed DB answers ErrClosed instead.
	ErrCommit = errors.New("bolttable: commit failed")
	// ErrPublished reports that Backup linked a complete copy at its path,
	// then failed to remove its temporary name or to fsync the directory.
	ErrPublished = errors.New("bolttable: backup published, not made durable")
	// ErrLocked reports that another process, or another handle in this one,
	// holds the file.
	ErrLocked = errors.New("bolttable: database file is locked")
	// ErrInvalidFile reports a file bbolt cannot read as a database: both
	// meta pages rejected, a page size too small to hold a meta page, fewer
	// bytes than its meta records, a freelist page bbolt cannot load or that
	// lists a page it must never reuse, or a page read that faulted or
	// panicked; bbolt's "file size too small" stays unclassified. A panic
	// inside bbolt's open, or a rollback that panics twice, leaves the file
	// mapped but not locked, and bbolt walks a NoFreelistSync file on an
	// unguarded goroutine.
	ErrInvalidFile = errors.New("bolttable: invalid database file")
	// ErrReset reports that Open could not run a reset Options.Reset allowed:
	// keeping the copy, or the reset transaction, failed, and the file is
	// left as it was. A file found unreadable during the reset is
	// ErrInvalidFile instead.
	ErrReset = errors.New("bolttable: reset failed")
	// ErrClosed reports a call on a closed DB or ReadDB, where it also
	// matches bbolt's ErrDatabaseNotOpen, and a call on a Live that is closed
	// or swapping. A writer that runs on its own
	// schedule reads it as a skipped write, never as a storage fault: it
	// never wraps ErrCommit.
	ErrClosed = errors.New("bolttable: database is closed")
	// ErrNewerBuild is what an Options.Inspect check wraps when the file
	// carries a store version stamp above the running build's own.
	ErrNewerBuild = errors.New("bolttable: file written by a newer build")
)

// OpenFailure names why a file cannot open as the outage class an app
// reports in place of the error, checked in this order: "newer" for
// ErrNewerBuild, "damaged" for ErrInvalidFile or ErrCorrupt, "format" for
// ErrFormat, "unwritable" for ErrReset or ErrCommit, which a hook's write at
// open returns, and "unreadable" for any other error. ok is false for nil
// and for ErrLocked, which is no outage.
func OpenFailure(err error) (class string, ok bool) {
	switch {
	case err == nil, errors.Is(err, ErrLocked):
		return "", false
	case errors.Is(err, ErrNewerBuild):
		return "newer", true
	case errors.Is(err, ErrInvalidFile), errors.Is(err, ErrCorrupt):
		return "damaged", true
	case errors.Is(err, ErrFormat):
		return "format", true
	case errors.Is(err, ErrReset), errors.Is(err, ErrCommit):
		return "unwritable", true
	}
	return "unreadable", true
}

// errDBClosed is every call's error on a closed DB. It keeps bbolt's
// sentinel, so a caller matching ErrDatabaseNotOpen keeps working.
var errDBClosed = fmt.Errorf("%w: %w", ErrClosed, bbolterrors.ErrDatabaseNotOpen)

var (
	errTxClosed = errors.New("bolttable: transaction has ended")
	errClosed   = errors.New("bolttable: transaction used after its function returned")
)

// FormatError names every namespace whose stored format differs from the
// schema; errors.Is(err, ErrFormat) matches it. Every name in Namespaces is
// one Reset.Allow is asked about and Reset.Force accepts.
type FormatError struct {
	Namespaces []NamespaceDigest
	// Missing, sorted, is what OpenReadOnly found absent and Open would
	// create: each declared namespace the file has no bucket for, and
	// "bolttable" when it lacks the library bucket or its change log. Open
	// never fills it.
	Missing []string
}

// NamespaceDigest is one mismatched top-level bucket. A zero Want is a
// stored bucket the schema does not declare, or the library's own bucket; a
// zero Stored is a bucket holding no digest, or any bucket of a file whose
// layout is not this library's. A declared namespace the file lacks is a
// FormatError.Missing name, never a NamespaceDigest.
type NamespaceDigest struct {
	Name         string
	Stored, Want [32]byte
}

func (e *FormatError) Error() string {
	msg := ErrFormat.Error()
	if len(e.Namespaces) > 0 {
		parts := make([]string, 0, len(e.Namespaces))
		for _, n := range e.Namespaces {
			parts = append(parts, fmt.Sprintf("%s (stored %s, want %s)", n.Name, shortDigest(n.Stored), shortDigest(n.Want)))
		}
		msg += ": " + strings.Join(parts, ", ")
	}
	if len(e.Missing) > 0 {
		msg += "; missing " + strings.Join(e.Missing, ", ")
	}
	return msg
}

// Is reports whether target is ErrFormat.
func (*FormatError) Is(target error) bool { return target == ErrFormat }

func shortDigest(d [32]byte) string {
	if d == ([32]byte{}) {
		return "none"
	}
	return hex.EncodeToString(d[:4])
}

// ConflictError names the unique index a Put collided on;
// errors.Is(err, ErrConflict) matches it. The transaction stays usable: the
// refused Put wrote nothing.
type ConflictError struct {
	Index string
}

func (e *ConflictError) Error() string { return ErrConflict.Error() + ": " + e.Index }

// Is reports whether target is ErrConflict.
func (*ConflictError) Is(target error) bool { return target == ErrConflict }
