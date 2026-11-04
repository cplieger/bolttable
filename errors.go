package bolttable

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
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
	// classing causes by errno falls back to "other" when none matches.
	ErrCommit = errors.New("bolttable: commit failed")
	// ErrPublished reports that Backup linked a complete copy at its path,
	// then failed to remove its temporary name or to fsync the directory.
	ErrPublished = errors.New("bolttable: backup published, not made durable")
	// ErrLocked reports that another process, or another handle in this one,
	// holds the file.
	ErrLocked = errors.New("bolttable: database file is locked")
	// ErrInvalidFile reports a file bbolt cannot read as a database: both
	// meta pages rejected, fewer bytes than its meta records, or a page read
	// that faulted or panicked. bbolt's "file size too small" stays
	// unclassified. A panic inside bbolt's own open leaves that handle's
	// mapping reserved until the process exits, and a file another program
	// wrote with NoFreelistSync is walked on a goroutine no guard reaches.
	ErrInvalidFile = errors.New("bolttable: invalid database file")
)

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
