// Package bolttable provides typed tables over a bbolt file: namespaces of
// tables with declared indexes, counters and cascades, a revision counter
// with a change feed, a per-namespace format digest with reset, and
// verification, statistics, online backup and compaction at open.
//
// A Schema is declared once, usually through Must, and sealed by Open. Keys
// go through a KeyCodec, so their byte layout, and with it the scan order, is
// the caller's; values through a Codec, JSON by default. Every declaration
// that shapes stored bytes enters its namespace's digest, and Open refuses a
// file whose stored digests differ with a *FormatError unless its
// ResetPolicy allows the reset.
//
// Open takes the exclusive lock on <path>.lock, a file it never removes, and
// under it removes stranded temporary files, inspects the file without
// writing, moves an invalid file aside or resets a changed namespace when
// Options allow it, compacts, and opens the file for writing; OpenReport
// tells what it did. OpenReadOnly holds the same lock shared when
// <path>.lock exists, leaves an absent lock file absent, and writes
// nothing. LockPath takes the lock for a caller's own file operations, and
// Options.Lock runs Open under it.
//
// A transaction that writes a non-silent table, Touches a subject or sets a
// Mark reserves the next revision and records one Change, which Since
// returns and OnCommit receives in revision order. Every slice the package
// returns or keeps is a copy.
package bolttable
