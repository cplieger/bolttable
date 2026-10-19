// Package bolttable provides typed tables over a bbolt file: namespaces of
// tables with declared indexes, counters and cascades, a revision counter
// with a change feed, a per-namespace format digest with reset, and
// verification, statistics, online backup and offline compaction.
//
// A Schema is declared once, usually through Must, and sealed by Open. Keys
// go through a KeyCodec, so their byte layout, and with it the scan order, is
// the caller's; values through a Codec, JSON by default. Every declaration
// that shapes stored bytes enters its namespace's digest, and Open refuses a
// file whose stored digests differ with a *FormatError.
//
// A transaction that writes a non-silent table, Touches a subject or sets a
// Mark reserves the next revision and records one Change, which Since
// returns and OnCommit receives in revision order. Every slice the package
// returns or keeps is a copy.
package bolttable
