package bolttable

import (
	"context"
	"fmt"
	"os"

	"go.etcd.io/bbolt"
	bbolterrors "go.etcd.io/bbolt/errors"
)

// TableStats is one table's row count and the bytes its pages use.
type TableStats struct {
	Name  string
	Rows  int
	Bytes int64
}

// NamespaceStats is one namespace's stored format digest and its tables.
type NamespaceStats struct {
	Name   string
	Tables []TableStats
	Format [32]byte
}

// Stats describes an open file. Rev and Oldest bound what Since returns.
// Commits counts every bbolt commit since Open whose meta page reached the
// file: one per Batch group, and silent-only, byte-equal and empty writes
// too. A rolled-back transaction does not count; one whose fdatasync failed
// after the meta write does, though its call returned ErrCommit, since
// bbolt's next transaction id moves past it
// (https://github.com/etcd-io/bbolt/blob/v1.5.0/tx.go#L607).
type Stats struct {
	Namespaces []NamespaceStats
	Rev        uint64
	Oldest     uint64
	Commits    uint64
	Space      Space
}

// Space is the file's size, the bytes its data uses and the bytes bbolt can
// reuse. InUseBytes + FreeBytes is at most FileBytes; the rest is bbolt's
// preallocated tail.
type Space struct {
	// FileBytes is the data file's size, including the tail bbolt grows the
	// file by ahead of use, which holds no page yet.
	FileBytes int64
	// InUseBytes is the pages below bbolt's high-water mark, as the newest
	// valid meta page records it, less FreeBytes.
	InUseBytes int64
	// FreeBytes is free and pending pages times the page size, as of the
	// last write commit or the open. Pending pages count: they become
	// reusable once the readers pinning them finish, and compaction drops
	// them.
	FreeBytes int64
}

// Space reads the file's size, in-use and free bytes without a transaction or
// a page walk: the high-water mark comes from the file's meta pages, read
// without mapping. After Close it returns an error matching bbolt's
// ErrDatabaseNotOpen.
func (db *DB) Space() (Space, error) {
	if db.closed.Load() {
		return Space{}, fmt.Errorf("bolttable: space: %w", bbolterrors.ErrDatabaseNotOpen)
	}
	return spaceOf(db.bolt, db.path, db.pageSize)
}

// spaceOf reads a path and page size cached at open: after bbolt's Close,
// Path is empty and Info panics (https://github.com/etcd-io/bbolt/blob/v1.5.0/db.go#L1124),
// while Stats stays safe.
func spaceOf(bdb *bbolt.DB, path string, pageSize int) (Space, error) {
	f, err := os.Open(path)
	if err != nil {
		return Space{}, fmt.Errorf("bolttable: space: %w", err)
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		return Space{}, fmt.Errorf("bolttable: space: %w", err)
	}
	size := uint64(fi.Size()) //nolint:gosec // a file's size is never negative
	// A commit rewriting one meta page leaves the other valid, so a torn read
	// picks the previous commit's.
	m, ok := chosenMeta(f, size)
	if !ok {
		return Space{}, fmt.Errorf("%w: %s: space: no valid meta page", ErrInvalidFile, path)
	}
	free := freeBytes(new(bdb.Stats()), pageSize)
	// The meta and the freelist counts are read apart, so a commit between them
	// can leave free briefly above the high water.
	inUse := max(int64(m.pgid*m.pageSize)-free, 0) //nolint:gosec // pgid pages fit in the file's size
	return Space{FileBytes: fi.Size(), InUseBytes: inUse, FreeBytes: free}, nil
}

// Stats reads the file's statistics in one read transaction, walking every
// table's pages.
func (db *DB) Stats(ctx context.Context) (Stats, error) {
	var st Stats
	err := db.View(ctx, func(r *ReadTx) error {
		lib, log, err := r.feed()
		if err != nil {
			return err
		}
		if st.Rev, err = readHead(lib); err != nil {
			return err
		}
		if st.Oldest, err = oldestRetained(log, st.Rev, db.retention); err != nil {
			return err
		}
		st.Commits = txID(r.btx) - db.baseTxID
		if st.Space, err = spaceOf(db.bolt, db.path, db.pageSize); err != nil {
			return err
		}
		for _, ns := range db.schema.namespaces {
			s, err := namespaceStats(r.btx, ns)
			if err != nil {
				return err
			}
			st.Namespaces = append(st.Namespaces, s)
		}
		return nil
	})
	return st, err
}

func namespaceStats(btx *bbolt.Tx, ns *Namespace) (NamespaceStats, error) {
	nsb := btx.Bucket([]byte(ns.name))
	if nsb == nil {
		return NamespaceStats{}, fmt.Errorf("%w: namespace %s has no bucket", ErrCorrupt, ns.name)
	}
	s := NamespaceStats{Name: ns.name, Format: storedDigest(nsb)}
	for _, td := range ns.tables {
		tb := nsb.Bucket([]byte{td.prefix})
		if tb == nil {
			return NamespaceStats{}, fmt.Errorf("%w: table %s has no bucket", ErrCorrupt, td.qname)
		}
		bs := tb.Stats()
		s.Tables = append(s.Tables, TableStats{Name: td.name, Rows: bs.KeyN, Bytes: int64(bs.BranchInuse + bs.LeafInuse + bs.InlineBucketInuse)})
	}
	return s, nil
}
