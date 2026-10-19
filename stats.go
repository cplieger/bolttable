package bolttable

import (
	"context"
	"fmt"
	"os"

	"go.etcd.io/bbolt"
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
// Commits counts every successful bbolt commit since Open: one per Batch
// group, and silent-only, byte-equal and empty writes too, since bbolt
// commits a meta page for each; failed and rolled-back transactions do not
// count (https://github.com/etcd-io/bbolt/blob/v1.5.0/tx.go#L267).
type Stats struct {
	Namespaces []NamespaceStats
	Rev        uint64
	Oldest     uint64
	Commits    uint64
	FileBytes  int64
	FreeBytes  int64
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
		fi, err := os.Stat(db.bolt.Path())
		if err != nil {
			return fmt.Errorf("bolttable: stats: %w", err)
		}
		st.FileBytes = fi.Size()
		bs := db.bolt.Stats()
		st.FreeBytes = int64(bs.FreePageN+bs.PendingPageN) * int64(db.bolt.Info().PageSize)
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
