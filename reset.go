package bolttable

import (
	"errors"
	"fmt"
	"os"
	"slices"

	"go.etcd.io/bbolt"
)

// ResetNamespaces empties the named namespaces of the existing file at path,
// sealing s; pass it every name a *FormatError lists. It runs before Open and
// needs the caller's exclusive access. Each name must be stored or declared,
// or be "bolttable", which recreates the library's bucket (revision head and
// change log) at the current layout number. One bbolt transaction deletes the
// stored named buckets and recreates the declared ones, every table starting
// at the highest sequence its namespace stored, so NextID never reissues an id
// across a prefix move, a rename or a removed table.
func ResetNamespaces(path string, s *Schema, names []string) error {
	if s == nil {
		return errors.New("bolttable: ResetNamespaces with a nil schema")
	}
	s.seal()
	want := s.digests()
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("bolttable: reset: %w", err)
	}
	bdb, err := bbolt.Open(path, 0o600, boltOptions(false))
	if err != nil {
		return fmt.Errorf("bolttable: open %s: %w", path, err)
	}
	err = bdb.Update(func(btx *bbolt.Tx) error { return reset(btx, s, want, names) })
	if cerr := bdb.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("bolttable: close %s: %w", path, cerr)
	}
	return err
}

func reset(btx *bbolt.Tx, s *Schema, want map[string][32]byte, names []string) error {
	names = slices.Compact(slices.Sorted(slices.Values(names)))
	for _, name := range names {
		_, declared := want[name]
		if name != libBucket && !declared && btx.Bucket([]byte(name)) == nil {
			return fmt.Errorf("bolttable: reset of %q, neither stored nor declared", name)
		}
	}
	for _, name := range names {
		seed, err := deleteIfPresent(btx, name)
		if err != nil {
			return err
		}
		if ns := s.nsByName[name]; ns != nil {
			if err := ensureNamespace(btx, ns, want[name], seed); err != nil {
				return err
			}
		}
	}
	return ensureLib(btx)
}

// deleteIfPresent deletes the top-level bucket name and returns its highest
// sequence.
func deleteIfPresent(btx *bbolt.Tx, name string) (uint64, error) {
	b := btx.Bucket([]byte(name))
	if b == nil {
		return 0, nil
	}
	seed := highestSequence(b)
	if err := btx.DeleteBucket([]byte(name)); err != nil {
		return 0, fmt.Errorf("bolttable: reset %s: %w", name, err)
	}
	return seed, nil
}

// highestSequence is the greatest sequence among b and its direct child
// buckets: every table's NextID counter in this layout, and any bucket's in
// another.
func highestSequence(b *bbolt.Bucket) uint64 {
	high := b.Sequence()
	_ = b.ForEachBucket(func(k []byte) error {
		high = max(high, b.Bucket(k).Sequence())
		return nil
	})
	return high
}
