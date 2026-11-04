package bolttable

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"go.etcd.io/bbolt"
)

// resetFile empties the named namespaces of the file at path, each one
// stored, declared or "bolttable", which recreates the library's bucket
// (revision head and change log) at the current layout number. One bbolt
// transaction, committed explicitly so the commit sits inside op's mark,
// deletes the stored named buckets and recreates the declared ones, every
// table starting at the highest sequence its namespace stored, so NextID
// never reissues an id across a prefix move, a rename or a removed table.
func resetFile(path string, s *Schema, want map[string][32]byte, names []string, op *faultOp) (err error) {
	bdb, f, err := openBolt(path, stepReset, boltOptions(false))
	if err != nil {
		return err
	}
	var btx *bbolt.Tx
	defer func() {
		if cerr := closeBolt(path, bdb, f, btx); err == nil {
			err = cerr
		}
	}()
	return guardFault(path, stepReset, op, func() error {
		var berr error
		if btx, berr = bdb.Begin(true); berr != nil {
			return fmt.Errorf("bolttable: reset %s: %w", path, berr)
		}
		defer func() {
			hook("reset-rollback")
			_ = btx.Rollback()
		}()
		hook("reset")
		if rerr := reset(btx, s, want, names, op); rerr != nil {
			return rerr
		}
		op.inBbolt = true
		hook("reset-commit")
		cerr := btx.Commit()
		op.inBbolt = false
		if cerr != nil {
			return fmt.Errorf("bolttable: reset %s: %w", path, cerr)
		}
		return nil
	})
}

func reset(btx *bbolt.Tx, s *Schema, want map[string][32]byte, names []string, op *faultOp) error {
	for _, name := range names {
		seed, err := deleteIfPresent(btx, name, op)
		if err != nil {
			return err
		}
		hook("reset-ensure")
		if ns := s.nsByName[name]; ns != nil {
			if err := ensureNamespace(btx, ns, want[name], seed); err != nil {
				return err
			}
		}
	}
	return ensureLib(btx)
}

// deleteIfPresent deletes the top-level bucket name and returns its highest
// sequence. Both walk the namespace's subtree, pages the inspection never
// read, so op marks them as bbolt's.
func deleteIfPresent(btx *bbolt.Tx, name string, op *faultOp) (uint64, error) {
	b := btx.Bucket([]byte(name))
	if b == nil {
		return 0, nil
	}
	op.inBbolt = true
	hook("reset-walk")
	seed := highestSequence(b)
	err := btx.DeleteBucket([]byte(name))
	op.inBbolt = false
	if err != nil {
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

// keepCopy copies the file at path to <path>.<digest>.<stamp>.old through
// <path>.copy-tmp and a hard link, makes it durable, and prunes the copies
// beyond the keep newest. A failure removes copy-tmp and leaves path as it
// was; a stray .old may remain.
func keepCopy(path, digest string, keep int) (string, error) {
	tmp := path + ".copy-tmp"
	old := path + "." + digest + "." + time.Now().UTC().Format(stampLayout) + ".old"
	if err := copyFile(path, tmp); err != nil {
		_ = removeFile(tmp)
		return "", fmt.Errorf("bolttable: copy %s: %w", path, err)
	}
	if err := linkFile(tmp, old); err != nil {
		_ = removeFile(tmp)
		return "", fmt.Errorf("bolttable: keep %s: %w", old, err)
	}
	if err := removeFile(tmp); err != nil {
		return "", fmt.Errorf("bolttable: remove %s: %w", tmp, err)
	}
	dir := filepath.Dir(path)
	if err := syncDir(dir); err != nil {
		return "", fmt.Errorf("bolttable: keep %s: %w", old, err)
	}
	return old, prune(dir, filepath.Base(path), keep)
}

// copyFileSync copies src to a new file dst, mode 0600, and syncs it.
func copyFileSync(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

// prune removes every <base>.<8 hex>.<stamp>.old in dir beyond the keep
// newest by stamp, and leaves every other name alone.
func prune(dir, base string, keep int) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("bolttable: prune: %w", err)
	}
	type kept struct {
		at   time.Time
		name string
	}
	var copies []kept
	for _, e := range entries {
		if at, ok := keptStamp(base, e.Name()); ok {
			copies = append(copies, kept{at, e.Name()})
		}
	}
	slices.SortFunc(copies, func(a, b kept) int { return b.at.Compare(a.at) })
	for _, c := range copies[min(keep, len(copies)):] {
		if err := removeFile(filepath.Join(dir, c.name)); err != nil {
			return fmt.Errorf("bolttable: prune: %w", err)
		}
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("bolttable: prune: %w", err)
	}
	return nil
}

// keptStamp parses name as <base>.<8 lowercase hex>.<stamp>.old exactly.
func keptStamp(base, name string) (time.Time, bool) {
	rest, ok := strings.CutPrefix(name, base+".")
	if !ok {
		return time.Time{}, false
	}
	rest, ok = strings.CutSuffix(rest, ".old")
	if !ok {
		return time.Time{}, false
	}
	digest, stamp, ok := strings.Cut(rest, ".")
	if b, err := hex.DecodeString(digest); !ok || err != nil || len(b) != 4 || hex.EncodeToString(b) != digest {
		return time.Time{}, false
	}
	at, err := time.Parse(stampLayout, stamp)
	if err != nil || at.Format(stampLayout) != stamp {
		return time.Time{}, false
	}
	return at, true
}
