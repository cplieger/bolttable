package bolttable

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"go.etcd.io/bbolt"
)

// backupTxBytes is how many key and value bytes one backup transaction
// copies before it commits, bounding the memory of a large copy.
const backupTxBytes = 64 << 20

// Test seams over the file-system steps of Backup and CompactIfFree.
var (
	removeFile = os.Remove
	syncDir    = syncDirectory
	backupWalk = func(string) {}
	beforeSwap = func(string) error { return nil }
)

// createExclusive is bbolt's OpenFile for a backup's temporary file, so an
// existing one, which another process may own, fails the open.
func createExclusive(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag|os.O_EXCL, perm)
}

// Backup writes a compacted copy of the file to path, mode 0600, from one
// read transaction while the database stays open. It walks into an
// exclusively created .<base>.bolttable-tmp beside path and publishes it with
// a hard link, which fails for any existing path, one created during the walk
// included. A failure before the link leaves no path and removes the
// temporary file, never one Backup did not create. After the link path is
// complete, and a failed removal of the temporary name or directory fsync
// wraps ErrPublished.
func (db *DB) Backup(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("bolttable: backup %s: %w", path, fs.ErrExist)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("bolttable: backup: %w", err)
	}
	dir, base := filepath.Split(path)
	tmp := filepath.Join(dir, "."+base+".bolttable-tmp")
	dst, err := bbolt.Open(tmp, 0o600, &bbolt.Options{
		Timeout: openTimeout, FreelistType: bbolt.FreelistMapType, NoSync: true, OpenFile: createExclusive,
	})
	if err != nil {
		if !errors.Is(err, fs.ErrExist) {
			_ = removeFile(tmp)
		}
		return fmt.Errorf("bolttable: backup temporary file: %w", err)
	}
	if err := db.copyTo(ctx, dst, path); err != nil {
		_ = dst.Close()
		_ = removeFile(tmp)
		return err
	}
	if err := os.Link(tmp, path); err != nil {
		_ = removeFile(tmp)
		return fmt.Errorf("bolttable: publish backup: %w", err)
	}
	if err := removeFile(tmp); err != nil {
		return fmt.Errorf("%w: remove %s: %w", ErrPublished, tmp, err)
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("%w: %w", ErrPublished, err)
	}
	return nil
}

// copyTo walks every bucket into dst, then syncs and closes it.
func (db *DB) copyTo(ctx context.Context, dst *bbolt.DB, path string) error {
	w := &copier{dst: dst}
	err := db.bolt.View(func(src *bbolt.Tx) error {
		var err error
		if w.tx, err = dst.Begin(true); err != nil {
			return err
		}
		if err := src.ForEach(func(name []byte, b *bbolt.Bucket) error {
			return w.bucket(ctx, nil, name, b)
		}); err != nil {
			return err
		}
		backupWalk(path)
		return w.tx.Commit()
	})
	if err != nil {
		if w.tx != nil {
			_ = w.tx.Rollback()
		}
		return fmt.Errorf("bolttable: backup: %w", err)
	}
	if err := dst.Sync(); err != nil {
		return fmt.Errorf("bolttable: backup sync: %w", err)
	}
	if err := dst.Close(); err != nil {
		return fmt.Errorf("bolttable: backup close: %w", err)
	}
	return nil
}

// copier copies keys into a destination transaction it commits every
// backupTxBytes. bbolt keeps a Put's value slice until commit
// (https://github.com/etcd-io/bbolt/blob/v1.5.0/bucket.go#L491), and the source's mapped pages
// stay valid for the whole read transaction, so values are not copied.
type copier struct {
	dst  *bbolt.DB
	tx   *bbolt.Tx
	size int
	keys int
}

func (w *copier) bucket(ctx context.Context, path [][]byte, name []byte, src *bbolt.Bucket) error {
	b, err := w.parent(path)
	if err != nil {
		return err
	}
	var nb *bbolt.Bucket
	if b == nil {
		nb, err = w.tx.CreateBucket(name)
	} else {
		nb, err = b.CreateBucket(name)
	}
	if err != nil {
		return err
	}
	if err := nb.SetSequence(src.Sequence()); err != nil {
		return err
	}
	path = append(path, name)
	return src.ForEach(func(k, v []byte) error {
		if v == nil {
			if child := src.Bucket(k); child != nil {
				return w.bucket(ctx, path, k, child)
			}
		}
		return w.put(ctx, path, k, v)
	})
}

func (w *copier) put(ctx context.Context, path [][]byte, k, v []byte) error {
	w.keys++
	if w.keys%ctxEvery == 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if w.size += len(k) + len(v); w.size > backupTxBytes {
		if err := w.tx.Commit(); err != nil {
			return err
		}
		tx, err := w.dst.Begin(true)
		if err != nil {
			w.tx = nil
			return err
		}
		w.tx, w.size = tx, len(k)+len(v)
	}
	b, err := w.parent(path)
	if err != nil {
		return err
	}
	b.FillPercent = 1
	return b.Put(k, v)
}

// parent resolves path in the current destination transaction; nil path is
// the root.
func (w *copier) parent(path [][]byte) (*bbolt.Bucket, error) {
	if len(path) == 0 {
		return nil, nil
	}
	b := w.tx.Bucket(path[0])
	for _, p := range path[1:] {
		if b == nil {
			break
		}
		b = b.Bucket(p)
	}
	if b == nil {
		return nil, errors.New("backup destination lost a bucket")
	}
	return b, nil
}

func syncDirectory(dir string) error {
	if dir == "" {
		dir = "."
	}
	d, err := os.Open(dir) //nolint:gosec // G703: the directory of the caller's own backup path
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}
