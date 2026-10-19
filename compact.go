package bolttable

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"

	"go.etcd.io/bbolt"
)

// CompactIfFree rewrites the file at path without its free pages when they
// reach both minFreeRatio of the file and minFreeBytes, and reports whether
// it did. An absent path answers (false, nil). It runs before Open and needs
// the caller's exclusive access: bbolt's lock is a flock on the open file's
// inode (https://github.com/etcd-io/bbolt/blob/v1.5.0/bolt_unix.go#L32), so a process waiting in
// Open across the rename would acquire the old one. The copy is
// written to <path>.compact-tmp (a stale one is removed first), synced, and
// renamed over path, so a crash at any point leaves path whole.
func CompactIfFree(path string, minFreeRatio float64, minFreeBytes int64) (bool, error) {
	if math.IsNaN(minFreeRatio) || minFreeRatio < 0 || minFreeRatio > 1 || minFreeBytes < 0 {
		return false, fmt.Errorf("bolttable: compact thresholds %v and %d out of range", minFreeRatio, minFreeBytes)
	}
	fi, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("bolttable: compact: %w", err)
	}
	tmp := path + ".compact-tmp"
	if rerr := removeFile(tmp); rerr != nil && !errors.Is(rerr, fs.ErrNotExist) {
		return false, fmt.Errorf("bolttable: remove stale %s: %w", tmp, rerr)
	}
	done, err := compactWhenFree(path, tmp, fi.Size(), minFreeRatio, minFreeBytes)
	if err != nil || !done {
		return false, err
	}
	if err := beforeSwap(tmp); err != nil {
		return false, err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = removeFile(tmp)
		return false, fmt.Errorf("bolttable: compact rename: %w", err)
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return true, fmt.Errorf("bolttable: compact: %w", err)
	}
	return true, nil
}

// compactWhenFree writes the compacted copy to tmp when the file's free
// pages reach both thresholds, and reports whether it did.
func compactWhenFree(path, tmp string, size int64, minFreeRatio float64, minFreeBytes int64) (bool, error) {
	ro := boltOptions(true)
	// A read-only open reads no freelist unless asked, and would report no
	// free pages (https://github.com/etcd-io/bbolt/blob/v1.5.0/db.go#L229).
	ro.PreLoadFreelist = true
	src, err := bbolt.Open(path, 0o600, ro)
	if err != nil {
		return false, fmt.Errorf("bolttable: compact open %s: %w", path, err)
	}
	bs := src.Stats()
	free := int64(bs.FreePageN+bs.PendingPageN) * int64(src.Info().PageSize)
	if free < minFreeBytes || float64(free) < minFreeRatio*float64(size) {
		return false, src.Close()
	}
	err = compactInto(src, tmp)
	if cerr := src.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("bolttable: compact close %s: %w", path, cerr)
	}
	if err != nil {
		_ = removeFile(tmp)
		return false, err
	}
	return true, nil
}

func compactInto(src *bbolt.DB, tmp string) error {
	dst, err := bbolt.Open(tmp, 0o600, &bbolt.Options{Timeout: openTimeout, FreelistType: bbolt.FreelistMapType, NoSync: true})
	if err != nil {
		return fmt.Errorf("bolttable: compact temporary file: %w", err)
	}
	if err := bbolt.Compact(dst, src, backupTxBytes); err != nil {
		_ = dst.Close()
		return fmt.Errorf("bolttable: compact: %w", err)
	}
	if err := dst.Sync(); err != nil {
		_ = dst.Close()
		return fmt.Errorf("bolttable: compact sync: %w", err)
	}
	if err := dst.Close(); err != nil {
		return fmt.Errorf("bolttable: compact close: %w", err)
	}
	return nil
}
