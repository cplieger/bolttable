package bolttable

import (
	"fmt"
	"math"
	"os"
	"path/filepath"

	"go.etcd.io/bbolt"
)

// CompactPolicy is when Open rewrites the file without its free pages: when
// its free bytes, free and pending pages times the page size, reach both
// MinFreeRatio of the file size and MinFreeBytes. The zero value never
// compacts. MinFreeRatio must lie in [0, 1] and MinFreeBytes must not be
// negative. A compaction that fails leaves the file whole and does not fail
// Open: OpenReport.CompactErr holds it.
type CompactPolicy struct {
	MinFreeRatio float64
	MinFreeBytes int64
}

func (c CompactPolicy) validate() error {
	if math.IsNaN(c.MinFreeRatio) || c.MinFreeRatio < 0 || c.MinFreeRatio > 1 || c.MinFreeBytes < 0 {
		return fmt.Errorf("bolttable: compact thresholds %v and %d out of range", c.MinFreeRatio, c.MinFreeBytes)
	}
	return nil
}

func (c CompactPolicy) due(m freeMeasure) bool {
	return m.free >= c.MinFreeBytes && float64(m.free) >= c.MinFreeRatio*float64(m.size)
}

// freeMeasure is a file's size and the bytes of it bbolt can reuse.
type freeMeasure struct{ size, free int64 }

// freeBytes counts pending pages too: compaction drops them.
func freeBytes(bs *bbolt.Stats, pageSize int) int64 {
	return int64(bs.FreePageN+bs.PendingPageN) * int64(pageSize)
}

// compactIfFree rewrites the file at path without its free pages when c is
// due, and reports whether it did. m is the inspection's measure, or nil to
// measure the file again. The copy is written to <path>.compact-tmp, synced,
// and renamed over path, so a crash at any point leaves path whole; a
// failure removes the copy. Reading the file runs inside guardRead.
func compactIfFree(path string, c CompactPolicy, m *freeMeasure) (bool, error) {
	if m != nil && !c.due(*m) {
		return false, nil
	}
	// openReader owns why a read-only handle needs PreLoadFreelist.
	ro := boltOptions(true)
	ro.PreLoadFreelist = true
	src, _, err := openBolt(path, stepCompact, ro)
	if err != nil {
		return false, err
	}
	if m == nil {
		due, merr := remeasure(path, src, c)
		if merr != nil || !due {
			_ = src.Close()
			return false, merr
		}
	}
	tmp := path + ".compact-tmp"
	if err := swapCompacted(path, tmp, src); err != nil {
		_ = removeFile(tmp)
		return false, err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return true, fmt.Errorf("bolttable: compact: %w", err)
	}
	return true, nil
}

// remeasure reports whether c is due on the file src has open.
func remeasure(path string, src *bbolt.DB, c CompactPolicy) (bool, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return false, fmt.Errorf("bolttable: compact: %w", err)
	}
	return c.due(freeMeasure{size: fi.Size(), free: freeBytes(new(src.Stats()), src.Info().PageSize)}), nil
}

// swapCompacted writes src's compacted copy to tmp, closes src, and renames
// tmp over path.
func swapCompacted(path, tmp string, src *bbolt.DB) error {
	err := guardRead(path, stepCompact, func() error { return compactInto(src, tmp) })
	if cerr := src.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("bolttable: compact close %s: %w", path, cerr)
	}
	if err != nil {
		return err
	}
	if err := beforeSwap(tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("bolttable: compact rename: %w", err)
	}
	return nil
}

// compactInto writes src's compacted copy to tmp. A page panic in src
// unwinds through it, so only the deferred close releases tmp's handle;
// bbolt's Compact has already rolled back its write transaction
// (https://github.com/etcd-io/bbolt/blob/v1.5.0/compact.go#L15).
func compactInto(src *bbolt.DB, tmp string) error {
	dst, err := boltOpen(tmp, 0o600, &bbolt.Options{Timeout: boltTryOnce, FreelistType: bbolt.FreelistMapType, NoSync: true})
	if err != nil {
		return fmt.Errorf("bolttable: compact temporary file: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = dst.Close()
		}
	}()
	if err := bbolt.Compact(dst, src, backupTxBytes); err != nil {
		return fmt.Errorf("bolttable: compact: %w", err)
	}
	if err := dst.Sync(); err != nil {
		return fmt.Errorf("bolttable: compact sync: %w", err)
	}
	closed = true
	if err := dst.Close(); err != nil {
		return fmt.Errorf("bolttable: compact close: %w", err)
	}
	return nil
}
