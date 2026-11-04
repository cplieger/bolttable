package bolttable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"slices"
)

// The meta and page layout of bbolt v1.5.0
// (https://github.com/etcd-io/bbolt/blob/v1.5.0/internal/common/meta.go,
// https://github.com/etcd-io/bbolt/blob/v1.5.0/internal/common/page.go).
const (
	pageHeaderSize = 16
	metaSize       = 64
	metaMagic      = 0xED0CDAED
	metaVersion    = 2
	freelistFlag   = 0x10
	noFreelist     = ^uint64(0)
)

// fileMeta is the meta page bbolt opens a file with.
type fileMeta struct {
	pageSize, freelist, pgid uint64
}

// checkPages reads, without mapping, everything bbolt's open reads from a
// stored file after it maps it: the meta page it picks, the file length the
// meta records, and the freelist page. bbolt panics on a bad freelist page
// after mapping the file, where nothing can unmap it
// (https://github.com/etcd-io/bbolt/blob/v1.5.0/db.go#L303), and accepts a
// freelist listing a page it must never hand out. A file bbolt refuses with
// an error of its own passes.
func checkPages(path, step string) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("bolttable: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("bolttable: open %s: %w", path, err)
	}
	size := uint64(fi.Size()) //nolint:gosec // a file's size is never negative
	if why := pagesFault(f, size); why != "" {
		return fmt.Errorf("%w: %s: %s: %s", ErrInvalidFile, path, step, why)
	}
	return nil
}

// pagesFault says what checkPages rejects in the size bytes of r, or "".
func pagesFault(r io.ReaderAt, size uint64) string {
	m, ok := chosenMeta(r, size)
	if !ok {
		return ""
	}
	// bbolt writes each meta page into a buffer of one page, and
	// freelistIDs's slot bound wraps on a page smaller than its header.
	if m.pageSize < pageHeaderSize+metaSize {
		return fmt.Sprintf("meta records page size %d", m.pageSize)
	}
	if m.pgid > size/m.pageSize {
		return fmt.Sprintf("truncated: meta records %d pages of %d bytes, file holds %d bytes", m.pgid, m.pageSize, size)
	}
	return badFreelist(r, m)
}

// readAt reads len(buf) bytes at off, which callers keep within the file,
// whose size fits an int64.
func readAt(r io.ReaderAt, buf []byte, off uint64) error {
	_, err := r.ReadAt(buf, int64(off)) //nolint:gosec // off is at most the file's size
	return err
}

// chosenMeta follows bbolt's open: the page size from the first valid meta
// page, then the valid meta of the two at that size with the higher txid
// (https://github.com/etcd-io/bbolt/blob/v1.5.0/db.go#L332,
// https://github.com/etcd-io/bbolt/blob/v1.5.0/db.go#L1141). It reports
// false where bbolt fails the open itself.
func chosenMeta(r io.ReaderAt, size uint64) (fileMeta, bool) {
	ps, ok := metaPageSize(r, size)
	if !ok || size < 2*ps {
		return fileMeta{}, false
	}
	m0, ok0 := readMetaAt(r, 0)
	m1, ok1 := readMetaAt(r, ps)
	a, okA, b, okB := m0, ok0, m1, ok1
	if m1.txid > m0.txid {
		a, okA, b, okB = m1, ok1, m0, ok0
	}
	switch {
	case okA:
		return fileMeta{pageSize: ps, freelist: a.freelist, pgid: a.pgid}, true
	case okB:
		return fileMeta{pageSize: ps, freelist: b.freelist, pgid: b.pgid}, true
	}
	return fileMeta{}, false
}

// metaPageSize is bbolt's getPageSize: the first meta page's size, else the
// second meta page's found at 1 KiB to 16 MiB, else the host page size when
// either meta page could be read.
func metaPageSize(r io.ReaderAt, size uint64) (uint64, bool) {
	var buf [0x1000]byte
	readable := false
	// ReadAt errs on any short read, so err alone gives bbolt's readable tests.
	if err := readAt(r, buf[:], 0); err == nil {
		readable = true
		if m, ok := decodeMeta(buf[:]); ok {
			return uint64(m.pageSize), true
		}
	}
	for i := range 15 {
		pos := uint64(1024) << i
		if pos+1024 >= size {
			break
		}
		if err := readAt(r, buf[:], pos); err == nil || errors.Is(err, io.EOF) {
			readable = true
			if m, ok := decodeMeta(buf[:]); ok {
				return uint64(m.pageSize), true
			}
		}
	}
	return uint64(os.Getpagesize()), readable //nolint:gosec // a page size is positive
}

type rawMeta struct {
	pageSize       uint32
	freelist, pgid uint64
	txid           uint64
}

func readMetaAt(r io.ReaderAt, off uint64) (rawMeta, bool) {
	var buf [pageHeaderSize + metaSize]byte
	if err := readAt(r, buf[:], off); err != nil {
		return rawMeta{}, false
	}
	return decodeMeta(buf[:])
}

// decodeMeta reads the meta in page, and reports whether bbolt's Validate
// accepts it: its magic, version and FNV-64a checksum of the 56 bytes before
// the checksum.
func decodeMeta(page []byte) (rawMeta, bool) {
	b := page[pageHeaderSize : pageHeaderSize+metaSize]
	le := binary.LittleEndian
	h := fnv.New64a()
	_, _ = h.Write(b[:56])
	if le.Uint32(b[0:]) != metaMagic || le.Uint32(b[4:]) != metaVersion || le.Uint64(b[56:]) != h.Sum64() {
		return rawMeta{}, false
	}
	return rawMeta{pageSize: le.Uint32(b[8:]), freelist: le.Uint64(b[32:]), pgid: le.Uint64(b[40:]), txid: le.Uint64(b[48:])}, true
}

// badFreelist says what is wrong with m's freelist page, read from r whose
// length holds m.pgid pages, or "" when bbolt can load it and every page it
// lists is one bbolt may hand out: past the meta pages, below the
// high-water mark, not the freelist's own, listed once.
func badFreelist(r io.ReaderAt, m fileMeta) string {
	if m.freelist == noFreelist {
		return ""
	}
	if m.freelist < 2 || m.freelist >= m.pgid {
		return fmt.Sprintf("meta names freelist page %d, high-water mark %d", m.freelist, m.pgid)
	}
	ids, overflow, why := freelistIDs(r, m)
	if why != "" {
		return why
	}
	slices.Sort(ids)
	for i, id := range ids {
		switch {
		case id < 2:
			return fmt.Sprintf("freelist lists meta page %d", id)
		case id >= m.pgid:
			return fmt.Sprintf("freelist lists page %d at or past the high-water mark %d", id, m.pgid)
		case id >= m.freelist && id <= m.freelist+overflow:
			return fmt.Sprintf("freelist lists its own page %d", id)
		case i > 0 && ids[i-1] == id:
			return fmt.Sprintf("freelist lists page %d twice", id)
		}
	}
	return ""
}

// freelistIDs reads the ids on m's freelist page, which lies below m.pgid,
// and its overflow count, or says why bbolt cannot read them.
func freelistIDs(r io.ReaderAt, m fileMeta) (ids []uint64, overflow uint64, why string) {
	var hdr [pageHeaderSize + 8]byte
	at := m.freelist * m.pageSize
	if err := readAt(r, hdr[:], at); err != nil {
		return nil, 0, fmt.Sprintf("freelist page %d unreadable: %v", m.freelist, err)
	}
	le := binary.LittleEndian
	if flags := le.Uint16(hdr[8:]); flags != freelistFlag {
		return nil, 0, fmt.Sprintf("freelist page %d has type flags %#x", m.freelist, flags)
	}
	overflow = uint64(le.Uint32(hdr[12:]))
	if overflow >= m.pgid-m.freelist {
		return nil, 0, fmt.Sprintf("freelist page %d overflows %d pages, past the high-water mark %d", m.freelist, overflow, m.pgid)
	}
	// A count of 0xFFFF moves the count into the first id slot.
	first, count := uint64(0), uint64(le.Uint16(hdr[10:]))
	if count == 0xFFFF {
		first, count = 1, le.Uint64(hdr[pageHeaderSize:])
	}
	if slots := ((overflow+1)*m.pageSize - pageHeaderSize) / 8; first > slots || count > slots-first {
		return nil, 0, fmt.Sprintf("freelist page %d counts %d ids, more than its pages hold", m.freelist, count)
	}
	raw := make([]byte, 8*count)
	if err := readAt(r, raw, at+pageHeaderSize+8*first); err != nil {
		return nil, 0, fmt.Sprintf("freelist page %d unreadable: %v", m.freelist, err)
	}
	ids = make([]uint64, count)
	for i := range ids {
		ids[i] = le.Uint64(raw[8*i:])
	}
	return ids, overflow, ""
}
