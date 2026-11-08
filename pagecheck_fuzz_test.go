package bolttable

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"

	"go.etcd.io/bbolt"
)

// fuzzFile lays out a file of size bytes as bbolt v1.5.0 does
// (internal/common/meta.go, internal/common/page.go): both meta pages
// recording ps, freelist and pgid, and a freelist page header with flags,
// count, overflow and its first slot, then ids from id upwards. Page 1's
// meta goes first, so a page too small for a meta cannot overwrite page 0's.
func fuzzFile(ps uint32, freelist, pgid uint64, flags, count uint16, overflow uint32, first, id uint64, size uint16) []byte {
	data := make([]byte, size)
	le := binary.LittleEndian
	for _, txid := range []uint64{1, 0} {
		off := txid*uint64(ps) + pageHeaderSize
		if off+metaSize > uint64(len(data)) {
			continue
		}
		meta := data[off : off+metaSize]
		le.PutUint32(meta[0:], metaMagic)
		le.PutUint32(meta[4:], metaVersion)
		le.PutUint32(meta[8:], ps)
		le.PutUint64(meta[32:], freelist)
		le.PutUint64(meta[40:], pgid)
		le.PutUint64(meta[48:], txid)
		h := fnv.New64a()
		_, _ = h.Write(meta[:56])
		le.PutUint64(meta[56:], h.Sum64())
	}
	if ps == 0 || freelist >= uint64(len(data))/uint64(ps) {
		return data
	}
	page := data[freelist*uint64(ps):]
	if len(page) < pageHeaderSize+8 {
		return data
	}
	le.PutUint16(page[8:], flags)
	le.PutUint16(page[10:], count)
	le.PutUint32(page[12:], overflow)
	le.PutUint64(page[16:], first)
	for i := 24; i+8 <= len(page); i += 8 {
		le.PutUint64(page[i:], id)
		id++
	}
	return data
}

// assertBoltOpens fails the test when bbolt panics or faults opening data
// and loading its freelist, the reads after mapping that checkPages vouches
// for. An error bbolt returns is its own refusal.
func assertBoltOpens(t *testing.T, data []byte) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "db.bolt")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	defer debug.SetPanicOnFault(debug.SetPanicOnFault(true))
	defer func() {
		if v := recover(); v != nil {
			t.Fatalf("bbolt panicked opening an accepted file: %v", v)
		}
	}()
	bdb, err := bbolt.Open(path, 0o600, &bbolt.Options{ReadOnly: true, PreLoadFreelist: true, InitialMmapSize: 0})
	if err == nil {
		_ = bdb.Close()
	}
}

// FuzzPagesFault_acceptsOnlyPagesInFile: whatever the stored bytes, the
// check answers without panicking, bbolt opens a file it accepts without
// panicking, and that file has pages that hold a meta page and, when it has
// a freelist, a freelist inside the high-water mark the file's length holds.
func FuzzPagesFault_acceptsOnlyPagesInFile(f *testing.F) {
	f.Add(uint32(4096), uint64(2), uint64(4), uint16(freelistFlag), uint16(0), uint32(0), uint64(0), uint64(3), uint16(16384))
	f.Add(uint32(4096), uint64(2), uint64(4), uint16(freelistFlag), uint16(1), uint32(0), uint64(3), uint64(3), uint16(16384))
	f.Add(uint32(4096), uint64(2), uint64(4), uint16(freelistFlag), uint16(0), uint32(2), uint64(0), uint64(3), uint16(16384))
	f.Add(uint32(8), uint64(50), uint64(600), uint16(freelistFlag), uint16(0xFFFF), uint32(0), uint64(1<<61-2), uint64(3), uint16(8192))
	f.Add(uint32(8), uint64(10), uint64(100), uint16(freelistFlag), uint16(0xFFFF), uint32(0), uint64(1<<28), uint64(3), uint16(8192))
	f.Add(uint32(79), uint64(2), uint64(4), uint16(freelistFlag), uint16(0), uint32(0), uint64(0), uint64(3), uint16(4096))
	f.Add(uint32(80), uint64(2), uint64(4), uint16(freelistFlag), uint16(1), uint32(0), uint64(3), uint64(3), uint16(4096))
	f.Add(uint32(4096), uint64(2), uint64(0), uint16(freelistFlag), uint16(0), uint32(0), uint64(0), uint64(3), uint16(16384))
	f.Add(uint32(4096), uint64(2), uint64(4), uint16(0x02), uint16(0), uint32(0), uint64(0), uint64(3), uint16(16384))
	f.Fuzz(func(t *testing.T, ps uint32, freelist, pgid uint64, flags, count uint16, overflow uint32, first, id uint64, size uint16) {
		data := fuzzFile(ps, freelist, pgid, flags, count, overflow, first, id, size)
		r := bytes.NewReader(data)
		if why := pagesFault(r, uint64(len(data))); why != "" {
			return
		}
		assertBoltOpens(t, data)
		m, ok := chosenMeta(r, uint64(len(data)))
		if !ok {
			return
		}
		if m.pageSize < pageHeaderSize+metaSize || m.pgid > uint64(len(data))/m.pageSize {
			t.Fatalf("accepted page size %d, high-water mark %d in %d bytes", m.pageSize, m.pgid, len(data))
		}
		if m.freelist == noFreelist {
			return
		}
		if m.freelist < 2 || m.freelist >= m.pgid {
			t.Fatalf("accepted freelist page %d, high-water mark %d", m.freelist, m.pgid)
		}
		if of := uint64(binary.LittleEndian.Uint32(data[m.freelist*m.pageSize+12:])); of >= m.pgid-m.freelist {
			t.Fatalf("accepted freelist page %d overflowing %d pages, high-water mark %d", m.freelist, of, m.pgid)
		}
	})
}
