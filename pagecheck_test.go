package bolttable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/fnv"
	"runtime/debug"
	"strings"
	"testing"
)

// setFreelist overwrites the closed file's freelist page with count and the
// ids after it, as bbolt v1.5.0 lays them out (internal/common/page.go).
func setFreelist(t *testing.T, path string, count uint16, ids ...uint64) {
	t.Helper()
	m := readMeta(t, path)
	patchFile(t, path, func(data []byte) []byte {
		page := data[int(m.freelist)*m.pageSize:]
		binary.LittleEndian.PutUint16(page[10:], count)
		for i, id := range ids {
			binary.LittleEndian.PutUint64(page[16+8*i:], id)
		}
		return data
	})
}

// setMetas rewrites both meta pages of the closed file through fn, which
// gets the 64-byte meta after the page header, and reseals their FNV-64a
// checksums over the meta's first 56 bytes (internal/common/meta.go).
func setMetas(t *testing.T, path string, fn func(meta []byte)) {
	t.Helper()
	ps := readMeta(t, path).pageSize
	patchFile(t, path, func(data []byte) []byte {
		for _, off := range []int{16, ps + 16} {
			meta := data[off : off+64]
			fn(meta)
			h := fnv.New64a()
			_, _ = h.Write(meta[:56])
			binary.LittleEndian.PutUint64(meta[56:], h.Sum64())
		}
		return data
	})
}

func setMetaFreelist(t *testing.T, path string, id uint64) {
	t.Helper()
	setMetas(t, path, func(meta []byte) { binary.LittleEndian.PutUint64(meta[32:], id) })
}

// freeID is a page id below the high-water mark that is neither a meta page
// nor the freelist page.
func freeID(t *testing.T, m meta) uint64 {
	t.Helper()
	for id := uint64(2); id < m.pgid; id++ {
		if id != m.freelist {
			return id
		}
	}
	t.Fatalf("fixture: no page besides the freelist below %d", m.pgid)
	return 0
}

// TestBadOpenPagesAreInvalid covers what bbolt reads while opening a file,
// after mapping it: a bad freelist page panics there, or is loaded and hands
// out pages bbolt must never reuse.
func TestBadOpenPagesAreInvalid(t *testing.T) {
	cases := map[string]struct {
		corrupt func(t *testing.T, path string, m meta)
		want    string
	}{
		"meta records page size 0": {want: "page size 0", corrupt: func(t *testing.T, path string, _ meta) {
			setMetas(t, path, func(meta []byte) { binary.LittleEndian.PutUint32(meta[8:], 0) })
		}},
		"meta names a meta page": {want: "freelist", corrupt: func(t *testing.T, path string, _ meta) { setMetaFreelist(t, path, 1) }},
		"meta names a page past the high-water mark": {want: "freelist", corrupt: func(t *testing.T, path string, m meta) {
			setMetaFreelist(t, path, m.pgid)
		}},
		"lists a meta page": {want: "freelist", corrupt: func(t *testing.T, path string, _ meta) { setFreelist(t, path, 1, 1) }},
		"lists a page past the high-water mark": {want: "freelist", corrupt: func(t *testing.T, path string, m meta) {
			setFreelist(t, path, 1, m.pgid)
		}},
		"lists a page twice": {want: "freelist", corrupt: func(t *testing.T, path string, m meta) {
			setFreelist(t, path, 2, freeID(t, m), freeID(t, m))
		}},
		"lists itself": {want: "freelist", corrupt: func(t *testing.T, path string, m meta) { setFreelist(t, path, 1, m.freelist) }},
		"counts more ids than its page holds": {want: "freelist", corrupt: func(t *testing.T, path string, m meta) {
			setFreelist(t, path, uint16((m.pageSize-16)/8+1))
		}},
		"overflows past the high-water mark": {want: "overflows", corrupt: func(t *testing.T, path string, m meta) {
			setFreelist(t, path, 0)
			patchFile(t, path, func(data []byte) []byte {
				binary.LittleEndian.PutUint32(data[int(m.freelist)*m.pageSize+12:], uint32(m.pgid-m.freelist))
				return data
			})
		}},
		// Below the header size the count bound wraps, and a count this large
		// panics allocating its ids.
		"meta records a page smaller than a meta page": {want: "page size 8", corrupt: func(t *testing.T, path string, _ meta) {
			const ps, freelist = 8, 50
			setMetas(t, path, func(meta []byte) {
				binary.LittleEndian.PutUint32(meta[8:], ps)
				binary.LittleEndian.PutUint64(meta[32:], freelist)
				binary.LittleEndian.PutUint64(meta[40:], 600)
			})
			patchFile(t, path, func(data []byte) []byte {
				page := data[freelist*ps:]
				binary.LittleEndian.PutUint16(page[8:], 0x10)
				binary.LittleEndian.PutUint16(page[10:], 0xFFFF)
				binary.LittleEndian.PutUint64(page[16:], 1<<61-2)
				return data
			})
		}},
	}
	opens := map[string]func(t *testing.T, path string) error{
		"Open": func(t *testing.T, path string) error {
			db, err := Open(path, newFixture(t).schema, Options{})
			if err == nil {
				_ = db.Close()
			}
			return err
		},
		"OpenReadOnly": func(t *testing.T, path string) error {
			rdb, err := OpenReadOnly(path, newFixture(t).schema, ReadOptions{})
			if err == nil {
				_ = rdb.Close()
			}
			return err
		},
	}
	for name, c := range cases {
		for via, open := range opens {
			t.Run(via+" "+name, func(t *testing.T) {
				path := storedFile(t)
				c.corrupt(t, path, readMeta(t, path))
				before := readFile(t, path)
				// A GC would run the finalizer of a descriptor the open leaked.
				defer debug.SetGCPercent(debug.SetGCPercent(-1))
				err := open(t, path)
				if !errors.Is(err, ErrInvalidFile) || !strings.Contains(err.Error(), c.want) {
					t.Errorf("%s = %v, want ErrInvalidFile saying %q", via, err, c.want)
				}
				if !bytes.Equal(before, readFile(t, path)) {
					t.Error("the file changed")
				}
				assertUnlocked(t, path)
			})
		}
	}
}

// A freelist naming only pages bbolt may hand out passes, in both count
// forms.
func TestValidFreelistOpens(t *testing.T) {
	path := storedFile(t)
	m := readMeta(t, path)
	setFreelist(t, path, 1, freeID(t, m))
	db := mustOpen(t, path, newFixture(t).schema, Options{})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	path = storedFile(t)
	m = readMeta(t, path)
	setFreelist(t, path, 0xFFFF, 1, freeID(t, m))
	db = mustOpen(t, path, newFixture(t).schema, Options{})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
