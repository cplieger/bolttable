package bolttable

import (
	"bytes"
	"cmp"
	"path/filepath"
	"slices"
	"testing"
)

func FuzzIndexEncoding_roundtrip(f *testing.F) {
	f.Add([]byte("a"), []byte("bc"), []byte("p"))
	f.Add([]byte("a\x00"), []byte("\x00\x01"), []byte{})
	f.Add([]byte{0x00, 0xFF, 0x00}, []byte{0x01}, []byte{0xFF})
	f.Fuzz(func(t *testing.T, ikey, pk, proj []byte) {
		for _, unique := range []bool{false, true} {
			d := &indexDef{unique: unique}
			k, v := d.encode(Entry{Key: ikey, Value: proj}, pk)
			gotKey, gotPK, gotProj, ok := d.split(k, v)
			if !ok || !bytes.Equal(gotKey, ikey) || !bytes.Equal(gotPK, pk) || !bytes.Equal(gotProj, proj) {
				t.Fatalf("unique %v: split(encode(%q, %q, %q)) = %q, %q, %q, %v", unique, ikey, pk, proj, gotKey, gotPK, gotProj, ok)
			}
		}
	})
}

// FuzzStoredBytes_noPanic feeds arbitrary stored bytes to every decoder of
// on-disk entries: a corrupt file must answer an error, never panic.
func FuzzStoredBytes_noPanic(f *testing.F) {
	f.Add([]byte{0xFF, 0xFF, 0x03})
	f.Add([]byte{0x80, 0x00, 0x00})
	f.Add(encodeChange(Change{Subjects: []Subject{Subject("s")}, Marks: []string{"m"}}))
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _, _ = splitUniqueValue(b)
		_, _, _ = splitNonUniqueKey(b)
		c, err := decodeChange(be64(1), b)
		if err != nil {
			return
		}
		// An overlong uvarint decodes too, so only the re-encoding is canonical.
		again, err := decodeChange(be64(1), encodeChange(c))
		if err != nil || !bytes.Equal(encodeChange(again), encodeChange(c)) {
			t.Fatalf("decodeChange(%x) = %+v does not survive a re-encoding: %+v, %v", b, c, again, err)
		}
	})
}

// fuzzRows reads data as rows of one length byte (mod 4) and that many
// index-key bytes; row i's primary key is the byte i.
func fuzzRows(data []byte) []entryRow {
	var rows []entryRow
	for i := 0; i < len(data) && len(rows) < 32; {
		n := int(data[i] % 4)
		i++
		end := min(i+n, len(data))
		rows = append(rows, entryRow{ikey: string(data[i:end]), pk: string([]byte{byte(len(rows))}), proj: "p"})
		i = end
	}
	return rows
}

// FuzzIndexRange_matchesModel checks every index scan against a filter of
// the stored (index key, primary key) pairs, sorted by both.
func FuzzIndexRange_matchesModel(f *testing.F) {
	f.Add([]byte("\x01a\x02a\x00\x02ab\x01b\x00\x01\xff\x02\xff\xff"), []byte(""), []byte("a"), []byte("b"), uint8(0))
	f.Add([]byte("\x01a\x02a\x00\x02ab\x01a"), []byte("a"), []byte("a"), []byte("a\x00"), uint8(1))
	f.Add([]byte("\x02\x00\x00\x01\x00\x03\x00\xff\x00"), []byte{0x00}, []byte{0x00}, []byte{0x00, 0xFF}, uint8(16))
	f.Add([]byte("\x03abc\x02ab\x01a\x00"), []byte("ab"), []byte(""), []byte("abd"), uint8(17))
	f.Add([]byte("\x01\xff\x02\xff\x00\x02\xff\xff"), []byte{0xFF}, []byte{0xFF, 0x00}, []byte{}, uint8(6))
	f.Fuzz(func(t *testing.T, data, prefix, from, to []byte, flags uint8) {
		rows := fuzzRows(data)
		rg := Range{Prefix: prefix, From: from, To: to, Reverse: flags&1 != 0}
		if flags&2 != 0 {
			rg.From = nil
		}
		if flags&4 != 0 {
			rg.To = nil
		}
		if flags&8 != 0 {
			rg.Prefix = nil
		}
		exact := flags&16 != 0
		if exact {
			rg = Exact(from)
			rg.Reverse = flags&1 != 0
		}
		in := func(k string) bool {
			if exact {
				return k == string(from)
			}
			return bytes.HasPrefix([]byte(k), rg.Prefix) &&
				(rg.From == nil || k >= string(rg.From)) && (rg.To == nil || k < string(rg.To))
		}
		fx := newSplitFixture()
		db, err := Open(filepath.Join(t.TempDir(), "db.bolt"), fx.schema, Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		seenUnique := map[string]bool{}
		var multi, unique []entryRow
		if err := db.Update(t.Context(), func(tx *Tx) error {
			for _, e := range rows {
				row := &splitRow{IKey: []byte(e.ikey), Proj: []byte(e.proj)}
				if e.ikey != "" && !seenUnique[e.ikey] {
					seenUnique[e.ikey] = true
					row.UKey = row.IKey
					if in(e.ikey) {
						unique = append(unique, e)
					}
				}
				if in(e.ikey) {
					multi = append(multi, e)
				}
				if err := fx.rows.Put(tx, []byte(e.pk), row); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		order := func(a, b entryRow) int { return cmp.Or(cmp.Compare(a.ikey, b.ikey), cmp.Compare(a.pk, b.pk)) }
		for name, want := range map[string][]entryRow{"non-unique": multi, "unique": unique} {
			slices.SortFunc(want, order)
			if rg.Reverse {
				slices.Reverse(want)
			}
			ix := fx.multi
			if name == "unique" {
				ix = fx.unique
			}
			var got []entryRow
			if err := db.View(t.Context(), func(r *ReadTx) error {
				got = scanEntries(t, r, ix, rg)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, want) {
				t.Fatalf("%s %+v over %q: got %q, want %q", name, rg, rows, got, want)
			}
		}
	})
}
