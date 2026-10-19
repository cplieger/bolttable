package bolttable

import (
	"bytes"
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
