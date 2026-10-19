package bolttable

import (
	"bytes"
	"errors"
	"slices"
	"testing"
)

type bytesFixture struct {
	schema *Schema
	kv     *Table[[]byte, item]
	tags   *Index[[]byte, item]
	names  *Index[[]byte, item]
}

func newBytesFixture() *bytesFixture {
	s := NewSchema("bytes")
	a := Must(s.Namespace("a"))
	f := &bytesFixture{schema: s}
	f.kv = Must(Define(a, 0x01, "kv", BytesKey{}, JSON[item](1), TableOptions{}))
	f.tags = Must(DefineIndex(f.kv, "tags", IndexOptions{Version: 1}, func(_ []byte, v *item) []Entry {
		var out []Entry
		for _, tag := range v.Tags {
			out = append(out, Entry{Key: []byte(tag), Value: []byte("p-" + tag)})
		}
		return out
	}))
	f.names = Must(DefineIndex(f.kv, "names", IndexOptions{Unique: true, Version: 1}, func(_ []byte, v *item) []Entry {
		return []Entry{{Key: []byte(v.Name), Value: []byte("n-" + v.Name)}}
	}))
	return f
}

func TestPutByteEqualWritesNothing(t *testing.T) {
	f := newFixture(t)
	var rec recorder
	db := mustOpen(t, tempPath(t), f.schema, Options{OnCommit: rec.on, Retention: 10})
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "x", N: 3}) })
	rev := headRev(t, db)
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "x", N: 3}) })
	if got := headRev(t, db); got != rev {
		t.Errorf("rev = %d after a byte-equal Put, want %d", got, rev)
	}
	if got := len(rec.all()); got != 1 {
		t.Errorf("OnCommit ran %d times, want 1", got)
	}
	view(t, db, func(r *ReadTx) error {
		changes, _, err := r.Since(0)
		if err != nil || len(changes) != 1 {
			t.Errorf("Since(0) = %v, %v; want the first Put only", changes, err)
		}
		return nil
	})
}

func scanKeys(t *testing.T, db *DB, tb *Table[[]byte, item], rg Range) []string {
	t.Helper()
	var keys []string
	view(t, db, func(r *ReadTx) error {
		return tb.Scan(r, rg, func(k []byte, _ *item) (bool, error) {
			keys = append(keys, string(k))
			return true, nil
		})
	})
	return keys
}

func TestScanPrefixFromReverse(t *testing.T) {
	f := newBytesFixture()
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	update(t, db, func(tx *Tx) error {
		for _, k := range []string{"a1", "a2", "a3", "b1", "\xff\x01", "\xff\xff", "\xff\xff\x01"} {
			if err := f.kv.Put(tx, []byte(k), &item{Name: k}); err != nil {
				return err
			}
		}
		return nil
	})
	cases := []struct {
		name string
		rg   Range
		want []string
	}{
		{"all", Range{}, []string{"a1", "a2", "a3", "b1", "\xff\x01", "\xff\xff", "\xff\xff\x01"}},
		{"prefix", Range{Prefix: []byte("a")}, []string{"a1", "a2", "a3"}},
		{"prefix from", Range{Prefix: []byte("a"), From: []byte("a2")}, []string{"a2", "a3"}},
		{"from below prefix", Range{Prefix: []byte("b"), From: []byte("a")}, []string{"b1"}},
		{"reverse prefix", Range{Prefix: []byte("a"), Reverse: true}, []string{"a3", "a2", "a1"}},
		{"reverse from", Range{Prefix: []byte("a"), From: []byte("a2"), Reverse: true}, []string{"a2", "a1"}},
		{"reverse from between keys", Range{From: []byte("a25"), Reverse: true}, []string{"a2", "a1"}},
		{"reverse all", Range{Reverse: true}, []string{"\xff\xff\x01", "\xff\xff", "\xff\x01", "b1", "a3", "a2", "a1"}},
		{"reverse prefix of 0xFF", Range{Prefix: []byte{0xff, 0xff}, Reverse: true}, []string{"\xff\xff\x01", "\xff\xff"}},
		{"no match", Range{Prefix: []byte("c")}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := scanKeys(t, db, f.kv, tc.rg); !slices.Equal(got, tc.want) {
				t.Errorf("keys = %q, want %q", got, tc.want)
			}
		})
	}
	t.Run("stop", func(t *testing.T) {
		n := 0
		view(t, db, func(r *ReadTx) error {
			return f.kv.Scan(r, Range{}, func([]byte, *item) (bool, error) { n++; return n < 2, nil })
		})
		if n != 2 {
			t.Errorf("visited %d rows, want 2", n)
		}
	})
	t.Run("delete while scanning", func(t *testing.T) {
		var seen []string
		update(t, db, func(tx *Tx) error {
			// A write first makes the leaf a live bbolt node, which a
			// deletion shifts under an open cursor.
			if err := f.kv.Put(tx, []byte("a0"), &item{Name: "a0"}); err != nil {
				return err
			}
			return f.kv.Scan(tx.Read(), Range{Prefix: []byte("a")}, func(k []byte, _ *item) (bool, error) {
				seen = append(seen, string(k))
				_, err := f.kv.Delete(tx, k)
				return true, err
			})
		})
		if !slices.Equal(seen, []string{"a0", "a1", "a2", "a3"}) {
			t.Errorf("visited %q while deleting, want every a key once", seen)
		}
		if got := scanKeys(t, db, f.kv, Range{Prefix: []byte("a")}); got != nil {
			t.Errorf("left %q", got)
		}
	})
}

func TestDeleteReportsPresence(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	update(t, db, func(tx *Tx) error {
		if err := f.items.Put(tx, 1, &item{Name: "x"}); err != nil {
			return err
		}
		for _, want := range []bool{true, false} {
			if got, err := f.items.Delete(tx, 1); err != nil || got != want {
				t.Errorf("Delete = %v, %v; want %v", got, err, want)
			}
		}
		return nil
	})
}

func TestPutRefusesBadRows(t *testing.T) {
	f := newBytesFixture()
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	update(t, db, func(tx *Tx) error {
		if err := f.kv.Put(tx, nil, &item{}); err == nil {
			t.Error("Put accepted an empty key")
		}
		if err := f.kv.Put(tx, []byte("k"), nil); err == nil {
			t.Error("Put accepted a nil value")
		}
		return nil
	})
}

func TestNextIDMonotonic(t *testing.T) {
	path := tempPath(t)
	f := newFixture(t)
	db, err := Open(path, f.schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var ids []uint64
	next := func(db *DB, tb *Table[uint64, item]) {
		update(t, db, func(tx *Tx) error {
			id, err := tb.NextID(tx)
			ids = append(ids, id)
			return err
		})
	}
	next(db, f.items)
	next(db, f.items)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	f = newFixture(t)
	db = mustOpen(t, path, f.schema, Options{})
	next(db, f.items)
	if !slices.Equal(ids, []uint64{1, 2, 3}) {
		t.Errorf("ids = %v, want 1, 2, 3", ids)
	}
	if rev := headRev(t, db); rev != 0 {
		t.Errorf("NextID reserved revision %d", rev)
	}
}

// reusingCodec returns one buffer from every Append, as a codec pooling its
// encoder would.
type reusingCodec struct{ buf *[]byte }

func (reusingCodec) Version() uint32 { return 1 }
func (c reusingCodec) Append(_ []byte, v *item) ([]byte, error) {
	*c.buf = append((*c.buf)[:0], v.Name...)
	return *c.buf, nil
}

func (reusingCodec) Decode(b []byte, v *item) error { v.Name = string(b); return nil }

func TestPutCopiesWhatItKeeps(t *testing.T) {
	s := NewSchema("reuse")
	var buf []byte
	tb := Must(Define(Must(s.Namespace("a")), 0x01, "t", Uint64Key{}, Codec[item](reusingCodec{&buf}), TableOptions{}))
	db := mustOpen(t, tempPath(t), s, Options{})
	update(t, db, func(tx *Tx) error {
		if err := tb.Put(tx, 1, &item{Name: "first"}); err != nil {
			return err
		}
		return tb.Put(tx, 2, &item{Name: "other"})
	})
	view(t, db, func(r *ReadTx) error {
		v, _, err := tb.Get(r, 1)
		if v.Name != "first" {
			t.Errorf("row 1 = %q, %v; Put kept the codec's buffer", v.Name, err)
		}
		return nil
	})
}

// TestReturnedSlicesAreCopies mutates every returned and passed slice inside
// the transaction that wrote the rows, where bbolt hands back the live node
// bytes it will commit.
func TestReturnedSlicesAreCopies(t *testing.T) {
	f := newBytesFixture()
	var rec recorder
	db := mustOpen(t, tempPath(t), f.schema, Options{OnCommit: rec.on, Retention: 10})
	update(t, db, func(tx *Tx) error {
		key := []byte("key")
		val := &item{Name: "n", Tags: []string{"t"}}
		if err := f.kv.Put(tx, key, val); err != nil {
			return err
		}
		touched := Subject("touched")
		tx.Touch(touched)
		key[0], touched[0] = 'X', 'X'
		val.Tags[0] = "mutated"
		r := tx.Read()
		if err := f.kv.Scan(r, Range{}, func(k []byte, _ *item) (bool, error) { k[0] = 'Y'; return true, nil }); err != nil {
			return err
		}
		k, proj, _, err := f.names.Lookup(r, []byte("n"))
		if err != nil {
			return err
		}
		k[0], proj[0] = 'Y', 'Y'
		return f.tags.Scan(r, Range{}, func(ik, k, p []byte) (bool, error) {
			ik[0], k[0], p[0] = 'Y', 'Y', 'Y'
			return true, nil
		})
	})
	for _, c := range rec.all() {
		for _, s := range c.Subjects {
			s[0] = 'Z'
		}
	}
	// A second write rewrites the change log from the bytes this
	// transaction read, so a Since result sharing them would persist.
	update(t, db, func(tx *Tx) error {
		changes, _, err := tx.Read().Since(0)
		if err != nil {
			return err
		}
		changes[0].Subjects[0][0] = 'Q'
		return f.kv.Put(tx, []byte("second"), &item{Name: "second"})
	})
	view(t, db, func(r *ReadTx) error {
		changes, _, err := r.Since(0)
		if err != nil {
			return err
		}
		if !hasSubject(changes[0], f.kv.Subject([]byte("key"))) || !hasSubject(changes[0], Subject("touched")) {
			t.Errorf("stored change %q changed with the caller's slices", changes[0].Subjects)
		}
		got, found, err := f.kv.Get(r, []byte("key"))
		if err != nil || !found || got.Tags[0] != "t" {
			t.Errorf("Get = %+v, %v, %v; the stored row changed", got, found, err)
		}
		k, proj, found, err := f.names.Lookup(r, []byte("n"))
		if err != nil || !found || string(k) != "key" || string(proj) != "n-n" {
			t.Errorf("Lookup = %q, %q, %v, %v after mutating returned slices", k, proj, found, err)
		}
		return f.tags.Scan(r, Range{}, func(ik, k, p []byte) (bool, error) {
			if string(ik) != "t" || string(k) != "key" || string(p) != "p-t" {
				t.Errorf("index entry = %q, %q, %q after mutating returned slices", ik, k, p)
			}
			return true, nil
		})
	})
	if s := f.kv.Subject([]byte("key")); !bytes.Equal(s, append([]byte{0x01}, "key"...)) {
		t.Errorf("Subject = %q", s)
	}
}

func TestReadAfterTransactionFails(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	var kept *ReadTx
	view(t, db, func(r *ReadTx) error { kept = r; return nil })
	if _, _, err := f.items.Get(kept, 1); !errors.Is(err, errTxClosed) {
		t.Errorf("Get on an ended ReadTx = %v, want errTxClosed", err)
	}
	var tx *Tx
	update(t, db, func(x *Tx) error { tx = x; return nil })
	if err := f.items.Put(tx, 1, &item{}); !errors.Is(err, errClosed) {
		t.Errorf("Put on an ended Tx = %v, want errClosed", err)
	}
}
