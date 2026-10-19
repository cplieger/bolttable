package bolttable

import (
	"cmp"
	"errors"
	"slices"
	"testing"
)

type entryRow struct {
	ikey, pk, proj string
}

// splitFixture is a table keyed by raw bytes whose one non-unique and one
// unique index each take their key and projection from the row.
type splitFixture struct {
	schema *Schema
	rows   *Table[[]byte, splitRow]
	multi  *Index[[]byte, splitRow]
	unique *Index[[]byte, splitRow]
}

type splitRow struct {
	IKey []byte `json:"ikey"`
	UKey []byte `json:"ukey,omitempty"`
	Proj []byte `json:"proj"`
}

func newSplitFixture() *splitFixture {
	s := NewSchema("split")
	a := Must(s.Namespace("a"))
	f := &splitFixture{schema: s}
	f.rows = Must(Define(a, 0x01, "rows", BytesKey{}, JSON[splitRow](1), TableOptions{}))
	f.multi = Must(DefineIndex(f.rows, "multi", IndexOptions{Version: 1}, func(_ []byte, v *splitRow) []Entry {
		return []Entry{{Key: v.IKey, Value: v.Proj}}
	}))
	f.unique = Must(DefineIndex(f.rows, "unique", IndexOptions{Unique: true, Version: 1}, func(_ []byte, v *splitRow) []Entry {
		if len(v.UKey) == 0 {
			return nil
		}
		return []Entry{{Key: v.UKey, Value: v.Proj}}
	}))
	return f
}

func scanEntries(t *testing.T, r *ReadTx, ix *Index[[]byte, splitRow], rg Range) []entryRow {
	t.Helper()
	var out []entryRow
	if err := ix.Scan(r, rg, func(ik, pk, proj []byte) (bool, error) {
		out = append(out, entryRow{string(ik), string(pk), string(proj)})
		return true, nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	return out
}

func TestNonUniqueEncodingDistinct(t *testing.T) {
	for _, whole := range []string{"abc", "a\x00b\x00", "\x00\x00\x01", "a\x00\xffb"} {
		t.Run("", func(t *testing.T) {
			f := newSplitFixture()
			db := mustOpen(t, tempPath(t), f.schema, Options{})
			var want []entryRow
			update(t, db, func(tx *Tx) error {
				for i := 1; i < len(whole); i++ {
					// Row i is (whole[:i], whole[i:]): the index key and the
					// primary key always concatenate to the same bytes.
					ik, pk := whole[:i], whole[i:]
					want = append(want, entryRow{ik, pk, "p" + pk})
					if err := f.rows.Put(tx, []byte(pk), &splitRow{IKey: []byte(ik), Proj: []byte("p" + pk)}); err != nil {
						return err
					}
				}
				return nil
			})
			slices.SortFunc(want, func(a, b entryRow) int {
				return cmp.Or(cmp.Compare(a.ikey, b.ikey), cmp.Compare(a.pk, b.pk))
			})
			view(t, db, func(r *ReadTx) error {
				if got := scanEntries(t, r, f.multi, Range{}); !slices.Equal(got, want) {
					t.Errorf("entries = %q, want %q", got, want)
				}
				for _, w := range want {
					got := scanEntries(t, r, f.multi, Range{From: []byte(w.ikey), Reverse: true})
					if len(got) == 0 || got[0].ikey != w.ikey {
						t.Errorf("reverse scan from %q starts at %q", w.ikey, got)
					}
				}
				return nil
			})
		})
	}
}

func TestUniqueProjectionDecodesAtEverySplit(t *testing.T) {
	whole := "k\x00ey\xff\x01proj"
	f := newSplitFixture()
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	update(t, db, func(tx *Tx) error {
		for i := 1; i <= len(whole); i++ {
			uk := []byte{byte(i)}
			if err := f.rows.Put(tx, []byte(whole[:i]), &splitRow{UKey: uk, Proj: []byte(whole[i:])}); err != nil {
				return err
			}
		}
		return nil
	})
	view(t, db, func(r *ReadTx) error {
		for i := 1; i <= len(whole); i++ {
			pk, proj, found, err := f.unique.Lookup(r, []byte{byte(i)})
			if err != nil || !found || string(pk) != whole[:i] || string(proj) != whole[i:] {
				t.Errorf("split %d: Lookup = %q, %q, %v, %v", i, pk, proj, found, err)
			}
		}
		return nil
	})
}

func TestIndexScanPrefixFrom(t *testing.T) {
	f := newSplitFixture()
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	rows := []entryRow{{"a", "1", "x"}, {"a", "2", "x"}, {"ab", "3", "x"}, {"b", "4", "x"}, {"b\x00", "5", "x"}}
	update(t, db, func(tx *Tx) error {
		for _, e := range rows {
			row := &splitRow{IKey: []byte(e.ikey), Proj: []byte(e.proj)}
			if e.pk != "2" {
				row.UKey = row.IKey
			}
			if err := f.rows.Put(tx, []byte(e.pk), row); err != nil {
				return err
			}
		}
		return nil
	})
	pks := func(es []entryRow) []string {
		var out []string
		for _, e := range es {
			out = append(out, e.pk)
		}
		return out
	}
	cases := []struct {
		name string
		rg   Range
		want []string
	}{
		{"prefix", Range{Prefix: []byte("a")}, []string{"1", "2", "3"}},
		{"from inclusive of every entry", Range{From: []byte("a")}, []string{"1", "2", "3", "4", "5"}},
		{"from between", Range{From: []byte("aa")}, []string{"3", "4", "5"}},
		{"prefix with zero byte", Range{Prefix: []byte("b\x00")}, []string{"5"}},
		{"reverse from", Range{From: []byte("ab"), Reverse: true}, []string{"3", "2", "1"}},
		{"reverse from below zero byte", Range{From: []byte("b"), Reverse: true}, []string{"4", "3", "2", "1"}},
		{"reverse prefix", Range{Prefix: []byte("b"), Reverse: true}, []string{"5", "4"}},
	}
	view(t, db, func(r *ReadTx) error {
		for _, tc := range cases {
			if got := pks(scanEntries(t, r, f.multi, tc.rg)); !slices.Equal(got, tc.want) {
				t.Errorf("%s: primaries = %q, want %q", tc.name, got, tc.want)
			}
		}
		if got := pks(scanEntries(t, r, f.unique, Range{From: []byte("ab"), Reverse: true})); !slices.Equal(got, []string{"3", "1"}) {
			t.Errorf("unique reverse from ab = %q, want 3 then 1", got)
		}
		return nil
	})
}

func TestUniqueConflictNamesIndex(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "x", N: 1}) })
	update(t, db, func(tx *Tx) error {
		err := f.items.Put(tx, 2, &item{Name: "x", N: 5})
		var ce *ConflictError
		if !errors.As(err, &ce) || ce.Index != "by_name" || !errors.Is(err, ErrConflict) {
			t.Errorf("Put = %v, want a *ConflictError naming by_name", err)
		}
		// The refused Put wrote nothing, so the transaction carries on.
		return f.items.Put(tx, 3, &item{Name: "y"})
	})
	view(t, db, func(r *ReadTx) error {
		if _, found, _ := f.items.Get(r, 2); found {
			t.Error("the refused row was stored")
		}
		if n, _ := f.total.Value(r); n != 1 {
			t.Errorf("total = %d, want 1", n)
		}
		if k, _, found, err := f.byName.Lookup(r, []byte("x")); !found || k != 1 || err != nil {
			t.Errorf("Lookup(x) = %d, %v, %v", k, found, err)
		}
		if _, _, found, _ := f.byName.Lookup(r, []byte("y")); !found {
			t.Error("the later Put in the same transaction was lost")
		}
		return nil
	})
	update(t, db, func(tx *Tx) error {
		if err := f.items.Put(tx, 1, &item{Name: "x", N: 2}); err != nil {
			t.Errorf("re-Put of the owner: %v", err)
		}
		if err := f.items.Put(tx, 1, &item{Name: "z"}); err != nil {
			return err
		}
		return f.items.Put(tx, 2, &item{Name: "x"})
	})
}

func TestSparseAndPerElementIndexes(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	update(t, db, func(tx *Tx) error {
		if err := f.items.Put(tx, 1, &item{Tags: []string{"red", "blue"}}); err != nil {
			return err
		}
		return f.items.Put(tx, 2, &item{Name: "two", Tags: []string{"red"}})
	})
	tagged := func(r *ReadTx, tag string) []uint64 {
		var ks []uint64
		if err := f.byTag.Scan(r, Range{Prefix: []byte(tag)}, func(_ []byte, k uint64, _ []byte) (bool, error) {
			ks = append(ks, k)
			return true, nil
		}); err != nil {
			t.Fatal(err)
		}
		return ks
	}
	named := func(r *ReadTx) int {
		n := 0
		if err := f.byName.Scan(r, Range{}, func([]byte, uint64, []byte) (bool, error) { n++; return true, nil }); err != nil {
			t.Fatal(err)
		}
		return n
	}
	view(t, db, func(r *ReadTx) error {
		if got := tagged(r, "red"); !slices.Equal(got, []uint64{1, 2}) {
			t.Errorf("red = %v", got)
		}
		if got := tagged(r, "blue"); !slices.Equal(got, []uint64{1}) {
			t.Errorf("blue = %v", got)
		}
		if n := named(r); n != 1 {
			t.Errorf("by_name holds %d entries, want 1 (row 1 has no name)", n)
		}
		return nil
	})
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "one", Tags: []string{"green"}}) })
	view(t, db, func(r *ReadTx) error {
		if got := tagged(r, "blue"); got != nil {
			t.Errorf("blue = %v after the row dropped the tag", got)
		}
		if got := tagged(r, "green"); !slices.Equal(got, []uint64{1}) {
			t.Errorf("green = %v", got)
		}
		if n := named(r); n != 2 {
			t.Errorf("by_name holds %d entries, want 2", n)
		}
		return nil
	})
	update(t, db, func(tx *Tx) error { _, err := f.items.Delete(tx, 1); return err })
	view(t, db, func(r *ReadTx) error {
		if got := tagged(r, "green"); got != nil {
			t.Errorf("green = %v after delete", got)
		}
		return nil
	})
}

func TestLookupNonUniqueErrors(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	view(t, db, func(r *ReadTx) error {
		if _, _, _, err := f.byTag.Lookup(r, []byte("red")); err == nil {
			t.Error("Lookup on a non-unique index succeeded")
		}
		return nil
	})
}

func TestOneRowTwoProjectionsFails(t *testing.T) {
	s := NewSchema("s")
	tb := Must(Define(Must(s.Namespace("a")), 0x01, "t", Uint64Key{}, JSON[item](1), TableOptions{}))
	Must(DefineIndex(tb, "i", IndexOptions{Version: 1}, func(uint64, *item) []Entry {
		return []Entry{{Key: []byte("k"), Value: []byte("1")}, {Key: []byte("k"), Value: []byte("2")}}
	}))
	db := mustOpen(t, tempPath(t), s, Options{})
	if err := db.Update(t.Context(), func(tx *Tx) error { return tb.Put(tx, 1, &item{}) }); err == nil {
		t.Error("Put stored one index key with two projections")
	}
}
