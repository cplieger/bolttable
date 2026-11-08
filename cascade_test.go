package bolttable

import (
	"encoding/binary"
	"slices"
	"testing"
)

// treeFixture: parents by id; children keyed parent id plus a suffix,
// cascaded by key prefix; grandchildren pointing at a child through an
// index, cascaded through it.
type treeFixture struct {
	schema   *Schema
	parents  *Table[uint64, item]
	children *Table[[]byte, item]
	kids     *Counter[[]byte, item]
	grand    *Table[uint64, item]
}

func childKey(parent uint64, suffix string) []byte {
	return append(binary.BigEndian.AppendUint64(nil, parent), suffix...)
}

func newTreeFixture() *treeFixture {
	s := NewSchema("tree")
	a := Must(s.Namespace("a"))
	f := &treeFixture{schema: s}
	f.parents = Must(Define(a, 0x01, "parents", Uint64Key{}, JSON[item](1), TableOptions{}))
	f.children = Must(Define(a, 0x02, "children", BytesKey{}, JSON[item](1), TableOptions{}))
	f.kids = Must(DefineCounter(f.children, "kids", 1, func([]byte, *item) int64 { return 1 }))
	f.grand = Must(Define(a, 0x03, "grand", Uint64Key{}, JSON[item](1), TableOptions{}))
	// A grandchild names its child in Name.
	byChild := Must(DefineIndex(f.grand, "by_child", IndexOptions{Version: 1}, func(_ uint64, v *item) []Entry {
		return []Entry{{Key: []byte(v.Name)}}
	}))
	if err := CascadePrefix(f.parents, f.children, 1, func(id uint64) []byte { return binary.BigEndian.AppendUint64(nil, id) }); err != nil {
		panic(err)
	}
	if err := CascadeIndex(f.children, byChild, 1, func(k []byte) []byte { return k }); err != nil {
		panic(err)
	}
	return f
}

func TestCascadeRecursive(t *testing.T) {
	f := newTreeFixture()
	var rec recorder
	db := mustOpen(t, tempPath(t), f.schema, Options{OnCommit: rec.on, Retention: 10})
	update(t, db, func(tx *Tx) error {
		for _, p := range []uint64{1, 2} {
			if err := f.parents.Put(tx, p, &item{}); err != nil {
				return err
			}
			for _, s := range []string{"x", "y"} {
				if err := f.children.Put(tx, childKey(p, s), &item{}); err != nil {
					return err
				}
			}
		}
		for g, child := range map[uint64][]byte{10: childKey(1, "x"), 11: childKey(1, "y"), 12: childKey(1, "x"), 20: childKey(2, "x")} {
			if err := f.grand.Put(tx, g, &item{Name: string(child)}); err != nil {
				return err
			}
		}
		return nil
	})
	update(t, db, func(tx *Tx) error {
		found, err := f.parents.Delete(tx, 1)
		if !found {
			t.Error("parent 1 not found")
		}
		return err
	})
	view(t, db, func(r *ReadTx) error {
		var children [][]byte
		if err := f.children.Scan(r, Range{}, func(k []byte, _ *item) (bool, error) {
			children = append(children, k)
			return true, nil
		}); err != nil {
			return err
		}
		if want := [][]byte{childKey(2, "x"), childKey(2, "y")}; !slices.EqualFunc(children, want, slices.Equal) {
			t.Errorf("children = %q, want parent 2's only", children)
		}
		var grand []uint64
		if err := f.grand.Scan(r, Range{}, func(k uint64, _ *item) (bool, error) {
			grand = append(grand, k)
			return true, nil
		}); err != nil {
			return err
		}
		if !slices.Equal(grand, []uint64{20}) {
			t.Errorf("grandchildren = %v, want [20]", grand)
		}
		if n, _ := f.kids.Value(r); n != 2 {
			t.Errorf("children counter = %d, want 2", n)
		}
		return nil
	})
	last := rec.all()[len(rec.all())-1]
	for _, s := range []Subject{f.parents.Subject(1), f.children.Subject(childKey(1, "y")), f.grand.Subject(12)} {
		if !hasSubject(last, s) {
			t.Errorf("the cascade's change lacks subject %x", s)
		}
	}
	if len(last.Subjects) != 1+2+3 {
		t.Errorf("the cascade's change has %d subjects, want 6", len(last.Subjects))
	}
}
