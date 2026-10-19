package bolttable

import (
	"bytes"
	"math"
	"slices"
	"testing"

	"go.etcd.io/bbolt"
)

func nsBucket(btx *bbolt.Tx, names ...string) *bbolt.Bucket {
	b := btx.Bucket([]byte(names[0]))
	for _, n := range names[1:] {
		b = b.Bucket([]byte(n))
	}
	return b
}

func seededFile(t *testing.T) string {
	t.Helper()
	path := tempPath(t)
	f := newFixture(t)
	db, err := Open(path, f.schema, Options{Retention: 10})
	if err != nil {
		t.Fatal(err)
	}
	update(t, db, func(tx *Tx) error {
		for i, it := range []item{{Name: "one", Tags: []string{"red"}, N: 2}, {Name: "two", Tags: []string{"red", "blue"}, N: 3}} {
			if err := f.items.Put(tx, uint64(i+1), &it); err != nil {
				return err
			}
		}
		return nil
	})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func byName(rep VerifyReport) map[string]Mismatch {
	m := map[string]Mismatch{}
	for _, x := range rep.Mismatches {
		m[x.Name] = x
	}
	return m
}

func TestVerifyFindsRawBboltEntry(t *testing.T) {
	path := seededFile(t)
	rawUpdate(t, path, func(btx *bbolt.Tx) error {
		a := nsBucket(btx, "a")
		// An extra unique entry, a missing tag entry and a wrong total.
		if err := a.Bucket([]byte("index:by_name")).Put([]byte("ghost"), []byte{8, 0, 0, 0, 0, 0, 0, 0, 9}); err != nil {
			return err
		}
		tag := append(append([]byte("blue"), nonUniqueEnd...), Uint64Key{}.AppendKey(nil, 2)...)
		if err := a.Bucket([]byte("index:by_tag")).Delete(tag); err != nil {
			return err
		}
		return a.Put([]byte("counter:total"), encodeTotal(99))
	})
	f := newFixture(t)
	db := mustOpen(t, path, f.schema, Options{Retention: 10})
	rev := headRev(t, db)
	rep, err := db.Verify(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	got := byName(rep)
	want := map[string]Mismatch{
		"a.by_name": {Name: "a.by_name", Extra: 1},
		"a.by_tag":  {Name: "a.by_tag", Missing: 1},
		"a.total":   {Name: "a.total", Missing: 1, Extra: 1},
	}
	if len(got) != len(want) {
		t.Errorf("mismatches = %+v, want %+v", rep.Mismatches, want)
	}
	for name, w := range want {
		if got[name] != w {
			t.Errorf("%s = %+v, want %+v", name, got[name], w)
		}
	}
	rep, err = db.Verify(t.Context(), true)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range rep.Mismatches {
		if !m.Repaired {
			t.Errorf("%s not repaired", m.Name)
		}
	}
	if rep, _ = db.Verify(t.Context(), false); len(rep.Mismatches) != 0 {
		t.Errorf("after repair: %+v", rep.Mismatches)
	}
	view(t, db, func(r *ReadTx) error {
		if n, _ := f.total.Value(r); n != 5 {
			t.Errorf("total = %d, want 5", n)
		}
		if _, _, found, _ := f.byName.Lookup(r, []byte("ghost")); found {
			t.Error("the extra entry survived repair")
		}
		return nil
	})
	if got := headRev(t, db); got != rev {
		t.Errorf("repair moved the revision from %d to %d", rev, got)
	}
}

func TestVerifyUndecodableRowSurvivesRepair(t *testing.T) {
	path := seededFile(t)
	key := Uint64Key{}.AppendKey(nil, 1)
	rawUpdate(t, path, func(btx *bbolt.Tx) error {
		return nsBucket(btx, "a", "\x10").Put(key, []byte("not json"))
	})
	f := newFixture(t)
	db := mustOpen(t, path, f.schema, Options{})
	for _, repair := range []bool{false, true} {
		rep, err := db.Verify(t.Context(), repair)
		if err != nil {
			t.Fatal(err)
		}
		got := byName(rep)
		if m := got["a.items"]; m.Undecodable != 1 || m.Repaired {
			t.Errorf("repair %v: a.items = %+v, want one undecodable row, not repaired", repair, m)
		}
		if m, ok := got["a.by_name"]; ok {
			t.Errorf("repair %v: the undecodable row's entries counted as %+v", repair, m)
		}
		if m := got["a.total"]; m.Undecodable != 1 || m.Repaired {
			t.Errorf("repair %v: a.total = %+v, want unchecked", repair, m)
		}
	}
	view(t, db, func(r *ReadTx) error {
		if raw := nsBucket(r.btx, "a", "\x10").Get(key); !bytes.Equal(raw, []byte("not json")) {
			t.Errorf("repair rewrote the undecodable row to %q", raw)
		}
		if _, _, found, _ := f.byName.Lookup(r, []byte("one")); !found {
			t.Error("repair deleted the undecodable row's index entry")
		}
		return nil
	})
}

func TestVerifyCleanFile(t *testing.T) {
	db := mustOpen(t, seededFile(t), newFixture(t).schema, Options{})
	rep, err := db.Verify(t.Context(), false)
	if err != nil || len(rep.Mismatches) != 0 {
		t.Errorf("Verify = %+v, %v", rep, err)
	}
}

func TestVerifyCountsColliding(t *testing.T) {
	path := seededFile(t)
	// A raw row whose name collides with row 1's unique key.
	rawUpdate(t, path, func(btx *bbolt.Tx) error {
		return nsBucket(btx, "a", "\x10").Put(Uint64Key{}.AppendKey(nil, 3), []byte(`{"name":"one"}`))
	})
	db := mustOpen(t, path, newFixture(t).schema, Options{})
	rep, err := db.Verify(t.Context(), false)
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, m := range rep.Mismatches {
		names = append(names, m.Name)
	}
	if !slices.Contains(names, "a.by_name") {
		t.Errorf("mismatches = %+v, want a.by_name", rep.Mismatches)
	}
}

// safeOrder orders weights so every running total stays inside int64, as the
// checked writes require: a negative weight while the total is not negative,
// a positive one while it is. It exists whenever the whole sum fits.
func safeOrder(weights []int64) []int {
	left := make([]int, len(weights))
	for i := range left {
		left[i] = i
	}
	var order []int
	var sum int64
	for len(left) > 0 {
		pick := 0
		for j, i := range left {
			if (sum >= 0) == (weights[i] < 0) {
				pick = j
				break
			}
		}
		i := left[pick]
		left = slices.Delete(left, pick, pick+1)
		order = append(order, i)
		sum += weights[i]
	}
	return order
}

func permutations(n int) [][]int {
	if n == 0 {
		return [][]int{{}}
	}
	var out [][]int
	for _, p := range permutations(n - 1) {
		for at := 0; at <= len(p); at++ {
			out = append(out, slices.Insert(slices.Clone(p), at, n-1))
		}
	}
	return out
}

// TestVerifyCounterSumIsOrderIndependent stores rows whose total fits int64
// under every assignment of weights to keys, each in a fresh file, so in some
// key orders Verify's partial sums leave int64 and in others they do not.
func TestVerifyCounterSumIsOrderIndependent(t *testing.T) {
	sets := map[string][]int64{
		"above MaxInt64": {math.MaxInt64, 1, -1, 3, -3},
		"below MinInt64": {math.MinInt64, -1, 1, -3, 3},
		"both bounds":    {math.MaxInt64, math.MinInt64, math.MaxInt64, -5, 2},
	}
	for name, weights := range sets {
		t.Run(name, func(t *testing.T) {
			var want int64
			for _, i := range safeOrder(weights) {
				want += weights[i]
			}
			f := newFixture(t)
			for _, keys := range permutations(len(weights)) {
				db := mustOpen(t, tempPath(t), f.schema, Options{})
				update(t, db, func(tx *Tx) error {
					for _, i := range safeOrder(weights) {
						if err := f.items.Put(tx, uint64(keys[i]+1), &item{N: weights[i]}); err != nil {
							return err
						}
					}
					return nil
				})
				rep, err := db.Verify(t.Context(), false)
				if err != nil || len(rep.Mismatches) != 0 {
					t.Errorf("keys %v: Verify = %+v, %v; want no mismatch", keys, rep.Mismatches, err)
				}
				view(t, db, func(r *ReadTx) error {
					if got, err := f.total.Value(r); got != want || err != nil {
						t.Errorf("keys %v: total = %d, %v; want %d", keys, got, err, want)
					}
					return nil
				})
			}
		})
	}
}

// TestVerifyCounterSumOutsideInt64 deletes one raw row so the remaining rows
// sum past an int64 bound while the stored total stays at it.
func TestVerifyCounterSumOutsideInt64(t *testing.T) {
	cases := map[string][]int64{
		"above MaxInt64": {math.MaxInt64, -1, 1},
		"below MinInt64": {math.MinInt64, 1, -1},
	}
	for name, weights := range cases {
		t.Run(name, func(t *testing.T) {
			path := tempPath(t)
			f := newFixture(t)
			db := mustOpen(t, path, f.schema, Options{})
			update(t, db, func(tx *Tx) error {
				for _, i := range safeOrder(weights) {
					if err := f.items.Put(tx, uint64(i+1), &item{N: weights[i]}); err != nil {
						return err
					}
				}
				return nil
			})
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			rawUpdate(t, path, func(btx *bbolt.Tx) error {
				return nsBucket(btx, "a", "\x10").Delete(Uint64Key{}.AppendKey(nil, 2))
			})
			db = mustOpen(t, path, f.schema, Options{})
			for _, repair := range []bool{false, true} {
				rep, err := db.Verify(t.Context(), repair)
				want := Mismatch{Name: "a.total", Missing: 1, Extra: 1}
				if err != nil || len(rep.Mismatches) != 1 || rep.Mismatches[0] != want {
					t.Errorf("Verify(repair %v) = %+v, %v; want only %+v", repair, rep.Mismatches, err, want)
				}
			}
			view(t, db, func(r *ReadTx) error {
				if got, _ := f.total.Value(r); got != weights[0] {
					t.Errorf("total = %d after repair, want %d unchanged", got, weights[0])
				}
				return nil
			})
		})
	}
}
