package bolttable

import (
	"math"
	"strings"
	"testing"
)

func TestCounterOverflow(t *testing.T) {
	cases := []struct {
		name  string
		seed  []item // rows 1..n, committed first
		write func(f *fixture, tx *Tx) error
	}{
		{"insert above MaxInt64", []item{{Name: "s", N: math.MaxInt64}}, func(f *fixture, tx *Tx) error {
			return f.items.Put(tx, 9, &item{Name: "w", N: 1})
		}},
		{"insert below MinInt64", []item{{Name: "s", N: math.MinInt64}}, func(f *fixture, tx *Tx) error {
			return f.items.Put(tx, 9, &item{Name: "w", N: -1})
		}},
		{"delete of MinInt64 weight", []item{{Name: "s", N: math.MinInt64}}, func(f *fixture, tx *Tx) error {
			_, err := f.items.Delete(tx, 1)
			return err
		}},
		{"delete leaving below MinInt64", []item{{Name: "s", N: 1}, {Name: "t", N: math.MinInt64}, {Name: "u", N: -1}}, func(f *fixture, tx *Tx) error {
			_, err := f.items.Delete(tx, 1)
			return err
		}},
		// The delta MaxInt64-MinInt64 wraps to -1, which the total alone would
		// absorb.
		{"update delta outside int64", []item{{Name: "s", N: 5}, {Name: "t", N: math.MinInt64}}, func(f *fixture, tx *Tx) error {
			return f.items.Put(tx, 2, &item{Name: "t", N: math.MaxInt64})
		}},
		{"update total above MaxInt64", []item{{Name: "s", N: math.MaxInt64 - 1}, {Name: "t", N: 0}}, func(f *fixture, tx *Tx) error {
			return f.items.Put(tx, 2, &item{Name: "t", N: 2})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			db := mustOpen(t, tempPath(t), f.schema, Options{Retention: 10})
			update(t, db, func(tx *Tx) error {
				for i, it := range tc.seed {
					if err := f.items.Put(tx, uint64(i+1), &it); err != nil {
						return err
					}
				}
				return nil
			})
			var want int64
			var wantNames int
			view(t, db, func(r *ReadTx) error {
				want, _ = f.total.Value(r)
				wantNames = countEntries(t, r, f.byName)
				return nil
			})
			rev := headRev(t, db)
			err := db.Update(t.Context(), func(tx *Tx) error {
				// The write's own error is dropped: the transaction must fail anyway.
				_ = tc.write(f, tx)
				return nil
			})
			if err == nil || !strings.Contains(err.Error(), "counter a.total") {
				t.Fatalf("Update = %v, want an error naming counter a.total", err)
			}
			view(t, db, func(r *ReadTx) error {
				if got, _ := f.total.Value(r); got != want {
					t.Errorf("total = %d, want %d", got, want)
				}
				if got := countEntries(t, r, f.byName); got != wantNames {
					t.Errorf("by_name holds %d entries, want %d", got, wantNames)
				}
				for i, it := range tc.seed {
					if got, found, _ := f.items.Get(r, uint64(i+1)); !found || got.N != it.N {
						t.Errorf("row %d = %+v, %v; want %+v", i+1, got, found, it)
					}
				}
				if _, found, _ := f.items.Get(r, 9); found {
					t.Error("the failed insert was stored")
				}
				return nil
			})
			if got := headRev(t, db); got != rev {
				t.Errorf("rev = %d after a failed write, want %d", got, rev)
			}
		})
	}
}

func countEntries(t *testing.T, r *ReadTx, ix *Index[uint64, item]) int {
	t.Helper()
	n := 0
	if err := ix.Scan(r, Range{}, func([]byte, uint64, []byte) (bool, error) { n++; return true, nil }); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCounterTracksRows(t *testing.T) {
	f := newFixture(t)
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	steps := []struct {
		write func(tx *Tx) error
		want  int64
	}{
		{func(tx *Tx) error { return f.items.Put(tx, 1, &item{N: 5}) }, 5},
		{func(tx *Tx) error { return f.items.Put(tx, 2, &item{N: -2}) }, 3},
		{func(tx *Tx) error { return f.items.Put(tx, 1, &item{N: 7}) }, 5},
		{func(tx *Tx) error { _, err := f.items.Delete(tx, 2); return err }, 7},
		{func(tx *Tx) error { _, err := f.items.Delete(tx, 1); return err }, 0},
	}
	for i, s := range steps {
		update(t, db, s.write)
		view(t, db, func(r *ReadTx) error {
			if got, err := f.total.Value(r); got != s.want || err != nil {
				t.Errorf("step %d: total = %d, %v; want %d", i, got, err, s.want)
			}
			return nil
		})
	}
}
