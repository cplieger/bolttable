package bolttable

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	"go.etcd.io/bbolt"
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
		page, err := r.Since(0, 100)
		if err != nil || len(page.Changes) != 1 {
			t.Errorf("Since(0) = %+v, %v; want the first Put only", page, err)
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
		{"reverse from", Range{Prefix: []byte("a"), From: []byte("a2"), Reverse: true}, []string{"a3", "a2"}},
		{"reverse to", Range{Prefix: []byte("a"), To: []byte("a3"), Reverse: true}, []string{"a2", "a1"}},
		{"reverse to between keys", Range{To: []byte("a25"), Reverse: true}, []string{"a2", "a1"}},
		{"to between keys", Range{To: []byte("a25")}, []string{"a1", "a2"}},
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
		touched := Synthetic([]byte("touched"))
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
		page, err := tx.Read().Since(0, 100)
		if err != nil {
			return err
		}
		page.Changes[0].Subjects[0][0] = 'Q'
		return f.kv.Put(tx, []byte("second"), &item{Name: "second"})
	})
	view(t, db, func(r *ReadTx) error {
		page, err := r.Since(0, 100)
		if err != nil {
			return err
		}
		changes := page.Changes
		if !hasSubject(changes[0], f.kv.Subject([]byte("key"))) || !hasSubject(changes[0], Synthetic([]byte("touched"))) {
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

func TestTableScanFromTo(t *testing.T) {
	f := newBytesFixture()
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	update(t, db, func(tx *Tx) error {
		for k := '1'; k <= '9'; k++ {
			if err := f.kv.Put(tx, []byte{byte(k)}, &item{Name: string(k)}); err != nil {
				return err
			}
		}
		return f.kv.Put(tx, []byte("55"), &item{Name: "55"})
	})
	b := func(s string) []byte { return []byte(s) }
	cases := []struct {
		name string
		rg   Range
		want []string
	}{
		{"from to", Range{From: b("3"), To: b("7")}, []string{"3", "4", "5", "55", "6"}},
		{"from to reverse", Range{From: b("3"), To: b("7"), Reverse: true}, []string{"6", "55", "5", "4", "3"}},
		{"from only", Range{From: b("8")}, []string{"8", "9"}},
		{"from only reverse", Range{From: b("8"), Reverse: true}, []string{"9", "8"}},
		{"to only", Range{To: b("3")}, []string{"1", "2"}},
		{"to only reverse", Range{To: b("3"), Reverse: true}, []string{"2", "1"}},
		{"from equal to", Range{From: b("5"), To: b("5")}, nil},
		{"from above to", Range{From: b("7"), To: b("3")}, nil},
		{"from above to reverse", Range{From: b("7"), To: b("3"), Reverse: true}, nil},
		{"empty to", Range{To: []byte{}}, nil},
		{"prefix inside", Range{Prefix: b("5"), From: b("3"), To: b("7")}, []string{"5", "55"}},
		{"prefix cut by to", Range{Prefix: b("5"), To: b("55")}, []string{"5"}},
		{"prefix cut by to reverse", Range{Prefix: b("5"), To: b("55"), Reverse: true}, []string{"5"}},
		{"prefix cut by from", Range{Prefix: b("5"), From: b("50")}, []string{"55"}},
		{"prefix outside", Range{Prefix: b("8"), To: b("7")}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := scanKeys(t, db, f.kv, tc.rg); !slices.Equal(got, tc.want) {
				t.Errorf("keys = %q, want %q", got, tc.want)
			}
		})
	}
	t.Run("delete while scanning reverse", func(t *testing.T) {
		var seen []string
		update(t, db, func(tx *Tx) error {
			return f.kv.Scan(tx.Read(), Range{From: b("2"), To: b("4"), Reverse: true}, func(k []byte, _ *item) (bool, error) {
				seen = append(seen, string(k))
				_, err := f.kv.Delete(tx, k)
				return true, err
			})
		})
		if !slices.Equal(seen, []string{"3", "2"}) {
			t.Errorf("visited %q while deleting, want 3 then 2", seen)
		}
	})
}

func TestTableScanExact(t *testing.T) {
	f := newBytesFixture()
	db := mustOpen(t, tempPath(t), f.schema, Options{})
	update(t, db, func(tx *Tx) error {
		for _, k := range []string{"4", "5", "5\x00", "55", "6"} {
			if err := f.kv.Put(tx, []byte(k), &item{Name: k}); err != nil {
				return err
			}
		}
		return nil
	})
	for _, reverse := range []bool{false, true} {
		rg := Exact([]byte("5"))
		rg.Reverse = reverse
		if got := scanKeys(t, db, f.kv, rg); !slices.Equal(got, []string{"5"}) {
			t.Errorf("Exact(5), reverse %v = %q, want only 5", reverse, got)
		}
	}
	if got := scanKeys(t, db, f.kv, Exact([]byte("7"))); got != nil {
		t.Errorf("Exact of an absent key = %q, want none", got)
	}
	key := []byte("5")
	rg := Exact(key)
	key[0] = '6'
	if got := scanKeys(t, db, f.kv, rg); !slices.Equal(got, []string{"5"}) {
		t.Errorf("Exact after its input changed = %q, want 5: Exact must copy", got)
	}
}

// jobsInState returns the keys the by_state index lists for state.
func jobsInState(t *testing.T, r *ReadTx, f *fixture, state string) []uint64 {
	t.Helper()
	var keys []uint64
	if err := f.byState.Scan(r, Exact([]byte(state)), func(_ []byte, k uint64, _ []byte) (bool, error) {
		keys = append(keys, k)
		return true, nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	return keys
}

func TestSilentTableIndexAndCounter(t *testing.T) {
	f := newFixture(t)
	var rec recorder
	db := mustOpen(t, tempPath(t), f.schema, Options{OnCommit: rec.on, Retention: 10})
	update(t, db, func(tx *Tx) error { return f.items.Put(tx, 1, &item{Name: "row"}) })
	check := func(step string, queued, running []uint64, weight int64) {
		t.Helper()
		view(t, db, func(r *ReadTx) error {
			if got := jobsInState(t, r, f, "queued"); !slices.Equal(got, queued) {
				t.Errorf("%s: queued = %v, want %v", step, got, queued)
			}
			if got := jobsInState(t, r, f, "running"); !slices.Equal(got, running) {
				t.Errorf("%s: running = %v, want %v", step, got, running)
			}
			if n, err := f.jobCount.Value(r); n != weight || err != nil {
				t.Errorf("%s: job weight = %d, %v; want %d", step, n, err, weight)
			}
			if rev, err := r.Rev(); rev != 1 || err != nil {
				t.Errorf("%s: Rev = %d, %v; want 1", step, rev, err)
			}
			if page, err := r.Since(0, 10); err != nil || len(page.Changes) != 1 {
				t.Errorf("%s: Since(0) = %+v, %v; want the one non-silent change", step, page, err)
			}
			return nil
		})
	}
	commits := stats(t, db).Commits
	steps := []struct {
		name            string
		write           func(tx *Tx) error
		queued, running []uint64
		weight          int64
	}{
		{"put", func(tx *Tx) error {
			if err := f.jobs.Put(tx, 1, &item{Name: "queued", N: 2}); err != nil {
				return err
			}
			return f.jobs.Put(tx, 2, &item{Name: "queued", N: 5})
		}, []uint64{1, 2}, nil, 7},
		{"move", func(tx *Tx) error { return f.jobs.Put(tx, 1, &item{Name: "running", N: 3}) }, []uint64{2}, []uint64{1}, 8},
		{"delete", func(tx *Tx) error { _, err := f.jobs.Delete(tx, 2); return err }, nil, []uint64{1}, 3},
	}
	for _, s := range steps {
		update(t, db, s.write)
		check(s.name, s.queued, s.running, s.weight)
		commits++
		if got := stats(t, db).Commits; got != commits {
			t.Errorf("%s: Commits = %d, want %d", s.name, got, commits)
		}
	}
	if n := len(rec.all()); n != 1 {
		t.Errorf("OnCommit ran %d times, want once for the non-silent write", n)
	}
}

func TestVerifySilentTable(t *testing.T) {
	path := tempPath(t)
	f := newFixture(t)
	db, err := Open(path, f.schema, Options{Retention: 10})
	if err != nil {
		t.Fatal(err)
	}
	update(t, db, func(tx *Tx) error {
		if err := f.jobs.Put(tx, 1, &item{Name: "queued", N: 2}); err != nil {
			return err
		}
		return f.jobs.Put(tx, 2, &item{Name: "running", N: 3})
	})
	view(t, db, func(r *ReadTx) error {
		if got := jobsInState(t, r, f, "queued"); !slices.Equal(got, []uint64{1}) {
			t.Errorf("queued before the damage = %v, want [1]", got)
		}
		if n, _ := f.jobCount.Value(r); n != 5 {
			t.Errorf("job weight before the damage = %d, want 5", n)
		}
		return nil
	})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	rawUpdate(t, path, func(btx *bbolt.Tx) error {
		a := nsBucket(btx, "a")
		entry := append(append([]byte("queued"), nonUniqueEnd...), Uint64Key{}.AppendKey(nil, 1)...)
		if err := a.Bucket([]byte("index:by_state")).Delete(entry); err != nil {
			return err
		}
		return a.Put([]byte("counter:job_weight"), encodeTotal(40))
	})
	f = newFixture(t)
	db = mustOpen(t, path, f.schema, Options{Retention: 10})
	rep, err := db.Verify(t.Context(), true)
	if err != nil {
		t.Fatal(err)
	}
	got := byName(rep)
	want := map[string]Mismatch{
		"a.by_state":   {Name: "a.by_state", Missing: 1, Repaired: true},
		"a.job_weight": {Name: "a.job_weight", Missing: 1, Extra: 1, Repaired: true},
	}
	if len(got) != len(want) || got["a.by_state"] != want["a.by_state"] || got["a.job_weight"] != want["a.job_weight"] {
		t.Errorf("mismatches = %+v, want %+v", rep.Mismatches, want)
	}
	if rep, _ := db.Verify(t.Context(), false); len(rep.Mismatches) != 0 {
		t.Errorf("after repair: %+v", rep.Mismatches)
	}
	view(t, db, func(r *ReadTx) error {
		if got := jobsInState(t, r, f, "queued"); !slices.Equal(got, []uint64{1}) {
			t.Errorf("queued after repair = %v, want [1]", got)
		}
		if n, _ := f.jobCount.Value(r); n != 5 {
			t.Errorf("job weight after repair = %d, want 5", n)
		}
		if rev, err := r.Rev(); rev != 0 || err != nil {
			t.Errorf("Rev after repair = %d, %v; want 0", rev, err)
		}
		return nil
	})
}
