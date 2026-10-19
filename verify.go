package bolttable

import (
	"bytes"
	"context"
	"fmt"
	"math/big"

	"go.etcd.io/bbolt"
)

// ctxEvery is how many keys a walk visits between context checks.
const ctxEvery = 1024

// Mismatch is one table, index or counter, named namespace.name, whose
// stored state differs from a rescan of its rows. For a table, Undecodable
// counts rows that do not decode; their index entries count as neither
// missing nor extra. For an index, Missing and Extra count entries. For a
// counter, a stored total differing from the exact sum of its rows' weights,
// or a sum outside int64, is one Missing and one Extra, and Undecodable
// counts rows its sum could not include, which leaves it unchecked.
type Mismatch struct {
	Name                        string
	Missing, Extra, Undecodable int
	Repaired                    bool
}

// VerifyReport lists every mismatch Verify found.
type VerifyReport struct {
	Mismatches []Mismatch
}

// Verify rescans every table in one transaction and recomputes every index
// entry and counter. With repair it rewrites index entries and counters, and
// only those: it never rewrites or deletes a row, so an undecodable row stays
// reported with Repaired false, and its remedy is a restore. A repair writes
// no revision.
func (db *DB) Verify(ctx context.Context, repair bool) (VerifyReport, error) {
	if err := ctx.Err(); err != nil {
		return VerifyReport{}, err
	}
	var rep VerifyReport
	walk := func(btx *bbolt.Tx) error {
		rep = VerifyReport{}
		for _, ns := range db.schema.namespaces {
			nsb := btx.Bucket([]byte(ns.name))
			for _, td := range ns.tables {
				ms, err := verifyTable(ctx, nsb, td, repair)
				if err != nil {
					return err
				}
				rep.Mismatches = append(rep.Mismatches, ms...)
			}
		}
		return nil
	}
	if !repair {
		return rep, db.bolt.View(walk)
	}
	return rep, commit(db.bolt.Update, walk)
}

// rescan is what a table's rows derive: per index the stored pairs they
// imply, per counter the exact sum, and the keys of rows that do not decode.
// A sum is exact because rows arrive in key order, not the order that kept
// every committed total inside int64, so a partial sum may leave it.
type rescan struct {
	expected    []map[string][]byte
	totals      []*big.Int
	undecodable map[string]struct{}
	collisions  []int
}

func scanRows(ctx context.Context, rows *bbolt.Bucket, td *tableDef) (rescan, error) {
	rs := rescan{
		expected:    make([]map[string][]byte, len(td.indexes)),
		totals:      make([]*big.Int, len(td.counters)),
		undecodable: map[string]struct{}{},
		collisions:  make([]int, len(td.indexes)),
	}
	for i := range rs.expected {
		rs.expected[i] = map[string][]byte{}
	}
	for i := range rs.totals {
		rs.totals[i] = new(big.Int)
	}
	n := 0
	c := rows.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		n++
		if n%ctxEvery == 0 {
			if err := ctx.Err(); err != nil {
				return rescan{}, err
			}
		}
		rs.add(td, k, v)
	}
	return rs, nil
}

func (rs *rescan) add(td *tableDef, k, v []byte) {
	f, err := td.facts(k, v)
	pairs := make([]map[string][]byte, len(td.indexes))
	for i, d := range td.indexes {
		if err == nil {
			pairs[i], err = d.pairs(f.entries[i], k)
		}
	}
	if err != nil {
		rs.undecodable[string(k)] = struct{}{}
		return
	}
	for i, p := range pairs {
		for ik, iv := range p {
			if _, taken := rs.expected[i][ik]; taken {
				rs.collisions[i]++
				continue
			}
			rs.expected[i][ik] = iv
		}
	}
	var w big.Int
	for i, n := range f.weights {
		rs.totals[i].Add(rs.totals[i], w.SetInt64(n))
	}
}

func verifyTable(ctx context.Context, nsb *bbolt.Bucket, td *tableDef, repair bool) ([]Mismatch, error) {
	if nsb == nil || nsb.Bucket([]byte{td.prefix}) == nil {
		return nil, fmt.Errorf("%w: table %s has no bucket", ErrCorrupt, td.qname)
	}
	rs, err := scanRows(ctx, nsb.Bucket([]byte{td.prefix}), td)
	if err != nil {
		return nil, err
	}
	var out []Mismatch
	if n := len(rs.undecodable); n > 0 {
		out = append(out, Mismatch{Name: td.qname, Undecodable: n})
	}
	for i, d := range td.indexes {
		m, err := verifyIndex(nsb.Bucket(d.bucket), d, rs.expected[i], rs.undecodable, repair)
		if err != nil {
			return nil, err
		}
		m.Missing += rs.collisions[i]
		if m.Missing+m.Extra > 0 {
			out = append(out, m)
		}
	}
	for i, d := range td.counters {
		m, err := verifyCounter(nsb, d, rs.totals[i], len(rs.undecodable), repair)
		if err != nil {
			return nil, err
		}
		if m.Missing+m.Extra+m.Undecodable > 0 {
			out = append(out, m)
		}
	}
	return out, nil
}

// verifyIndex compares an index bucket with the pairs its rows imply,
// consuming expected.
func verifyIndex(ib *bbolt.Bucket, d *indexDef, expected map[string][]byte, undecodable map[string]struct{}, repair bool) (Mismatch, error) {
	m := Mismatch{Name: d.qname}
	extra := extraEntries(ib, d, expected, undecodable)
	m.Extra = len(extra)
	m.Missing = len(expected)
	if !repair || m.Missing+m.Extra == 0 {
		return m, nil
	}
	for _, k := range extra {
		if err := ib.Delete(k); err != nil {
			return m, err
		}
	}
	for _, k := range sortedKeys(expected) {
		if err := ib.Put([]byte(k), expected[k]); err != nil {
			return m, err
		}
	}
	m.Repaired = true
	return m, nil
}

// extraEntries returns the stored keys no row implies, skipping those of
// undecodable rows and deleting each matched key from expected.
func extraEntries(ib *bbolt.Bucket, d *indexDef, expected map[string][]byte, undecodable map[string]struct{}) [][]byte {
	var extra [][]byte
	c := ib.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if want, ok := expected[string(k)]; ok && bytes.Equal(want, v) {
			delete(expected, string(k))
			continue
		}
		if _, pk, _, ok := d.split(k, v); ok {
			if _, skip := undecodable[string(pk)]; skip {
				continue
			}
		}
		extra = append(extra, bytes.Clone(k))
	}
	return extra
}

func verifyCounter(nsb *bbolt.Bucket, d *counterDef, total *big.Int, undecodable int, repair bool) (Mismatch, error) {
	m := Mismatch{Name: d.qname}
	if undecodable > 0 {
		m.Undecodable = undecodable
		return m, nil
	}
	fits := total.IsInt64()
	stored, err := readTotal(nsb, d)
	if err == nil && fits && stored == total.Int64() {
		return m, nil
	}
	m.Missing, m.Extra = 1, 1
	if !repair || !fits {
		return m, nil
	}
	if err := nsb.Put(d.key, encodeTotal(total.Int64())); err != nil {
		return m, err
	}
	m.Repaired = true
	return m, nil
}
