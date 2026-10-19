package bolttable

import (
	"encoding/binary"
	"fmt"
	"math"

	"go.etcd.io/bbolt"
)

type counterDef struct {
	table   *tableDef
	qname   string
	name    string
	key     []byte
	version uint32
}

// Counter keeps the sum of a weight over every row of its table, updated in
// the same transaction as the row.
type Counter[K, V any] struct {
	def *counterDef
}

// DefineCounter declares a counter summing weight over the table's rows. It
// fails for a nil table or function, a zero version, a name already used in
// the namespace, and after Open.
func DefineCounter[K, V any](t *Table[K, V], name string, version uint32, weight func(k K, v *V) int64) (*Counter[K, V], error) {
	if t == nil || weight == nil {
		return nil, fmt.Errorf("bolttable: counter %q needs a table and a weight function", name)
	}
	ns := t.def.ns
	var c *Counter[K, V]
	err := ns.schema.declare("counter "+ns.qualify(name), func() error {
		if version == 0 {
			return fmt.Errorf("bolttable: counter %s has a zero version", ns.qualify(name))
		}
		if err := ns.claimName(name); err != nil {
			return err
		}
		d := &counterDef{table: t.def, qname: ns.qualify(name), name: name, key: []byte("counter:" + name), version: version}
		t.def.counters = append(t.def.counters, d)
		t.weightFns = append(t.weightFns, weight)
		c = &Counter[K, V]{def: d}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return c, nil
}

// Value returns the counter's total.
func (c *Counter[K, V]) Value(r *ReadTx) (int64, error) {
	ns, err := r.namespace(c.def.table)
	if err != nil {
		return 0, err
	}
	return readTotal(ns, c.def)
}

type counterTotal struct {
	key   []byte
	total int64
}

// encodeTotal and readTotal store a total as its two's-complement bits.
func encodeTotal(v int64) []byte {
	return binary.BigEndian.AppendUint64(nil, uint64(v)) //nolint:gosec // a bit-preserving round trip with readTotal
}

func readTotal(ns *bbolt.Bucket, d *counterDef) (int64, error) {
	raw := ns.Get(d.key)
	if raw == nil {
		return 0, nil
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("%w: counter %s", ErrCorrupt, d.qname)
	}
	return int64(binary.BigEndian.Uint64(raw)), nil //nolint:gosec // a bit-preserving round trip with encodeTotal
}

// planCounters computes each counter's total after a row moves from prev to
// next, failing when a delta or total leaves int64. It writes nothing.
func planCounters(ns *bbolt.Bucket, td *tableDef, prev, next rowFacts) ([]counterTotal, error) {
	totals := make([]counterTotal, 0, len(td.counters))
	for i, d := range td.counters {
		var before, after int64
		if prev.weights != nil {
			before = prev.weights[i]
		}
		if next.weights != nil {
			after = next.weights[i]
		}
		if before == after {
			continue
		}
		cur, err := readTotal(ns, d)
		if err != nil {
			return nil, err
		}
		delta, ok := checkedSub(after, before)
		if ok {
			cur, ok = checkedAdd(cur, delta)
		}
		if !ok {
			return nil, fmt.Errorf("bolttable: counter %s leaves int64", d.qname)
		}
		totals = append(totals, counterTotal{key: d.key, total: cur})
	}
	return totals, nil
}

func checkedAdd(a, b int64) (int64, bool) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, false
	}
	return a + b, true
}

func checkedSub(a, b int64) (int64, bool) {
	if (b < 0 && a > math.MaxInt64+b) || (b > 0 && a < math.MinInt64+b) {
		return 0, false
	}
	return a - b, true
}
