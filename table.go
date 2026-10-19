package bolttable

import (
	"bytes"
	"errors"
	"fmt"
)

// TableOptions configures a table. A Silent table's writes reserve no
// revision, record no subject and never reach Since; a non-silent write
// inside BeforeCommit fails the transaction.
type TableOptions struct {
	Silent bool
}

// Subject names one row in the change feed: a table's prefix byte plus the
// encoded key.
type Subject []byte

// Range selects the keys a Scan visits: those starting with Prefix, from
// From onward (inclusive; when Reverse, From is the inclusive upper bound),
// in byte order or, when Reverse, its reverse. Nil Prefix and From select
// every key.
type Range struct {
	Prefix  []byte
	From    []byte
	Reverse bool
}

type tableDef struct {
	ns    *Namespace
	facts func(kb, vb []byte) (rowFacts, error)
	// qname is namespace.name, the name errors and Mismatch carry.
	qname    string
	name     string
	indexes  []*indexDef
	counters []*counterDef
	cascades []*cascadeDef
	keyVer   uint32
	valVer   uint32
	prefix   byte
	silent   bool
}

// rowFacts is what a stored row derives: per index its entries and per
// counter its weight, aligned with tableDef.indexes and tableDef.counters.
type rowFacts struct {
	entries [][]Entry
	weights []int64
}

// errUndecodable is the facts error for a row whose key or value does not
// decode; callers turn it into a corruptRow naming the table.
var errUndecodable = errors.New("bolttable: undecodable row")

// Table is a typed table declared in a namespace.
type Table[K, V any] struct {
	key       KeyCodec[K]
	val       Codec[V]
	def       *tableDef
	indexFns  []func(K, *V) []Entry
	weightFns []func(K, *V) int64
}

// Define declares a table under a prefix byte unique across the schema and a
// name unique in its namespace. It fails for a nil namespace or codec, a zero
// codec version, a duplicate prefix or name, and after Open.
func Define[K, V any](ns *Namespace, prefix byte, name string, k KeyCodec[K], v Codec[V], o TableOptions) (*Table[K, V], error) {
	if ns == nil {
		return nil, fmt.Errorf("bolttable: table %q in a nil namespace", name)
	}
	if k == nil || v == nil {
		return nil, fmt.Errorf("bolttable: table %s needs a key and a value codec", ns.qualify(name))
	}
	var t *Table[K, V]
	err := ns.schema.declare("table "+ns.qualify(name), func() error {
		if k.Version() == 0 || v.Version() == 0 {
			return fmt.Errorf("bolttable: table %s has a zero codec version", ns.qualify(name))
		}
		if other, dup := ns.schema.prefixes[prefix]; dup {
			return fmt.Errorf("bolttable: table %s reuses prefix 0x%02x of %s", ns.qualify(name), prefix, other)
		}
		if err := ns.claimName(name); err != nil {
			return err
		}
		td := &tableDef{
			ns: ns, qname: ns.qualify(name), name: name, prefix: prefix,
			keyVer: k.Version(), valVer: v.Version(), silent: o.Silent,
		}
		t = &Table[K, V]{key: k, val: v, def: td}
		td.facts = t.facts
		ns.tables = append(ns.tables, td)
		ns.schema.prefixes[prefix] = td.qname
		return nil
	})
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (t *Table[K, V]) facts(kb, vb []byte) (rowFacts, error) {
	k, err := t.key.ParseKey(bytes.Clone(kb))
	if err != nil {
		return rowFacts{}, errUndecodable
	}
	var v V
	if err := t.val.Decode(bytes.Clone(vb), &v); err != nil {
		return rowFacts{}, errUndecodable
	}
	f := rowFacts{entries: make([][]Entry, len(t.indexFns)), weights: make([]int64, len(t.weightFns))}
	for i, fn := range t.indexFns {
		f.entries[i] = fn(k, &v)
	}
	for i, fn := range t.weightFns {
		f.weights[i] = fn(k, &v)
	}
	return f, nil
}

// Get returns the row under k; found is false for an absent row. A stored
// value that does not decode is ErrCorrupt.
func (t *Table[K, V]) Get(r *ReadTx, k K) (v V, found bool, err error) {
	b, err := r.rows(t.def)
	if err != nil {
		return v, false, err
	}
	kb := t.key.AppendKey(nil, k)
	raw := b.Get(kb)
	if raw == nil {
		return v, false, nil
	}
	if derr := t.val.Decode(bytes.Clone(raw), &v); derr != nil {
		var zero V
		return zero, false, corruptRow(t.def.qname, kb)
	}
	return v, true, nil
}

// Put stores v under k, updating every index and counter of the table in the
// same transaction. A value whose encoding equals the stored bytes writes
// nothing and records no subject. It fails with a *ConflictError when a
// unique index key belongs to another row, writing nothing; a counter
// leaving int64 fails it and the whole transaction.
func (t *Table[K, V]) Put(tx *Tx, k K, v *V) error {
	if v == nil {
		return fmt.Errorf("bolttable: table %s: nil value", t.def.qname)
	}
	vb, err := t.val.Append(nil, v)
	if err != nil {
		return fmt.Errorf("bolttable: table %s: encode value: %w", t.def.qname, err)
	}
	return tx.putRow(t.def, t.key.AppendKey(nil, k), vb)
}

// Delete removes the row under k, its index entries and counter weights, and
// runs the table's cascades; it reports whether a row was there.
func (t *Table[K, V]) Delete(tx *Tx, k K) (bool, error) {
	return tx.deleteRow(t.def, t.key.AppendKey(nil, k))
}

// Subject returns the change-feed subject of the row under k.
func (t *Table[K, V]) Subject(k K) Subject {
	return t.key.AppendKey([]byte{t.def.prefix}, k)
}

// NextID returns the table's next sequence number, starting at 1. It records
// no subject; on a non-silent table it fails the transaction inside
// BeforeCommit, as a write does. After ResetNamespaces it stays above every
// id the namespace issued before.
func (t *Table[K, V]) NextID(tx *Tx) (uint64, error) {
	if err := tx.writeCheck(t.def); err != nil {
		return 0, err
	}
	b, err := tx.read.rows(t.def)
	if err != nil {
		return 0, err
	}
	return b.NextSequence()
}

// Scan calls fn for each row in rg until fn returns false or an error. A key
// or value that does not decode stops the scan with ErrCorrupt. Inside a
// write transaction fn may write the table it scans.
func (t *Table[K, V]) Scan(r *ReadTx, rg Range, fn func(K, *V) (bool, error)) error {
	b, err := r.rows(t.def)
	if err != nil {
		return err
	}
	var hi []byte
	if rg.From != nil {
		hi = append(bytes.Clone(rg.From), 0)
	}
	return scanBucket(b, &cursorRange{prefix: rg.Prefix, lo: rg.From, hi: hi, reverse: rg.Reverse}, r.btx.Writable(),
		func(kb, vb []byte) (bool, error) {
			k, err := t.key.ParseKey(bytes.Clone(kb))
			if err != nil {
				return false, corruptRow(t.def.qname, kb)
			}
			var v V
			if err := t.val.Decode(bytes.Clone(vb), &v); err != nil {
				return false, corruptRow(t.def.qname, kb)
			}
			return fn(k, &v)
		})
}

func (tx *Tx) putRow(td *tableDef, kb, vb []byte) error {
	if err := tx.writeCheck(td); err != nil {
		return err
	}
	if len(kb) == 0 {
		return fmt.Errorf("bolttable: table %s: empty key", td.qname)
	}
	ns, b, err := tx.read.buckets(td)
	if err != nil {
		return err
	}
	old := b.Get(kb)
	if old != nil && bytes.Equal(old, vb) {
		return nil
	}
	next, err := td.facts(kb, vb)
	if err != nil {
		return fmt.Errorf("bolttable: table %s: the encoded value does not decode", td.qname)
	}
	var prev rowFacts
	if old != nil {
		if prev, err = td.facts(kb, old); err != nil {
			return corruptRow(td.qname, kb)
		}
	}
	ops, err := planIndexes(ns, td, kb, prev, next)
	if err != nil {
		return err
	}
	totals, err := planCounters(ns, td, prev, next)
	if err != nil {
		return tx.fail(err)
	}
	if err := b.Put(bytes.Clone(kb), bytes.Clone(vb)); err != nil {
		return tx.fail(fmt.Errorf("bolttable: table %s: %w", td.qname, err))
	}
	if err := applyDerived(ns, ops, totals); err != nil {
		return tx.fail(err)
	}
	tx.subject(td, kb)
	return nil
}

func (tx *Tx) deleteRow(td *tableDef, kb []byte) (bool, error) {
	if err := tx.writeCheck(td); err != nil {
		return false, err
	}
	ns, b, err := tx.read.buckets(td)
	if err != nil {
		return false, err
	}
	old := b.Get(kb)
	if old == nil {
		return false, nil
	}
	prev, err := td.facts(kb, old)
	if err != nil {
		return false, corruptRow(td.qname, kb)
	}
	ops, err := planIndexes(ns, td, kb, prev, rowFacts{})
	if err != nil {
		return false, err
	}
	totals, err := planCounters(ns, td, prev, rowFacts{})
	if err != nil {
		return false, tx.fail(err)
	}
	if err := b.Delete(kb); err != nil {
		return false, tx.fail(fmt.Errorf("bolttable: table %s: %w", td.qname, err))
	}
	if err := applyDerived(ns, ops, totals); err != nil {
		return false, tx.fail(err)
	}
	tx.subject(td, kb)
	for _, c := range td.cascades {
		if err := tx.cascade(ns, c, kb); err != nil {
			return false, tx.fail(err)
		}
	}
	return true, nil
}
