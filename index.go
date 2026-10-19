package bolttable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"

	"go.etcd.io/bbolt"
)

// Entry is one index entry a row produces. Key is the index key; Value is the
// projection a lookup or scan returns without reading the row.
type Entry struct {
	Key, Value []byte
}

// IndexOptions configures an index. A Unique index gives each key at most
// one row. Version enters the namespace's format digest and must not be zero.
type IndexOptions struct {
	Unique  bool
	Version uint32
}

// nonUniqueEnd ends a non-unique entry's escaped key, before the primary key.
// It sorts below every escaped byte (0x00 is escaped as 0x00 0xFF), so a key
// that is a prefix of another sorts first and (a, bc) never meets (ab, c).
var nonUniqueEnd = []byte{0x00, 0x01}

type indexDef struct {
	table   *tableDef
	qname   string
	name    string
	bucket  []byte
	version uint32
	unique  bool
}

// Index is a declared index over a table. Each row maps to any number of
// entries: none makes the index sparse, several make it per-element.
type Index[K, V any] struct {
	table *Table[K, V]
	def   *indexDef
}

// DefineIndex declares an index whose entries entries computes from a row.
// It fails for a nil table or function, a zero version, a name already used
// in the namespace, and after Open.
func DefineIndex[K, V any](t *Table[K, V], name string, o IndexOptions, entries func(k K, v *V) []Entry) (*Index[K, V], error) {
	if t == nil || entries == nil {
		return nil, fmt.Errorf("bolttable: index %q needs a table and an entries function", name)
	}
	ns := t.def.ns
	var ix *Index[K, V]
	err := ns.schema.declare("index "+ns.qualify(name), func() error {
		if o.Version == 0 {
			return fmt.Errorf("bolttable: index %s has a zero version", ns.qualify(name))
		}
		if err := ns.claimName(name); err != nil {
			return err
		}
		d := &indexDef{
			table: t.def, qname: ns.qualify(name), name: name,
			bucket: []byte("index:" + name), version: o.Version, unique: o.Unique,
		}
		t.def.indexes = append(t.def.indexes, d)
		t.indexFns = append(t.indexFns, entries)
		ix = &Index[K, V]{table: t, def: d}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ix, nil
}

// Lookup returns the row key and projection under a unique index key; found
// is false when no row has it. It fails on a non-unique index.
func (ix *Index[K, V]) Lookup(r *ReadTx, key []byte) (k K, proj []byte, found bool, err error) {
	if !ix.def.unique {
		return k, nil, false, fmt.Errorf("bolttable: Lookup on non-unique index %s", ix.def.qname)
	}
	ib, err := r.index(ix.def)
	if err != nil {
		return k, nil, false, err
	}
	v := ib.Get(key)
	if v == nil {
		return k, nil, false, nil
	}
	pk, proj, ok := splitUniqueValue(v)
	if !ok {
		return k, nil, false, corruptIndex(ix.def.qname, key)
	}
	if k, err = ix.table.key.ParseKey(bytes.Clone(pk)); err != nil {
		return k, nil, false, corruptIndex(ix.def.qname, key)
	}
	return k, bytes.Clone(proj), true, nil
}

// Scan calls fn for each entry whose index key falls in rg, in index-key
// order, until fn returns false or an error. Range compares index keys, so a
// resumable scan needs an index key unique per row.
func (ix *Index[K, V]) Scan(r *ReadTx, rg Range, fn func(ikey []byte, k K, proj []byte) (bool, error)) error {
	ib, err := r.index(ix.def)
	if err != nil {
		return err
	}
	cr := cursorRange{prefix: rg.Prefix, lo: rg.From, reverse: rg.Reverse}
	if ix.def.unique {
		if rg.From != nil {
			cr.hi = append(bytes.Clone(rg.From), 0)
		}
	} else {
		cr.prefix = escapeKey(rg.Prefix)
		if rg.From != nil {
			cr.lo = escapeKey(rg.From)
			// Every entry of key From sorts below From's escape plus 0x00 0x02.
			cr.hi = append(escapeKey(rg.From), 0x00, 0x02)
		}
	}
	return scanBucket(ib, &cr, r.btx.Writable(), func(sk, sv []byte) (bool, error) {
		ikey, pk, proj, ok := ix.def.split(sk, sv)
		if !ok {
			return false, corruptIndex(ix.def.qname, sk)
		}
		k, err := ix.table.key.ParseKey(bytes.Clone(pk))
		if err != nil {
			return false, corruptIndex(ix.def.qname, sk)
		}
		return fn(ikey, k, bytes.Clone(proj))
	})
}

// split decodes a stored entry into its index key, primary key and
// projection; the index key is a fresh slice.
func (d *indexDef) split(sk, sv []byte) (ikey, pk, proj []byte, ok bool) {
	if d.unique {
		pk, proj, ok = splitUniqueValue(sv)
		return bytes.Clone(sk), pk, proj, ok
	}
	ikey, pk, ok = splitNonUniqueKey(sk)
	return ikey, pk, sv, ok
}

// encode lays out one entry of row pk: a unique entry is the index key alone,
// valued uvarint(len(pk)), pk, projection; a non-unique entry is the escaped
// index key, nonUniqueEnd, pk, valued the projection.
func (d *indexDef) encode(e Entry, pk []byte) (key, val []byte) {
	if d.unique {
		val = binary.AppendUvarint(nil, uint64(len(pk)))
		val = append(append(val, pk...), e.Value...)
		return bytes.Clone(e.Key), val
	}
	key = escapeKey(e.Key)
	key = append(append(key, nonUniqueEnd...), pk...)
	return key, bytes.Clone(e.Value)
}

// pairs encodes a row's entries as stored key to stored value. A row giving
// one index key two projections, or a unique index an empty key, is an
// error.
func (d *indexDef) pairs(entries []Entry, pk []byte) (map[string][]byte, error) {
	m := make(map[string][]byte, len(entries))
	for _, e := range entries {
		if d.unique && len(e.Key) == 0 {
			return nil, fmt.Errorf("bolttable: index %s: empty unique key", d.qname)
		}
		k, v := d.encode(e, pk)
		if prev, dup := m[string(k)]; dup && !bytes.Equal(prev, v) {
			return nil, fmt.Errorf("bolttable: index %s: one row gives a key two projections", d.qname)
		}
		m[string(k)] = v
	}
	return m, nil
}

func escapeKey(k []byte) []byte {
	dst := make([]byte, 0, len(k)+2)
	for _, c := range k {
		if c == 0x00 {
			dst = append(dst, 0x00, 0xFF)
		} else {
			dst = append(dst, c)
		}
	}
	return dst
}

// splitNonUniqueKey returns the unescaped index key and the primary key.
func splitNonUniqueKey(sk []byte) (ikey, pk []byte, ok bool) {
	ikey = make([]byte, 0, len(sk))
	for i := 0; i < len(sk); i++ {
		if sk[i] != 0x00 {
			ikey = append(ikey, sk[i])
			continue
		}
		if i+1 == len(sk) {
			return nil, nil, false
		}
		switch sk[i+1] {
		case 0xFF:
			ikey = append(ikey, 0x00)
			i++
		case 0x01:
			return ikey, sk[i+2:], true
		default:
			return nil, nil, false
		}
	}
	return nil, nil, false
}

func splitUniqueValue(sv []byte) (pk, proj []byte, ok bool) {
	n, w := binary.Uvarint(sv)
	if w <= 0 {
		return nil, nil, false
	}
	rest := sv[w:]
	if n > uint64(len(rest)) {
		return nil, nil, false
	}
	return rest[:n], rest[n:], true
}

type indexOp struct {
	bucket []byte
	key    []byte
	val    []byte
	del    bool
}

func entriesAt(f rowFacts, i int) []Entry {
	if f.entries == nil {
		return nil
	}
	return f.entries[i]
}

// planIndexes computes the index writes that move a row's entries from prev
// to next, refusing a unique key another row holds. It writes nothing.
func planIndexes(ns *bbolt.Bucket, td *tableDef, pk []byte, prev, next rowFacts) ([]indexOp, error) {
	var ops []indexOp
	for i, d := range td.indexes {
		ib := ns.Bucket(d.bucket)
		if ib == nil {
			return nil, fmt.Errorf("%w: index %s has no bucket", ErrCorrupt, d.qname)
		}
		got, err := d.plan(ib, pk, entriesAt(prev, i), entriesAt(next, i))
		if err != nil {
			return nil, err
		}
		ops = append(ops, got...)
	}
	return ops, nil
}

func (d *indexDef) plan(ib *bbolt.Bucket, pk []byte, before, after []Entry) ([]indexOp, error) {
	oldPairs, err := d.pairs(before, pk)
	if err != nil {
		return nil, err
	}
	newPairs, err := d.pairs(after, pk)
	if err != nil {
		return nil, err
	}
	var ops []indexOp
	for _, k := range sortedKeys(oldPairs) {
		if _, keep := newPairs[k]; !keep {
			ops = append(ops, indexOp{bucket: d.bucket, key: []byte(k), del: true})
		}
	}
	for _, k := range sortedKeys(newPairs) {
		v := newPairs[k]
		if ov, had := oldPairs[k]; had && bytes.Equal(ov, v) {
			continue
		}
		if err := d.claim(ib, []byte(k), pk); err != nil {
			return nil, err
		}
		ops = append(ops, indexOp{bucket: d.bucket, key: []byte(k), val: v})
	}
	return ops, nil
}

// claim refuses a unique key another row holds.
func (d *indexDef) claim(ib *bbolt.Bucket, key, pk []byte) error {
	if !d.unique {
		return nil
	}
	cur := ib.Get(key)
	if cur == nil {
		return nil
	}
	owner, _, ok := splitUniqueValue(cur)
	if !ok {
		return corruptIndex(d.qname, key)
	}
	if !bytes.Equal(owner, pk) {
		return &ConflictError{Index: d.name}
	}
	return nil
}

func sortedKeys(m map[string][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// applyDerived writes planned index entries and counter totals.
func applyDerived(ns *bbolt.Bucket, ops []indexOp, totals []counterTotal) error {
	for _, op := range ops {
		ib := ns.Bucket(op.bucket)
		var err error
		if op.del {
			err = ib.Delete(op.key)
		} else {
			err = ib.Put(op.key, op.val)
		}
		if err != nil {
			return fmt.Errorf("bolttable: index write: %w", err)
		}
	}
	for _, c := range totals {
		if err := ns.Put(c.key, encodeTotal(c.total)); err != nil {
			return fmt.Errorf("bolttable: counter write: %w", err)
		}
	}
	return nil
}
