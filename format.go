package bolttable

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"slices"

	"go.etcd.io/bbolt"
)

// layoutNumber is the library's own on-disk layout: the library bucket's
// keys, a namespace bucket's keys and the index and change encodings. A
// change to any of them bumps it, which makes every older file a foreign
// layout that ResetNamespaces must clear.
const layoutNumber uint32 = 1

var (
	keyLayout  = []byte("layout")
	keyHead    = []byte("head")
	keyChanges = []byte("changes")
	keyDigest  = []byte("digest")
)

// digests returns every declared namespace's format digest by name.
func (s *Schema) digests() map[string][32]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := make(map[string][32]byte, len(s.namespaces))
	for _, ns := range s.namespaces {
		m[ns.name] = s.digest(ns)
	}
	return m
}

// digest is SHA-256 over the schema and namespace names and every
// declaration that shapes stored bytes, in prefix order, each field length-
// or width-delimited so no two declaration lists hash alike.
func (s *Schema) digest(ns *Namespace) [32]byte {
	w := digestWriter{h: sha256.New()}
	w.str("bolttable namespace v1")
	w.str(s.name)
	w.str(ns.name)
	for _, t := range slices.SortedFunc(slices.Values(ns.tables), func(a, b *tableDef) int { return cmp.Compare(a.prefix, b.prefix) }) {
		w.octet('T')
		w.octet(t.prefix)
		w.str(t.name)
		w.u32(t.keyVer)
		w.u32(t.valVer)
		w.flag(t.silent)
		for _, d := range slices.SortedFunc(slices.Values(t.indexes), func(a, b *indexDef) int { return cmp.Compare(a.name, b.name) }) {
			w.octet('I')
			w.str(d.name)
			w.flag(d.unique)
			w.u32(d.version)
		}
		for _, d := range slices.SortedFunc(slices.Values(t.counters), func(a, b *counterDef) int { return cmp.Compare(a.name, b.name) }) {
			w.octet('C')
			w.str(d.name)
			w.u32(d.version)
		}
		for _, c := range sortedCascades(t.cascades) {
			w.octet('X')
			w.octet(c.child.prefix)
			w.str(c.viaName())
			w.u32(c.version)
		}
	}
	var sum [32]byte
	w.h.Sum(sum[:0])
	return sum
}

func (c *cascadeDef) viaName() string {
	if c.via == nil {
		return ""
	}
	return c.via.name
}

func sortedCascades(cs []*cascadeDef) []*cascadeDef {
	return slices.SortedFunc(slices.Values(cs), func(a, b *cascadeDef) int {
		return cmp.Or(cmp.Compare(a.child.prefix, b.child.prefix), cmp.Compare(a.viaName(), b.viaName()), cmp.Compare(a.version, b.version))
	})
}

type digestWriter struct{ h hash.Hash }

// The hash.Hash Write contract never returns an error, so these ignore it.
func (w digestWriter) octet(c byte) { _, _ = w.h.Write([]byte{c}) }

func (w digestWriter) u32(v uint32) {
	_, _ = w.h.Write(binary.BigEndian.AppendUint32(nil, v))
}

func (w digestWriter) str(s string) {
	_, _ = w.h.Write(binary.AppendUvarint(nil, uint64(len(s))))
	_, _ = w.h.Write([]byte(s))
}

func (w digestWriter) flag(b bool) {
	if b {
		w.octet(1)
	} else {
		w.octet(0)
	}
}

// openPlan is what Open must create in a file whose format matches.
type openPlan struct {
	ensure []*Namespace
	lib    bool
}

// inspect compares a file's top-level buckets with the schema. A mismatch is
// a *FormatError; otherwise the plan names what is missing. It writes nothing.
func inspect(btx *bbolt.Tx, s *Schema, want map[string][32]byte) (openPlan, *FormatError, error) {
	var names []string
	if err := btx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
		names = append(names, string(name))
		return nil
	}); err != nil {
		return openPlan{}, nil, err
	}
	lib := btx.Bucket([]byte(libBucket))
	if (lib == nil && len(names) > 0) || (lib != nil && !layoutMatches(lib)) {
		return openPlan{}, foreignLayout(names, want), nil
	}
	if fe := digestMismatches(btx, names, want); fe != nil {
		return openPlan{}, fe, nil
	}
	plan, err := missing(btx, s, lib)
	return plan, nil, err
}

func digestMismatches(btx *bbolt.Tx, names []string, want map[string][32]byte) *FormatError {
	var fe FormatError
	for _, name := range names {
		if name == libBucket {
			continue
		}
		stored := storedDigest(btx.Bucket([]byte(name)))
		if w, declared := want[name]; !declared || stored != w {
			fe.Namespaces = append(fe.Namespaces, NamespaceDigest{Name: name, Stored: stored, Want: w})
		}
	}
	if len(fe.Namespaces) == 0 {
		return nil
	}
	return &fe
}

// missing plans what a matching file lacks: the library bucket or its log,
// and any declared namespace, table or index bucket.
func missing(btx *bbolt.Tx, s *Schema, lib *bbolt.Bucket) (openPlan, error) {
	var plan openPlan
	if lib == nil || lib.Bucket(keyChanges) == nil {
		plan.lib = true
	} else if _, err := readHead(lib); err != nil {
		return openPlan{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ns := range s.namespaces {
		if !namespaceComplete(btx.Bucket([]byte(ns.name)), ns) {
			plan.ensure = append(plan.ensure, ns)
		}
	}
	return plan, nil
}

// foreignLayout lists every top-level bucket of a file that is not in this
// library's layout: none of its digests can be trusted.
func foreignLayout(names []string, want map[string][32]byte) *FormatError {
	fe := &FormatError{}
	for _, name := range names {
		fe.Namespaces = append(fe.Namespaces, NamespaceDigest{Name: name, Want: want[name]})
	}
	return fe
}

func layoutMatches(lib *bbolt.Bucket) bool {
	raw := lib.Get(keyLayout)
	return len(raw) == 4 && binary.BigEndian.Uint32(raw) == layoutNumber
}

func storedDigest(nsb *bbolt.Bucket) [32]byte {
	var d [32]byte
	if raw := nsb.Get(keyDigest); len(raw) == len(d) {
		copy(d[:], raw)
	}
	return d
}

func namespaceComplete(nsb *bbolt.Bucket, ns *Namespace) bool {
	if nsb == nil {
		return false
	}
	for _, t := range ns.tables {
		if nsb.Bucket([]byte{t.prefix}) == nil {
			return false
		}
		for _, d := range t.indexes {
			if nsb.Bucket(d.bucket) == nil {
				return false
			}
		}
	}
	return true
}

// ensureNamespace creates or completes ns's bucket under digest d, starting
// every table's sequence at least at seed.
func ensureNamespace(btx *bbolt.Tx, ns *Namespace, d [32]byte, seed uint64) error {
	nsb, err := btx.CreateBucketIfNotExists([]byte(ns.name))
	if err != nil {
		return fmt.Errorf("bolttable: create namespace %s: %w", ns.name, err)
	}
	if err := nsb.Put(keyDigest, d[:]); err != nil {
		return fmt.Errorf("bolttable: namespace %s digest: %w", ns.name, err)
	}
	for _, t := range ns.tables {
		tb, err := nsb.CreateBucketIfNotExists([]byte{t.prefix})
		if err != nil {
			return fmt.Errorf("bolttable: create table %s: %w", t.qname, err)
		}
		if seed > tb.Sequence() {
			if err := tb.SetSequence(seed); err != nil {
				return fmt.Errorf("bolttable: seed table %s: %w", t.qname, err)
			}
		}
		for _, d := range t.indexes {
			if _, err := nsb.CreateBucketIfNotExists(d.bucket); err != nil {
				return fmt.Errorf("bolttable: create index %s: %w", d.qname, err)
			}
		}
	}
	return nil
}

// ensureLib creates the library bucket at the current layout number, or
// completes one that lacks its change log.
func ensureLib(btx *bbolt.Tx) error {
	lib, err := btx.CreateBucketIfNotExists([]byte(libBucket))
	if err != nil {
		return fmt.Errorf("bolttable: create library bucket: %w", err)
	}
	if lib.Get(keyLayout) == nil {
		if err := lib.Put(keyLayout, binary.BigEndian.AppendUint32(nil, layoutNumber)); err != nil {
			return fmt.Errorf("bolttable: library layout: %w", err)
		}
	}
	if _, err := lib.CreateBucketIfNotExists(keyChanges); err != nil {
		return fmt.Errorf("bolttable: create change log: %w", err)
	}
	return nil
}

// apply creates what plan names.
func (plan openPlan) apply(btx *bbolt.Tx, want map[string][32]byte) error {
	if plan.lib {
		if err := ensureLib(btx); err != nil {
			return err
		}
	}
	for _, ns := range plan.ensure {
		if err := ensureNamespace(btx, ns, want[ns.name], 0); err != nil {
			return err
		}
	}
	return nil
}
