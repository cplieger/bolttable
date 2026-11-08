package bolttable

import (
	"bytes"
	"errors"
	"fmt"

	"go.etcd.io/bbolt"
)

type cascadeDef struct {
	child *tableDef
	via   *indexDef
	// prefix maps the deleted parent's stored key to the child-key or
	// index-key prefix to delete.
	prefix  func(pkb []byte) ([]byte, error)
	version uint32
}

// CascadePrefix makes deleting a parent row delete, in the same transaction,
// every child row whose encoded key starts with prefix(parent key), and
// recursively their own cascades. Parent and child share one namespace.
func CascadePrefix[PK, PV, CK, CV any](parent *Table[PK, PV], child *Table[CK, CV], version uint32, prefix func(PK) []byte) error {
	if parent == nil || child == nil || prefix == nil {
		return errors.New("bolttable: cascade needs a parent, a child and a prefix function")
	}
	return addCascade(parent, child.def, nil, version, prefix)
}

// CascadeIndex makes deleting a parent row delete, in the same transaction,
// every row of via's table holding an entry of via whose index key starts
// with prefix(parent key), and recursively their own cascades. Parent and
// index share one namespace.
func CascadeIndex[PK, PV, CK, CV any](parent *Table[PK, PV], via *Index[CK, CV], version uint32, prefix func(PK) []byte) error {
	if parent == nil || via == nil || prefix == nil {
		return errors.New("bolttable: cascade needs a parent, an index and a prefix function")
	}
	return addCascade(parent, via.def.table, via.def, version, prefix)
}

func addCascade[PK, PV any](parent *Table[PK, PV], child *tableDef, via *indexDef, version uint32, prefix func(PK) []byte) error {
	ns := parent.def.ns
	what := "cascade from " + parent.def.qname + " to " + child.qname
	return ns.schema.declare(what, func() error {
		if child.ns != ns {
			return fmt.Errorf("bolttable: %s crosses namespaces", what)
		}
		if version == 0 {
			return fmt.Errorf("bolttable: %s has a zero version", what)
		}
		parent.def.cascades = append(parent.def.cascades, &cascadeDef{
			child: child, via: via, version: version,
			prefix: func(pkb []byte) ([]byte, error) {
				pk, err := parent.key.ParseKey(bytes.Clone(pkb))
				if err != nil {
					return nil, corruptRow(parent.def.qname, pkb)
				}
				return prefix(pk), nil
			},
		})
		return nil
	})
}

// cascade deletes the child rows c selects for the deleted parent key pkb.
func (tx *Tx) cascade(ns *bbolt.Bucket, c *cascadeDef, pkb []byte) error {
	p, err := c.prefix(pkb)
	if err != nil {
		return err
	}
	keys, err := cascadeKeys(ns, c, p)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if _, err := tx.deleteRow(c.child, k); err != nil {
			return err
		}
	}
	return nil
}

// cascadeKeys collects, before any deletion moves the cursor, the distinct
// child keys under prefix p.
func cascadeKeys(ns *bbolt.Bucket, c *cascadeDef, p []byte) ([][]byte, error) {
	name, seek := []byte{c.child.prefix}, p
	if c.via != nil {
		name = c.via.bucket
		if !c.via.unique {
			seek = escapeKey(p)
		}
	}
	b := ns.Bucket(name)
	if b == nil {
		return nil, fmt.Errorf("%w: cascade target of %s has no bucket", ErrCorrupt, c.child.qname)
	}
	var keys [][]byte
	seen := map[string]struct{}{}
	cur := b.Cursor()
	for k, v := cur.Seek(seek); k != nil && bytes.HasPrefix(k, seek); k, v = cur.Next() {
		pk := k
		if c.via != nil {
			var ok bool
			if _, pk, _, ok = c.via.split(k, v); !ok {
				return nil, corruptIndex(c.via.qname, k)
			}
		}
		if _, dup := seen[string(pk)]; !dup {
			seen[string(pk)] = struct{}{}
			keys = append(keys, bytes.Clone(pk))
		}
	}
	return keys, nil
}
