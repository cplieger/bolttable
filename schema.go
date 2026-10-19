package bolttable

import (
	"errors"
	"fmt"
	"sync"
)

// libBucket is the library's own top-level bucket: the layout number, the
// revision head and the change log. No namespace may take its name.
const libBucket = "bolttable"

// Schema is the set of namespaces a file holds. Declarations build it before
// the first Open or ResetNamespaces; either seals it, and a declaration
// after that fails.
type Schema struct {
	nsByName   map[string]*Namespace
	prefixes   map[byte]string
	name       string
	namespaces []*Namespace
	mu         sync.Mutex
	sealed     bool
}

// NewSchema returns an empty schema. Its name enters every namespace's
// format digest.
func NewSchema(name string) *Schema {
	return &Schema{name: name, nsByName: map[string]*Namespace{}, prefixes: map[byte]string{}}
}

// Namespace is one top-level bucket holding its tables, with its own format
// digest, so one namespace's format change never resets another's rows.
// Prefixes are unique across the schema, so a Subject stays prefix plus key.
type Namespace struct {
	schema *Schema
	names  map[string]struct{}
	name   string
	tables []*tableDef
}

// Namespace declares a namespace. It fails for an empty or duplicate name,
// the library's own bucket name "bolttable", and after Open.
func (s *Schema) Namespace(name string) (*Namespace, error) {
	if s == nil {
		return nil, errors.New("bolttable: namespace on a nil schema")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.sealed:
		return nil, fmt.Errorf("bolttable: namespace %q declared after Open", name)
	case name == "":
		return nil, errors.New("bolttable: empty namespace name")
	case name == libBucket:
		return nil, fmt.Errorf("bolttable: namespace name %q is the library's own bucket", name)
	case s.nsByName[name] != nil:
		return nil, fmt.Errorf("bolttable: duplicate namespace %q", name)
	}
	ns := &Namespace{schema: s, name: name, names: map[string]struct{}{}}
	s.namespaces = append(s.namespaces, ns)
	s.nsByName[name] = ns
	return ns, nil
}

// Must returns v, panicking with err when err is not nil; for package-level
// declarations.
func Must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// seal ends declaration; every later declaration fails.
func (s *Schema) seal() {
	s.mu.Lock()
	s.sealed = true
	s.mu.Unlock()
}

// declare runs fn under the schema lock, refusing it after Open.
func (s *Schema) declare(what string, fn func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed {
		return fmt.Errorf("bolttable: %s declared after Open", what)
	}
	return fn()
}

// claimName reserves a table, index or counter name in ns; the three share
// one set because a Mismatch names any of them as namespace.name.
func (ns *Namespace) claimName(name string) error {
	if name == "" {
		return fmt.Errorf("bolttable: empty name in namespace %q", ns.name)
	}
	if _, dup := ns.names[name]; dup {
		return fmt.Errorf("bolttable: duplicate name %q in namespace %q", name, ns.name)
	}
	ns.names[name] = struct{}{}
	return nil
}

func (ns *Namespace) qualify(name string) string { return ns.name + "." + name }
