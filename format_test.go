package bolttable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"strings"
	"testing"

	"go.etcd.io/bbolt"
)

// knobs varies each declaration that enters namespace a's digest.
type knobs struct {
	tableName  string
	idxName    string
	ctrName    string
	keyVer     uint32
	valVer     uint32
	idxVer     uint32
	ctrVer     uint32
	cascadeVer uint32
	prefix     byte
	silent     bool
	unique     bool
	cascadeIdx bool
}

func baseKnobs() knobs {
	return knobs{
		tableName: "items", idxName: "by_name", ctrName: "total",
		keyVer: 1, valVer: 1, idxVer: 1, ctrVer: 1, cascadeVer: 1, prefix: 0x10,
	}
}

func digestSchema(k knobs) *Schema {
	s := NewSchema("digest")
	a := Must(s.Namespace("a"))
	b := Must(s.Namespace("b"))
	items := Must(Define(a, k.prefix, k.tableName, verKey{Uint64Key{}, k.keyVer}, JSON[item](k.valVer), TableOptions{Silent: k.silent}))
	Must(DefineIndex(items, k.idxName, IndexOptions{Unique: k.unique, Version: k.idxVer}, func(uint64, *item) []Entry { return nil }))
	Must(DefineCounter(items, k.ctrName, k.ctrVer, func(uint64, *item) int64 { return 0 }))
	kids := Must(Define(a, 0x30, "kids", Uint64Key{}, JSON[item](1), TableOptions{}))
	owner := Must(DefineIndex(kids, "owner", IndexOptions{Version: 1}, func(uint64, *item) []Entry { return nil }))
	if k.cascadeIdx {
		if err := CascadeIndex(items, owner, k.cascadeVer, func(uint64) []byte { return nil }); err != nil {
			panic(err)
		}
	} else if err := CascadePrefix(items, kids, k.cascadeVer, func(uint64) []byte { return nil }); err != nil {
		panic(err)
	}
	Must(Define(b, 0x20, "other", Uint64Key{}, JSON[item](1), TableOptions{}))
	return s
}

type verKey struct {
	Uint64Key
	v uint32
}

func (k verKey) Version() uint32 { return k.v }

func TestDigestPerDeclarationKind(t *testing.T) {
	base := digestSchema(baseKnobs()).digests()
	if base["a"] == base["b"] {
		t.Fatal("namespaces a and b share a digest")
	}
	if again := digestSchema(baseKnobs()).digests(); again["a"] != base["a"] {
		t.Fatal("the digest is not deterministic")
	}
	cases := map[string]func(*knobs){
		"key codec version":   func(k *knobs) { k.keyVer = 2 },
		"value codec version": func(k *knobs) { k.valVer = 2 },
		"silence":             func(k *knobs) { k.silent = true },
		"table prefix":        func(k *knobs) { k.prefix = 0x11 },
		"table name":          func(k *knobs) { k.tableName = "things" },
		"index name":          func(k *knobs) { k.idxName = "by_title" },
		"index uniqueness":    func(k *knobs) { k.unique = true },
		"index version":       func(k *knobs) { k.idxVer = 2 },
		"counter name":        func(k *knobs) { k.ctrName = "sum" },
		"counter version":     func(k *knobs) { k.ctrVer = 2 },
		"cascade version":     func(k *knobs) { k.cascadeVer = 2 },
		"cascade via index":   func(k *knobs) { k.cascadeIdx = true },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			k := baseKnobs()
			change(&k)
			got := digestSchema(k).digests()
			if got["a"] == base["a"] {
				t.Error("namespace a's digest did not change")
			}
			if got["b"] != base["b"] {
				t.Error("namespace b's digest changed")
			}
		})
	}
}

func TestOpenNamesTheChangedNamespace(t *testing.T) {
	path := tempPath(t)
	db, err := Open(path, digestSchema(baseKnobs()), Options{})
	if err != nil {
		t.Fatal(err)
	}
	stored := db.schema.digests()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	k := baseKnobs()
	k.idxVer = 2
	changed := digestSchema(k)
	_, err = Open(path, changed, Options{})
	var fe *FormatError
	if !errors.As(err, &fe) || !errors.Is(err, ErrFormat) {
		t.Fatalf("Open = %v, want a *FormatError", err)
	}
	want := []NamespaceDigest{{Name: "a", Stored: stored["a"], Want: changed.digests()["a"]}}
	if !slices.Equal(fe.Namespaces, want) {
		t.Errorf("Namespaces = %+v, want %+v", fe.Namespaces, want)
	}
}

// layoutTo rewrites the stored layout number of a closed file.
func layoutTo(t *testing.T, path string, n uint32) {
	t.Helper()
	rawUpdate(t, path, func(btx *bbolt.Tx) error {
		return btx.Bucket([]byte(libBucket)).Put(keyLayout, binary.BigEndian.AppendUint32(nil, n))
	})
}

func closedFile(t *testing.T, s *Schema) string {
	t.Helper()
	path := tempPath(t)
	db, err := Open(path, s, Options{Retention: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestOpenRefusesOtherLayoutNumber(t *testing.T) {
	f := newFixture(t)
	path := closedFile(t, f.schema)
	layoutTo(t, path, 2)
	before := readFile(t, path)
	_, err := Open(path, newFixture(t).schema, Options{})
	var fe *FormatError
	if !errors.As(err, &fe) {
		t.Fatalf("Open = %v, want a *FormatError", err)
	}
	want := f.schema.digests()
	got := map[string]NamespaceDigest{}
	for _, n := range fe.Namespaces {
		got[n.Name] = n
	}
	if len(got) != 3 || got["a"].Want != want["a"] || got["b"].Want != want["b"] || got[libBucket].Want != ([32]byte{}) {
		t.Errorf("Namespaces = %+v, want a, b and bolttable", fe.Namespaces)
	}
	for _, n := range fe.Namespaces {
		if n.Stored != ([32]byte{}) {
			t.Errorf("%s: a foreign layout's digest was trusted", n.Name)
		}
	}
	if !bytes.Equal(before, readFile(t, path)) {
		t.Error("a refused Open changed the file")
	}
}

func TestLibraryBucketIsNotANamespace(t *testing.T) {
	path := closedFile(t, newFixture(t).schema)
	mustOpen(t, path, newFixture(t).schema, Options{})
	if _, err := NewSchema("s").Namespace(libBucket); err == nil {
		t.Error("a schema declared the namespace bolttable")
	}
}

func TestForeignLayout(t *testing.T) {
	path := tempPath(t)
	rawUpdate(t, path, func(btx *bbolt.Tx) error {
		for _, name := range []string{"a", "legacy"} {
			if _, err := btx.CreateBucket([]byte(name)); err != nil {
				return err
			}
		}
		return nil
	})
	before := readFile(t, path)
	f := newFixture(t)
	_, err := Open(path, f.schema, Options{})
	var fe *FormatError
	if !errors.As(err, &fe) {
		t.Fatalf("Open = %v, want a *FormatError", err)
	}
	want := []NamespaceDigest{{Name: "a", Want: f.schema.digests()["a"]}, {Name: "legacy"}}
	if !slices.Equal(fe.Namespaces, want) {
		t.Errorf("Namespaces = %+v, want %+v", fe.Namespaces, want)
	}
	if !bytes.Equal(before, readFile(t, path)) {
		t.Error("a refused Open changed the file")
	}
}

func TestUndigestedBucket(t *testing.T) {
	path := closedFile(t, newFixture(t).schema)
	rawUpdate(t, path, func(btx *bbolt.Tx) error {
		_, err := btx.CreateBucket([]byte("legacy"))
		return err
	})
	_, err := Open(path, newFixture(t).schema, Options{})
	var fe *FormatError
	if !errors.As(err, &fe) {
		t.Fatalf("Open = %v, want a *FormatError", err)
	}
	if want := []NamespaceDigest{{Name: "legacy"}}; !slices.Equal(fe.Namespaces, want) {
		t.Errorf("Namespaces = %+v, want %+v", fe.Namespaces, want)
	}
}

func TestUndeclaredNamespace(t *testing.T) {
	f := newFixture(t)
	path := closedFile(t, f.schema)
	s := NewSchema("test")
	Must(s.Namespace("a"))
	_, err := Open(path, s, Options{})
	var fe *FormatError
	if !errors.As(err, &fe) {
		t.Fatalf("Open = %v, want a *FormatError", err)
	}
	names := []string{}
	for _, n := range fe.Namespaces {
		names = append(names, n.Name)
	}
	if !slices.Equal(names, []string{"a", "b"}) {
		t.Fatalf("names = %v, want a (differing) and b (undeclared)", names)
	}
	if b := fe.Namespaces[1]; b.Want != ([32]byte{}) || b.Stored != f.schema.digests()["b"] {
		t.Errorf("b = %+v, want its stored digest and a zero Want", b)
	}
}

func TestOpenErrorWritesNothing(t *testing.T) {
	path := closedFile(t, newFixture(t).schema)
	before := readFile(t, path)
	s := NewSchema("test")
	Must(Define(Must(s.Namespace("a")), 0x10, "items", Uint64Key{}, JSON[item](9), TableOptions{}))
	Must(Define(Must(s.Namespace("c")), 0x40, "new", Uint64Key{}, JSON[item](1), TableOptions{}))
	if _, err := Open(path, s, Options{}); !errors.Is(err, ErrFormat) {
		t.Fatalf("Open = %v, want ErrFormat", err)
	}
	if !bytes.Equal(before, readFile(t, path)) {
		t.Error("Open created a namespace while another one mismatched")
	}
}

func TestOpenCreatesMissingNamespace(t *testing.T) {
	path := closedFile(t, newFixture(t).schema)
	f := newFixture(t)
	c := Must(Define(Must(f.schema.Namespace("c")), 0x40, "new", Uint64Key{}, JSON[item](1), TableOptions{}))
	db := mustOpen(t, path, f.schema, Options{})
	update(t, db, func(tx *Tx) error { return c.Put(tx, 1, &item{Name: "x"}) })
}

func TestFormatErrorMessage(t *testing.T) {
	err := &FormatError{Namespaces: []NamespaceDigest{{Name: "a", Stored: [32]byte{0xAB, 0xCD, 0xEF, 0x01}}}}
	if got := err.Error(); !strings.Contains(got, "a (stored abcdef01, want none)") {
		t.Errorf("Error() = %q", got)
	}
}
