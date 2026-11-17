package bolttable

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestDeclarationErrors(t *testing.T) {
	type decl func(s *Schema) error
	table := func(ns *Namespace, prefix byte, name string) (*Table[uint64, item], error) {
		return Define(ns, prefix, name, Uint64Key{}, JSON[item](1), TableOptions{})
	}
	noEntries := func(uint64, *item) []Entry { return nil }
	noWeight := func(uint64, *item) int64 { return 0 }
	noPrefix := func(uint64) []byte { return nil }
	cases := []struct {
		name string
		want string
		run  decl
	}{
		{"duplicate namespace", "duplicate namespace", func(s *Schema) error {
			Must(s.Namespace("a"))
			_, err := s.Namespace("a")
			return err
		}},
		{"empty namespace name", "empty namespace", func(s *Schema) error {
			_, err := s.Namespace("")
			return err
		}},
		{"library bucket name", "library's own bucket", func(s *Schema) error {
			_, err := s.Namespace("bolttable")
			return err
		}},
		{"synthetic prefix", "reserved for synthetic subjects", func(s *Schema) error {
			_, err := table(Must(s.Namespace("a")), SyntheticPrefix, "x")
			return err
		}},
		{"duplicate prefix across namespaces", "reuses prefix 0x10", func(s *Schema) error {
			Must(table(Must(s.Namespace("a")), 0x10, "x"))
			_, err := table(Must(s.Namespace("b")), 0x10, "y")
			return err
		}},
		{"duplicate table name", `duplicate name "x"`, func(s *Schema) error {
			ns := Must(s.Namespace("a"))
			Must(table(ns, 0x10, "x"))
			_, err := table(ns, 0x11, "x")
			return err
		}},
		{"index name taken by a table", `duplicate name "x"`, func(s *Schema) error {
			ns := Must(s.Namespace("a"))
			tb := Must(table(ns, 0x10, "x"))
			_, err := DefineIndex(tb, "x", IndexOptions{Version: 1}, noEntries)
			return err
		}},
		{"counter name taken by an index", `duplicate name "i"`, func(s *Schema) error {
			tb := Must(table(Must(s.Namespace("a")), 0x10, "x"))
			Must(DefineIndex(tb, "i", IndexOptions{Version: 1}, noEntries))
			_, err := DefineCounter(tb, "i", 1, noWeight)
			return err
		}},
		{"empty table name", "empty name", func(s *Schema) error {
			_, err := table(Must(s.Namespace("a")), 0x10, "")
			return err
		}},
		{"zero key codec version", "zero codec version", func(s *Schema) error {
			_, err := Define(Must(s.Namespace("a")), 0x10, "x", zeroKey{}, JSON[item](1), TableOptions{})
			return err
		}},
		{"zero value codec version", "zero codec version", func(s *Schema) error {
			_, err := Define(Must(s.Namespace("a")), 0x10, "x", Uint64Key{}, JSON[item](0), TableOptions{})
			return err
		}},
		{"nil codec", "needs a key and a value codec", func(s *Schema) error {
			_, err := Define[uint64, item](Must(s.Namespace("a")), 0x10, "x", nil, JSON[item](1), TableOptions{})
			return err
		}},
		{"nil namespace", "nil namespace", func(*Schema) error {
			_, err := table(nil, 0x10, "x")
			return err
		}},
		{"zero index version", "zero version", func(s *Schema) error {
			tb := Must(table(Must(s.Namespace("a")), 0x10, "x"))
			_, err := DefineIndex(tb, "i", IndexOptions{}, noEntries)
			return err
		}},
		{"nil index function", "entries function", func(s *Schema) error {
			tb := Must(table(Must(s.Namespace("a")), 0x10, "x"))
			_, err := DefineIndex(tb, "i", IndexOptions{Version: 1}, nil)
			return err
		}},
		{"zero counter version", "zero version", func(s *Schema) error {
			tb := Must(table(Must(s.Namespace("a")), 0x10, "x"))
			_, err := DefineCounter(tb, "c", 0, noWeight)
			return err
		}},
		{"zero cascade version", "zero version", func(s *Schema) error {
			ns := Must(s.Namespace("a"))
			return CascadePrefix(Must(table(ns, 0x10, "x")), Must(table(ns, 0x11, "y")), 0, noPrefix)
		}},
		{"cross-namespace cascade by prefix", "crosses namespaces", func(s *Schema) error {
			p := Must(table(Must(s.Namespace("a")), 0x10, "x"))
			c := Must(table(Must(s.Namespace("b")), 0x11, "y"))
			return CascadePrefix(p, c, 1, noPrefix)
		}},
		{"cross-namespace cascade by index", "crosses namespaces", func(s *Schema) error {
			p := Must(table(Must(s.Namespace("a")), 0x10, "x"))
			c := Must(table(Must(s.Namespace("b")), 0x11, "y"))
			return CascadeIndex(p, Must(DefineIndex(c, "i", IndexOptions{Version: 1}, noEntries)), 1, noPrefix)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run(NewSchema("s"))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want one containing %q", err, tc.want)
			}
		})
	}
}

func TestDeclarationsAfterOpenFail(t *testing.T) {
	f := newFixture(t)
	mustOpen(t, tempPath(t), f.schema, Options{})
	ns := f.items.def.ns
	cases := map[string]func() error{
		"namespace": func() error { _, err := f.schema.Namespace("late"); return err },
		"table": func() error {
			_, err := Define(ns, 0x30, "late", Uint64Key{}, JSON[item](1), TableOptions{})
			return err
		},
		"index": func() error {
			_, err := DefineIndex(f.items, "late", IndexOptions{Version: 1}, func(uint64, *item) []Entry { return nil })
			return err
		},
		"counter": func() error {
			_, err := DefineCounter(f.items, "late", 1, func(uint64, *item) int64 { return 0 })
			return err
		},
		"cascade": func() error { return CascadePrefix(f.items, f.quiet, 1, func(uint64) []byte { return nil }) },
	}
	for name, run := range cases {
		t.Run(name, func(t *testing.T) {
			if err := run(); err == nil || !strings.Contains(err.Error(), "after Open") {
				t.Errorf("err = %v, want a declared-after-Open error", err)
			}
		})
	}
	if got := len(f.items.def.indexes); got != 2 {
		t.Errorf("refused declarations changed the table: %d indexes, want 2", got)
	}
}

func TestMustPanicsWithErr(t *testing.T) {
	want := errors.New("boom")
	defer func() {
		if got := recover(); got != want {
			t.Errorf("panic value = %v, want %v", got, want)
		}
	}()
	Must(0, want)
	t.Error("Must returned on an error")
}

func TestMustReturnsValue(t *testing.T) {
	if got := Must(7, nil); got != 7 {
		t.Errorf("Must = %d, want 7", got)
	}
}

type zeroKey struct{ Uint64Key }

func (zeroKey) Version() uint32 { return 0 }

func TestTablePrefix(t *testing.T) {
	f := newFixture(t)
	cases := []struct {
		name  string
		table *Table[uint64, item]
		want  byte
	}{
		{"items", f.items, 0x10},
		{"silent table", f.quiet, 0x11},
		{"other namespace", f.other, 0x20},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var p interface{ Prefix() byte } = c.table
			if got := p.Prefix(); got != c.want {
				t.Errorf("Prefix = 0x%02x, want 0x%02x", got, c.want)
			}
			if got := c.table.Subject(7)[0]; got != c.want {
				t.Errorf("Subject's first byte = 0x%02x, want the prefix 0x%02x", got, c.want)
			}
		})
	}
}

func TestSchemaPrefixesListsRecordedTables(t *testing.T) {
	define := func(ns *Namespace, prefix byte, silent bool) {
		Must(Define(ns, prefix, fmt.Sprintf("t%02x", prefix), Uint64Key{}, JSON[item](1), TableOptions{Silent: silent}))
	}
	t.Run("fixture", func(t *testing.T) {
		if got, want := newFixture(t).schema.Prefixes(), []byte{0x10, 0x20}; !bytes.Equal(got, want) {
			t.Errorf("Prefixes = %x, want %x", got, want)
		}
	})
	t.Run("declared out of order", func(t *testing.T) {
		s := NewSchema("order")
		b := Must(s.Namespace("b"))
		a := Must(s.Namespace("a"))
		define(b, 0x40, false)
		define(a, 0x05, false)
		define(b, 0x07, true)
		define(a, 0x30, false)
		if got, want := s.Prefixes(), []byte{0x05, 0x30, 0x40}; !bytes.Equal(got, want) {
			t.Errorf("Prefixes = %x, want %x", got, want)
		}
	})
	t.Run("only silent tables", func(t *testing.T) {
		s := NewSchema("silent")
		define(Must(s.Namespace("a")), 0x01, true)
		if got := s.Prefixes(); len(got) != 0 {
			t.Errorf("Prefixes = %x, want none", got)
		}
	})
	t.Run("after Open", func(t *testing.T) {
		f := newFixture(t)
		mustOpen(t, tempPath(t), f.schema, Options{})
		if got, want := f.schema.Prefixes(), []byte{0x10, 0x20}; !bytes.Equal(got, want) {
			t.Errorf("Prefixes = %x, want %x", got, want)
		}
	})
	t.Run("caller owns the result", func(t *testing.T) {
		f := newFixture(t)
		got := f.schema.Prefixes()
		if len(got) == 0 {
			t.Fatal("Prefixes = none, want the fixture's two")
		}
		got[0] = 0x99
		if again, want := f.schema.Prefixes(), []byte{0x10, 0x20}; !bytes.Equal(again, want) {
			t.Errorf("Prefixes after a caller's write = %x, want %x", again, want)
		}
	})
}
