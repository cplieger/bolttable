package bolttable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// childEnv tells a re-executed test binary which side of a freelist test it
// runs: a panic in bbolt's page checker kills the process, so the assertion
// lives in the parent.
const childEnv = "BOLTTABLE_FREELIST_CHILD"

var leafMarker = []byte("bolttable-leaf-marker")

type rowsFixture struct {
	schema *Schema
	rows   *Table[uint64, []byte]
	big    *Table[uint64, []byte]
}

type rawCodec struct{}

func (rawCodec) Version() uint32 { return 1 }
func (rawCodec) Append(dst []byte, v *[]byte) ([]byte, error) {
	return append(dst, *v...), nil
}

func (rawCodec) Decode(b []byte, v *[]byte) error { *v = b; return nil }

func newRowsFixture() *rowsFixture {
	s := NewSchema("freelist")
	a := Must(s.Namespace("a"))
	return &rowsFixture{
		schema: s,
		rows:   Must(Define(a, 0x01, "rows", Uint64Key{}, Codec[[]byte](rawCodec{}), TableOptions{})),
		big:    Must(Define(a, 0x02, "big", Uint64Key{}, Codec[[]byte](rawCodec{}), TableOptions{})),
	}
}

// buildCorruptFile writes 2,000 rows in one transaction, so every page
// holding the marker is reachable, then overwrites the header of the first
// such leaf page with garbage.
func buildCorruptFile(t *testing.T) string {
	t.Helper()
	path := tempPath(t)
	f := newRowsFixture()
	db, err := Open(path, f.schema, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(t.Context(), func(tx *Tx) error {
		for i := range uint64(2000) {
			v := append(bytes.Repeat([]byte{'v'}, 80), leafMarker...)
			if err := f.rows.Put(tx, i, &v); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	pageSize := db.bolt.Info().PageSize
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	data := readFile(t, path)
	const leafFlag = 0x02
	for off := 2 * pageSize; off+pageSize <= len(data); off += pageSize {
		page := data[off : off+pageSize]
		if binary.LittleEndian.Uint16(page[8:10]) == leafFlag && bytes.Contains(page, leafMarker) {
			copy(page[8:16], bytes.Repeat([]byte{0xFF}, 8))
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			return path
		}
	}
	t.Fatal("no leaf page holds the marker")
	return ""
}

// runChild re-executes this test binary as the named child and returns its
// combined output and error.
func runChild(t *testing.T, test, mode, path string) (string, error) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+test+"$", "-test.count=1")
	cmd.Env = append(os.Environ(), childEnv+"="+mode+":"+path)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// childMode runs the child side when this process is one, and reports
// whether it was.
func childMode(t *testing.T, want string, fn func(path string) error) bool {
	t.Helper()
	mode, path, ok := strings.Cut(os.Getenv(childEnv), ":")
	if !ok || mode != want {
		return false
	}
	if err := fn(path); err != nil {
		fmt.Fprintln(os.Stderr, "child:", err)
		os.Exit(3)
	}
	os.Exit(0)
	return true
}

func TestOpenDoesNotWalkFile(t *testing.T) {
	if childMode(t, "open", func(path string) error {
		db, err := Open(path, newRowsFixture().schema, Options{})
		if err != nil {
			return err
		}
		return db.Close()
	}) {
		return
	}
	path := buildCorruptFile(t)
	if out, err := runChild(t, "TestOpenDoesNotWalkFile", "open", path); err != nil {
		t.Fatalf("Open over a corrupt leaf: child %v\n%s", err, out)
	}
}

func TestFailedCommitDoesNotWalkFile(t *testing.T) {
	if childMode(t, "commit", func(path string) error {
		f := newRowsFixture()
		db, err := Open(path, f.schema, Options{})
		if err != nil {
			return err
		}
		if err := db.Update(t.Context(), func(tx *Tx) error {
			v := []byte("small")
			return f.big.Put(tx, 2, &v)
		}); err != nil {
			return err
		}
		// bbolt grows the file in 16 MiB steps
		// (https://github.com/etcd-io/bbolt/blob/v1.5.0/internal/common/types.go#L30), so only a MaxSize
		// below it makes a write's fresh page allocation fail, which takes
		// bbolt's rollback path.
		db.bolt.MaxSize = 1
		err = db.Update(t.Context(), func(tx *Tx) error {
			v := bytes.Repeat([]byte{'b'}, 4<<20)
			return f.big.Put(tx, 1, &v)
		})
		if !errors.Is(err, ErrCommit) {
			return fmt.Errorf("Update = %w, want ErrCommit", err)
		}
		return db.Close()
	}) {
		return
	}
	path := buildCorruptFile(t)
	if out, err := runChild(t, "TestFailedCommitDoesNotWalkFile", "commit", path); err != nil {
		t.Fatalf("failed commit over a corrupt leaf: child %v\n%s", err, out)
	}
}
