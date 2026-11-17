package bolttable

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"go.etcd.io/bbolt"
)

// childEnv tells a re-executed test binary which child it runs: a panic in
// bbolt's page checker kills the process and a file lock belongs to one, so
// those assertions live in the parent.
const childEnv = "BOLTTABLE_TEST_CHILD"

func childCommand(t *testing.T, test, mode, path string) *exec.Cmd {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+test+"$", "-test.count=1")
	// Under -race each child would sleep atexit_sleep_ms (1 s by default,
	// https://go.dev/doc/articles/race_detector#Options) before exiting. The
	// sanitizer flag parser applies options left to right, so the appended one
	// wins (https://github.com/llvm/llvm-project/blob/main/compiler-rt/lib/sanitizer_common/sanitizer_flag_parser.cpp).
	gorace := strings.TrimSpace(os.Getenv("GORACE") + " atexit_sleep_ms=0")
	cmd.Env = append(os.Environ(), childEnv+"="+mode+":"+path, "GORACE="+gorace)
	return cmd
}

func runChild(t *testing.T, test, mode, path string) (string, error) {
	t.Helper()
	out, err := childCommand(t, test, mode, path).CombinedOutput()
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

type item struct {
	Name  string   `json:"name"`
	Tags  []string `json:"tags,omitempty"`
	Owner uint64   `json:"owner,omitempty"`
	N     int64    `json:"n,omitempty"`
}

// fixture is a schema of one namespace "a" with a table of items by uint64
// key, a unique name index, a per-tag index, a weight counter, a silent
// table, and a silent table of jobs with a non-unique state index (the
// item's Name) and a weight counter, plus a namespace "b" with one table.
type fixture struct {
	schema   *Schema
	items    *Table[uint64, item]
	byName   *Index[uint64, item]
	byTag    *Index[uint64, item]
	total    *Counter[uint64, item]
	quiet    *Table[uint64, item]
	jobs     *Table[uint64, item]
	byState  *Index[uint64, item]
	jobCount *Counter[uint64, item]
	other    *Table[uint64, item]
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	return fixtureAt(t, 1)
}

// fixtureAt is the fixture with the items table's value codec at version v,
// which changes only namespace a's digest.
func fixtureAt(t *testing.T, v uint32) *fixture {
	t.Helper()
	s := NewSchema("test")
	a := Must(s.Namespace("a"))
	b := Must(s.Namespace("b"))
	f := &fixture{schema: s}
	f.items = Must(Define(a, 0x10, "items", Uint64Key{}, JSON[item](v), TableOptions{}))
	f.byName = Must(DefineIndex(f.items, "by_name", IndexOptions{Unique: true, Version: 1}, func(_ uint64, v *item) []Entry {
		if v.Name == "" {
			return nil
		}
		return []Entry{{Key: []byte(v.Name), Value: []byte("p:" + v.Name)}}
	}))
	f.byTag = Must(DefineIndex(f.items, "by_tag", IndexOptions{Version: 1}, func(_ uint64, v *item) []Entry {
		out := make([]Entry, 0, len(v.Tags))
		for _, tag := range v.Tags {
			out = append(out, Entry{Key: []byte(tag), Value: []byte(v.Name)})
		}
		return out
	}))
	f.total = Must(DefineCounter(f.items, "total", 1, func(_ uint64, v *item) int64 { return v.N }))
	f.quiet = Must(Define(a, 0x11, "quiet", Uint64Key{}, JSON[item](1), TableOptions{Silent: true}))
	f.jobs = Must(Define(a, 0x12, "jobs", Uint64Key{}, JSON[item](1), TableOptions{Silent: true}))
	f.byState = Must(DefineIndex(f.jobs, "by_state", IndexOptions{Version: 1}, func(_ uint64, v *item) []Entry {
		return []Entry{{Key: []byte(v.Name)}}
	}))
	f.jobCount = Must(DefineCounter(f.jobs, "job_weight", 1, func(_ uint64, v *item) int64 { return v.N }))
	f.other = Must(Define(b, 0x20, "other", Uint64Key{}, JSON[item](1), TableOptions{}))
	return f
}

func tempPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "db.bolt")
}

func mustOpen(t *testing.T, path string, s *Schema, o Options) *DB {
	t.Helper()
	db, err := Open(path, s, o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func update(t *testing.T, db *DB, fn func(*Tx) error) {
	t.Helper()
	if err := db.Update(t.Context(), fn); err != nil {
		t.Fatalf("Update: %v", err)
	}
}

func view(t *testing.T, db *DB, fn func(*ReadTx) error) {
	t.Helper()
	if err := db.View(t.Context(), fn); err != nil {
		t.Fatalf("View: %v", err)
	}
}

func headRev(t *testing.T, db *DB) uint64 {
	t.Helper()
	var rev uint64
	view(t, db, func(r *ReadTx) error {
		var err error
		rev, err = r.Rev()
		return err
	})
	return rev
}

// recorder collects OnCommit deliveries.
type recorder struct {
	changes []Change
	mu      sync.Mutex
}

func (r *recorder) on(c Change) {
	r.mu.Lock()
	r.changes = append(r.changes, c)
	r.mu.Unlock()
}

func (r *recorder) all() []Change {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.changes)
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}

// rawUpdate writes the closed file at path through bbolt directly.
func rawUpdate(t *testing.T, path string, fn func(*bbolt.Tx) error) {
	t.Helper()
	bdb, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bbolt.Open: %v", err)
	}
	defer func() { _ = bdb.Close() }()
	if err := bdb.Update(fn); err != nil {
		t.Fatalf("raw update: %v", err)
	}
}

func hasSubject(c Change, s Subject) bool {
	return slices.ContainsFunc(c.Subjects, func(x Subject) bool { return bytes.Equal(x, s) })
}
