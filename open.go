package bolttable

import (
	"cmp"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"go.etcd.io/bbolt"
)

// stampLayout stamps .invalid and .old names in UTC, so names sort by time.
const stampLayout = "20060102T150405.000000000Z"

// InvalidFilePolicy is what Open does with a file wrapping ErrInvalidFile.
type InvalidFilePolicy uint8

const (
	// InvalidFileFail returns the error and leaves the file untouched.
	InvalidFileFail InvalidFilePolicy = iota
	// InvalidFileMoveAside renames the file to <path>.<stamp>.invalid, stamp
	// the UTC time as 20060102T150405.000000000Z, and opens a new empty file.
	// Open moves at most one file aside: a new file that is invalid fails.
	InvalidFileMoveAside
)

// ResetPolicy decides what Open resets. The zero value resets nothing.
type ResetPolicy struct {
	// Allow reports whether a namespace a *FormatError lists may be reset.
	// Open asks it once per listed name, in order, and resets only when it
	// accepts every one; nil accepts none. Its panic reaches the caller.
	Allow func(NamespaceDigest) bool
	// Force names namespaces Open resets even when their format matches.
	// Each must be stored in the file, declared by the schema, or
	// "bolttable"; a new file stores nothing, so there it resets nothing.
	Force []string
	// Keep is how many .old copies of the file to keep, the new one
	// included: before a reset Open copies the file to
	// <path>.<8 hex>.<stamp>.old, the hex from the first reset name's stored
	// digest, and removes the older copies. Zero takes no copy and removes
	// none; negative is an error.
	Keep int
}

// OpenReport is what Open did to the file before it opened it.
type OpenReport struct {
	// Invalid is why the file was moved aside; it wraps ErrInvalidFile.
	Invalid error
	// CompactErr is a compaction that failed and left the file as it was,
	// or a directory fsync that failed after it, with Compacted set.
	CompactErr error
	// MovedAside is the invalid file's new path; "" when none.
	MovedAside string
	// Kept is the .old copy taken before the reset; "" when none.
	Kept string
	// Reset is every name reset, sorted, with the digest stored and the one
	// recreated; Want is zero for a name the schema does not declare.
	Reset     []NamespaceDigest
	Compacted bool
}

// OpenReport returns what Open did to the file before it opened it.
func (db *DB) OpenReport() OpenReport {
	r := db.report
	r.Reset = slices.Clone(r.Reset)
	return r
}

// Open opens or creates the file at path, mode 0600, under s, sealing s.
// Under the exclusive lock on path, taken or adopted from o.Lock and held
// until Close, it removes stranded temporary files, inspects a stored file
// without writing, applies o.InvalidFile, o.Reset and o.Compact, and creates
// the namespaces the file lacks; OpenReport tells what it did. A format
// change o.Reset refuses fails with the *FormatError, the file unchanged,
// and another holder of the lock with ErrLocked. The path follows LockPath's
// rule and the lock its filesystem and platform limits.
func Open(path string, s *Schema, o Options) (db *DB, err error) { //nolint:gocritic // hugeParam: callers build Options inline
	if s == nil {
		return nil, errors.New("bolttable: Open with a nil schema")
	}
	if perr := checkPath(path); perr != nil {
		return nil, perr
	}
	retention, err := retentionOf(o.Retention)
	if err != nil {
		return nil, err
	}
	if o.MaxBatchSize < 0 || o.MaxBatchDelay < 0 {
		return nil, fmt.Errorf("bolttable: negative batch limit (MaxBatchSize %d, MaxBatchDelay %v)", o.MaxBatchSize, o.MaxBatchDelay)
	}
	if o.Reset.Keep < 0 {
		return nil, fmt.Errorf("bolttable: negative Reset.Keep %d", o.Reset.Keep)
	}
	if cerr := o.Compact.validate(); cerr != nil {
		return nil, cerr
	}
	if o.InvalidFile > InvalidFileMoveAside {
		return nil, fmt.Errorf("bolttable: unknown InvalidFile policy %d", o.InvalidFile)
	}
	if o.Lock != nil && o.LockWait != 0 {
		return nil, fmt.Errorf("bolttable: Options.Lock with LockWait %v: Open waits for no lock it adopts", o.LockWait)
	}
	s.seal()
	unlock, err := takeLock(path, o.Lock, o.LockWait)
	if err != nil {
		return nil, err
	}
	defer func() {
		if db == nil {
			_ = unlock()
		}
	}()
	p := &opening{path: path, schema: s, want: s.digests(), opts: o}
	if db, err = p.run(); err != nil {
		return nil, err
	}
	db.unlock = unlock
	db.retention = retention
	db.report = p.report
	db.bolt.MaxBatchSize = cmp.Or(o.MaxBatchSize, defaultMaxBatchSize)
	db.bolt.MaxBatchDelay = cmp.Or(o.MaxBatchDelay, defaultMaxBatchDelay)
	return db, nil
}

// takeLock takes path's lock, or adopts the caller's, and returns what
// undoes it.
func takeLock(path string, adopted *Lock, wait time.Duration) (func() error, error) {
	if adopted != nil {
		if err := adopted.adopt(path); err != nil {
			return nil, err
		}
		return func() error { adopted.free(); return nil }, nil
	}
	l, err := LockPath(path, wait)
	if err != nil {
		return nil, err
	}
	return l.Release, nil
}

// opening is one run of Open's procedure.
type opening struct {
	schema *Schema
	want   map[string][32]byte
	// stored is the stored file's top-level buckets and their digests.
	stored map[string][32]byte
	// measure is the inspection's, nil once a reset changed the file.
	measure *freeMeasure
	path    string
	report  OpenReport
	opts    Options
	op      faultOp
}

// run runs the procedure. Once a file is moved aside, every error names the
// moved file and joins why it was moved, since the original path is gone.
func (p *opening) run() (*DB, error) {
	db, err := p.steps()
	if err != nil && p.report.MovedAside != "" {
		return nil, fmt.Errorf("bolttable: open %s: moved aside to %s: %w", p.path, p.report.MovedAside, errors.Join(p.report.Invalid, err))
	}
	return db, err
}

func (p *opening) steps() (*DB, error) {
	for _, tmp := range []string{p.path + ".compact-tmp", p.path + ".copy-tmp"} {
		if err := removeFile(tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("bolttable: remove stranded %s: %w", tmp, err)
		}
	}
	size, err := storedSize(p.path)
	if err != nil {
		return nil, err
	}
	if size > 0 {
		err = p.prepare(size)
	} else {
		err = p.checkForce()
	}
	if err != nil {
		return nil, err
	}
	for {
		db, err := p.openReadWrite()
		if !errors.Is(err, ErrInvalidFile) {
			return db, err
		}
		if merr := p.invalid(err); merr != nil {
			return nil, merr
		}
	}
}

// storedSize is the size of the file at path, zero when it is absent: a
// read-only bbolt open cannot initialize it
// (https://github.com/etcd-io/bbolt/blob/v1.5.0/db.go#L272).
func storedSize(path string) (int64, error) {
	fi, err := os.Stat(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("bolttable: %w", err)
	case !fi.Mode().IsRegular():
		// bbolt reads a directory as a file whose meta pages fail to read,
		// which it reports as ErrInvalid.
		return 0, fmt.Errorf("bolttable: open %s: not a regular file", path)
	}
	return fi.Size(), nil
}

// prepare inspects a stored file, resets and compacts it, or moves it
// aside.
func (p *opening) prepare(size int64) error {
	fe, err := p.inspect(size)
	if err == nil {
		err = p.reset(fe)
	}
	if err == nil {
		err = p.compact()
	}
	if !errors.Is(err, ErrInvalidFile) {
		return err
	}
	return p.invalid(err)
}

// inspect compares the stored file with the schema through a read-only
// handle, writing nothing, and measures its free pages.
func (p *opening) inspect(size int64) (*FormatError, error) {
	var fe *FormatError
	err := readOnly(p.path, stepInspect, func(bdb *bbolt.DB) error {
		p.measure = &freeMeasure{size: size, free: freeBytes(new(bdb.Stats()), bdb.Info().PageSize)}
		return bdb.View(func(btx *bbolt.Tx) error {
			p.stored = map[string][32]byte{}
			if err := btx.ForEach(func(name []byte, b *bbolt.Bucket) error {
				p.stored[string(name)] = storedDigest(b)
				return nil
			}); err != nil {
				return err
			}
			_, f, err := inspect(btx, p.schema, p.want)
			fe = f
			return err
		})
	})
	if err != nil {
		return nil, err
	}
	return fe, nil
}

// readOnly runs fn over a read-only handle on path inside guardRead, then
// closes the handle. openReader owns why the handle loads the freelist.
func readOnly(path, step string, fn func(*bbolt.DB) error) error {
	o := boltOptions(true)
	o.PreLoadFreelist = true
	bdb, _, err := openBolt(path, step, o)
	if err != nil {
		return err
	}
	err = guardRead(path, step, func() error { return fn(bdb) })
	if cerr := bdb.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("bolttable: close %s: %w", path, cerr)
	}
	return err
}

// reset resets every namespace fe lists when Allow accepts each, plus the
// Force names, and returns fe when Allow refuses one. Allow runs outside
// both guards.
func (p *opening) reset(fe *FormatError) error {
	if fe != nil && !p.allowed(fe) {
		return fe
	}
	if err := p.checkForce(); err != nil {
		return err
	}
	digests := p.resetList(fe)
	if len(digests) == 0 {
		return nil
	}
	names := make([]string, 0, len(digests))
	for _, n := range digests {
		names = append(names, n.Name)
	}
	if keep := p.opts.Reset.Keep; keep > 0 {
		kept, err := keepCopy(p.path, copyDigest(digests), keep)
		if err != nil {
			return err
		}
		p.report.Kept = kept
	}
	if err := resetFile(p.path, p.schema, p.want, names, &p.op); err != nil {
		return err
	}
	p.report.Reset, p.measure = digests, nil
	return nil
}

// allowed asks Allow about every name fe lists, in order, and reports
// whether it accepted them all.
func (p *opening) allowed(fe *FormatError) bool {
	allow := p.opts.Reset.Allow
	ok := allow != nil
	for _, n := range fe.Namespaces {
		if allow != nil && !allow(n) {
			ok = false
		}
	}
	return ok
}

// resetList is every name fe lists plus the Force names, once each, sorted.
func (p *opening) resetList(fe *FormatError) []NamespaceDigest {
	listed := map[string]NamespaceDigest{}
	for _, name := range p.opts.Reset.Force {
		listed[name] = NamespaceDigest{Name: name, Stored: p.stored[name], Want: p.want[name]}
	}
	if fe != nil {
		for _, n := range fe.Namespaces {
			listed[n.Name] = n
		}
	}
	digests := make([]NamespaceDigest, 0, len(listed))
	for _, name := range slices.Sorted(maps.Keys(listed)) {
		digests = append(digests, listed[name])
	}
	return digests
}

// checkForce refuses a Force name neither stored, declared nor the
// library's, before anything is written.
func (p *opening) checkForce() error {
	for _, name := range p.opts.Reset.Force {
		_, declared := p.want[name]
		_, stored := p.stored[name]
		if !declared && !stored && name != libBucket {
			return fmt.Errorf("bolttable: Reset.Force names %q, neither stored, declared nor %q", name, libBucket)
		}
	}
	return nil
}

// copyDigest is the first 8 hex digits of the first non-zero stored digest
// among the sorted reset names, which names the .old copy.
func copyDigest(reset []NamespaceDigest) string {
	for _, n := range reset {
		if n.Stored != ([32]byte{}) {
			return hex.EncodeToString(n.Stored[:4])
		}
	}
	return "00000000"
}

// compact runs the Compact policy; only an invalid file fails Open.
func (p *opening) compact() error {
	if p.opts.Compact == (CompactPolicy{}) {
		return nil
	}
	done, err := compactIfFree(p.path, p.opts.Compact, p.measure)
	if errors.Is(err, ErrInvalidFile) {
		return err
	}
	p.report.Compacted, p.report.CompactErr = done, err
	return nil
}

// invalid moves the file aside for err, which wraps ErrInvalidFile, and
// checks Force against the new file, or returns err when the policy fails or
// a file was already moved.
func (p *opening) invalid(err error) error {
	if p.opts.InvalidFile != InvalidFileMoveAside || p.report.MovedAside != "" {
		return err
	}
	aside := p.path + "." + time.Now().UTC().Format(stampLayout) + ".invalid"
	if rerr := os.Rename(p.path, aside); rerr != nil {
		return fmt.Errorf("bolttable: move aside %s: %w", p.path, rerr)
	}
	p.report.Invalid, p.report.MovedAside = err, aside
	p.stored, p.measure = nil, nil
	if serr := syncDir(filepath.Dir(p.path)); serr != nil {
		return fmt.Errorf("bolttable: move aside not yet durable: %w", serr)
	}
	return p.checkForce()
}

// openReadWrite opens the file for writing and starts the DB over it.
func (p *opening) openReadWrite() (db *DB, err error) {
	bdb, f, err := openBolt(p.path, stepOpen, boltOptions(false))
	if err != nil {
		return nil, err
	}
	var wtx *bbolt.Tx
	defer func() {
		if db == nil {
			_ = closeBolt(p.path, bdb, f, wtx)
		}
	}()
	err = guardFault(p.path, stepOpen, &p.op, func() error {
		var serr error
		db, serr = start(bdb, p.schema, &p.opts, p.want, &wtx)
		return serr
	})
	if err != nil {
		return nil, err
	}
	return db, nil
}
