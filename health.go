package bolttable

import (
	"errors"
	"sync"
	"time"
)

// WriteHealth keeps the outcome of writes per caller-named class. The zero
// value is ready; it is safe for concurrent use and must not be copied after
// first use.
type WriteHealth struct {
	classes map[string]WriteRecord
	mu      sync.Mutex
}

// WriteRecord is one class's write outcomes since its WriteHealth was made.
type WriteRecord struct {
	// LastErr is the latest ErrCommit, kept after a recovery.
	LastErr error
	// FirstFailed starts the current failing run; zero when not failing.
	FirstFailed time.Time
	LastFailed  time.Time
	LastOK      time.Time
	// Failures counts every ErrCommit recorded and is never reset.
	Failures uint64
	// Failing reports that the latest commit or failure recorded failed.
	Failing bool
}

// Record notes the result of one Update or Batch call of class and returns
// the class's record after it. nil records a commit, which clears a failing
// run of that class only; an error wrapping ErrCommit records a failure;
// every other error (the function's own, *ConflictError, ErrCorrupt, a
// context error) changes nothing.
func (h *WriteHealth) Record(class string, err error) WriteRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	rec := h.classes[class]
	switch {
	case err == nil:
		rec.Failing, rec.FirstFailed, rec.LastOK = false, time.Time{}, time.Now()
	case errors.Is(err, ErrCommit):
		now := time.Now()
		if !rec.Failing {
			rec.FirstFailed = now
		}
		rec.Failing, rec.LastFailed, rec.LastErr = true, now, err
		rec.Failures++
	default:
		return rec
	}
	if h.classes == nil {
		h.classes = map[string]WriteRecord{}
	}
	h.classes[class] = rec
	return rec
}

// Class returns the record of class; false when Record never changed it.
func (h *WriteHealth) Class(class string) (WriteRecord, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	rec, ok := h.classes[class]
	return rec, ok
}
