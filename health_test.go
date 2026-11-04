package bolttable

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"
)

var errDisk = fmt.Errorf("%w: %w", ErrCommit, syscall.ENOSPC)

func TestWriteHealthCommitFailureSetsClass(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var h WriteHealth
		now := time.Now()
		got := h.Record("large", errDisk)
		want := WriteRecord{LastErr: errDisk, FirstFailed: now, LastFailed: now, Failures: 1, Failing: true}
		if got != want {
			t.Errorf("Record = %+v, want %+v", got, want)
		}
		if rec, ok := h.Class("large"); !ok || rec != want {
			t.Errorf("Class = %+v, %v; want %+v, true", rec, ok, want)
		}
	})
}

func TestWriteHealthOnlySameClassClears(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var h WriteHealth
		failedAt := time.Now()
		h.Record("large", errDisk)
		time.Sleep(time.Minute)
		h.Record("small", nil)
		if rec, _ := h.Class("large"); !rec.Failing || rec.FirstFailed != failedAt {
			t.Errorf("after a small commit, large = %+v; want still failing since %v", rec, failedAt)
		}
		time.Sleep(time.Minute)
		okAt := time.Now()
		got := h.Record("large", nil)
		want := WriteRecord{LastErr: errDisk, LastFailed: failedAt, LastOK: okAt, Failures: 1}
		if got != want {
			t.Errorf("after a large commit, large = %+v; want %+v", got, want)
		}
		if rec, ok := h.Class("small"); !ok || rec != (WriteRecord{LastOK: failedAt.Add(time.Minute)}) {
			t.Errorf("small = %+v, %v; want only its commit", rec, ok)
		}
	})
}

func TestWriteHealthIgnoresRefusals(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var h WriteHealth
		h.Record("large", errDisk)
		before, _ := h.Class("large")
		refusals := []error{
			errors.New("fn refused"),
			&ConflictError{Index: "a.by_name"},
			fmt.Errorf("%w: row", ErrCorrupt),
			context.Canceled,
		}
		time.Sleep(time.Minute)
		for _, err := range refusals {
			if got := h.Record("large", err); got != before {
				t.Errorf("Record(%v) = %+v, want the record unchanged %+v", err, got, before)
			}
			if got := h.Record("fresh", err); got != (WriteRecord{}) {
				t.Errorf("Record(fresh, %v) = %+v, want the zero record", err, got)
			}
		}
		if rec, ok := h.Class("fresh"); ok {
			t.Errorf("refusals created class fresh: %+v", rec)
		}
	})
}

func TestWriteHealthStreakStart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var h WriteHealth
		first := time.Now()
		h.Record("large", errDisk)
		time.Sleep(time.Minute)
		second := time.Now()
		other := fmt.Errorf("%w: second", ErrCommit)
		got := h.Record("large", other)
		if got.FirstFailed != first || got.LastFailed != second || got.Failures != 2 || got.LastErr != other || !got.Failing {
			t.Errorf("after two failures = %+v; want the streak from %v to %v, 2 failures, the second error", got, first, second)
		}
	})
}

func TestWriteHealthZeroValue(t *testing.T) {
	var h WriteHealth
	if rec, ok := h.Class("any"); ok || rec != (WriteRecord{}) {
		t.Errorf("zero WriteHealth Class = %+v, %v; want the zero record, false", rec, ok)
	}
}

func TestWriteHealthConcurrent(t *testing.T) {
	var h WriteHealth
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Go(func() {
			if i%2 == 0 {
				h.Record("c", errDisk)
			} else {
				h.Record("c", nil)
			}
			_, _ = h.Class("c")
		})
	}
	wg.Wait()
	if rec, _ := h.Class("c"); rec.Failures != 32 {
		t.Errorf("Failures = %d, want 32", rec.Failures)
	}
}
