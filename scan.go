package bolttable

import (
	"bytes"

	"go.etcd.io/bbolt"
)

// cursorRange is a Range in stored-key bytes: keys starting with prefix,
// from lo (inclusive) forward, or below hi (exclusive) in reverse.
type cursorRange struct {
	prefix, lo, hi []byte
	reverse        bool
}

// scanBucket visits b's keys in cr until visit returns false or an error. In
// a write transaction visit may write b, so the cursor re-seeks from the
// last key after each visit instead of trusting its position.
func scanBucket(b *bbolt.Bucket, cr *cursorRange, writable bool, visit func(k, v []byte) (bool, error)) error {
	c := b.Cursor()
	k, v := cr.first(c)
	for k != nil && bytes.HasPrefix(k, cr.prefix) {
		last := k
		if writable {
			last = bytes.Clone(k)
		}
		more, err := visit(k, v)
		if err != nil || !more {
			return err
		}
		k, v = cr.next(c, last, writable)
	}
	return nil
}

func (cr *cursorRange) first(c *bbolt.Cursor) (k, v []byte) {
	if !cr.reverse {
		start := cr.prefix
		if bytes.Compare(cr.lo, start) > 0 {
			start = cr.lo
		}
		return c.Seek(start)
	}
	hi := prefixEnd(cr.prefix)
	if cr.hi != nil && (hi == nil || bytes.Compare(cr.hi, hi) < 0) {
		hi = cr.hi
	}
	if hi == nil {
		return c.Last()
	}
	return lastBelow(c, hi)
}

func (cr *cursorRange) next(c *bbolt.Cursor, last []byte, writable bool) (k, v []byte) {
	switch {
	case !writable && !cr.reverse:
		return c.Next()
	case !writable:
		return c.Prev()
	case !cr.reverse:
		if k, v = c.Seek(last); k != nil && bytes.Equal(k, last) {
			return c.Next()
		}
		return k, v
	default:
		return lastBelow(c, last)
	}
}

// lastBelow positions c on the greatest key below hi.
func lastBelow(c *bbolt.Cursor, hi []byte) (k, v []byte) {
	if k, _ = c.Seek(hi); k == nil {
		return c.Last()
	}
	return c.Prev()
}

// prefixEnd returns the least key above every key starting with p, or nil
// when there is none (p empty or all 0xFF).
func prefixEnd(p []byte) []byte {
	end := bytes.Clone(p)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}
