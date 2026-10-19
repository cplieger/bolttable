package bolttable

import (
	"bytes"
	"math"
	"testing"
)

func TestUint64KeyOrder(t *testing.T) {
	values := []uint64{0, 1, 2, 255, 256, 1 << 32, 1<<32 + 1, math.MaxUint64 - 1, math.MaxUint64}
	var k Uint64Key
	for i := 1; i < len(values); i++ {
		lo, hi := k.AppendKey(nil, values[i-1]), k.AppendKey(nil, values[i])
		if bytes.Compare(lo, hi) >= 0 {
			t.Errorf("key(%d) = %x does not sort below key(%d) = %x", values[i-1], lo, values[i], hi)
		}
	}
	for _, v := range values {
		got, err := k.ParseKey(k.AppendKey(nil, v))
		if err != nil || got != v {
			t.Errorf("round trip of %d = %d, %v", v, got, err)
		}
	}
	if _, err := k.ParseKey([]byte{1, 2, 3}); err == nil {
		t.Error("ParseKey accepted a 3-byte key")
	}
	if k.Version() != 1 {
		t.Errorf("Uint64Key version = %d, want 1", k.Version())
	}
}

func TestBytesKey(t *testing.T) {
	var k BytesKey
	in := []byte{0, 1, 0xFF}
	out, err := k.ParseKey(in)
	if err != nil || !bytes.Equal(out, in) {
		t.Fatalf("ParseKey = %x, %v", out, err)
	}
	out[0] = 9
	if in[0] != 0 {
		t.Error("ParseKey returned the caller's slice")
	}
	if _, err := k.ParseKey(nil); err == nil {
		t.Error("ParseKey accepted an empty key")
	}
	if got := k.AppendKey([]byte{7}, in); !bytes.Equal(got, []byte{7, 0, 1, 0xFF}) {
		t.Errorf("AppendKey = %x", got)
	}
	if k.Version() != 1 {
		t.Errorf("BytesKey version = %d, want 1", k.Version())
	}
}

func TestJSONCodec(t *testing.T) {
	c := JSON[item](3)
	if c.Version() != 3 {
		t.Errorf("version = %d, want 3", c.Version())
	}
	b, err := c.Append([]byte("x"), &item{Name: "n", N: 2})
	if err != nil || string(b) != `x{"name":"n","n":2}` {
		t.Fatalf("Append = %s, %v", b, err)
	}
	var v item
	if err := c.Decode(b[1:], &v); err != nil || v.Name != "n" || v.N != 2 {
		t.Errorf("Decode = %+v, %v", v, err)
	}
	if err := c.Decode([]byte("{"), &v); err == nil {
		t.Error("Decode accepted truncated JSON")
	}
	if _, err := JSON[chan int](1).Append(nil, new(chan int)); err == nil {
		t.Error("Append encoded a channel")
	}
}
