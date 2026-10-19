package bolttable

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
)

// KeyCodec lays a table's keys out as bytes; the byte order is the order
// Scan visits. Version enters the namespace's format digest, so a layout
// change bumps it. ParseKey receives a copy it may keep.
type KeyCodec[K any] interface {
	Version() uint32
	AppendKey(dst []byte, k K) []byte
	ParseKey(b []byte) (K, error)
}

// Codec encodes a table's values. Version enters the namespace's format
// digest, so an encoding change bumps it. Append must be deterministic: a Put
// whose encoding equals the stored bytes writes nothing. Decode receives a
// copy it may keep.
type Codec[V any] interface {
	Version() uint32
	Append(dst []byte, v *V) ([]byte, error)
	Decode(b []byte, v *V) error
}

// JSON returns a Codec over encoding/json at the given version.
func JSON[V any](version uint32) Codec[V] { return jsonCodec[V]{version: version} }

type jsonCodec[V any] struct{ version uint32 }

func (c jsonCodec[V]) Version() uint32 { return c.version }

func (jsonCodec[V]) Append(dst []byte, v *V) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return append(dst, b...), nil
}

func (jsonCodec[V]) Decode(b []byte, v *V) error { return json.Unmarshal(b, v) }

// Uint64Key encodes a uint64 big-endian, so byte order is numeric order.
type Uint64Key struct{}

// Version is 1.
func (Uint64Key) Version() uint32 { return 1 }

// AppendKey appends k's 8 big-endian bytes.
func (Uint64Key) AppendKey(dst []byte, k uint64) []byte { return binary.BigEndian.AppendUint64(dst, k) }

// ParseKey refuses anything but 8 bytes.
func (Uint64Key) ParseKey(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("uint64 key is %d bytes, want 8", len(b))
	}
	return binary.BigEndian.Uint64(b), nil
}

// BytesKey stores a []byte key as is; Put refuses an empty one.
type BytesKey struct{}

// Version is 1.
func (BytesKey) Version() uint32 { return 1 }

// AppendKey appends k.
func (BytesKey) AppendKey(dst, k []byte) []byte { return append(dst, k...) }

// ParseKey returns a copy of b.
func (BytesKey) ParseKey(b []byte) ([]byte, error) {
	if len(b) == 0 {
		return nil, errors.New("empty bytes key")
	}
	return append([]byte(nil), b...), nil
}

var (
	_ KeyCodec[uint64] = Uint64Key{}
	_ KeyCodec[[]byte] = BytesKey{}
)
