package bolttable

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// fingerprint is 8 bytes of SHA-256 over an encoded key, in hex: enough to
// tell two corrupt keys apart, never enough to recover a key that is itself
// a secret's hash.
func fingerprint(key []byte) string {
	sum := sha256.Sum256(key)
	return hex.EncodeToString(sum[:8])
}

func corruptRow(table string, key []byte) error {
	return fmt.Errorf("%w: table %s, key %s", ErrCorrupt, table, fingerprint(key))
}

func corruptIndex(index string, key []byte) error {
	return fmt.Errorf("%w: index %s, key %s", ErrCorrupt, index, fingerprint(key))
}
