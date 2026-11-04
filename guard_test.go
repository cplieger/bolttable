package bolttable

import (
	"testing"

	"go.etcd.io/bbolt"
)

// start must hand out the write transaction it begins, which closeBolt
// needs when a panic leaves it open.
func TestStartHandsOutItsWriteTx(t *testing.T) {
	path := closedFile(t, newFixture(t).schema)
	s := withNamespaceC(newFixture(t))
	bdb, f, err := openBolt(path, stepOpen, boltOptions(false))
	if err != nil {
		t.Fatal(err)
	}
	var wtx *bbolt.Tx
	db, err := start(bdb, s, &Options{}, s.digests(), &wtx)
	if err != nil {
		_ = closeBolt(path, bdb, f, wtx)
		t.Fatal(err)
	}
	db.unlock = func() error { return nil }
	defer func() { _ = db.Close() }()
	if wtx == nil {
		t.Fatal("start created namespace c without handing out its transaction")
	}
	if wtx.DB() != nil {
		t.Error("the handed-out transaction is still open")
	}
}
