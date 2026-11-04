# bolttable

[![Go Reference](https://pkg.go.dev/badge/github.com/cplieger/bolttable.svg)](https://pkg.go.dev/github.com/cplieger/bolttable) [![Go version](https://img.shields.io/github/go-mod/go-version/cplieger/bolttable)](https://github.com/cplieger/bolttable/blob/main/go.mod) [![Mutation](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/cplieger/bolttable/badges/mutation.json)](https://github.com/cplieger/bolttable/issues?q=label%3Agremlins-tracker)

bolttable gives your Go code typed tables, declared indexes and a change feed over one bbolt file.

It replaces the hand-written bucket code, index upkeep, migration ladder and startup steps that grow around a bbolt store. It depends only on bbolt, `golang.org/x/sys` and the standard library, needs Go 1.27.3 or later and is licensed under Apache-2.0.

## Why use it

- Tables are generic over key and value types. A key codec you choose sets the byte layout and the scan order, without reflection or struct tags.
- An index maps a row to its entries in the row's transaction. `Verify` rescans every table, reports each index entry or counter that disagrees, and can rewrite it.
- Each transaction that changes a non-silent table records one change at the next revision. `Since` returns changes after a revision you hold, a page at a time, and `OnCommit` receives every commit in commit order.
- Each schema namespace has its own format digest. A changed declaration fails `Open` naming only that namespace, unless your reset policy allows it: `Open` then keeps a copy of the file, empties that namespace, and the others keep their rows.
- `Open` takes a lock file beside the data file and does every startup step under it. It removes stranded temporary files, can move a corrupt or truncated file aside, resets, compacts and reports what it did. A second process gets `ErrLocked` after a wait you choose.
- `OpenReadOnly` opens a file for a report or a restore check and has no write method. `Space` reads the file's size and free bytes without a transaction, `Backup` copies the file while it stays open, and `WriteHealth` tracks, per class of write, whether commits are failing and since when, for a health check.

Consider [storm](https://github.com/asdine/storm) or [bolthold](https://github.com/timshannon/bolthold) if you want tables from struct tags and queries built at run time, or bbolt alone for a few buckets.

## Install

```sh
go get github.com/cplieger/bolttable@latest
```

## Usage

```go
var (
    schema   = bolttable.NewSchema("app")
    accounts = bolttable.Must(schema.Namespace("accounts"))
    users    = bolttable.Must(bolttable.Define(accounts, 0x01, "users",
        bolttable.Uint64Key{}, bolttable.JSON[user](1), bolttable.TableOptions{}))
)

db, err := bolttable.Open(path, schema, bolttable.Options{
    Retention: 1000,
    LockWait:  5 * time.Second,
    // A format change in a namespace you can rebuild resets it, keeping two
    // copies of the file; any other format change fails Open.
    Reset: bolttable.ResetPolicy{
        Allow: func(n bolttable.NamespaceDigest) bool { return n.Name == "cache" },
        Keep:  2,
    },
})
if err != nil {
    return err
}
defer db.Close()

err = db.Update(ctx, func(tx *bolttable.Tx) error {
    id, err := users.NextID(tx)
    if err != nil {
        return err
    }
    return users.Put(tx, id, &user{Email: "ada@example.com"})
})
```

The package example on pkg.go.dev adds a unique index, a counter, a subject that names no row, `OnCommit` and a page of the feed, and `go test` keeps it true.

## API

- `NewSchema`, `Schema.Namespace`, `Define`, `DefineIndex`, `DefineCounter`, `CascadePrefix` and `CascadeIndex` declare the layout. `Must` wraps them for package-level variables.
- `Open` runs the open procedure that `Options.LockWait`, `Lock`, `Reset`, `Compact` and `InvalidFile` set, and `DB.OpenReport` tells what it did. `LockPath` takes the same lock for your own file operations, and `OpenReadOnly` returns a `*ReadDB`.
- `DB.Update`, `DB.Batch` and `DB.View` run transactions. `Table` has `Get`, `Put`, `Delete`, `Scan`, `NextID` and `Subject`. `Index` has `Lookup` and `Scan`, and `Counter` has `Value`. A `Range` scans a prefix, a `[From, To)` span or one `Exact` key.
- `Tx.Mark`, `Tx.Touch` and `Tx.AfterCommit` add to a transaction's change, and `Synthetic` builds a subject that names no row. `ReadTx.Since` pages the feed and `ReadTx.Rev` reads its head. `Options.BeforeCommit` chooses what each change records.
- `DB.Verify`, `DB.Stats`, `DB.Space` and `DB.Backup` look after the file. `WriteHealth` records write outcomes, and `Options.CommitFault` lets a test fail chosen commits.
- `ErrFormat`, `ErrConflict`, `ErrCorrupt`, `ErrCommit`, `ErrPublished`, `ErrLocked` and `ErrInvalidFile` are the errors a caller can match, with `*FormatError` and `*ConflictError` carrying the details.

The full reference is on [pkg.go.dev](https://pkg.go.dev/github.com/cplieger/bolttable).

## Contributing

Issues and pull requests are welcome. The [shared contributing rules](https://github.com/cplieger/.github/blob/main/CONTRIBUTING.md) apply.

## Disclaimer

This project is built with care and follows security best practices, but it is intended for personal / self-hosted use. No guarantees of fitness for production environments. Use at your own risk.

This project was built with AI-assisted tooling using [Claude](https://claude.com), [GPT](https://openai.com), and [Kiro](https://kiro.dev). The human maintainer defines architecture, supervises implementation, and makes all final decisions.

## License

Apache-2.0. See [LICENSE](LICENSE).
