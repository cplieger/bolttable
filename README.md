# bolttable

[![Go Reference](https://pkg.go.dev/badge/github.com/cplieger/bolttable.svg)](https://pkg.go.dev/github.com/cplieger/bolttable) [![Go version](https://img.shields.io/github/go-mod/go-version/cplieger/bolttable)](https://github.com/cplieger/bolttable/blob/main/go.mod) [![Mutation](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/cplieger/bolttable/badges/mutation.json)](https://github.com/cplieger/bolttable/issues?q=label%3Agremlins-tracker)

bolttable gives your Go code typed tables, declared indexes and a change feed over one bbolt file.

It replaces the hand-written bucket code, index upkeep, migration ladder and startup steps that grow around a bbolt store. It depends only on bbolt, `golang.org/x/sync`, `golang.org/x/sys` and the standard library and is licensed under Apache-2.0. It runs on Linux, macOS and the BSDs; on Windows and other platforms, `Open`, `OpenReadOnly` and `LockPath` return an error wrapping `errors.ErrUnsupported`.

## Why use it

- Tables are generic over key and value types. A key codec you choose sets the byte layout and the scan order, without reflection or struct tags.
- An index maps a row to its entries in the row's transaction. `Verify` rescans every table, reports each index entry or counter that disagrees, and can rewrite it.
- Each transaction that changes a non-silent table records one change at the next revision. `Since` returns changes after a revision you hold, a page at a time, and `OnCommit` receives every commit in commit order.
- Each schema namespace has its own format digest. A changed declaration fails `Open` naming only that namespace, unless your reset policy allows it: `Open` then keeps a copy of the file, empties that namespace, and the others keep their rows.
- `Open` takes a lock file beside the data file and does every startup step under it. It removes stranded temporary files, runs your own check of the stored file before it changes anything, can move a corrupt or truncated file aside, resets, compacts and reports what it did. A second process gets `ErrLocked` after a wait you choose.
- `Live` is one handle to a live file that every store holds in place of a `*DB`. `Swap` replaces the file under all of them while it stays locked, and hooks you register reload what you keep in memory at each open. `OpenFailure` turns an open error into a fixed class (`damaged`, `format`, `newer`, `unreadable` or `unwritable`) for an outage page or health check that must not show error text.
- `OpenReadOnly` opens a file for a report or a restore check and has no write method. `Space` reads the file's size, the bytes its data uses and the bytes bbolt can reuse, without a transaction, `Backup` copies the file while it stays open, and `WriteHealth` tracks, per class of write, whether commits are failing and since when, for a health check.

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

### Replacing a live file

When a process must replace its own file, a restore from backup for instance, build its stores on a `Live` instead of a `*DB`:

```go
live, err := bolttable.NewLive(path, schema, bolttable.Options{LockWait: 5 * time.Second})
if err != nil {
    return err // an invalid option, or another process holds the lock
}
defer live.Close()
live.OnOpen(reloadCache) // runs at every open, before any call is admitted
if err := live.Open(); err != nil {
    class, _ := bolttable.OpenFailure(err)
    slog.Error("store unavailable", "class", class, "err", err)
}

// Later, put a staged copy in place of the file.
ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
defer cancel()
err = live.Swap(ctx, func() error { return os.Rename(staged, path) })
```

`NewLive` holds the lock for the handle's whole life: the `Options.Lock` you pass, which it never releases, or one it takes, which `Close` releases. Until `Close`, that lock's `Release` fails, and an `Open` or a second `NewLive` under it returns `ErrLocked`. A handle whose file failed to open stays usable: its calls return `ErrClosed` and `Err` says why, until an `Open` or a `Swap` opens it.

During a `Swap`, every call on the handle returns `ErrClosed` at once. `Swap` waits for the calls already running, then closes the file, runs your rename, reopens the file with the same schema and options, and runs the `OnOpen` hooks before it admits calls again. **The wait is cooperative:** no bbolt transaction reads a context inside its function, so `ctx` bounds only how long `Swap` waits and never cuts a running call short. When `ctx` ends first, `Swap` closes nothing, admits calls again and returns `ctx`'s error. A failed rename, open or hook leaves the handle closed with `Err` set. A `Swap` on a closed handle skips the close, so it can also replace a file that never opened.

A writer that runs on its own schedule should treat `ErrClosed` as a skipped write, not a failure. A size check reads `Space` through the handle, which answers the open file's sizes, or `ErrClosed` while no file is open or a swap is running. Work that reads in one transaction and writes in a later one takes `Pin` at its start and runs on that `*DB`: after a swap it returns `ErrClosed`, so nothing read from the old file lands in the new one.

`Options.Inspect` checks a stored file in a read-only transaction before `Open` resets, compacts or creates anything, and its error refuses the open with the file left as it was. A damaged page the check reads is `ErrInvalidFile`, which `Options.InvalidFile` handles like any unreadable file, while a panic in your own check reaches you. A check that finds a version stamp above the running build's own wraps `ErrNewerBuild`, which `OpenFailure` reports as `newer`.

## API

- `NewSchema`, `Schema.Namespace`, `Define`, `DefineIndex`, `DefineCounter`, `CascadePrefix` and `CascadeIndex` declare the layout. `Must` wraps them for package-level variables, and `Schema.Prefixes` lists the prefixes of the tables that record changes.
- `Open` runs the open procedure that `Options.LockWait`, `Lock`, `Reset`, `Compact`, `InvalidFile` and `Inspect` set, and `DB.OpenReport` tells what it did. `LockPath` takes the same lock for your own file operations, and `OpenReadOnly` returns a `*ReadDB`.
- `NewLive` returns a `*Live`, the swappable handle, with `OnOpen`, `Open`, `Err`, `Refuse`, `View`, `Update`, `Batch`, `Backup`, `Space`, `Epoch`, `Pin`, `Swap` and `Close`. `Epoch` stays the old file's until a `Swap` or `Close` has closed it, which waits for its transactions and their commit callbacks, and is zero while no file is open.
- `DB.Update`, `DB.Batch` and `DB.View` run transactions. `Table` has `Get`, `Put`, `Delete`, `Scan`, `NextID`, `Subject` and `Prefix`. `Index` has `Lookup` and `Scan`, and `Counter` has `Value`. A `Range` scans a prefix, a `[From, To)` span or one `Exact` key.
- `Tx.Mark`, `Tx.Touch` and `Tx.AfterCommit` add to a transaction's change, and `Synthetic` builds a subject that names no row. `ReadTx.Since` pages the feed and `ReadTx.Rev` reads its head. `Options.BeforeCommit` chooses what each change records.
- `DB.Verify`, `DB.Stats`, `DB.Space` and `DB.Backup` look after the file. `WriteHealth` records write outcomes, and `Options.CommitFault` lets a test fail chosen commits.
- `ErrFormat`, `ErrConflict`, `ErrCorrupt`, `ErrCommit`, `ErrPublished`, `ErrLocked`, `ErrInvalidFile`, `ErrReset`, `ErrClosed` and `ErrNewerBuild` are the errors a caller can match, with `*FormatError` and `*ConflictError` carrying the details. `DB.Close` waits for the writes in progress, their `OnCommit` and `AfterCommit` callbacks included, so a callback that closes its own DB, or closes or swaps a `Live` serving it, deadlocks, and so does one that opens or refuses that `Live` while it is being closed or swapped. After `DB.Close`, every call that returns an error returns one matching both `ErrClosed` and bbolt's `ErrDatabaseNotOpen`. `OpenFailure` maps an open error to its outage class.

The full reference is on [pkg.go.dev](https://pkg.go.dev/github.com/cplieger/bolttable).

## Contributing

Issues and pull requests are welcome. The [shared contributing rules](https://github.com/cplieger/.github/blob/main/CONTRIBUTING.md) apply.

## Disclaimer

This project is built with care and follows security best practices, but it is intended for personal / self-hosted use. No guarantees of fitness for production environments. Use at your own risk.

This project was built with AI assistance. The human maintainer defines architecture, supervises implementation, and makes all final decisions.

## License

Apache-2.0. See [LICENSE](LICENSE).
