package bolttable_test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/cplieger/bolttable"
)

type user struct {
	Email string `json:"email"`
	Admin bool   `json:"admin"`
}

var (
	schema   = bolttable.NewSchema("app")
	accounts = bolttable.Must(schema.Namespace("accounts"))
	users    = bolttable.Must(bolttable.Define(accounts, 0x01, "users",
		bolttable.Uint64Key{}, bolttable.JSON[user](1), bolttable.TableOptions{}))
	byEmail = bolttable.Must(bolttable.DefineIndex(users, "by_email",
		bolttable.IndexOptions{Unique: true, Version: 1},
		func(_ uint64, u *user) []bolttable.Entry { return []bolttable.Entry{{Key: []byte(u.Email)}} }))
	admins = bolttable.Must(bolttable.DefineCounter(users, "admins", 1,
		func(_ uint64, u *user) int64 {
			if u.Admin {
				return 1
			}
			return 0
		}))
)

func Example() {
	dir, err := os.MkdirTemp("", "bolttable-example")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	db, err := bolttable.Open(filepath.Join(dir, "app.bolt"), schema, bolttable.Options{
		Retention: 1000,
		LockWait:  5 * time.Second,
		// A format change in a namespace the app can rebuild resets it,
		// keeping two copies of the file; any other one fails Open.
		Reset: bolttable.ResetPolicy{
			Allow: func(n bolttable.NamespaceDigest) bool { return n.Name == "cache" },
			Keep:  2,
		},
		OnCommit: func(c bolttable.Change) { fmt.Println("revision", c.Rev, "subjects", len(c.Subjects)) },
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	err = db.Update(ctx, func(tx *bolttable.Tx) error {
		id, err := users.NextID(tx)
		if err != nil {
			return err
		}
		tx.Touch(bolttable.Synthetic([]byte("signups")))
		return users.Put(tx, id, &user{Email: "ada@example.com", Admin: true})
	})
	if err != nil {
		log.Fatal(err)
	}

	err = db.View(ctx, func(r *bolttable.ReadTx) error {
		id, _, found, err := byEmail.Lookup(r, []byte("ada@example.com"))
		if err != nil || !found {
			return err
		}
		n, err := admins.Value(r)
		if err != nil {
			return err
		}
		fmt.Println("user", id, "admins", n)
		page, err := r.Since(0, 100)
		for _, c := range page.Changes {
			fmt.Printf("change %d: %d subjects, signups touched %v\n", c.Rev, len(c.Subjects), slices.ContainsFunc(c.Subjects, isSignups))
		}
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
	// Output:
	// revision 1 subjects 2
	// user 1 admins 1
	// change 1: 2 subjects, signups touched true
}

func isSignups(s bolttable.Subject) bool {
	return bytes.Equal(s, bolttable.Synthetic([]byte("signups")))
}
