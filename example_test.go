package bolttable_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

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
		OnCommit:  func(c bolttable.Change) { fmt.Println("revision", c.Rev, "subjects", len(c.Subjects)) },
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
		fmt.Println("user", id, "admins", n)
		return err
	})
	if err != nil {
		log.Fatal(err)
	}
	// Output:
	// revision 1 subjects 1
	// user 1 admins 1
}
