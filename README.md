# INSTALL
```shell
go get github.com/cd365/hey@latest
```

# FOR EXAMPLE
```go
package main

import (
	"database/sql"
	"time"

	"github.com/cd365/hey"
)

func main() {
	var db *sql.DB

	way := hey.NewWay(db)
	way.Fix = hey.FixPgsql // PostgreSQL

	table := "account"

	add := way.Add(table)

	/* insert one */
	add.Set("name", "Jack").
		Set("email", "jack@gmail.com").
		Default("created_at", time.Now().Unix()).
		Add()

	/* insert batch */
	add.Batch(
		map[string]interface{}{
			"name":  "Alice",
			"email": "alice@gmail.com",
		},
		map[string]interface{}{
			"name":  "Tom",
			"email": "tom@gmail.com",
		},
		// ...
	).Add()

	/* delete */
	del := way.Del(table)
	del.Where(
		way.Filter().In("id", 1, 2, 3),
	).Del()

	/* update */
	mod := way.Mod(table)
	mod.Where(
		way.Filter().
			In("id", 1, 2, 3),
	).
		Incr("times", 1).
		Set("name", "Jerry").
		SecSet("updated_at", time.Now().Unix()).
		Mod()

	/* select count */
	get := way.Get(table)
	get.Where(
		way.Filter().IsNotNull("id"),
	).Count()

	type ScanStruct struct {
		Name  string `db:"name"`
		Email string `db:"email"`
	}
	result := make([]*ScanStruct, 0)
	query := get.Column("name", "email").
		Where(way.Filter().MoreThan("id", 0)).
		Desc("id").
		Limit(10).
		Offset(0)
	query.Get(&result)

	query.Query(func(rows *sql.Rows) (err error) {
		name := new(string)
		email := new(string)
		if err = rows.Scan(&name, &email); err != nil {
			return
		}
		// todo ...
		return nil
	})

	/* transaction */
	way.TxTry(func(tx *hey.Way) (err error) {
		tx.Add(table).Add() // todo...
		tx.Del(table).Del() // todo...
		tx.Mod(table).Mod() // todo...
		tx.Get(table)       // todo...
		return
	}, 1)
}
```