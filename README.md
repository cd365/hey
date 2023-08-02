# INSTALL
```shell
go get github.com/cd365/hey@latest
```

# FOR EXAMPLE
```go
package main

import (
	"database/sql"

	"github.com/cd365/hey"
)

func main() {
	var db *sql.DB

	way := hey.NewWay(db)
	way.Fix(hey.FixPgsql) // PostgreSQL

	table := "account"

	add := way.Add(table)

	/* insert one */
	add.Column("name", "email").Values([]interface{}{
		"Jack",
		"jack@gmail.com",
	}).Add()

	/* insert batch */
	add.Column("name", "email").Values(
		[]interface{}{
			"Alice",
			"alice@gmail.com",
		},
		[]interface{}{
			"Tom",
			"tom@gmail.com",
		},
		// ...
	)

	/* delete */
	del := way.Del(table)
	del.Where(hey.NewFilter().In("id", 1, 2, 3)).Del()

	/* update */
	mod := way.Mod(table)
	mod.Where(hey.NewFilter().In("id", 1, 2, 3)).Incr("times", 1).Set("name", "Jerry").Mod()

	/* select count */
	get := way.Get(table)
	get.Where(hey.NewFilter().IsNotNull("id")).Count()

	type ScanStruct struct {
		Name  string `db:"name"`
		Email string `db:"email"`
	}
	result := make([]*ScanStruct, 0)
	query := get.Column("name", "email").
		Where(hey.NewFilter().MoreThan("id", 0)).
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
	way.Trans(func(tx *hey.Way) (err error) {
		tx.Add(table).Add() // todo...
		tx.Del(table).Del() // todo...
		tx.Mod(table).Mod() // todo...
		tx.Get(table)       // todo...
		return
	})
}
```