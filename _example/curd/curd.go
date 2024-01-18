package main

import (
	"database/sql"
	"time"

	"github.com/cd365/hey"
)

var (
	db *sql.DB
)

func main() {

	way := hey.NewWay(
		db,
		hey.WithPrepare(hey.DefaultPgsql.Prepare),
	)

	table := "account"

	add := way.Add(table)

	/* insert one */
	add.FieldValue("name", "Jack").
		FieldValue("email", "jack@gmail.com").
		DefaultFieldValue("created_at", time.Now().Unix()).
		Add()

	/* insert batch */
	addFields := []string{
		"name",
		"email",
	}
	addValues := [][]interface{}{
		{
			"Alice",
			"alice@gmail.com",
		},
		{
			"Tom",
			"tom@gmail.com",
		},
		// ...
	}
	add.FieldsValues(addFields, addValues).Add()

	/* delete */
	del := way.Del(table)
	del.Where(
		way.F().In("id", 1, 2, 3),
	).Del()

	/* update */
	mod := way.Mod(table)
	mod.Where(
		way.F().In("id", 1, 2, 3),
	).
		Incr("times", 1).
		Set("name", "Jerry").
		DefaultSet("updated_at", time.Now().Unix()).
		Mod()

	/* select count */
	get := way.Get(table)
	get.Where(
		way.F().IsNotNull("id"),
	).Count()

	type ScanStruct struct {
		Name  string `db:"name"`
		Email string `db:"email"`
	}
	result := make([]*ScanStruct, 0)
	query := get.Column("name", "email").
		Where(way.F().Greater("id", 0)).
		Desc("id").
		Limit(10).
		Offset(0)
	query.Get(&result)

	query.Query(func(rows *sql.Rows) (err error) {
		name, email := "", ""
		pName, pEmail := &name, &email
		if err = rows.Scan(&pName, &pEmail); err != nil {
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
