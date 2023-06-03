# What is HEY?
	hey is an assistant tool similar to ORM to operate the database; its purpose is to allow you to perform addition, deletion, modification and query efficiently and quickly.

# Advantages of HEY

	hey does not limit you to use the database driver, you only need to provide the database connection pool instance *sql.DB

	hey only supports the execution of a single SQL statement, if you need to execute SQL scripts in batches, please use (*sql.DB).Exec()

	hey optional logging, you can use the CallTakeWarn() method to set the SQL execution time-consuming threshold, unit: milliseconds

	hey uses ? as the placeholder of the SQL statement by default, if your database instance does not support it, please use the Prepare() method to preprocess the SQL statement

	hey uses the tag-db value of the structure to match the query result field by default, if the tag does not match your business, you can use the Scanner() method to customize the matching rules

	hey supports transactions, and allows nested calls to facilitate you to care more about business implementation

	hey cleverly implements the conditional filtering of SQL statements, so you can easily use Filter to filter delete, update, query

	hey When querying SQL, you can choose to use Scanner[reflection implementation] and rows.Scan() to customize the scan query results, performance and speed are up to you

	Supports raw SQL execution by default, and raw SQL statements are valid in logs, transactions, Scanner, and SQL preprocessing


# INSTALL
```shell
go get github.com/cd365/hey@v1.0.0
```

# FOR EXAMPLE
```go
package main

import (
	"database/sql"

	"github.com/cd365/hey"
)

func way() {
	var db *sql.DB

	way := hey.NewWay(db)
	// way.Prepare(hey.PreparePostgresql) // PostgreSQL
	// way.Logger(nil).CallTakeWarn(10) // logger
	// way.Scanner(func(rows *sql.Rows, result interface{}) error {
	// 	return hey.ScanSliceStruct(rows, result, "database")
	// }) // custom scanner

	/* account table */
	account := way.Table("account")

	/* insert one */
	account.Insert(func(tmp *hey.Insert) {
		tmp.Field("name", "email").Value(
			[]interface{}{
				"Jack",
				"jack@gmail.com",
			})
	})

	/* insert batch */
	account.Insert(func(tmp *hey.Insert) {
		tmp.Field("name", "email").Value(
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
	})

	/* delete */
	account.Delete(func(tmp *hey.Delete) {
		tmp.Where(hey.NewFilter().In("id", 1, 2, 3))
		// tmp.Force()
	})

	/* update */
	account.Update(func(tmp *hey.Update) {
		tmp.Where(hey.NewFilter().In("id", 1, 2, 3))
		// tmp.Force()
		tmp.Incr("times", 1)
		tmp.Set("name", "Jerry")
	})

	/* select count */
	account.Select().Where(hey.NewFilter().IsNotNull("id")).Count()

	type ScanStruct struct {
		Name  string `db:"name"`
		Email string `db:"email"`
	}
	result := make([]*ScanStruct, 0)
	query := account.Select().
		Field("name", "email").
		Where(
			hey.NewFilter().
				MoreThan("id", 0),
		).
		Desc("id").
		Limit(10).
		Offset(0)
	query.Scan(&result)

	query.Get(func(rows *sql.Rows) (err error) {
		name := new(string)
		email := new(string)
		if err = rows.Scan(&name, &email); err != nil {
			return
		}
		// todo ...
		return nil
	})

	/* transaction */
	way.Transaction(func(tx *hey.Way) (err error) {
		account := tx.Table("account")
		account.Insert(func(tmp *hey.Insert) { /* todo... */ })
		account.Delete(func(tmp *hey.Delete) { /* todo... */ })
		account.Update(func(tmp *hey.Update) { /* todo... */ })
		account.Select() /* todo... */
		return
	})
}
```