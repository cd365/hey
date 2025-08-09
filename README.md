## What is hey?

Hey is a SQL building tool that helps you quickly generate commonly used SQL statements. <br>
For example: INSERT, DELETE, UPDATE, SELECT ...

## hey's mission
1. Support as many SQL general syntax as possible.
2. Write less or no original strings in business code, such as "username", "SUM(balance)", "id = ?", "SELECT id, name FROM your_table_name" ...
3. Try to avoid using reflection when scanning and querying data to reduce time consumption.
4. When the database table structure changes, your code will be able to feel it immediately.
5. When you implement a business, focus more on the business rather than on building SQL statements.
6. Allows the use of custom caches to reduce query request pressure on relational databases.
7. It (hey) can help you build complex SQL statements, especially complex query SQL statements.

## INSTALL
```shell
go get github.com/cd365/hey/v5@latest
```

## EXAMPLE
```go
package main

import (
	"database/sql"
	"os"
	"time"

	"github.com/cd365/hey/v5"
	"github.com/cd365/logger/v9"
	_ "github.com/go-sql-driver/mysql" /* Registering the database driver */
	_ "github.com/lib/pq"              /* Registering the database driver */
	_ "github.com/mattn/go-sqlite3"    /* Registering the database driver */
)

// Postgresql Connect to postgresql
func Postgresql() (*hey.Way, error) {
	db, err := sql.Open("postgres", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetConnMaxIdleTime(time.Minute * 3)
	db.SetMaxOpenConns(1000)
	db.SetMaxIdleConns(200)

	way := hey.NewWay(db)

	cfg := way.GetCfg()

	cfg.Manual = hey.Postgresql()
	cfg.Manual.Replacer = hey.NewReplacer() // Optional, You can customize it by using "table_or_column_name" instead of table_or_column_name

	cfg.DeleteMustUseWhere = true
	cfg.UpdateMustUseWhere = true
	cfg.TransactionMaxDuration = time.Second * 5
	cfg.WarnDuration = time.Millisecond * 200
	// cfg.TransactionOptions = &sql.TxOptions{Isolation: sql.LevelReadCommitted}

	way.SetLogger(logger.NewLogger(os.Stdout)) // Optional, Record SQL call log

	return way, nil
}

// Mysql Connect to mysql
func Mysql() (*hey.Way, error) {
	db, err := sql.Open("mysql", "root:root@tcp(localhost:3306)/mysql?charset=utf8mb4&collation=utf8mb4_unicode_ci&timeout=90s&multiStatements=true")
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetConnMaxIdleTime(time.Minute * 3)
	db.SetMaxOpenConns(1000)
	db.SetMaxIdleConns(200)

	way := hey.NewWay(db)

	cfg := way.GetCfg()

	cfg.Manual = hey.Mysql()
	cfg.Manual.Replacer = hey.NewReplacer() // Optional, You can customize it by using `table_or_column_name` instead of table_or_column_name

	cfg.DeleteMustUseWhere = true
	cfg.UpdateMustUseWhere = true
	cfg.TransactionMaxDuration = time.Second * 5
	cfg.WarnDuration = time.Millisecond * 200
	// cfg.TransactionOptions = &sql.TxOptions{Isolation: sql.LevelRepeatableRead}

	way.SetLogger(logger.NewLogger(os.Stdout)) // Optional, Record SQL call log

	return way, nil
}

// Sqlite Connect to sqlite
func Sqlite() (*hey.Way, error) {
	db, err := sql.Open("sqlite3", "my_database.db")
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetConnMaxIdleTime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(2)

	way := hey.NewWay(db)

	cfg := way.GetCfg()

	cfg.Manual = hey.Mysql()
	cfg.Manual.Replacer = hey.NewReplacer() // Optional, You can customize it by using `table_or_column_name` instead of table_or_column_name

	cfg.DeleteMustUseWhere = true
	cfg.UpdateMustUseWhere = true
	cfg.TransactionMaxDuration = time.Second * 5
	cfg.WarnDuration = time.Millisecond * 200
	// cfg.TransactionOptions = &sql.TxOptions{Isolation: sql.LevelReadCommitted}

	way.SetLogger(logger.NewLogger(os.Stdout)) // Optional, Record SQL call log

	return way, nil
}
```
