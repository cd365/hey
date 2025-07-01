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

## INSTALL
```shell
go get github.com/cd365/hey/v3@latest
```

## How to use hey?
Here are some examples of use:

```go
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/cd365/hey/v3"
	"github.com/cd365/logger/v9"
	_ "github.com/go-sql-driver/mysql" /* Registering the database driver */
	_ "github.com/lib/pq"              /* Registering the database driver */
	_ "github.com/mattn/go-sqlite3"    /* Registering the database driver */
	"os"
	"sync"
	"time"
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
	cfg.Manual.Replace = hey.NewReplace() // Optional, You can customize it by using "table_or_column_name" instead of table_or_column_name

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
	cfg.Manual.Replace = hey.NewReplace() // Optional, You can customize it by using `table_or_column_name` instead of table_or_column_name

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
	cfg.Manual.Replace = hey.NewReplace() // Optional, You can customize it by using `table_or_column_name` instead of table_or_column_name

	cfg.DeleteMustUseWhere = true
	cfg.UpdateMustUseWhere = true
	cfg.TransactionMaxDuration = time.Second * 5
	cfg.WarnDuration = time.Millisecond * 200
	// cfg.TransactionOptions = &sql.TxOptions{Isolation: sql.LevelReadCommitted}

	way.SetLogger(logger.NewLogger(os.Stdout)) // Optional, Record SQL call log

	return way, nil
}

type ExampleAnyStruct struct {
	Name string `db:"name"` // Table column name
	Age  int    `db:"age"`  // Table column age
}

// Insert Inserting Data
func Insert(way *hey.Way) (affectedRowsOrLastInsertId int64, err error) {
	add := way.Add("your_table_name")

	// Timeout control can be set through the context.
	add.Context(context.Background())

	// Custom SQL statement comments.
	add.Comment("The first insert statement")

	common := func(add *hey.Add) {
		add.ExceptPermit(func(except hey.UpsertColumns, permit hey.UpsertColumns) {
			// Set the list of fields that are not allowed to be added through the `except` object.
			// Set the list of fields that can only be added through the `permit` object.
			except.Add("id", "deleted_at")
		}).Default(func(add *hey.Add) {
			now := way.Now()
			add.ColumnValue("created_at", now.Unix())
			add.ColumnValue("updated_at", now.Unix())
			add.ColumnValue("gender", "female")
		})
	}

	// Method A:
	{
		common(add) // Optional
		affectedRowsOrLastInsertId, err = add.Create(ExampleAnyStruct{}).Add()
		if err != nil {
			return
		}
	}

	// Method B:
	{
		// common(add) // Optional
		affectedRowsOrLastInsertId, err = add.Create(
			map[string]any{
				"your_column_name": "your_column_value",
			},
		).Add()
		if err != nil {
			return
		}
	}

	// Method C:
	{
		// common(add) // Optional
		lists := make([]*ExampleAnyStruct, 0)
		affectedRowsOrLastInsertId, err = add.Create(lists).Add()
		if err != nil {
			return
		}
	}

	// Method D:
	{
		cols := []string{"column1", "column2", "column3"}
		affectedRowsOrLastInsertId, err = add.CmderValues(way.Get("your_query_data_table").Select(cols...), cols).Add()
		if err != nil {
			return
		}
	}

	// Method E:
	{
		// mysql: insert one and get last insert id
		// common(add) // Optional
		affectedRowsOrLastInsertId, err = add.AddOne(func(add hey.AddOneReturnSequenceValue) {
			add.Execute(func(ctx context.Context, stmt *hey.Stmt, args []any) (sequenceValue int64, err error) {
				result, err := stmt.ExecuteContext(ctx, args)
				if err != nil {
					return 0, err
				}
				return result.LastInsertId()
			})
		})
		if err != nil {
			return
		}

		// postgresql: insert one and get last insert id
		// common(add) // Optional
		affectedRowsOrLastInsertId, err = add.AddOne(func(add hey.AddOneReturnSequenceValue) {
			add.Adjust(func(prepare string, args []any) (string, []any) {
				return fmt.Sprintf("%s RETURNING %s", prepare, "id"), args
			}).Execute(func(ctx context.Context, stmt *hey.Stmt, args []any) (sequenceValue int64, err error) {
				err = stmt.QueryRowContext(ctx, func(rows *sql.Row) error { return rows.Scan(&sequenceValue) }, args...)
				return
			})
		})
		if err != nil {
			return
		}
	}

	return affectedRowsOrLastInsertId, nil
}

func filterIdIn[T int | int64 | string](ids ...T) func(f hey.Filter) {
	return func(f hey.Filter) {
		// Type A
		f.In("ids", ids)
		// Type B
		f.In("ids", hey.ArrayToArray(ids, func(k int, v T) any { return v }))
	}
}

// Delete Deleting Data
func Delete(way *hey.Way) (affectedRows int64, err error) {
	del := way.Del("your_table_name")
	// del.Table("your_table_name")

	// Timeout control can be set through the context.
	del.Context(context.Background())

	// Custom SQL statement comments.
	del.Comment("The first delete statement")

	// Method A for WHERE:
	del.Where(func(f hey.Filter) { f.Equal("id", 1) })

	// Method B for WHERE:
	where := way.F()
	where.Equal("id", 1)
	del.Where(func(f hey.Filter) { f.Use(where) })

	// Method C for WHERE:
	del.Where(filterIdIn(1, 2, 3))

	// By default, the constructed SQL statements are output to the terminal for easy debugging.
	// way.Debug(del)

	return del.Del()
}

// Update Updating data
func Update(way *hey.Way) (affectedRows int64, err error) {
	mod := way.Mod("your_table_name")
	// mod.Table("your_table_name")

	// Timeout control can be set through the context.
	mod.Context(context.Background())

	// Custom SQL statement comments.
	mod.Comment("The first update statement")

	// Remember to set the conditional filter when executing the update statement.
	where := way.F().Equal("id", 1)
	where.Clean().Equal("id", 2)
	mod.Where(func(f hey.Filter) { f.Use(where) })

	mod.ExceptPermit(func(except hey.UpsertColumns, permit hey.UpsertColumns) {
		except.Add("id", "created_at", "deleted_at")
	})
	mod.Default(func(mod *hey.Mod) {
		now := mod.GetWay().Now()
		// now = way.Now()
		mod.Set("updated_at", now.Unix())
	})
	mod.Set("name", "Jack")
	mod.Incr("nums", 1)
	mod.Decr("nums", -1)

	return mod.Mod()
}

// Select Querying data
func Select(way *hey.Way) error {
	get := way.Get("your_table_name")
	// get.Table("your_table_name")

	// Timeout control can be set through the context.
	get.Context(context.Background())

	// Custom SQL statement comments.
	get.Comment("The first select statement")

	// Query a piece of data.
	one := &ExampleAnyStruct{}
	if err := get.Limit(1).Get(one); err != nil {
		if !errors.Is(err, hey.ErrNoRows) {
			return err
		}
	}
	// Query a piece of data in ascending order according to id.
	_ = get.Asc("id").Limit(1).Get(one)
	// Query a piece of data in descending order according to id.
	_ = get.Desc("id").Limit(1).Get(one)

	// Query multiple records
	more := make([]*ExampleAnyStruct, 0)
	_ = get.Asc("id").Limit(10).Get(&more)
	_ = get.Asc("id").Limit(10).Offset(10).Get(&more)

	// Specify a list of columns
	_ = get.Select("id", "name", "age").Limit(10).Get(&more)

	// Setting the WHERE condition
	_ = get.Where(func(f hey.Filter) {
		f.GreaterThan("id", 0)
		f.Equal("status", 1)
	}).Limit(10).Get(&more)

	// Setting complex WHERE conditions
	_ = get.Where(func(f hey.Filter) {
		// f.Not()
		f.Group(func(g hey.Filter) {
			g.Group(func(g hey.Filter) {
				g.Group(func(g hey.Filter) {
					g.Equal("role", "student")
					g.LessThanEqual("age", 18)
				})
				g.OrGroup(func(g hey.Filter) {
					g.Equal("gender", "female")
					g.Between("age", 12, 18)
				})
			})
			g.OrGroup(func(g hey.Filter) {
				g.Equal("privilege", "Y")
			})
		})
		f.Equal("status", 1)
		f.IsNull("is_delete")
	}).Limit(10).Get(&more)

	// Group By
	_ = get.Group("age").Having(func(f hey.Filter) { /* TODO */ }).Get(&more)

	// Join queries

	joinGroupBy := make([]string, 0)
	_ = get.Alias(hey.AliasA).Join(func(join hey.QueryJoin) {
		a := join.GetMaster()
		b := join.NewTable("inner_join_table_name_b", hey.AliasB)

		// Joining with subqueries
		// b = join.NewSubquery(way.Get(), hey.AliasB)

		// Special note: When the left table uses a nil value, the table name specified by the Get object will be used for connection by default.
		join.InnerJoin(nil, b, join.OnEqual("left_column_name_a", "right_column_name_b"))

		// Use the specified table as the left table.
		c := join.NewTable("inner_join_table_name_c", hey.AliasC)
		join.InnerJoin(b, c, join.OnEqual("left_column_name_b", "right_column_name_c"))

		/* When the names of the fields in two tables are the same, you can use the following abbreviations. */
		// join.InnerJoin(b, c, join.Using("user_id"))

		// Set the query columns list
		join.SelectGroupsColumns(
			join.TableSelect(a, "id"),
			join.TableSelect(b, "name"),
			join.TableSelect(c, "age"),
			/* ... */
		)
		join.SelectTableColumnAlias(a, "email", "admin_email")
		join.SelectTableColumnAlias(b, "email", "user_email")
		join.SelectTableColumnAlias(c, "score", "score_result")

		// Using a column list prefixed with a table alias in a GROUP BY statement.
		joinGroupBy = join.TableSelect(a, "id", "name")
	}).
		Where(func(f hey.Filter) {}).
		Desc("age").
		Group(joinGroupBy...).
		Limit(10).
		Get(&more)

	// Custom scan data.
	_ = get.ScanAll(func(rows *sql.Rows) error {
		tmp := &ExampleAnyStruct{}
		if err := rows.Scan(&tmp.Name /* ... */); err != nil {
			return err
		}
		more = append(more, tmp)
		return nil
	})

	// Subquery
	_ = get.Subquery(way.Get("table_name"), "a").
		Select("id", "name", "age").
		Where(func(f hey.Filter) {}).
		Desc("id").
		Limit(10).
		Get(&more)

	// Multiple columns comparison
	queried := make([]*ExampleAnyStruct, 0)
	_ = get.Where(func(f hey.Filter) {
		f.InCols(
			[]string{"name", "age"},
			hey.ColumnsInValues(queried, func(tmp *ExampleAnyStruct) []any { return []any{tmp.Name, tmp.Age} })...,
		)
	})

	// Quickly scan data into any structure, do not use reflect scan data.
	prepare, args := get.Select("name", "age").Limit(10).Desc("id").Cmd()
	more, _ = hey.RowsScanStructAll[ExampleAnyStruct](context.Background(), get.GetWay(), func(rows *sql.Rows, v *ExampleAnyStruct) error {
		return rows.Scan(&v.Name, &v.Age)
	}, prepare, args...)

	// Quickly scan a piece of data into any structure, do not use reflect scan data.
	prepare, args = get.Select("name", "age").Limit(1).Desc("id").Cmd()
	one, err := hey.RowsScanStructOne[ExampleAnyStruct](context.Background(), get.GetWay(), func(rows *sql.Rows, v *ExampleAnyStruct) error {
		return rows.Scan(&v.Name, &v.Age)
	}, prepare, args...)
	if err != nil {
		if !errors.Is(err, hey.ErrNoRows) {
			return err
		}
	}

	return nil
}

// Transaction Atomic batch processing
func Transaction(way *hey.Way) error {
	// Automatically commit or rollback the transaction by returning an error through the closure function.
	// The transaction will be committed only when the closure function is successfully called and the returned error value is nil.
	if false {
		return way.Transaction(nil, func(tx *hey.Way) error {
			rows, err := tx.Del("table1").Where(func(f hey.Filter) { f.Equal("id", 1) }).Del()
			if err != nil {
				return err
			}
			if rows == 0 {
				return errors.New("delete failed")
			}
			rows, err = tx.Mod("table2").Where(func(f hey.Filter) { f.Equal("id", 1) }).
				Incr("balance", 10.02).
				Incr("version", 1).
				Set("updated_at", tx.Now().Unix()).
				Mod()
			if err != nil {
				return err
			}
			if rows == 0 {
				return errors.New("update failed")
			}
			return nil
		}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	}

	// Manually manage transactions, beware of code panic.
	ctx, cancel := context.WithTimeout(context.Background(), way.GetCfg().TransactionMaxDuration)
	defer cancel()
	tx, err := way.Begin(ctx)
	if err != nil {
		return err
	}
	// defer tx.Rollback() // Want to give it a try?

	/* TODO ... Your business logic */

	// _ = tx.Commit()
	_ = tx.Rollback()
	return nil
}

// Transaction1 Minimize other code in transactions.
func Transaction1(way *hey.Way) error {
	add := way.Add("example_table_1").
		Create(map[string]interface{}{})
	del := way.Del("example_table_2").
		Where(func(f hey.Filter) {
			f.Equal("id", 1)
		})
	mod := way.Mod("example_table_3").
		Set("name", "Jerry").
		Where(func(f hey.Filter) {
			f.Equal("id", 1)
		})
	get := way.Get("example_table_4").
		Select("name", "age").
		Where(func(f hey.Filter) {
			f.Equal("id", 1)
		}).
		Limit(1).
		Desc("id")

	{
		// handle object add, del, mod, get ...
		// TODO ...
	}

	// Generate SQL statements first.
	prepare1, args1 := add.Cmd()
	prepare2, args2 := del.Cmd()
	prepare3, args3 := mod.Cmd()
	prepare4, args4 := get.Cmd()

	var had *ExampleAnyStruct

	err := way.Transaction(nil, func(tx *hey.Way) error {
		rows, err := tx.Exec(prepare1, args1...)
		if err != nil {
			return err
		}
		if rows == 0 {
			return errors.New("insert failed")
		}

		had, err = hey.RowsScanStructOne[ExampleAnyStruct](
			nil, tx,
			func(rows *sql.Rows, v *ExampleAnyStruct) error {
				return rows.Scan(&v.Name, &v.Age /*, .... */)
			},
			prepare4,
			args4...,
		)
		if err != nil {
			if errors.Is(err, hey.ErrNoRows) {
				// todo
				// return errors.New("record does not exists")
			} else {
				return err
			}
		}
		{
			// use had todo ...
			// Replace the parameters in the following SQL statement to be executed ?
			_ = had
		}

		rows, err = tx.Exec(prepare2, args2...)
		if err != nil {
			return err
		}
		if rows == 0 {
			return errors.New("delete failed")
		}

		rows, err = tx.Exec(prepare3, args3...)
		if err != nil {
			return err
		}
		if rows == 0 {
			return errors.New("update failed")
		}

		return nil
	})
	if err != nil {
		return err
	}

	// TODO other logic codes.

	return nil
}

type ExampleAnyStructUpdate struct {
	Id   int64   `json:"id" db:"id" validate:"required,min=1"`              // Table column id
	Name *string `json:"name" db:"name" validate:"omitempty,min=0,max=255"` // Table column name
	Age  *int    `json:"age" db:"age" validate:"omitempty,min=0,max=150"`   // Table column age
}

// Others Use cases.
func Others(way *hey.Way) error {

	// UNION, UNION ALL, EXCEPT, INTERSECT
	{
		get1 := way.Get("table1").Limit(10)
		get2 := way.Get("table2").Limit(10)
		get3 := way.Get("table3").Limit(10)

		get := hey.UnionAllCmder(get1, get2, get3)
		// get = hey.UnionCmder(get1, get2, get3)
		// get = hey.ExceptCmder(get1, get2, get3)
		// get = hey.IntersectCmder(get1, get2, get3)
		lists := make([]*ExampleAnyStruct, 0)
		err := way.Get().Subquery(get, hey.AliasA).Get(&lists)
		if err != nil {
			return err
		}
	}

	// Update any structure with tag.
	{
		update := &ExampleAnyStructUpdate{}
		_, err := way.Mod("table_name").ExceptPermit(func(except hey.UpsertColumns, permit hey.UpsertColumns) {
			except.Add("id", "created_at", "deleted_at")
			// permit.Add("name", "age")
		}).Update(update).Mod()
		if err != nil {
			return err
		}
	}

	// Update by comparing the property values of the structure.
	{
		origin := &ExampleAnyStruct{}
		update := &ExampleAnyStructUpdate{}
		_, err := way.Mod("table_name").ExceptPermit(func(except hey.UpsertColumns, permit hey.UpsertColumns) {
			except.Add("id", "created_at", "deleted_at")
			// permit.Add("name", "age")
		}).Compare(origin, update).Mod()
		if err != nil {
			return err
		}
	}

	// Select GROUP BY
	{
		t1 := way.TA()
		get := way.Get("table_name").Alias(t1.Alias())
		get.GetSelect().AddAll("uid", t1.SUM("balance", "balance"))
		get.Group("uid").Having(func(f hey.Filter) {
			f.GreaterThanEqual(t1.SUM("balance"), 100)
		})
		_ = get.Get(nil)
	}

	// Update the same column for multiple records.
	{
		// When you need to update the following data at the same time.
		// UPDATE account SET name = 'Alice', age = 18 WHERE ( id = 1 )
		// UPDATE account SET name = 'Bob', age = 20 WHERE ( id = 2 )
		// UPDATE account SET name = 'Tom', age = 21 WHERE ( id = 3 )
		ctx, cancel := context.WithTimeout(context.Background(), way.GetCfg().TransactionMaxDuration)
		defer cancel()

		args := [][]any{
			{"Alice", 18, 1},
			{"Bob", 20, 2},
			{"Tom", 21, 3},
		}
		mod := way.Mod("account").Set("name", "").Set("age", 0).Where(func(f hey.Filter) { f.Equal("id", 1) })
		prepare, _ := mod.Cmd()

		// Efficiently re-execute the same SQL statement by simply changing the parameters.
		affectedRows, err := way.BatchUpdateContext(ctx, prepare, args)
		if err != nil {
			return err
		}
		_ = affectedRows
	}

	return nil
}

// UsingCache Using cache query data.
func UsingCache(way *hey.Way, cacher hey.Cacher, keyLock *hey.KeyLock) (data []*ExampleAnyStruct, err error) {
	// The cacher is usually implemented in Memcached, Redis, current program memory, or even files.
	cache := hey.NewCache(cacher)

	get := way.Get("your_table_name").Select("name", "age").Desc("id").Limit(20).Offset(0)

	cacheCmder := hey.NewCacheCmder(cache, get)

	data = make([]*ExampleAnyStruct, 0)

	exists, err := cacheCmder.GetUnmarshal(&data)
	if err != nil {
		return nil, err
	}

	if exists {
		// The data has been obtained from the cache and no longer needs to be queried from the database.
		return data, nil
	}

	// Prepare to query data from the database.
	cacheKey, _ := cacheCmder.GetCacheKey()

	mutex := keyLock.Get(cacheKey)

	mutex.Lock()
	defer mutex.Unlock()

	exists, err = cacheCmder.GetUnmarshal(&data)
	if err != nil {
		return nil, err
	}

	if exists {
		// The data has been obtained from the cache and no longer needs to be queried from the database.
		return data, nil
	}

	// Querying data from database.
	if err = get.Get(&data); err != nil {
		return nil, err
	}

	// Cache query data.
	if err = cacheCmder.MarshalSet(data, cache.DurationRange(time.Second, 7, 9)); err != nil {
		return nil, err
	}

	return data, nil
}
```