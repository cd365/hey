package examples

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/cd365/hey/v6/cst"

	"github.com/cd365/hey/v6"
)

func TestDelete(t *testing.T) {
	ast := Assert(t)

	// WHERE
	tmp := way.Table(table1).Where(F().GreaterThan(id, 0))
	script := tmp.ToDelete()
	ast.Equal("DELETE FROM table1 WHERE ( id > ? )", script.Prepare)

	// IN subquery
	{
		tmp.WhereFunc(func(f hey.Filter) {
			f.ToEmpty().In(id, way.Table(table1).Select(id).Where(F().Equal(status, 1)).Desc(id).Limit(10))
		})
		script = tmp.ToDelete()
		ast.Equal("DELETE FROM table1 WHERE ( id IN ( SELECT id FROM table1 WHERE ( status = ? ) ORDER BY id DESC LIMIT 10 ) )", script.Prepare)
	}

	// WITH + subquery
	{
		tmp.ToEmpty()
		tmp.With("a", way.Table(table1).Select(id).Where(F().Equal(status, 1)).Desc(id).Limit(10))
		tmp.WhereFunc(func(f hey.Filter) {
			f.ToEmpty().In(id, way.Table("a").Select(id))
		})
		script = tmp.ToDelete()
		ast.Equal("WITH a AS ( SELECT id FROM table1 WHERE ( status = ? ) ORDER BY id DESC LIMIT 10 ) DELETE FROM table1 WHERE ( id IN ( SELECT id FROM a ) )", script.Prepare)
	}

	// JOIN
	{
		a, b := "a", "b"
		tmp = way.Table(table1).Alias(a).InnerJoin(func(j hey.SQLJoin) (left hey.SQLAlias, right hey.SQLAlias, assoc hey.SQLJoinAssoc) {
			right = j.Table(table2, b)
			assoc = j.OnEqual(field1, field2)
			return left, right, assoc
		})
		where := F().CompareEqual("a.field3", "b.field3").Equal("a.status", 0).In("a.id", 1, 2, 3)
		tmp.Where(where)
		script = tmp.ToDelete()
		ast.Equal("DELETE FROM table1 AS a INNER JOIN table2 AS b ON a.field1 = b.field2 WHERE ( ( a.field3 = b.field3 AND a.status = ? AND a.id IN ( ?, ?, ? ) ) )", script.Prepare)
	}
}

func TestInsert(t *testing.T) {
	ast := Assert(t)

	tmp := way.Table(table1)
	tmp.InsertFunc(func(i hey.SQLInsert) {
		// i.ToEmpty()
		i.ColumnValue(field1, "value1")
		i.ColumnValue(field2, "value2")
		i.ColumnValue(status, 1)
		// i.ColumnValue(createdAt, tmp.W().Now().Unix())
		i.Default(createdAt, tmp.W().Now().Unix())
	})
	script := tmp.ToInsert()
	ast.Equal("INSERT INTO table1 ( field1, field2, status, created_at ) VALUES ( ?, ?, ?, ? )", script.Prepare)

	// Insert a record and get the auto-incremented id value.
	{
		tmp.ToEmpty()
		tmp.InsertFunc(func(i hey.SQLInsert) {
			i.ToEmpty()
			i.Create(map[string]any{
				"username": "test001",
			})
			i.Returning(func(r hey.SQLReturning) {
				/* postgresql */
				r.Returning(id).Execute(func(ctx context.Context, stmt *hey.Stmt, args ...any) (id int64, err error) {
					err = stmt.QueryRow(ctx, func(row *sql.Row) error {
						return row.Scan(&id)
					}, args...)
					return id, err
				})

				/* mysql */
				// r.Execute(func(ctx context.Context, stmt *hey.Stmt, args ...any) (id int64, err error) {
				// 	result, err := stmt.Exec(ctx, args...)
				// 	if err != nil {
				// 		return 0, err
				// 	}
				// 	return result.LastInsertId()
				// })
			})
			script = tmp.ToInsert()
			ast.Equal("INSERT INTO table1 ( username ) VALUES ( ? ) RETURNING id", script.Prepare)
		})
	}

	// Insert multiple records.
	{

		type example struct {
			Username string `json:"username" db:"username"`
			Age      uint16 `json:"age" db:"age"`
			Other    any    `json:"other" db:"-"` // Ignore the current attribute when interacting with the database.
		}
		tmp.ToEmpty()
		tmp.InsertFunc(func(i hey.SQLInsert) {
			values := []*example{
				{
					Username: "test001",
					Age:      18,
				},
				{
					Username: "test002",
					Age:      18,
				},
				{
					Username: "test003",
					Age:      19,
				},
			}
			i.Forbid(id)
			i.Create(values)
		})
		script = tmp.ToInsert()
		ast.Equal("INSERT INTO table1 ( username, age ) VALUES ( ?, ? ), ( ?, ? ), ( ?, ? )", script.Prepare)
	}

	// More ways to call ...
}

func TestUpdate(t *testing.T) {
	ast := Assert(t)

	tmp := way.Table(table1)
	tmp.UpdateFunc(func(f hey.Filter, u hey.SQLUpdateSet) {
		// f.ToEmpty()
		// u.ToEmpty()
		f.GreaterThan(id, 0)
		u.Set(status, 1)
		u.Set(field1, "")
		u.Default(updatedAt, tmp.W().Now().Unix())
	})
	script := tmp.ToUpdate()
	ast.Equal("UPDATE table1 SET status = ?, field1 = ?, updated_at = ? WHERE ( id > ? )", script.Prepare)

	{
		tmp.ToEmpty()
		tmp.With("a", way.Table(table1).Select(id).Desc(id).Limit(10))
		tmp.UpdateFunc(func(f hey.Filter, u hey.SQLUpdateSet) {
			f.ToEmpty()
			u.ToEmpty()
			f.Equal(id, 1).OrGroup(func(g hey.Filter) {
				g.In(id, way.Table("a").Select(id))
			})

			u.Assign("column1", "column2")
			u.Compare("old-struct-pointer", "new-struct-pointer")
			u.Decr("num", 10)
			u.Incr("version_num", 1)
			u.Default(updatedAt, tmp.W().Now().Unix())
			u.Forbid(id, createdAt)
			u.Remove("username", "email")
			u.Select("age", "gender", "address")
			// u.Update(map[string]any{}) // Support map[string]any, *hey.SQL, hey.Maker, [*]AnyStruct.
		})
		script = tmp.ToUpdate()
		ast.Equal("WITH a AS ( SELECT id FROM table1 ORDER BY id DESC LIMIT 10 ) UPDATE table1 SET column1 = column2, num = num - ?, version_num = version_num + ?, updated_at = ? WHERE ( id = ? OR id IN ( SELECT id FROM a ) )", script.Prepare)
	}

	// More ways to call ...
}

func TestSelect(t *testing.T) {
	ast := Assert(t)

	tmp := way.Table(table1)
	script := tmp.ToSelect()
	ast.Equal("SELECT * FROM table1", script.Prepare)

	// ORDER BY xxx LIMIT n
	{
		tmp.ToEmpty()
		tmp.Asc(id).Limit(1)
		script = tmp.ToSelect()
		ast.Equal("SELECT * FROM table1 ORDER BY id ASC LIMIT 1", script.Prepare)
	}

	// OFFSET
	{
		tmp.ToEmpty()
		tmp.Asc(id).Limit(1).Offset(10)
		script = tmp.ToSelect()
		ast.Equal("SELECT * FROM table1 ORDER BY id ASC LIMIT 1 OFFSET 10", script.Prepare)
	}

	// PAGE
	{
		tmp.ToEmpty()
		tmp.Asc(id).Page(2, 10)
		script = tmp.ToSelect()
		ast.Equal("SELECT * FROM table1 ORDER BY id ASC LIMIT 10 OFFSET 10", script.Prepare)
	}

	// comment
	{
		tmp.ToEmpty()
		tmp.Comment("test comment").Asc(id).Page(2, 10)
		script = tmp.ToSelect()
		ast.Equal("/*test comment*/ SELECT * FROM table1 ORDER BY id ASC LIMIT 10 OFFSET 10", script.Prepare)
	}

	// SELECT columns
	{
		tmp.ToEmpty()
		tmp.Select(id, status).Asc(id).Limit(1)
		script = tmp.ToSelect()
		ast.Equal("SELECT id, status FROM table1 ORDER BY id ASC LIMIT 1", script.Prepare)
	}

	// GROUP BY
	{
		tmp.ToEmpty()
		tmp.Select(id).Group(id).GroupFunc(func(g hey.SQLGroupBy) {
			g.Having(func(having hey.Filter) {
				having.GreaterThanEqual(id, 0)
			})
		}).Asc(id).Limit(1)
		script = tmp.ToSelect()
		ast.Equal("SELECT id FROM table1 GROUP BY id HAVING ( id >= ? ) ORDER BY id ASC LIMIT 1", script.Prepare)
	}

	// DISTINCT
	{
		tmp.ToEmpty()
		tmp.Distinct().Select(id, status).Asc(id).Limit(1)
		script = tmp.ToSelect()
		ast.Equal("SELECT DISTINCT id, status FROM table1 ORDER BY id ASC LIMIT 1", script.Prepare)
	}

	// WITH
	{
		a := "a"
		c := table1
		tmpWith := way.Table(a).Comment("test1").WithFunc(func(w hey.SQLWith) {
			w.Set(
				a,
				way.Table(c).Select(id, "status").WhereFunc(func(f hey.Filter) {
					f.Equal("status", 1)
				}).Desc(id).Limit(10).ToSelect(),
			)
		}).Asc(id).Limit(1)
		script = tmpWith.ToSelect()
		ast.Equal("/*test1*/ WITH a AS ( SELECT id, status FROM table1 WHERE ( status = ? ) ORDER BY id DESC LIMIT 10 ) SELECT * FROM a ORDER BY id ASC LIMIT 1", script.Prepare)
	}

	// JOIN
	{
		a, b, c, e := "a", "b", "c", "e"
		where := way.F()
		get := way.Table(c).Alias(a)
		get.LeftJoin(func(j hey.SQLJoin) (left hey.SQLAlias, right hey.SQLAlias, assoc hey.SQLJoinAssoc) {
			right = j.Table(e, b)
			// j.PrefixSelect(j.GetTable(), cst.Asterisk)
			j.Select(
				j.Prefix(j.GetTable(), cst.Asterisk),
				hey.Alias(hey.Coalesce(j.Prefix(right, "first_name"), hey.SQLString("")), "first_name"), // string
				hey.Alias(j.Prefix(right, "last_name"), "last_name"),                                    // pointer string
			)
			assoc = j.OnEqual(id, "company_id")
			aid := j.Prefix(j.GetTable(), id)
			where.GreaterThan(aid, 0)
			get.Desc(aid)
			return left, right, assoc
		})
		get.Where(where)
		get.Limit(10).Offset(1)
		// count, err = get.Count(ctx)
		script = get.ToSelect()
		ast.Equal("SELECT a.*, COALESCE(b.first_name,'') AS first_name, b.last_name AS last_name FROM c AS a LEFT JOIN e AS b ON a.id = b.company_id WHERE ( a.id > ? ) ORDER BY a.id DESC LIMIT 10 OFFSET 1", script.Prepare)
	}

	// More ways to call ...
}

func TestTransaction(t *testing.T) {
	ast := Assert(t)
	err := (error)(nil)

	rows := int64(0)

	idEqual := func(idValue any) hey.Filter { return way.F().Equal(id, idValue) }
	modify := map[string]any{
		"salary": 1500,
	}

	delete3 := way.Table("example3").Where(idEqual(3))
	delete4 := way.Table("example4").Where(idEqual(4))

	ctx := context.Background()
	err = way.Transaction(ctx, func(tx *hey.Way) error {
		remove := tx.Table(table1).Where(idEqual(1))
		// _, _ = remove.Delete(ctx)
		script := remove.ToDelete()
		fmt.Println(script.Prepare)

		update := tx.Table(table2).Where(idEqual(1)).UpdateFunc(func(f hey.Filter, u hey.SQLUpdateSet) {
			u.Update(modify)
		})
		// _, _ = update.Update(ctx)
		script = update.ToDelete()
		fmt.Println(script.Prepare)

		if false {
			rows, err = tx.Execute(ctx, delete3.ToDelete())
			if err != nil {
				return err
			}
			if rows <= 0 {
				return hey.ErrNoRowsAffected
			}
			rows, err = delete4.V(tx).Delete(ctx)
			if err != nil {
				return err
			}
			if rows <= 0 {
				return hey.ErrNoRowsAffected
			}
		}

		return nil
	})
	ast.Equal(nil, err)

	// Custom handling of transaction.
	err = func() (err error) {
		tx := (*hey.Way)(nil)
		tx, err = way.Begin(ctx)
		if err != nil {
			return err
		}

		success := false

		defer func() {
			if !success {
				// for example:
				_ = tx.Rollback()
				if err == nil {
					// panic occurred in the database transaction.
					err = fmt.Errorf("%v", recover())
				}
			}
		}()

		/*
			This is where your business logic is handled.
		*/

		if err = tx.Commit(); err != nil {
			return err
		}

		success = true
		return err
	}()
	ast.Equal(nil, err)
}
