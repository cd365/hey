package hey

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

/* INSERT SQL */
func TestNewAdd(t *testing.T) {

	ast := assert.New(t)
	way := testWay()

	add := way.Add("example")
	add.ColumnValue("username", "Alice")
	add.ColumnValue("email", "example@gmail.com")
	add.ColumnValue("age", 21)
	ast.Equal("INSERT INTO example ( username, email, age ) VALUES ( ?, ?, ? )", add.ToSQL().Prepare, equalMessage)

	// example INSERT ONE AND RETURNING id value
	if way.GetDatabase() != nil {
		lastInsertId, err := add.AddOne(func(addOne AddOneReturnSequenceValue) {
			// MySQL or Sqlite
			addOne.Execute(func(ctx context.Context, stmt *Stmt, args ...any) (sequenceValue int64, err error) {
				result, err := stmt.ExecuteContext(ctx, args...)
				if err != nil {
					return 0, err
				}
				return result.LastInsertId()
			})

			// PostgreSQL
			addOne.Prepare(func(tmp *SQL) {
				tmp.Prepare = fmt.Sprintf("%s RETURNING %s", tmp.Prepare, "id")
			}).Execute(func(ctx context.Context, stmt *Stmt, args ...any) (sequenceValue int64, err error) {
				result, err := stmt.ExecuteContext(ctx, args...)
				if err != nil {
					return 0, err
				}
				return result.RowsAffected()
			})
		})
		_, _ = lastInsertId, err
	}

	{
		add = way.Add("example")
		columns := []string{"username", "email", "age"}
		values := [][]any{
			{
				"Alice", "example@gmail.com", 21,
			},
		}
		add.ColumnsValues(columns, values)
		ast.Equal("INSERT INTO example ( username, email, age ) VALUES ( ?, ?, ? )", add.ToSQL().Prepare, equalMessage)
		values = append(values, []any{
			"Bob", "example@yahoo.com", 21,
		})
		add.ColumnsValues(columns, values)
		ast.Equal("INSERT INTO example ( username, email, age ) VALUES ( ?, ?, ? ), ( ?, ?, ? )", add.ToSQL().Prepare, equalMessage)
	}

	{
		add = way.Add("example")
		// When using the map[string]any type, the order of column names may not be determined when inserting multiple columns of data.
		add.Create(map[string]any{
			"username": "Alice",
		})
		ast.Equal("INSERT INTO example ( username ) VALUES ( ? )", add.ToSQL().Prepare, equalMessage)

		add = way.Add("example")
		create := map[string]any{
			"username": "Alice",
			"email":    "example@gmail.com",
			"age":      21,
		}
		columns := make([]string, 0, len(create))
		values := make([]any, 0, len(create))
		for column := range create {
			columns = append(columns, column)
		}
		sort.Strings(columns)
		for _, column := range columns {
			values = append(values, create[column])
		}
		add.ColumnsValues(columns, [][]any{values})
		ast.Equal("INSERT INTO example ( age, email, username ) VALUES ( ?, ?, ? )", add.ToSQL().Prepare, equalMessage)
	}

	{
		type User struct {
			Id       int64  `json:"id" db:"id"`
			Email    string `json:"email" db:"email"`
			Username string `json:"username" db:"username"`
			Age      int    `json:"age" db:"age"`
		}
		user := User{
			Email:    "example@gmail.com",
			Username: "Alice",
			Age:      21,
		}
		add = way.Add("example")
		add.ExceptPermit(func(except SQLUpsertColumn, permit SQLUpsertColumn) {
			except.Add("id")
		})
		add.Create(user)
		ast.Equal("INSERT INTO example ( email, username, age ) VALUES ( ?, ?, ? )", add.ToSQL().Prepare, equalMessage)

		{
			add = way.Add("example")
			add.ExceptPermit(func(except SQLUpsertColumn, permit SQLUpsertColumn) {
				except.Add("id")
			})
			add.Create(&user)
			ast.Equal("INSERT INTO example ( email, username, age ) VALUES ( ?, ?, ? )", add.ToSQL().Prepare, equalMessage)
		}

		{
			users := make([]*User, 0)
			users = append(users, &user)
			add = way.Add("example")
			add.ExceptPermit(func(except SQLUpsertColumn, permit SQLUpsertColumn) {
				except.Add("id")
			})
			add.Create(users)
			ast.Equal("INSERT INTO example ( email, username, age ) VALUES ( ?, ?, ? )", add.ToSQL().Prepare, equalMessage)

			{
				add = way.Add("example")
				add.ExceptPermit(func(except SQLUpsertColumn, permit SQLUpsertColumn) {
					except.Add("id")
				})
				// Setting default column-value.
				add.Default(func(add *Add) {
					timestamp := way.Now().Unix()
					add.ColumnValue("created_at", timestamp)
					add.ColumnValue("updated_at", timestamp)
				})
				users = append(users, &User{
					Email:    "Bob",
					Username: "example@yahoo.com",
					Age:      21,
				})
				users = append(users, &User{
					Email:    "Jeery",
					Username: "example@yahoo.com",
					Age:      21,
				})
				add.Create(users)
				ast.Equal("INSERT INTO example ( email, username, age, created_at, updated_at ) VALUES ( ?, ?, ?, ?, ? ), ( ?, ?, ?, ?, ? ), ( ?, ?, ?, ?, ? )", add.ToSQL().Prepare, equalMessage)
			}
		}
	}

	{
		add = way.Add("example")
		add.MakerValues(NewSQL("SELECT email, username, age FROM example WHERE ( status = 1 )"), []string{"email", "username", "age"})
		ast.Equal("INSERT INTO example ( email, username, age ) SELECT email, username, age FROM example WHERE ( status = 1 )", add.ToSQL().Prepare, equalMessage)
	}

}

/* DELETE SQL */
func TestNewDel(t *testing.T) {

	ast := assert.New(t)
	way := testWay()

	del := way.Del("example")
	// By default, no delete statement is constructed if no WHERE condition is set.
	ast.Equal(EmptyString, del.ToSQL().Prepare, equalMessage)
	{
		way.cfg.DeleteMustUseWhere = false
		ast.Equal("DELETE FROM example", del.ToSQL().Prepare, equalMessage)
		way.cfg.DeleteMustUseWhere = true
	}

	del.Comment("Here is the comment to delete the SQL statement")
	del.Where(func(f Filter) {
		f.Equal("id", 3)
	})
	ast.Equal("/*Here is the comment to delete the SQL statement*/DELETE FROM example WHERE ( id = ? )", del.ToSQL().Prepare)

}

/* UPDATE SQL */
func TestNewMod(t *testing.T) {

	ast := assert.New(t)
	way := testWay()

	mod := way.Mod("example")
	mod.Set("status", 0)
	// By default, no update statement is constructed if no WHERE condition is set.
	ast.Equal(EmptyString, mod.ToSQL().Prepare, equalMessage)
	{
		way.cfg.DeleteMustUseWhere = false
		ast.Equal("UPDATE example SET status = ?", mod.ToSQL().Prepare, equalMessage)
		way.cfg.DeleteMustUseWhere = true
	}

	mod.Where(func(f Filter) {
		f.Equal("id", 3)
	})
	ast.Equal("UPDATE example SET status = ? WHERE ( id = ? )", mod.ToSQL().Prepare)

	f := way.F().Equal("id", 1)

	{
		mod = way.Mod("example")
		mod.Filter(f)
		mod.Incr("times", 1)
		ast.Equal("UPDATE example SET times = times + ? WHERE ( id = ? )", mod.ToSQL().Prepare)
		// Setting default update column-value.
		mod.Default(func(mod *Mod) {
			mod.Decr("num", 1)
			mod.Set("updated_at", way.Now().Unix())
		})
		ast.Equal("UPDATE example SET times = times + ?, num = num - ?, updated_at = ? WHERE ( id = ? )", mod.ToSQL().Prepare)
	}

	{
		mod = way.Mod("example")
		mod.Filter(f)
		// When using the map[string]any type, the order of column names may not be determined when updating multiple columns of data.
		mod.Update(map[string]any{
			"status": 1,
		})
		ast.Equal("UPDATE example SET status = ? WHERE ( id = ? )", mod.ToSQL().Prepare)
	}

	{
		type User struct {
			Id      int64   `json:"id" db:"id"`
			Email   *string `json:"email" db:"email"`
			Summary *string `json:"summary" db:"summary"`
			Age     *int    `json:"age" db:"age"`
		}
		update := &User{
			Id: 1,
		}
		public := func(mod *Mod) {
			mod.ExceptPermit(func(except SQLUpsertColumn, permit SQLUpsertColumn) {
				except.Add("id", "created_at")
			})
			mod.Default(func(mod *Mod) {
				mod.Set("updated_at", way.Now().Unix())
			})
		}
		mod = way.Mod("example").Filter(f)
		public(mod)
		email := "example@gmail.com"
		update.Email = &email
		mod.Update(update)
		ast.Equal("UPDATE example SET updated_at = ?, email = ? WHERE ( id = ? )", mod.ToSQL().Prepare)

		mod = way.Mod("example").Filter(f)
		public(mod)
		summary := "example-value"
		update.Summary = &summary
		mod.Update(update)
		ast.Equal("UPDATE example SET updated_at = ?, email = ?, summary = ? WHERE ( id = ? )", mod.ToSQL().Prepare)
	}

	{
		type User struct {
			Id        int64  `json:"id" db:"id"`
			Email     string `json:"email" db:"email"`
			Username  string `json:"username" db:"username"`
			Age       int    `json:"age" db:"age"`
			CreatedAt int64  `json:"created_at" db:"created_at"`
			UpdatedAt int64  `json:"updated_at" db:"updated_at"`
		}
		mod = way.Mod("example").Filter(f)
		old := &User{
			Id:        1,
			Email:     "example@gmail.com",
			Username:  "Alice",
			Age:       21,
			CreatedAt: 1701234567,
			UpdatedAt: 1701234567,
		}
		tmp := *old
		too := &tmp
		too.Email = "example@yahoo.com"
		too.UpdatedAt = way.Now().Unix()
		mod.Compare(old, too, "id", "created_at")
		ast.Equal("UPDATE example SET email = ?, updated_at = ? WHERE ( id = ? )", mod.ToSQL().Prepare)

		{
			too.Age = 25
			mod = way.Mod("example").Filter(f)
			mod.Compare(old, too, "id", "created_at")
			ast.Equal("UPDATE example SET email = ?, age = ?, updated_at = ? WHERE ( id = ? )", mod.ToSQL().Prepare)
		}
	}

}

/* SELECT SQL */
func TestNewGet(t *testing.T) {

	ast := assert.New(t)
	way := testWay()

	get := (*Get)(nil)

	{
		get = way.Get("example")
		ast.Equal("SELECT * FROM example", get.ToSQL().Prepare)
	}

	{
		get = way.Get().Table("example")
		get.Select("id", "username", "age")
		ast.Equal("SELECT id, username, age FROM example", get.ToSQL().Prepare)
	}

	{
		get = way.Get().Table("example")
		get.Select("id", "username", "age")
		get.Desc("id")
		ast.Equal("SELECT id, username, age FROM example ORDER BY id DESC", get.ToSQL().Prepare)
	}

	{
		get = way.Get().Table("example")
		get.Select("id", "username", "age")
		get.Desc("id")
		get.Limit(10)
		ast.Equal("SELECT id, username, age FROM example ORDER BY id DESC LIMIT 10", get.ToSQL().Prepare)
	}

	{
		get = way.Get().Table("example")
		get.Select("id", "username", "age")
		get.Desc("id")
		get.Limit(10)
		get.Offset(10)
		ast.Equal("SELECT id, username, age FROM example ORDER BY id DESC LIMIT 10 OFFSET 10", get.ToSQL().Prepare)
	}

	{
		get = way.Get().Table("example")
		ast.Equal("SELECT COUNT(*) AS counts FROM example", get.CountSQL().Prepare)
	}

	{
		get = way.Get().Table("example").Where(func(f Filter) {
			f.MoreThanEqual("id", 100000)
		})
		ast.Equal("SELECT COUNT(*) AS counts FROM example WHERE ( id >= ? )", get.CountSQL().Prepare)
	}

	{
		get = way.Get().Table("example")
		get.Select("age", "COUNT(*) AS counts")
		get.Group("age")
		ast.Equal("SELECT age, COUNT(*) AS counts FROM example GROUP BY age", get.ToSQL().Prepare)
	}

	whereA := way.F().Equal("status", 0).Equal("deleted_at", 0)
	whereB := way.F().Between("age", 10, 20).MoreThan("score", 85)

	selectA := way.Get("example").Filter(whereA, whereB)
	selectB := way.Get("example").Where(func(f Filter) { f.Use(whereA).OrGroup(func(f Filter) { f.Use(whereB) }) })

	{
		get = way.Get().With(func(w SQLWith) {
			w.Set(AliasA, selectA)
			w.Set(AliasB, selectB)
		}).Table(AliasA).Where(func(f Filter) {
			ta, tb := way.TA(), way.TB()
			f.CompareMoreThan(ta.Column("id"), tb.Column("id"))
		})
		get.Comment("comment example")
		ast.Equal("/*comment example*/WITH a AS ( SELECT * FROM example WHERE ( ( status = ? AND deleted_at = ? ) AND ( age BETWEEN ? AND ? AND score > ? ) ) ), b AS ( SELECT * FROM example WHERE ( ( status = ? AND deleted_at = ? ) OR ( age BETWEEN ? AND ? AND score > ? ) ) ) SELECT * FROM a WHERE ( a.id > b.id )", get.ToSQL().Prepare)
	}

	{
		ta := way.TA()
		tb := way.TB()
		/* ON table1.column1 = table2.column2 */
		get = way.Get().Table("example").Alias(ta.Alias()).Join(func(j SQLJoin) {
			b := j.NewTable("table2", tb.Alias())
			j.InnerJoin(nil, b, j.OnEqual("id", "id"))
		})
		get.Select(ta.ColumnAll("id", "username", "age", "created_at", "updated_at")...)
		get.Desc(ta.Column("id"))
		get.Limit(10).Offset(0)
		ast.Equal("SELECT a.id, a.username, a.age, a.created_at, a.updated_at FROM example AS a INNER JOIN table2 AS b ON a.id = b.id ORDER BY a.id DESC LIMIT 10", get.ToSQL().Prepare)

		/* USING ( column1, column2 ... ) */
		joinGroupByColumns := make([]string, 0)
		get = way.Get().Table("example").Alias(ta.Alias()).Join(func(j SQLJoin) {
			b := j.NewTable("table2", tb.Alias())
			j.InnerJoin(j.GetMaster(), b, j.Using("id", "username"))

			if false {
				a := j.GetMaster()
				j.SelectGroupsColumns(
					j.TableSelect(a, "id", "name", "username"),
					/* ... */
				)
				j.SelectTableColumnAlias(b, "email", "user_email")
				joinGroupByColumns = j.TableSelect(a, "username")
				joinGroupByColumns = append(joinGroupByColumns, j.TableSelect(b, "activity_id")...)
			}
		})
		if false {
			get.Group(joinGroupByColumns...)
		}
		get.Select(ta.ColumnAll("id", "username", "age", "created_at", "updated_at")...)
		get.Desc(ta.Column("id"))
		get.Limit(10).Offset(0)
		ast.Equal("SELECT a.id, a.username, a.age, a.created_at, a.updated_at FROM example AS a INNER JOIN table2 AS b USING ( id, username ) ORDER BY a.id DESC LIMIT 10", get.ToSQL().Prepare)

		/* ON ( table1.column1 = table2.column2 AND table2.column3 = 1 AND table2.column4 IS NOT NULL ... ) */
		get = way.Get().Table("example").Alias(ta.Alias()).Join(func(j SQLJoin) {
			b := j.NewTable("table2", tb.Alias())
			j.LeftJoin(nil, b, j.On(func(o SQLJoinOn, leftAlias string, rightAlias string) {
				o.Equal(ta.Alias(), "id", tb.Alias(), "id")
				o.Filter(func(f Filter) {
					f.Equal(tb.Column("status"), 0)
				})
			}))
		})
		get.Select(ta.ColumnAll("id", "username", "age", "created_at", "updated_at")...)
		get.Desc(ta.Column("id"))
		get.Limit(10).Offset(0)
		ast.Equal("SELECT a.id, a.username, a.age, a.created_at, a.updated_at FROM example AS a LEFT JOIN table2 AS b ON ( a.id = b.id AND b.status = ? ) ORDER BY a.id DESC LIMIT 10", get.ToSQL().Prepare)
	}

	{
		/* SELECT * FROM subquery ... */
		get = way.Get().Subquery(selectA, AliasA).Desc("id").Limit(10).Offset(10)
		ast.Equal("SELECT * FROM ( SELECT * FROM example WHERE ( ( status = ? AND deleted_at = ? ) AND ( age BETWEEN ? AND ? AND score > ? ) ) ) AS a ORDER BY id DESC LIMIT 10 OFFSET 10", get.ToSQL().Prepare)
	}

	if way.GetDatabase() != nil {
		others(way)
	}

}

func others(way *Way) error {
	type ExampleAnyStruct struct {
		Name string `db:"name"` // Table column name
		Age  int    `db:"age"`  // Table column age
	}
	type ExampleAnyStructUpdate struct {
		Id   int64   `json:"id" db:"id" validate:"required,min=1"`              // Table column id
		Name *string `json:"name" db:"name" validate:"omitempty,min=0,max=255"` // Table column name
		Age  *int    `json:"age" db:"age" validate:"omitempty,min=0,max=150"`   // Table column age
	}

	// UNION, UNION ALL, EXCEPT, INTERSECT
	{
		get1 := way.Get("table1").Limit(10)
		get2 := way.Get("table2").Limit(10)
		get3 := way.Get("table3").Limit(10)
		get := UnionAllMaker(get1, get2, get3)
		// get = UnionMaker(get1, get2, get3)
		// get = ExceptMaker(get1, get2, get3)
		// get = IntersectMaker(get1, get2, get3)
		lists := make([]*ExampleAnyStruct, 0)
		err := way.Get().Subquery(get, AliasA).Get(&lists)
		if err != nil {
			return err
		}
	}

	// Update any structure with tag.
	{
		update := &ExampleAnyStructUpdate{}
		_, err := way.Mod("table_name").ExceptPermit(func(except SQLUpsertColumn, permit SQLUpsertColumn) {
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
		_, err := way.Mod("table_name").ExceptPermit(func(except SQLUpsertColumn, permit SQLUpsertColumn) {
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
		get.Group("uid").Having(func(f Filter) {
			f.MoreThanEqual(t1.SUM("balance"), 100)
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
		mod := way.Mod("account").Set("name", "").Set("age", 0).Where(func(f Filter) { f.Equal("id", 1) })
		script := mod.ToSQL()

		// Efficiently re-execute the same SQL statement by simply changing the parameters.
		affectedRows, err := way.BatchUpdateContext(ctx, script.Prepare, args)
		if err != nil {
			return err
		}
		_ = affectedRows
	}
	return nil
}
