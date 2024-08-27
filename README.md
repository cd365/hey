## What is hey?

Hey is a SQL building tool that helps you quickly generate commonly used SQL statements. <br>
For example: INSERT, DELETE, UPDATE, SELECT ...

## INSTALL
```shell
go get github.com/cd365/hey@latest
```

## EXAMPLE
```go
package heyexample

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/cd365/hey"
	model "github.com/cd365/hey/_example"
	"github.com/cd365/hey/pgsql"
	_ "github.com/lib/pq"
)

var (
	t   = model.NewMember()
	db  *sql.DB
	way *hey.Way
)

func init() {
	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/hey",
		os.Getenv("PGSQL_USERNAME"),
		os.Getenv("PGSQL_PASSWORD"),
		os.Getenv("PGSQL_HOST"),
		os.Getenv("PGSQL_PORT"),
	)
	dbc, err := sql.Open("postgres", dsn)
	if err != nil {
		panic(err)
	}
	db = dbc
	opts := []hey.Opts{
		hey.WithPrepare(pgsql.Prepare),
	}
	opts = append(opts, hey.WithLogger(func(log *hey.LogSQL) {
		if log.Args == nil {
			return
		}
		query := pgsql.PrepareString(log.Prepare, log.Args.Args)
		costs := log.Args.EndAt.Sub(log.Args.StartAt)
		fmt.Println(query, costs.String())
	}))
	way = hey.NewWay(db, opts...)
}

func Insert() {
	fields := []string{t.MemberId, t.Username, t.Email, t.Mobile, t.Nickname}
	values := []any{time.Now().UnixNano(), "username1", "email1@gmail.com", "123123123", "nickname1"}
	{
		create := &model.AddMember{
			MemberId: time.Now().UnixNano(),
			Username: "Alice",
		}
		permit := fields[:]
		permit = append(permit, t.AddAt, t.ModAt)
		now := time.Now()
		_, err := way.Add(t.Table()).
			Context(context.TODO()).
			Comment("Add 1.").
			Except(t.Id).
			// Except(t.ModAt).
			Permit(permit...).
			Create(create).
			FieldValue(t.Category, 123).
			Default(func(a *hey.Add) { a.FieldValue(t.AddAt, now.Unix()).FieldValue(t.ModAt, now.Unix()) }).
			Add()
		if err != nil {
			panic(err)
		}
	}
	{
		create := map[string]any{
			t.MemberId: time.Now().UnixNano(),
			t.Username: "Alice",
			t.Nickname: "777",
			t.AddAt:    time.Now().Unix(),
		}
		permit := fields[:]
		permit = append(permit, t.AddAt)
		_, err := way.Add(t.Table()).
			Context(context.TODO()).
			Comment("Add 2.").
			Except(t.Nickname).
			Permit(permit...).
			Create(create).
			Add()
		if err != nil {
			panic(err)
		}
	}
	{
		add := way.Add(t.Table()).
			Comment("Add 3.").
			Except(t.Id).
			FieldsValues(fields, [][]any{values})
		_, err := add.Add()
		if err != nil {
			panic(err)
		}
	}
	{
		now := time.Now()
		if !way.TxNil() {
			now = way.Now()
		}
		add := way.Add(t.Table())
		add.Comment("Add 4.")
		add.FieldValue(t.MemberId, time.Now().UnixNano())
		add.FieldValue(t.Username, "Alice")
		add.FieldValue(t.AddAt, now.Unix())
		add.FieldValue(t.ModAt, now.Unix())
		_, err := add.Add()
		if err != nil {
			panic(err)
		}
	}
	{

		fields0 := fields[:]
		fields1 := []string{
			"CASE WHEN member_id > 0 THEN member_id + 10 ELSE member_id END AS member_id",
			t.Username,
			t.Email,
			t.Mobile,
			t.Nickname,
		}
		add := way.Add(t.Table()).
			Comment("Add 5.")
		get := way.Get(t.Table()).
			Column(fields1...).
			// Where(way.F().Between(t.MemberId, 10000, 10010)).
			Desc(t.Id).Limit(10)
		add.ValuesSubQueryGet(get, fields0...)
		add.OnConflict(
			pgsql.InsertOnConflict(
				[]string{t.MemberId},
				fields0,
			))
		_, err := add.Add()
		if err != nil {
			fmt.Println(err.Error())
		}
	}
	{
		var id int64
		create := map[string]any{
			t.MemberId: time.Now().UnixNano(),
			t.Username: "Jerry",
		}
		now := time.Now()
		err := way.Add(t.Table()).
			Comment("Add 6.").
			Except(t.Id).
			// Except(t.ModAt).
			Create(create).
			Default(func(a *hey.Add) { a.FieldValue(t.AddAt, now.Unix()).FieldValue(t.ModAt, now.Unix()) }).
			Returning(func(row *sql.Row) error {
				return row.Scan(&id)
			}, t.Id)
		if err != nil {
			panic(err)
		}

		// Does the current driver support getting the insert id?
		// create[t.MemberId] = time.Now().UnixNano()
		// _, err := way.Add(t.Table()).
		// 	Except(t.Id).
		//  Comment("Add 7.").
		// 	Create(create).
		// 	UseResult(func(result sql.Result) error {
		// 		if cid, err := result.LastInsertId(); err != nil {
		// 			return err
		// 		} else {
		// 			id = cid
		// 		}
		// 		return nil
		// 	}).Add()
		// if err != nil {
		// 	panic(err)
		// }
	}
	{
		create := &model.AddMember{
			MemberId: time.Now().UnixNano(),
			Username: "Alice",
			Nickname: "Alice123",
			Email:    "alice@gmail.com",
		}
		_, err := way.Add(t.Table()).
			Context(context.TODO()).
			Comment("Add 8.").
			Except(t.Id).
			Create(create).
			// ON CONFLICT DO NOTHING
			OnConflict(pgsql.InsertOnConflict(
				[]string{t.MemberId}, // conflict columns
				nil,                  // update columns(value empty do nothing)
			)).
			// ON CONFLICT DO UPDATE SET ...
			// OnConflict(pgsql.InsertOnConflict(
			// 	[]string{t.MemberId}, // conflict columns
			// 	[]string{
			// 		t.Username,
			// 		t.Nickname,
			// 		t.Email,
			// 	}, // update columns(value empty do nothing)
			// )).
			Add()
		if err != nil {
			panic(err)
		}
	}
	{
		create := &model.AddMember{
			MemberId: time.Now().UnixNano(),
			Username: "Alice",
			Nickname: "Alice123",
			Email:    "alice@gmail.com",
		}
		_, err := way.Add(t.Table()).
			Context(context.TODO()).
			Comment("Add 9.").
			Except(t.Id).
			Create(create).
			// ON CONFLICT DO NOTHING
			// OnConflict(pgsql.InsertOnConflict(
			// 	[]string{t.MemberId}, // conflict columns
			// 	nil,                  // update columns(value empty do nothing)
			// )).
			// ON CONFLICT DO UPDATE SET ...
			OnConflict(pgsql.InsertOnConflict(
				[]string{t.MemberId}, // conflict columns
				[]string{
					t.Username,
					t.Nickname,
					t.Email,
				}, // update columns(value empty do nothing)
			)).
			Add()
		if err != nil {
			panic(err)
		}
	}
}

// Delete // The delete statement is usually used less frequently and only requires setting the table to be deleted and the corresponding conditions.
func Delete() {
	del := way.Del(t.Table()).Comment("Del 1.")
	where := way.F()
	// where.Equal(t.Id, 1)
	get := way.Get(t.Table()).Column(t.Id).Where(way.F().GreaterThanEqual(t.Id, 1000)).Limit(10).Asc(t.MemberId)
	where.InGet(t.Id, get)
	_, err := del.Where(where).Del()
	if err != nil {
		panic(err)
	}
}

func Update() {
	var id int64
	{
		err := way.Get(t.Table()).Column(t.Id).Limit(1).ScanOne(&id)
		if err != nil {
			panic(err)
		}
	}

	idEqual := way.F().Equal(t.Id, id)

	{
		now := way.Now()
		except := []string{t.Id}
		except = append(except, t.AddAt)
		mod := way.Mod(t.Table()).Comment("Mod 1.").
			Except(except...)
		mod.Set(t.Nickname, "123")
		mod.Default(func(u *hey.Mod) { u.Set(t.ModAt, now.Unix()) })
		mod.Decr(t.Category, 1)
		mod.Expr(t.State, fmt.Sprintf("%s = %s * ?", t.State, t.State), 2)
		// mod.FieldsValues([]string{t.Nickname}, []any{"112233"})
		mod.Where(idEqual)
		_, err := mod.Mod()
		if err != nil {
			panic(err)
		}
	}
	{
		now := way.Now()
		except := []string{t.Id}
		except = append(except, t.AddAt)
		mod := way.Mod(t.Table()).Comment("Mod 2.").
			Except(except...)
		// Updated through the structure question, the structure attribute (the corresponding field of the table) should be a pointer type.
		mod.Modify(map[string]any{
			t.Nickname: "789",
			t.ModAt:    now.Unix(),
		})
		mod.Where(idEqual)
		_, err := mod.Mod()
		if err != nil {
			panic(err)
		}
	}
	{
		now := way.Now()
		except := []string{t.Id}
		except = append(except, t.AddAt)
		mod := way.Mod(t.Table()).Comment("Mod 3.").Except(except...)
		update := &model.Member{
			Nickname: "123",
		}
		updateNew := update.COPY()
		updateNew.Nickname = "112233"
		mod.Update(update, updateNew)
		mod.Except(t.Category)
		mod.Default(func(u *hey.Mod) { u.Set(t.ModAt, now.Unix()).Set(t.Category, 5) })
		mod.Where(idEqual)
		_, err := mod.Mod()
		if err != nil {
			panic(err)
		}
	}
	{
		now := way.Now()
		except := []string{t.Id}
		except = append(except, t.AddAt)
		mod := way.Mod(t.Table()).Comment("Mod 4.").
			Except(except...)
		mod.SetCase(t.Category, func(c *hey.Case) {
			c.If(func(f hey.Filter) {
				f.GreaterThan(t.Category, 0)
			}, "category + 1")
			c.If(func(f hey.Filter) {
				f.LessThan(t.Category, 0)
			}, "category - 1")
			c.Else("category + ?", 0)
		})
		mod.Default(func(u *hey.Mod) { u.Set(t.ModAt, now.Unix()) })
		mod.Where(idEqual)
		_, err := mod.Mod()
		if err != nil {
			panic(err)
		}
	}
}

func Select() {
	result := make([]*model.Member, 0)
	{
		get := way.Get(t.Table()).Comment("Get 1.")
		get.Limit(1).Desc(t.Id)
		err := get.Get(&result)
		if err != nil {
			panic(err)
		}
	}
	{
		get := way.Get(t.Table()).Comment("Get 2.")
		get.Column(t.Id)
		get.Limit(1).Desc(t.Id)
		err := get.Get(&result)
		if err != nil {
			panic(err)
		}
	}
	{
		get := way.Get(t.Table()).Comment("Get 3.")
		get.Column(t.Id)
		get.Where(way.F().Equal(t.Id, 10010))
		get.Limit(1).Desc(t.Id)
		err := get.Get(&result)
		if err != nil {
			panic(err)
		}
	}
	{
		a := way.AliasA()
		b := way.AliasB()
		get := way.Get(t.Table()).Comment("Get 4.")
		get.Alias(a.V())
		get.Column(a.V(t.Id), b.V(t.MemberId))
		get.InnerJoin(func(j *hey.GetJoin) {
			j.Table(t.Table()).Alias(b.V()).Using(t.Id)
		})
		get.Where(way.F().Equal(a.V(t.Id), 10010))
		get.Limit(1).Desc(t.Id)
		err := get.Get(&result)
		if err != nil {
			panic(err)
		}
	}
	{
		a := way.AliasA()
		b := way.AliasB()
		get := way.Get(t.Table()).Comment("Get 5.")
		get.Alias(a.V())
		get.Column(a.V(t.Id), b.V(t.MemberId))
		get.InnerJoin(func(j *hey.GetJoin) {
			j.Table(t.Table()).Alias(b.V()).OnEqual(a.V(t.MemberId), b.V(t.MemberId))
		})
		get.Where(way.F().Equal(a.V(t.Id), 10010))
		get.Limit(1).Desc(t.Id)
		err := get.Get(&result)
		if err != nil {
			panic(err)
		}
	}

	{
		a := way.AliasA()
		column := hey.PrefixColumn(a.V(), t.Id, t.MemberId)
		get := way.Get(t.Table()).Comment("Get 6.")
		get.Alias(a.V())
		get.Column(column...)
		get.AddColCase(func(c *hey.Case) {
			c.If(func(f hey.Filter) {
				f.Equal(a.V(t.Gender), 1)
			}, "'Male'")
			c.If(func(f hey.Filter) {
				f.Equal(a.V(t.Gender), 2)
			}, "'Female'")
			c.Else("'Unknown'")
			c.Alias(t.Gender)
		})
		get.Where(way.F().Equal(a.V(t.Id), 10010))
		get.Limit(1).Desc(a.V(t.Id))
		err := get.Get(&result)
		if err != nil {
			panic(err)
		}
	}
	{
		a := way.AliasA()
		column := hey.PrefixColumn(a.V(), t.MemberId)
		column = append(column, a.SUM(t.State))
		get := way.Get(t.Table()).Comment("Get 7.")
		get.Alias(a.V())
		get.Column(column...)
		get.Where(way.F().Between(a.V(t.Id), 10010, 20020).GreaterThanEqual(a.V(t.State), 0))
		get.Group(a.V(t.MemberId))
		get.Having(way.F().GreaterThanEqual(a.Sum(t.State), 0).OrGroup(func(g hey.Filter) {
			g.LessThanEqual(a.Sum(t.State), -2)
		}))
		get.Limit(10).Offset(1).Desc(a.V(t.MemberId))
		err := get.Get(&result)
		if err != nil {
			panic(err)
		}
	}
}

func Where() {
	{
		f := hey.F()

		// Recommended
		f.Equal(t.Id, 1)

		// Not recommended, but original writing is supported
		f.And("id = 1")
		f.Or("id = ?", 2)
	}
	{
		f := hey.F()
		f.Equal(t.Id, 1)
		fmt.Println(f.SQL())
	}
	{
		f := hey.F()
		f.Equal(t.Id, 1).Between(t.Id, 1, 2).IsNull(t.Id)
		fmt.Println(f.SQL())
	}
	{
		f := hey.F()
		f.Equal(t.Id, 1).Between(t.Id, 1, 2).
			OrGroup(func(g hey.Filter) {
				g.Equal(t.Id, 7).Between(t.Id, 8, 9)
			})
		fmt.Println(f.SQL())
	}
	{
		f := hey.F()
		f.Group(func(g hey.Filter) {
			g.Equal(t.Id, 1).Between(t.Id, 1, 2)
		}).OrGroup(func(g hey.Filter) {
			g.Equal(t.Id, 7).Between(t.Id, 8, 9)
		})
		fmt.Println(f.SQL())
	}
	{
		f := hey.F()
		f.And(t.Id + " > 0")
		f.And("field1 < 9")
		f.Group(func(g hey.Filter) {
			g.NotBetween(t.Id, 6, 8)
			// g.Not().And("id BETWEEN ? AND ?", 6, 8)
		})
		fmt.Println(f.SQL())
	}
	{
		f := hey.F()
		f.Equal(t.Id, 1)
		fmt.Println(f.SQL())
		fmt.Println(f.IsEmpty())
		f.Clean()
		fmt.Println(f.IsEmpty())
		f.InCols(
			[]string{t.MemberId, t.Category, t.State},
			[][]interface{}{
				{1, 1, 1},
				{2, 2, 2},
				{3, 3, 3},
				// ...
			}...)
		fmt.Println(f.SQL())
	}
	{
		f := hey.F()
		f.Group(func(g hey.Filter) {
			g.GreaterThan(t.MemberId, 0)
		})
		f1 := hey.F()
		f1.Equal(t.Id, 1)
		f1.Use(f)
		fmt.Println(f1.SQL())
	}
	{
		f := hey.F()
		f.Group(func(g hey.Filter) {
			g.GreaterThan(t.MemberId, 0)
			g.IsNotNull(t.Id)
		})
		f1 := hey.F().New(f)
		fmt.Println(f1.SQL())
	}
	{
		f := hey.F()
		f.In(t.Id, 1, 2, 3)
		f.Group(func(g hey.Filter) {
			g.InSql(t.Id, "SELECT id FROM table WHERE field1 BETWEEN ? AND ?", 1, 10)
			g.OrGroup(func(g hey.Filter) {
				g.InSql(t.Id, "SELECT id FROM table WHERE field2 BETWEEN 100 AND 200")
			})
		})
		fmt.Println(f.SQL())
	}
	{
		get1 := hey.NewGet(nil)
		get1.Table(t.Table()).Column(t.Id).Where(hey.F().Between(t.MemberId, 1, 10))
		get2 := hey.NewGet(nil)
		get2.Table(t.Table()).Column(t.Id).Where(hey.F().Between(t.Category, 100, 200))
		get := hey.NewGet(nil).UnionAll(get1, get2)
		f := hey.F()
		f.In(t.Id, 1, 2, 3)
		f.OrGroup(func(g hey.Filter) {
			g.Not().InGet(t.Id, get)
		})
		fmt.Println(f.SQL())
	}
	{
		f := hey.F()
		f.Like(t.MemberId, "%k%")
		fmt.Println(f.SQL())
	}
	{
		f := hey.F()
		f.IsNotNull(t.Id)
		fmt.Println(f.SQL())
	}
}

func Transaction() {
	err := way.TxTry(func(tx *hey.Way) error {
		err := tx.Get(t.Table()). /* ... */ ScanOne()
		if err != nil {
			return err
		}
		_, err = tx.Add(t.Table()). /* ... */ Add()
		if err != nil {
			return err
		}
		_, err = tx.Del(t.Table()). /* ... */ Del()
		if err != nil {
			return err
		}
		_, err = tx.Mod(t.Table()). /* ... */ Mod()
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}
```