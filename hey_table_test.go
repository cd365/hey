package hey

import (
	"testing"

	"github.com/cd365/hey/v7/cst"
)

const (
	account = "account"
)

func TestTable_Label(t *testing.T) {
	table := way.Table(account)
	table.Labels("label1", "label2", "label3")
	assert(table.ToSelect(), "/*label1,label2,label3*/ SELECT * FROM account")

	table.ToEmpty()
	table.Desc("id").Limit(10)
	assert(table.ToSelect(), "SELECT * FROM account ORDER BY id DESC LIMIT 10")
}

func TestTable_With(t *testing.T) {
	table := way.Table(cst.A)
	where1 := way.F()
	way.NewTimeFilter(where1).Today("created_at")
	table.With(cst.A, way.Table(account).Where(where1).Select("id", "name", "email").ToSelect())
	where2 := way.F().GreaterThanEqual("age", 18)
	table.With(cst.B, way.Table(account).Where(where2).Select("id", "name", "email", "username").ToSelect())
	a := way.T(cst.A)
	b := way.T(cst.B)
	ac := a.Column
	bc := b.Column
	bca := func(column string) string { return bc(column, column) }
	table.LeftJoin(func(join SQLJoin) (SQLAlias, SQLJoinOn) {
		return join.Table(cst.B, cst.Empty), join.Equal(ac("id"), bc("id"))
	})
	table.Select(
		a.ColumnAllSQL("id", "name", "email"),
		bca("username"),
	)
	where := way.F().IsNotNull(bc("username"))
	table.Where(where)
	table.Desc(ac("id"))
	table.Limit(10)
	table.Offset(0)
	assert(table.ToSelect(), "WITH a AS ( SELECT id, name, email FROM account WHERE ( created_at BETWEEN ? AND ? ) ), b AS ( SELECT id, name, email, username FROM account WHERE ( age >= ? ) ) SELECT a.id, a.name, a.email, b.username AS username FROM a LEFT JOIN b ON a.id = b.id WHERE ( b.username IS NOT NULL ) ORDER BY a.id DESC LIMIT 10 OFFSET 0")
}

func TestTable_Select(t *testing.T) {
	{
		query := way.Table(nil).Select(way.Func("VERSION"))
		assert(query.ToSelect(), "SELECT VERSION()")

		query.ToEmpty()
		query.Select(way.Func(cst.COALESCE, NewSQL("'A'"), NewSQL("'B'"), NewSQL("'C'")))
		assert(query.ToSelect(), "SELECT COALESCE('A','B','C')")

		query.ToEmpty()
		query.Select(way.Func(cst.COALESCE, 1, 2, 3))
		assert(query.ToSelect(), "SELECT COALESCE(1,2,3)")

		query.ToEmpty()
		query.Select(way.Alias(way.Func(cst.COALESCE, 1, 2, 3), cst.A))
		assert(query.ToSelect(), "SELECT COALESCE(1,2,3) AS a")
	}

	table := way.Table(account)
	assert(table.ToSelect(), "SELECT * FROM account")

	table.ToEmpty()
	table.Distinct()
	table.Select("email")
	assert(table.ToSelect(), "SELECT DISTINCT email FROM account")

	table.ToEmpty()
	var maker Maker = NewSQL("email")
	table.Select("id", NewSQL("name"), maker)
	assert(table.ToSelect(), "SELECT id, name, email FROM account")

	table.ToEmpty()
	assert(table.ToSelect(), "SELECT * FROM account")

	table.ToEmpty()
	table.Select("id", "name", "username")
	assert(table.ToSelect(), "SELECT id, name, username FROM account")

	table.ToEmpty()
	table.Select([]string{"id", "name", "username"})
	assert(table.ToSelect(), "SELECT id, name, username FROM account")

	table.ToEmpty()
	table.Select([]string{"id", "name"}, "username")
	assert(table.ToSelect(), "SELECT id, name, username FROM account")

	table.ToEmpty()
	table.Select([]string{"id", "name"}, []string{"username"})
	assert(table.ToSelect(), "SELECT id, name, username FROM account")

	table.ToEmpty()
	query := way.Config().NewSQLSelect(way)
	query.Select("id", "name", "username")
	table.Select(query, "email")
	assert(table.ToSelect(), "SELECT id, name, username, email FROM account")

	table.ToEmpty()
	// Often used in conjunction with the EXISTS statement.
	table.Select("1")
	assert(table.ToSelect(), "SELECT 1 FROM account")
}

func TestTable_Table(t *testing.T) {
	table := way.Table(nil)
	assert(table.ToSelect(), "")

	table.ToEmpty()
	table.Table("")
	assert(table.ToSelect(), "")

	table.ToEmpty()
	table.Table(account)
	assert(table.ToSelect(), "SELECT * FROM account")

	table.ToEmpty()
	table.Table(
		way.Table(account).
			Where(
				NewSQL("id IN ( ?, ?, ? )", 1, 2, 3),
			).
			Select("id", "name", "age").
			ToSelect(),
	).Alias(cst.A).Asc("id").Limit(10).Offset(10)
	assert(table.ToSelect(), "SELECT * FROM ( SELECT id, name, age FROM account WHERE ( id IN ( ?, ?, ? ) ) ) AS a ORDER BY id ASC LIMIT 10 OFFSET 10")
}

func TestTable_Alias(t *testing.T) {
	table := way.Table(account)
	assert(table.ToSelect(), "SELECT * FROM account")

	table.ToEmpty()
	table.Alias(cst.A)
	assert(table.ToSelect(), "SELECT * FROM account AS a")
}

func TestTable_Join(t *testing.T) {
	table := way.Table(account)

	a := way.T(cst.A)
	b := way.T(cst.B)
	ac := a.Column
	bc := b.Column
	acs := a.ColumnAll
	table.Alias(a.Table())
	table.InnerJoin(func(join SQLJoin) (SQLAlias, SQLJoinOn) {
		return join.Table(account, cst.B), join.Equal(ac("pid"), bc("id"))
	})
	table.Select(
		acs("id", "name"),
		bc("id", "child_id"),
		bc("name", "child_name"),
	)
	where := way.F()
	where.IsNotNull(bc("name"))
	table.Where(where)
	table.Desc(ac("serial_num"))
	table.Page(1, 1000)
	assert(table.ToSelect(), "SELECT a.id, a.name, b.id AS child_id, b.name AS child_name FROM account AS a INNER JOIN account AS b ON a.pid = b.id WHERE ( b.name IS NOT NULL ) ORDER BY a.serial_num DESC LIMIT 1000 OFFSET 0")
}

func TestTable_Where(t *testing.T) {
	table := way.Table(account)
	assert(table.ToSelect(), "SELECT * FROM account")

	where := way.F()
	where.Equal("id", 1)
	table.Where(where)
	assert(table.ToSelect(), "SELECT * FROM account WHERE ( id = ? )")

	table.WhereFunc(func(f Filter) {
		f.ToEmpty()
		f.Between("status", 1, 3)
	})
	assert(table.ToSelect(), "SELECT * FROM account WHERE ( status BETWEEN ? AND ? )")

	table.ToEmpty()

	extract := way.NewExtractFilter(where)

	{
		createdAt := "1701234567,1711234567"
		where.ToEmpty()
		extract.Int64Between("created_at", nil)
		table.Where(where)
		assert(table.ToSelect(), "SELECT * FROM account")

		where.ToEmpty()
		extract.Int64Between("created_at", &createdAt)
		table.Where(where)
		assert(table.ToSelect(), "SELECT * FROM account WHERE ( created_at BETWEEN ? AND ? )")
	}

	{
		where.ToEmpty()
		extract.StringIn("username", nil)
		table.Where(where)
		assert(table.ToSelect(), "SELECT * FROM account")

		username := ""
		where.ToEmpty()
		extract.StringIn("username", &username)
		table.Where(where)
		assert(table.ToSelect(), "SELECT * FROM account")

		username = "username1"
		where.ToEmpty()
		extract.StringIn("username", &username)
		table.Where(where)
		assert(table.ToSelect(), "SELECT * FROM account WHERE ( username = ? )")

		username = "username1,username2,username3"
		where.ToEmpty()
		extract.StringIn("username", &username)
		table.Where(where)
		assert(table.ToSelect(), "SELECT * FROM account WHERE ( username IN ( ?, ?, ? ) )")
	}

	{
		like := ""
		where.ToEmpty()
		extract.LikeSearch(nil, "name", "username", "email")
		table.Where(where)
		assert(table.ToSelect(), "SELECT * FROM account")

		where.ToEmpty()
		extract.LikeSearch(&like, "name", "username", "email")
		table.Where(where)
		assert(table.ToSelect(), "SELECT * FROM account")

		like = "a"
		where.ToEmpty()
		extract.LikeSearch(&like, "name", "username", "email")
		table.Where(where)
		assert(table.ToSelect(), "SELECT * FROM account WHERE ( name LIKE ? OR username LIKE ? OR email LIKE ? )")
	}

	{
		where.ToEmpty()
		way.NewTimeFilter(where).LastMinutes("created_at", 15)
		table.Where(where)
		assert(table.ToSelect(), "SELECT * FROM account WHERE ( created_at BETWEEN ? AND ? )")
	}
}

func TestTable_GroupBy(t *testing.T) {
	table := way.Table(account)
	assert(table.ToSelect(), "SELECT * FROM account")

	table.Group("email")
	assert(table.ToSelect(), "SELECT * FROM account GROUP BY email")

	// Using HAVING
	table.GroupFunc(func(g SQLGroupBy) {
		g.Having(func(having Filter) {
			having.IsNotNull("email")
		})
	})
	assert(table.ToSelect(), "SELECT * FROM account GROUP BY email HAVING ( email IS NOT NULL )")
}

func TestTable_Window(t *testing.T) {
	a := way.T(cst.A)
	b := way.T(cst.B)
	ac := a.Column
	bc := b.Column
	table1 := "employee"
	table2 := "company"
	id := cst.Id
	name := "name"
	email := "email"
	departmentId := "department_id"
	serialNum := "serial_num"
	createdAt := "created_at"
	aca := func(column string) string { return ac(column, column) }
	{
		query := way.Table(table1).Alias(a.Table()).LeftJoin(func(join SQLJoin) (SQLAlias, SQLJoinOn) {
			joinTable := join.Table(table2, b.Table())
			joinOn := join.Equal(ac(departmentId), bc(id))
			join.Select(aca(id))
			join.Select(a.ColumnAll(name, email, departmentId))
			join.Select(
				way.WindowFunc("max_salary").Max(ac(departmentId)).OverFunc(func(o SQLWindowFuncOver) {
					o.Partition(ac(departmentId))
					o.Desc(ac(id))
				}),
				way.WindowFunc("avg_salary").Avg(ac(departmentId)).OverFunc(func(o SQLWindowFuncOver) {
					o.Partition(ac(departmentId))
					o.Desc(ac(id))
				}),
				way.WindowFunc("min_salary").Min(ac(departmentId)).OverFunc(func(o SQLWindowFuncOver) {
					o.Partition(ac(departmentId))
					o.Desc(ac(id))
				}),
			)
			join.Select(
				bc(name, "department_name"),
			)
			join.Select(
				Alias(Coalesce(bc(createdAt), 0), "department_created_at"),
			)
			return joinTable, joinOn
		})
		query.WhereFunc(func(f Filter) {
			f.GreaterThan(ac(id), 0)
		})
		query.Desc(ac(id))
		query.Desc(ac(serialNum))
		query.Limit(1)
		assert(query.ToSelect(), "SELECT a.id AS id, a.name, a.email, a.department_id, MAX(a.department_id) OVER ( PARTITION BY a.department_id ORDER BY a.id DESC ) AS max_salary, AVG(a.department_id) OVER ( PARTITION BY a.department_id ORDER BY a.id DESC ) AS avg_salary, MIN(a.department_id) OVER ( PARTITION BY a.department_id ORDER BY a.id DESC ) AS min_salary, b.name AS department_name, COALESCE(b.created_at,0) AS department_created_at FROM employee AS a LEFT JOIN company AS b ON a.department_id = b.id WHERE ( a.id > ? ) ORDER BY a.id DESC, a.serial_num DESC LIMIT 1")
	}

	// With window alias
	{
		wa := "wa"
		query := way.Table(table1).Alias(a.Table()).LeftJoin(func(join SQLJoin) (SQLAlias, SQLJoinOn) {
			joinTable := join.Table(table2, b.Table())
			joinOn := join.Equal(ac(departmentId), bc(id))
			join.Select(aca(id))
			join.Select(a.ColumnAll(name, email, departmentId))
			join.Select(
				way.WindowFunc("max_salary").Max(ac(departmentId)).Over(wa),
				way.WindowFunc("avg_salary").Avg(ac(departmentId)).Over(wa),
				way.WindowFunc("min_salary").Min(ac(departmentId)).Over(wa),
			)
			join.Select(
				bc(name, "department_name"),
			)
			join.Select(
				Alias(Coalesce(bc(createdAt), 0), "department_created_at"),
			)
			return joinTable, joinOn
		})
		query.Window(wa, func(o SQLWindowFuncOver) {
			o.Partition(ac(departmentId))
			o.Desc(ac(id))
		})
		query.WhereFunc(func(f Filter) {
			f.GreaterThan(ac(id), 0)
		})
		query.Desc(ac(id))
		query.Desc(ac(serialNum))
		query.Limit(1)
		assert(query.ToSelect(), "SELECT a.id AS id, a.name, a.email, a.department_id, MAX(a.department_id) OVER wa AS max_salary, AVG(a.department_id) OVER wa AS avg_salary, MIN(a.department_id) OVER wa AS min_salary, b.name AS department_name, COALESCE(b.created_at,0) AS department_created_at FROM employee AS a LEFT JOIN company AS b ON a.department_id = b.id WHERE ( a.id > ? ) WINDOW wa AS ( PARTITION BY a.department_id ORDER BY a.id DESC ) ORDER BY a.id DESC, a.serial_num DESC LIMIT 1")
	}
}

func TestTable_OrderString(t *testing.T) {
	table := way.Table(account)
	assert(table.ToSelect(), "SELECT * FROM account")

	table.OrderString(nil)
	assert(table.ToSelect(), "SELECT * FROM account")

	order := "id:a,username:d"
	table.OrderString(&order)
	table.Limit(10)
	assert(table.ToSelect(), "SELECT * FROM account ORDER BY id ASC, username DESC LIMIT 10")
}

func TestTable_Count(t *testing.T) {
	table := way.Table(account)

	assert(table.ToCount(), "SELECT COUNT(*) AS counts FROM account")

	assert(table.ToCount("COUNT(*)"), "SELECT COUNT(*) FROM account")

	assert(table.ToCount("COUNT(*) counts"), "SELECT COUNT(*) counts FROM account")

	assert(table.ToCount("COUNT(*) AS counts"), "SELECT COUNT(*) AS counts FROM account")

	assert(table.ToCount("COUNT(1)"), "SELECT COUNT(1) FROM account")

	assert(table.ToCount("COUNT(1) counts"), "SELECT COUNT(1) counts FROM account")

	assert(table.ToCount("COUNT(1) AS counts"), "SELECT COUNT(1) AS counts FROM account")

	assert(table.ToCount("COUNT(id)"), "SELECT COUNT(id) FROM account")

	assert(table.ToCount("COUNT(id) counts"), "SELECT COUNT(id) counts FROM account")

	assert(table.ToCount("COUNT(id) AS counts"), "SELECT COUNT(id) AS counts FROM account")
}

func TestTable_Exists(t *testing.T) {
	table := way.Table(account)
	assert(table.ToExists(), "SELECT EXISTS ( SELECT 1 FROM account ) AS a")

	where := way.F()
	where.Equal(cst.Id, 1)
	table.Where(where)
	assert(table.ToExists(), "SELECT EXISTS ( SELECT 1 FROM account WHERE ( id = ? ) ) AS a")

	{
		table.ToEmpty()
		where.ToEmpty()
		where.Equal("status", 1)
		query := way.Table(account).Select("id", "username").Where(where).Desc("id").Limit(1)
		table.Table(query.ToSelect())
		table.Alias(cst.A)
		assert(table.ToExists(), "SELECT EXISTS ( SELECT 1 FROM ( SELECT id, username FROM account WHERE ( status = ? ) ORDER BY id DESC LIMIT 1 ) AS a ) AS a")
	}
}

func TestTable_Insert(t *testing.T) {
	table := way.Table(account)
	table.InsertFunc(func(i SQLInsert) {
		i.ColumnValue("name", "name1")
		i.ColumnValue("age", 18)
	})
	assert(table.ToInsert(), "INSERT INTO account ( name, age ) VALUES ( ?, ? )")

	table.ToEmpty()
	table.InsertFunc(func(i SQLInsert) {
		i.ColumnValue("name", "name1")
		i.ColumnValue("age", 18)
		i.ColumnValue("integrity_score", 101.05)
		now := way.Now()
		i.Default("created_at", now.Unix())
		i.Default("updated_at", now.Unix())
	})
	assert(table.ToInsert(), "INSERT INTO account ( name, age, integrity_score, created_at, updated_at ) VALUES ( ?, ?, ?, ?, ? )")

	table.ToEmpty()
	table.InsertFunc(func(i SQLInsert) {
		m := NewMap()
		m.Set("username", "username2")
		m.Set("age", 18)
		i.Create(m)
		now := way.Now()
		i.Default("created_at", now.Unix())
		i.Default("updated_at", now.Unix())
	})
	assert(table.ToInsert(), "INSERT INTO account ( age, username, created_at, updated_at ) VALUES ( ?, ?, ?, ? )")

	table.ToEmpty()
	table.InsertFunc(func(i SQLInsert) {
		m := NewMap()
		m.Set("username", "username2")
		m.Set("age", 18)
		i.Create(m.Map())
		now := way.Now()
		i.Default("created_at", now.Unix())
		i.Default("updated_at", now.Unix())
	})
	assert(table.ToInsert(), "INSERT INTO account ( age, username, created_at, updated_at ) VALUES ( ?, ?, ?, ? )")

	// Insert one and return the id, *Table.Insert will return the id of the inserted row.
	switch way.Config().Manual.DatabaseType {
	case cst.Mysql, cst.Sqlite:
		table.ToEmpty()
		table.InsertFunc(func(i SQLInsert) {
			i.ColumnValue("username", "username2")
			now := way.Now()
			i.Default("created_at", now.Unix())
			i.Default("updated_at", now.Unix())
		})
		assert(table.ToInsert(), "INSERT INTO account ( username, created_at, updated_at ) VALUES ( ?, ?, ? )")
	case cst.Postgresql:
		table.ToEmpty()
		table.InsertFunc(func(i SQLInsert) {
			i.ColumnValue("username", "username2")
			now := way.Now()
			i.Default("created_at", now.Unix())
			i.Default("updated_at", now.Unix())
		})
		assert(table.ToInsert(), "INSERT INTO account ( username, created_at, updated_at ) VALUES ( ?, ?, ? ) RETURNING id")

		table.ToEmpty()
		table.InsertFunc(func(i SQLInsert) {
			i.ColumnValue("email", "email2")
			i.ColumnValue("username", "username2")
			i.ColumnValue("nickname", "nickname2")
			now := way.Now()
			i.Default("created_at", now.Unix())
			i.Default("updated_at", now.Unix())
			i.OnConflict(func(o SQLOnConflict) {
				o.SetOnConflict("email", "username")
				o.DoUpdateSet(func(u SQLOnConflictUpdateSet) {
					u.Excluded("nickname", "updated_at")
				})
			})
		})
		assert(table.ToInsert(), "INSERT INTO account ( email, username, nickname, created_at, updated_at ) VALUES ( ?, ?, ?, ?, ? ) ON CONFLICT ( email, username ) DO UPDATE SET nickname = EXCLUDED.nickname, updated_at = EXCLUDED.updated_at")
	default:
	}

	table.ToEmpty()
	where := way.F()
	where.Equal("status", 1)
	where.Between("created_at", 1701234567, 1701235567)
	where.Equal("deleted_at", 0)
	table.InsertFunc(func(i SQLInsert) {
		i.Column("age", "name", "username")
		i.SetSubquery(
			way.Table(account).Select("age", "name", "username").Where(where).ToSelect(),
		)
	})
	assert(table.ToInsert(), "INSERT INTO account ( age, name, username ) SELECT age, name, username FROM account WHERE ( status = ? AND created_at BETWEEN ? AND ? AND deleted_at = ? )")
}

func TestTable_Delete(t *testing.T) {
	table := way.Table(account)
	if way.Config().DeleteRequireWhere {
		assert(table.ToDelete(), "")
	}
	table.WhereFunc(func(f Filter) {
		f.Equal(cst.Id, 1)
	})
	assert(table.ToDelete(), "DELETE FROM account WHERE ( id = ? )")

	table.Labels("delete one")
	assert(table.ToDelete(), "/*delete one*/ DELETE FROM account WHERE ( id = ? )")

	where := way.F()
	table.ToEmpty()
	where.In(cst.Id, 1, 2, 3)
	where.IsNull("deleted_at")
	table.Where(where)
	assert(table.ToDelete(), "DELETE FROM account WHERE ( id IN ( ?, ?, ? ) AND deleted_at IS NULL )")
}

func TestTable_Update(t *testing.T) {
	table := way.Table(account)
	if way.Config().UpdateRequireWhere {
		assert(table.ToUpdate(), "")
	}
	table.WhereFunc(func(f Filter) {
		f.Equal(cst.Id, 1)
	})
	assert(table.ToUpdate(), "")

	table.Labels("update example")
	table.UpdateFunc(func(f Filter, u SQLUpdateSet) {
		f.ToEmpty()
		f.Equal(cst.Id, 1)
		u.Set("username", "username1")
	})
	assert(table.ToUpdate(), "/*update example*/ UPDATE account SET username = ? WHERE ( id = ? )")

	table.UpdateFunc(func(f Filter, u SQLUpdateSet) {
		u.Default("updated_at", way.Now().Unix())
	})
	assert(table.ToUpdate(), "/*update example*/ UPDATE account SET username = ?, updated_at = ? WHERE ( id = ? )")

	table.ToEmpty()
	table.UpdateFunc(func(f Filter, u SQLUpdateSet) {
		f.Equal(cst.Id, 1)
		u.Update(map[string]any{
			"username": "username2",
		})
		u.Default("updated_at", way.Now().Unix())
	})
	assert(table.ToUpdate(), "UPDATE account SET username = ?, updated_at = ? WHERE ( id = ? )")

	table.ToEmpty()
	table.UpdateFunc(func(f Filter, u SQLUpdateSet) {
		f.Equal(cst.Id, 1)
		u.Update(map[string]any{
			"username": "username2",
		})
		u.Assign("email", cst.NULL)
		u.Assign("column1", "column2")
		u.Default("updated_at", way.Now().Unix())
	})
	assert(table.ToUpdate(), "UPDATE account SET username = ?, email = NULL, column1 = column2, updated_at = ? WHERE ( id = ? )")

	table.ToEmpty()
	table.UpdateFunc(func(f Filter, u SQLUpdateSet) {
		f.Equal(cst.Id, 1)
		f.Equal("deleted_at", 0)
		u.Update(map[string]any{
			"username": "username2",
		})
		u.Remove("username")
		u.Assign("email", cst.NULL)
		u.Assign("column1", "column2")
		u.Default("updated_at", way.Now().Unix())
	})
	assert(table.ToUpdate(), "UPDATE account SET email = NULL, column1 = column2, updated_at = ? WHERE ( id = ? AND deleted_at = ? )")

	table.ToEmpty()
	table.UpdateFunc(func(f Filter, u SQLUpdateSet) {
		f.Equal(cst.Id, 1)
		f.IsNotNull("deleted_at")
		m := NewMap()
		m.Set("username", "username2")
		m.Set("age", 18)
		u.Update(m)
		u.Default("updated_at", way.Now().Unix())
	})
	assert(table.ToUpdate(), "UPDATE account SET age = ?, username = ?, updated_at = ? WHERE ( id = ? AND deleted_at IS NOT NULL )")
}
