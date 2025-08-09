package hey

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestF(t *testing.T) {
	ast := assert.New(t)
	way := testWay()

	f := way.F()
	f.Equal("id", 1)
	ast.Equal("id = ?", f.ToSQL().Prepare, equalMessage)

	f.Group(func(f Filter) {
		f.Like("username", "Alice")
		f.OrGroup(func(f Filter) {
			f.Like("username", "Bob")
		})
	})
	ast.Equal("( id = ? AND ( username LIKE ? OR username LIKE ? ) )", f.ToSQL().Prepare, equalMessage)

	{
		f = F().SetWay(way)
		f.IsNotNull("email")
		ast.Equal("email IS NOT NULL", f.ToSQL().Prepare, equalMessage)

		f.InGroup([]string{"id", "username"}, []any{1, "Alice"}, []any{2, "Bob"}, []any{3, "Jeery"})
		ast.Equal("( email IS NOT NULL AND ( id, username ) IN ( ( ?, ? ), ( ?, ? ), ( ?, ? ) ) )", f.ToSQL().Prepare, equalMessage)
	}

	{
		f.Clean()
		ast.Equal(true, f.Empty(), equalMessage)

		f.In("status", 1, 2, 3)
		f.In("name", []string{"Alice", "Bob", "Jeery"})
		f.AnyQuantifier(nil)
		f.AnyQuantifier(func(q Quantifier) {
			q.LessThanEqual("money", NewSQL("SELECT money FROM example WHERE ( age = ? )", 18))
		})
		ast.Equal("( status IN ( ?, ?, ? ) AND name IN ( ?, ?, ? ) AND money <= ANY ( SELECT money FROM example WHERE ( age = ? ) ) )", f.ToSQL().Prepare)
	}

	{
		f.Clean()
		idEqual := f.New().LessThan("id", 10)
		f.Use(idEqual)
		ast.Equal("id < ?", f.ToSQL().Prepare, equalMessage)
		f.Not()
		ast.Equal("NOT id < ?", f.ToSQL().Prepare, equalMessage)
	}

	{
		f.Clean()
		f.CompareEqual("a.pid - 5", "b.pid")
		f.CompareLessThan("a.level", "b.level")
		ast.Equal("( a.pid - 5 = b.pid AND a.level < b.level )", f.ToSQL().Prepare, equalMessage)
	}

	{
		f.Clean()
		f.Between("birthday", 1701234567, 1711234567)
		f.OrGroup(func(f Filter) {
			f.Equal("gender", "female")
			f.Equal("age", 21)
		})
		ast.Equal("( birthday BETWEEN ? AND ? OR ( gender = ? AND age = ? ) )", f.ToSQL().Prepare, equalMessage)
		f.Not()
		ast.Equal("NOT ( birthday BETWEEN ? AND ? OR ( gender = ? AND age = ? ) )", f.ToSQL().Prepare, equalMessage)
	}

	{
		f.Clean()
		/* SELECT column11, column12, ... FROM table1 WHERE EXISTS ( SELECT 1 FROM table2 WHERE ( table1.id = table2.id ) )*/
		/* SELECT column11, column12, ... FROM table1 WHERE ( status = 1 AND EXISTS ( SELECT 1 FROM table2 WHERE ( table1.id = table2.id ) ) ) */
		/* SELECT column11, column12, ... FROM table1 WHERE ( status = 1 AND EXISTS ( SELECT 1 FROM table2 WHERE ( table1.id = table2.id AND table1.key = table2.key ) ) ) */
		exists := NewSQL("SELECT 1 FROM table2 WHERE ( table1.id = table2.id )")
		f.Exists(exists)
		ast.Equal("EXISTS ( SELECT 1 FROM table2 WHERE ( table1.id = table2.id ) )", f.ToSQL().Prepare, equalMessage)

		f.Clean()
		f.Equal("status", 1)
		f.Exists(exists)
		ast.Equal("( status = ? AND EXISTS ( SELECT 1 FROM table2 WHERE ( table1.id = table2.id ) ) )", f.ToSQL().Prepare, equalMessage)

		exists = NewSQL("SELECT 1 FROM table2 WHERE ( table1.id = table2.id AND table1.key = table2.key )")
		f.Clean()
		f.Equal("status", 1)
		f.Exists(exists)
		ast.Equal("( status = ? AND EXISTS ( SELECT 1 FROM table2 WHERE ( table1.id = table2.id AND table1.key = table2.key ) ) )", f.ToSQL().Prepare, equalMessage)
	}

	{
		f.Clean()
		f.Use(nil)
		ast.Equal("", f.ToSQL().Prepare, equalMessage)
	}
}
