package hey

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewSQLInsertValue(t *testing.T) {
	ast := assert.New(t)
	way := testWay()

	var v SQLInsertValue

	{
		v = NewSQLInsertValue()
		v.SetValues([]any{1, 2, 3})
		ast.Equal("( ?, ?, ? )", v.ToSQL().Prepare)
	}

	{
		v = NewSQLInsertValue()
		v.SetValues([]any{1, 2, 3}, []any{4, 5, 6}, []any{7, 8, 9})
		ast.Equal("( ?, ?, ? ), ( ?, ?, ? ), ( ?, ?, ? )", v.ToSQL().Prepare)
	}

	{
		v = NewSQLInsertValue()
		v.SetSubquery(way.Get("example").Select("username", "salary").Where(func(f Filter) { f.Equal("status", 1) }).Desc("salary").Limit(5))
		ast.Equal("SELECT username, salary FROM example WHERE ( status = ? ) ORDER BY salary DESC LIMIT 5", v.ToSQL().Prepare)
	}
}

func TestNewSQLUpdateSet(t *testing.T) {
	ast := assert.New(t)
	way := testWay()

	var u SQLUpdateSet

	{
		u = NewSQLUpdateSet(way)
		u.Set("salary", 5000)
		ast.Equal("salary = ?", u.ToSQL().Prepare)
	}

	{
		u = NewSQLUpdateSet(way)
		u.Set("salary", 5000)
		u.Set("status", 2)
		ast.Equal("salary = ?, status = ?", u.ToSQL().Prepare)
	}

	{
		u = NewSQLUpdateSet(way)
		u.Incr("salary", 500)
		u.Set("updated_at", time.Now().Unix())
		ast.Equal("salary = salary + ?, updated_at = ?", u.ToSQL().Prepare)
	}

	{
		u = NewSQLUpdateSet(way)
		u.SetSlice([]string{"salary", "status"}, []any{5000, 5})
		ast.Equal("salary = ?, status = ?", u.ToSQL().Prepare)
	}

	{
		// Use custom SQL statements and parameters.
		u = NewSQLUpdateSet(way)
		u.Update("salary = salary + performance / ?", 10)
		ast.Equal("salary = salary + performance / ?", u.ToSQL().Prepare)
		ast.Equal([]any{10}, u.ToSQL().Args)
	}
}

func TestNewSQLCase(t *testing.T) {
	ast := assert.New(t)
	way := testWay()

	c := NewSQLCase(way)
	c.Case("sizes")
	c.When(func(w SQLWhenThen) {
		w.When(1).Then(c.V("small"))
		w.When(2).Then(c.V("medium"))
		w.When(3).Then(c.V("large"))
	})
	c.Else(c.V("unknown"))
	ast.Equal("CASE sizes WHEN 1 THEN 'small' WHEN 2 THEN 'medium' WHEN 3 THEN 'large' ELSE 'unknown' END", c.ToSQL().Prepare)

	{
		c = NewSQLCase(way)
		c.Case("sizes")
		c.When(func(w SQLWhenThen) {
			w.When(c.V("S")).Then(c.V("small"))
			w.When(c.V("M")).Then(c.V("medium"))
			w.When(c.V("L")).Then(c.V("large"))
			w.When(c.V("")).Then(c.V("")) // WHEN '' THEN ''
			w.When().Then(c.V(""))        // WHEN NULL THEN ''
		})
		c.Else(c.V("unknown"))
		ast.Equal("CASE sizes WHEN 'S' THEN 'small' WHEN 'M' THEN 'medium' WHEN 'L' THEN 'large' WHEN '' THEN '' WHEN NULL THEN '' ELSE 'unknown' END", c.ToSQL().Prepare)
	}

	{
		c = NewSQLCase(way)
		c.Case("sizes")
		c.When(func(w SQLWhenThen) {
			w.When(c.V("S")).Then(c.V("small"))
			w.When(c.V("M")).Then(c.V("medium"))
			w.When(c.V("L")).Then(c.V("large"))
			w.When().Then(c.V("unknown")) // WHEN NULL THEN 'unknown'
		})
		c.Else(c.V("unknown"))
		ast.Equal("CASE sizes WHEN 'S' THEN 'small' WHEN 'M' THEN 'medium' WHEN 'L' THEN 'large' WHEN NULL THEN 'unknown' ELSE 'unknown' END", c.ToSQL().Prepare)
	}

	{
		c = NewSQLCase(way)
		c.Case("sizes")
		c.When(func(w SQLWhenThen) {
			w.When(c.V("S")).Then(c.V("small"))
			w.When(c.V("M")).Then(c.V("medium"))
			w.When(c.V("L")).Then(c.V("large"))
			w.When().Then() // WHEN NULL THEN NULL
		})
		c.Else(c.V("unknown"))
		ast.Equal("CASE sizes WHEN 'S' THEN 'small' WHEN 'M' THEN 'medium' WHEN 'L' THEN 'large' WHEN NULL THEN NULL ELSE 'unknown' END", c.ToSQL().Prepare)
	}

	{
		c = NewSQLCase(way)
		c.When(func(w SQLWhenThen) {
			w.When(way.F().Equal("sizes", 1)).Then(c.V("small"))
			w.When(way.F().Equal("sizes", 2)).Then(c.V("medium"))
			w.When(way.F().Equal("sizes", 3)).Then(c.V("large"))
		})
		c.Else(c.V("unknown"))
		ast.Equal("CASE WHEN sizes = ? THEN 'small' WHEN sizes = ? THEN 'medium' WHEN sizes = ? THEN 'large' ELSE 'unknown' END", c.ToSQL().Prepare)
		ast.Equal([]any{1, 2, 3}, c.ToSQL().Args)
		c.Alias("my_sizes")
		ast.Equal("CASE WHEN sizes = ? THEN 'small' WHEN sizes = ? THEN 'medium' WHEN sizes = ? THEN 'large' ELSE 'unknown' END AS my_sizes", c.ToSQL().Prepare)
	}
}

func TestNewSQLInsertOnConflict(t *testing.T) {
	ast := assert.New(t)
	way := testWay()

	insertPrepare := "INSERT INTO example ( username, department, salary ) VALUES ( ?, ?, ? )"
	insertArgs := []any{"Alice", "Sales Department", 1000}
	ioc := NewSQLInsertOnConflict(way, insertPrepare, insertArgs...)
	ioc.OnConflict("username", "department")
	ioc.DoUpdateSet(func(u SQLInsertOnConflictUpdateSet) {
		u.Excluded("username", "department")
	})
	ast.Equal("INSERT INTO example ( username, department, salary ) VALUES ( ?, ?, ? ) ON CONFLICT ( username, department ) DO UPDATE SET username = EXCLUDED.username, department = EXCLUDED.department", ioc.ToSQL().Prepare)

	ioc = NewSQLInsertOnConflict(way, insertPrepare, insertArgs...)
	ioc.OnConflict("username", "department")
	ast.Equal("INSERT INTO example ( username, department, salary ) VALUES ( ?, ?, ? ) ON CONFLICT ( username, department ) DO NOTHING", ioc.ToSQL().Prepare)
}

func TestNewTableColumn(t *testing.T) {
	ast := assert.New(t)
	way := testWay()

	table := way.T()

	ast.Equal("MIN(salary)", table.Min("salary"))
	ast.Equal("SUM(salary)", table.Sum("salary"))
	ast.Equal("COALESCE(MAX(salary), 0)", table.MAX("salary"))
	ast.Equal("salary", table.Column("salary"))

	ta := way.TA()
	ast.Equal("MIN(a.salary)", ta.Min("salary"))
	ast.Equal("SUM(a.salary)", ta.Sum("salary"))
	ast.Equal("COALESCE(MAX(a.salary), 0)", ta.MAX("salary"))
	ast.Equal("COALESCE(MAX(a.salary), 0) AS salary", ta.MAX("salary", "salary"))
	ast.Equal("a.salary", ta.Column("salary"))

	ta.ColumnAll("id", "salary")
	ast.Equal([]string{"a.id", "a.salary"}, ta.ColumnAll("id", "salary"))

	ta.SetAlias(AliasC)
	ast.Equal("c.salary AS salary", ta.Column("salary", "salary"))
}

func TestNewWindowFunc(t *testing.T) {
	ast := assert.New(t)
	way := testWay()

	windowFunc := way.WindowFunc()
	windowFunc.RowNumber().Partition("department").Desc("salary").Alias("row_num")
	ast.Equal("ROW_NUMBER() OVER ( PARTITION BY department ORDER BY salary DESC ) AS row_num", windowFunc.ToSQL().Prepare, equalMessage)

	windowFunc = way.WindowFunc("rank")
	windowFunc.Rank().Partition("department").Desc("salary")
	ast.Equal("RANK() OVER ( PARTITION BY department ORDER BY salary DESC ) AS rank", windowFunc.ToSQL().Prepare, equalMessage)

	windowFunc = way.WindowFunc("dense_rank")
	windowFunc.DenseRank().Partition("department").Desc("salary")
	ast.Equal("DENSE_RANK() OVER ( PARTITION BY department ORDER BY salary DESC ) AS dense_rank", windowFunc.ToSQL().Prepare, equalMessage)

	windowFunc = way.WindowFunc("quartile")
	windowFunc.NTile(4).Partition("department").Desc("salary")
	ast.Equal("NTILE(4) OVER ( PARTITION BY department ORDER BY salary DESC ) AS quartile", windowFunc.ToSQL().Prepare, equalMessage)

	windowFunc = way.WindowFunc("dept_total_salary")
	windowFunc.Sum("salary").Partition("department")
	ast.Equal("SUM(salary) OVER ( PARTITION BY department ) AS dept_total_salary", windowFunc.ToSQL().Prepare, equalMessage)

	windowFunc = way.WindowFunc("dept_avg_salary")
	windowFunc.Avg("salary").Partition("department")
	ast.Equal("AVG(salary) OVER ( PARTITION BY department ) AS dept_avg_salary", windowFunc.ToSQL().Prepare, equalMessage)

	windowFunc = way.WindowFunc("running_total")
	windowFunc.Sum("salary").Partition("department").Desc("salary")
	windowFunc.Rows(func(frame SQLWindowFuncFrame) {
		frame.UnboundedPreceding().CurrentRow()
	})
	ast.Equal("SUM(salary) OVER ( PARTITION BY department ORDER BY salary DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS running_total", windowFunc.ToSQL().Prepare, equalMessage)

	{
		windowFunc = way.WindowFunc("prev_salary")
		windowFunc.Lag("salary", 1, 0).Partition("department").Asc("salary")
		ast.Equal("LAG(salary, 1, 0) OVER ( PARTITION BY department ORDER BY salary ASC ) AS prev_salary", windowFunc.ToSQL().Prepare, equalMessage)

		windowFunc = way.WindowFunc("next_salary")
		windowFunc.Lead("salary", 1, 0).Partition("department").Asc("salary")
		ast.Equal("LEAD(salary, 1, 0) OVER ( PARTITION BY department ORDER BY salary ASC ) AS next_salary", windowFunc.ToSQL().Prepare, equalMessage)
	}

	{

		windowFunc = way.WindowFunc("cume_dist")
		windowFunc.Window("CUME_DIST()").Partition("department").Asc("salary")
		ast.Equal("CUME_DIST() OVER ( PARTITION BY department ORDER BY salary ASC ) AS cume_dist", windowFunc.ToSQL().Prepare, equalMessage)

		windowFunc = way.WindowFunc("percent_rank")
		windowFunc.Window("PERCENT_RANK()").Partition("department").Asc("salary")
		ast.Equal("PERCENT_RANK() OVER ( PARTITION BY department ORDER BY salary ASC ) AS percent_rank", windowFunc.ToSQL().Prepare, equalMessage)
	}

	windowFunc = way.WindowFunc("sliding_avg_salary")
	windowFunc.Avg("salary").Partition("department").Asc("salary")
	windowFunc.Rows(func(frame SQLWindowFuncFrame) {
		frame.NPreceding(1).NFollowing(1)
	})
	ast.Equal("AVG(salary) OVER ( PARTITION BY department ORDER BY salary ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING ) AS sliding_avg_salary", windowFunc.ToSQL().Prepare, equalMessage)

	windowFunc = way.WindowFunc("dept_count")
	windowFunc.Count().Partition("department")
	ast.Equal("COUNT(*) OVER ( PARTITION BY department ) AS dept_count", windowFunc.ToSQL().Prepare, equalMessage)

	windowFunc = way.WindowFunc("sliding_avg_salary")
	windowFunc.Avg("salary").Partition("department").Asc("salary")
	windowFunc.Rows(func(frame SQLWindowFuncFrame) {
		frame.Prepare("INTERVAL '7' DAY PRECEDING").CurrentRow()
	})
	ast.Equal("AVG(salary) OVER ( PARTITION BY department ORDER BY salary ASC ROWS BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW ) AS sliding_avg_salary", windowFunc.ToSQL().Prepare, equalMessage)
}
