package hey

import (
	"testing"
	"time"

	"github.com/cd365/hey/v7/cst"
)

func TestFilter_Equal(t *testing.T) {
	where := way.F()
	where.Equal(cst.Id, 32)
	assert(where, "id = ?")
}

func TestFilter_LessThan(t *testing.T) {
	where := way.F()
	where.LessThan(cst.Id, 32)
	assert(where, "id < ?")
}

func TestFilter_LessThanEqual(t *testing.T) {
	where := way.F()
	where.LessThanEqual(cst.Id, 32)
	assert(where, "id <= ?")
}

func TestFilter_GreaterThan(t *testing.T) {
	where := way.F()
	where.GreaterThan(cst.Id, 32)
	assert(where, "id > ?")
}

func TestFilter_GreaterThanEqual(t *testing.T) {
	where := way.F()
	where.GreaterThanEqual(cst.Id, 32)
	assert(where, "id >= ?")
}

func TestFilter_Between(t *testing.T) {
	where := way.F()
	where.Between(cst.Id, 1, 2)
	assert(where, "id BETWEEN ? AND ?")
	where.ToEmpty()
	now := way.Now()
	where.Between("created_at", now.Format(time.DateTime), now.Add(time.Hour*8).Format(time.DateTime))
	assert(where, "created_at BETWEEN ? AND ?")
}

func TestFilter_In(t *testing.T) {
	where := way.F()
	where.In(cst.Id, 1)
	assert(where, "id = ?")
	where.ToEmpty()
	where.In(cst.Id, "1")
	assert(where, "id = ?")
	where.ToEmpty()
	where.In(cst.Id, 0.5)
	assert(where, "id = ?")
	where.ToEmpty()

	where.In(cst.Id, []string{"1"})
	assert(where, "id = ?")
	where.ToEmpty()

	where.In(cst.Id, []string{})
	assert(where, "")
	where.ToEmpty()

	where.In(cst.Id, 1, 2, 3)
	assert(where, "id IN ( ?, ?, ? )")
	where.ToEmpty()

	where.In(cst.Id, "1", "2", "3")
	assert(where, "id IN ( ?, ?, ? )")
	where.ToEmpty()

	where.In(cst.Id, 1.1, 2.2, 3.3)
	assert(where, "id IN ( ?, ?, ? )")
	where.ToEmpty()

	where.In(cst.Id, []int{1, 2, 3})
	assert(where, "id IN ( ?, ?, ? )")
	where.ToEmpty()

	where.In(cst.Id, []int64{1, 2, 3})
	assert(where, "id IN ( ?, ?, ? )")
	where.ToEmpty()

	where.In(cst.Id, []float64{1.1, 2.2, 3.3})
	assert(where, "id IN ( ?, ?, ? )")
	where.ToEmpty()

	where.In(cst.Id, []string{"1", "2", "3"})
	assert(where, "id IN ( ?, ?, ? )")
	where.ToEmpty()
}

func TestFilter_InGroup(t *testing.T) {
	where := way.F()
	where.InGroup(
		[]string{
			"name", "age", "email",
		},
		[][]any{
			{"Alice", 18, "alice@gmail.com"},
			{"Bob", 20, "bob@gmail.com"},
			{"Jerry", 30, "jerry@gmail.com"},
		},
	)
	assert(where, "( name, age, email ) IN ( ( ?, ?, ? ), ( ?, ?, ? ), ( ?, ?, ? ) )")
	where.ToEmpty()
}

func TestFilter_Exists(t *testing.T) {
	where := way.F()
	where.Exists(
		way.Table("account").
			Where(
				way.F().
					CompareEqual("a.id", "b.id"),
			).
			Select("1").
			ToSelect(),
	)
	assert(where, "EXISTS ( SELECT 1 FROM account WHERE ( a.id = b.id ) )")
}

func TestFilter_Like(t *testing.T) {
	where := way.F()
	where.Like("name", "%example%")
	assert(where, "name LIKE ?")
}

func TestFilter_IsNull(t *testing.T) {
	where := way.F()
	where.IsNull("email")
	assert(where, "email IS NULL")
}

func TestFilter_NotEqual(t *testing.T) {
	where := way.F()
	where.NotEqual(cst.Id, 32)
	assert(where, "id <> ?")
}

func TestFilter_NotBetween(t *testing.T) {
	where := way.F()
	where.NotBetween(cst.Id, 1, 2)
	assert(where, "id NOT BETWEEN ? AND ?")
	where.ToEmpty()
	now := way.Now()
	where.NotBetween("created_at", now.Format(time.DateTime), now.Add(time.Hour*8).Format(time.DateTime))
	assert(where, "created_at NOT BETWEEN ? AND ?")
}

func TestFilter_NotIn(t *testing.T) {
	where := way.F()
	where.NotIn(cst.Id, 1)
	assert(where, "id <> ?")
	where.ToEmpty()
	where.NotIn(cst.Id, "1")
	assert(where, "id <> ?")
	where.ToEmpty()
	where.NotIn(cst.Id, 0.5)
	assert(where, "id <> ?")
	where.ToEmpty()

	where.NotIn(cst.Id, []string{"1"})
	assert(where, "id <> ?")
	where.ToEmpty()

	where.NotIn(cst.Id, []string{})
	assert(where, "")
	where.ToEmpty()

	where.NotIn(cst.Id, 1, 2, 3)
	assert(where, "id NOT IN ( ?, ?, ? )")
	where.ToEmpty()

	where.NotIn(cst.Id, "1", "2", "3")
	assert(where, "id NOT IN ( ?, ?, ? )")
	where.ToEmpty()

	where.NotIn(cst.Id, []int{1, 2, 3})
	assert(where, "id NOT IN ( ?, ?, ? )")
	where.ToEmpty()

	where.NotIn(cst.Id, []string{"1", "2", "3"})
	assert(where, "id NOT IN ( ?, ?, ? )")
	where.ToEmpty()
}

func TestFilter_NotInGroup(t *testing.T) {
	where := way.F()
	where.NotInGroup(
		[]string{
			"name", "age", "email",
		},
		[][]any{
			{"Alice", 18, "alice@gmail.com"},
			{"Bob", 20, "bob@gmail.com"},
			{"Jerry", 30, "jerry@gmail.com"},
		},
	)
	assert(where, "( name, age, email ) NOT IN ( ( ?, ?, ? ), ( ?, ?, ? ), ( ?, ?, ? ) )")
	where.ToEmpty()
}

func TestFilter_NotExists(t *testing.T) {
	where := way.F()
	where.NotExists(
		way.Table("account").
			Where(
				way.F().
					CompareEqual("a.id", "b.id"),
			).
			Select("1").
			ToSelect(),
	)
	assert(where, "NOT EXISTS ( SELECT 1 FROM account WHERE ( a.id = b.id ) )")
}

func TestFilter_NotLike(t *testing.T) {
	where := way.F()
	where.NotLike("name", "%example%")
	assert(where, "name NOT LIKE ?")
}

func TestFilter_IsNotNull(t *testing.T) {
	where := way.F()
	where.IsNotNull("email")
	assert(where, "email IS NOT NULL")
}

func TestFilter_Keyword(t *testing.T) {
	where := way.F()
	where.Keyword("keyword-value", "name", "username", "email")
	assert(where, "( name LIKE ? OR username LIKE ? OR email LIKE ? )")
}

func TestFilter_AllCompare(t *testing.T) {
	where := way.F()
	where.AllCompare(func(q Quantifier) {
		q.Equal(
			"id",
			way.Table("account").
				Select("id").
				Where(way.F().Between("age", 18, 25)).
				ToSelect(),
		)
	})
	assert(where, "id = ALL ( SELECT id FROM account WHERE ( age BETWEEN ? AND ? ) )")
}

func TestFilter_AnyCompare(t *testing.T) {
	where := way.F()
	where.AnyCompare(func(q Quantifier) {
		q.GreaterThan(
			"age",
			way.Table("account").
				Select(way.Alias(Avg("age"), "age")).
				Where(way.F().GreaterThanEqual("age", 18)).
				ToSelect(),
		)
	})
	assert(where, "age > ANY ( SELECT AVG(age) AS age FROM account WHERE ( age >= ? ) )")
}

func TestFilter_CompareEqual(t *testing.T) {
	where := way.F()
	where.CompareEqual("column1", "column2")
	assert(where, "column1 = column2")
}

func TestFilter_CompareNotEqual(t *testing.T) {
	where := way.F()
	where.CompareNotEqual("column1", "column2")
	assert(where, "column1 <> column2")
}

func TestFilter_CompareGreaterThan(t *testing.T) {
	where := way.F()
	where.CompareGreaterThan("column1", "column2")
	assert(where, "column1 > column2")
}

func TestFilter_CompareGreaterThanEqual(t *testing.T) {
	where := way.F()
	where.CompareGreaterThanEqual("column1", "column2")
	assert(where, "column1 >= column2")
}

func TestFilter_CompareLessThan(t *testing.T) {
	where := way.F()
	where.CompareLessThan("column1", "column2")
	assert(where, "column1 < column2")
}

func TestFilter_CompareLessThanEqual(t *testing.T) {
	where := way.F()
	where.CompareLessThanEqual("column1", "column2")
	assert(where, "column1 <= column2")
}

func TestExtractFilter(t *testing.T) {
	where := way.F()

	id := "id"
	createdAt := "created_at"
	updatedAt := "updated_at"
	salary := "salary"
	name := "name"
	email := "email"
	username := "username"

	idValue := "111,222,333"
	createdAtValue := "1701234567,1801234567"
	updatedAtValue := "1711234567,1811234567"
	salaryValue := "1000,5000"
	nameValue := "aaa,ccc"

	way.NewExtractFilter(where).
		BetweenInt(createdAt, &createdAtValue).
		BetweenInt64(updatedAt, nil).
		BetweenInt64(updatedAt, &updatedAtValue).
		BetweenFloat64(salary, &salaryValue).
		BetweenString(name, &nameValue).
		InIntDirect(id, &idValue).
		InInt64Direct(id, &idValue).
		InStringDirect(name, &nameValue).
		InIntVerify(id, &idValue, func(index int, value int) bool {
			return value > 0
		}).
		InInt64Verify(id, &idValue, func(index int, value int64) bool {
			return value > 0
		}).
		InStringVerify(name, &nameValue, func(index int, value string) bool {
			return value != ""
		})
	assert(where, "( created_at BETWEEN ? AND ? AND updated_at BETWEEN ? AND ? AND salary BETWEEN ? AND ? AND name BETWEEN ? AND ? AND id IN ( ?, ?, ? ) AND id IN ( ?, ?, ? ) AND name IN ( ?, ? ) AND id IN ( ?, ?, ? ) AND id IN ( ?, ?, ? ) AND name IN ( ?, ? ) )")

	where.ToEmpty()
	way.NewExtractFilter(where).
		InIntVerify(id, &idValue, func(index int, value int) bool {
			return value > 0
		}).
		InInt64(id, &idValue, func(index int, value int64) bool {
			return value > 0
		}, KeepOnlyFirst).
		InString(id, &idValue, func(index int, value string) bool {
			return value != ""
		}, KeepOnlyLast)
	assert(where, "( id IN ( ?, ?, ? ) AND id = ? AND id = ? )")

	where.ToEmpty()
	like := "Jack"
	way.NewExtractFilter(where).
		LikeSearch(&like, email, name, username)
	assert(where, "( email LIKE ? OR name LIKE ? OR username LIKE ? )")

	where.ToEmpty()
	way.NewExtractFilter(where).
		LikeSearch(nil, email, name)
	assert(where, "")

	where.ToEmpty()
	like = ""
	way.NewExtractFilter(where).
		LikeSearch(&like, email, name)
	assert(where, "")
}

func TestTimeFilter(t *testing.T) {
	where := way.F()

	createdAt := "created_at"
	now := time.Now()
	way.NewTimeFilter(where).
		SetTime(now).
		LastMinutes(createdAt, 7).
		LastHours(createdAt, 7).
		Today(createdAt).
		Yesterday(createdAt).
		LastDays(createdAt, 7).
		ThisMonth(createdAt).
		LastMonth(createdAt).
		LastMonths(createdAt, 3).
		ThisQuarter(createdAt).
		LastQuarter(createdAt).
		LastQuarters(createdAt, 2).
		LastQuarters(createdAt, 20).
		ThisYear(createdAt).
		LastYear(createdAt).
		LastYears(createdAt, 3)
	assert(where, "( created_at BETWEEN ? AND ? AND created_at BETWEEN ? AND ? AND created_at BETWEEN ? AND ? AND created_at BETWEEN ? AND ? AND created_at BETWEEN ? AND ? AND created_at BETWEEN ? AND ? AND created_at BETWEEN ? AND ? AND created_at BETWEEN ? AND ? AND created_at BETWEEN ? AND ? AND created_at BETWEEN ? AND ? AND created_at BETWEEN ? AND ? AND created_at BETWEEN ? AND ? AND created_at BETWEEN ? AND ? AND created_at BETWEEN ? AND ? AND created_at BETWEEN ? AND ? )")
}
