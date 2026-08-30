package hey

import (
	"database/sql/driver"
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/cd365/hey/v8/cst"
)

type nilUnsafeFilterMaker struct {
	script *SQL
}

func (s *nilUnsafeFilterMaker) ToSQL() *SQL {
	return s.script.Clone()
}

type pointerFilterValuer struct {
	value string
	null  bool
}

func (s *pointerFilterValuer) Value() (driver.Value, error) {
	if s.null {
		return nil, nil
	}
	return "encoded:" + s.value, nil
}

func TestFilter_Equal(t *testing.T) {
	where := way.F()
	where.Equal(cst.Id, 32)
	assert(where, "id = ?")
}

func TestFilter_NamedStringValues(t *testing.T) {
	type columnName string
	type likePattern string

	where := way.F().
		Equal(columnName("id"), 32).
		Like(columnName("name"), likePattern("%example%"))
	script := where.ToSQL()

	assert(script, "( id = ? AND name LIKE ? )")
	if !reflect.DeepEqual(script.Args, []any{32, "%example%"}) {
		t.Fatalf("unexpected args: %#v", script.Args)
	}
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

func TestFilter_BetweenSubqueryArgumentOrder(t *testing.T) {
	subquery := NewSQL("SELECT max_id FROM limits WHERE kind = ?", "upper")
	where := way.F().Between(cst.Id, 10, subquery)
	script := where.ToSQL()

	assert(script, "id BETWEEN ? AND ( SELECT max_id FROM limits WHERE kind = ? )")
	if !reflect.DeepEqual(script.Args, []any{10, "upper"}) {
		t.Fatalf("unexpected args: %#v", script.Args)
	}
	if subquery.Prepare != "SELECT max_id FROM limits WHERE kind = ?" {
		t.Fatalf("subquery was modified: %q", subquery.Prepare)
	}
}

func TestFilter_BetweenTypedNilBoundary(t *testing.T) {
	var upper *SQL

	where := way.F().Between(cst.Id, 10, upper)
	assert(where, "id >= ?")

	where.ToEmpty()
	where.NotBetween(cst.Id, 10, upper)
	assert(where, "id < ?")
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

func TestFilter_InSliceSubquery(t *testing.T) {
	subquery := NewSQL("SELECT id FROM account WHERE age >= ?", 18)
	where := way.F().In(cst.Id, []any{subquery})
	script := where.ToSQL()

	assert(script, "id IN ( SELECT id FROM account WHERE age >= ? )")
	if !reflect.DeepEqual(script.Args, []any{18}) {
		t.Fatalf("unexpected args: %#v", script.Args)
	}
}

func TestFilter_InRejectsMixedSubqueryAndValues(t *testing.T) {
	subquery := NewSQL("SELECT id FROM account")

	where := way.F().In(cst.Id, subquery, 1)
	if !where.IsEmpty() {
		t.Fatalf("unexpected filter: %q", where.ToSQL().Prepare)
	}

	where.NotIn(cst.Id, 1, subquery)
	if !where.IsEmpty() {
		t.Fatalf("unexpected filter: %q", where.ToSQL().Prepare)
	}
}

func TestFilter_InWrapsCompleteCompoundSubquery(t *testing.T) {
	subquery := NewSQL("( SELECT id FROM account ) UNION SELECT id FROM archive")
	where := way.F().In(cst.Id, subquery)

	assert(where, "id IN ( ( SELECT id FROM account ) UNION SELECT id FROM archive )")
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

func TestFilter_InGroupRejectsEmptyColumn(t *testing.T) {
	where := way.F().InGroup(
		[]string{"id", ""},
		[][]any{{1, 2}},
	)
	if !where.IsEmpty() {
		t.Fatalf("unexpected filter: %q", where.ToSQL().Prepare)
	}

	where.InGroup(
		[]string{"id", " "},
		[][]any{{1, 2}},
	)
	if !where.IsEmpty() {
		t.Fatalf("unexpected filter: %q", where.ToSQL().Prepare)
	}
}

func TestFilter_InGroupColumnCountMismatch(t *testing.T) {
	where := way.F().InGroup(
		[]string{"name", "age", "email"},
		[][]any{{"Alice", 18}},
	)
	if !where.IsEmpty() {
		t.Fatalf("unexpected filter: %q", where.ToSQL().Prepare)
	}
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

	where.ToEmpty()
	where.Like("name", NewSQL("other_name"))
	assert(where, "name LIKE other_name")
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

	where.ToEmpty()
	where.NotBetween(cst.Id, nil, 18)
	assert(where, "id > ?")

	where.ToEmpty()
	where.NotBetween(cst.Id, 18, nil)
	assert(where, "id < ?")
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

	where.NotIn(cst.Id, nil)
	assert(where, "")

	where.NotIn(cst.Id, 1, nil, 2)
	script := where.ToSQL()
	assert(script, "id NOT IN ( ?, ? )")
	if !reflect.DeepEqual(script.Args, []any{1, 2}) {
		t.Fatalf("unexpected args: %#v", script.Args)
	}
}

func TestFilter_NotInDiscardsNestedNilPointers(t *testing.T) {
	var value *int
	nested := &value

	where := way.F().NotIn(cst.Id, 1, nested, 2)
	script := where.ToSQL()

	assert(script, "id NOT IN ( ?, ? )")
	if !reflect.DeepEqual(script.Args, []any{1, 2}) {
		t.Fatalf("unexpected args: %#v", script.Args)
	}
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

	var emptyAge *int
	where.NotInGroup(
		[]string{"name", "age"},
		[][]any{
			{"user1", nil},
			{"user2", 18},
			{"user3", emptyAge},
		},
	)
	script := where.ToSQL()
	assert(script, "( name, age ) NOT IN ( ( ?, ? ) )")
	if !reflect.DeepEqual(script.Args, []any{"user2", 18}) {
		t.Fatalf("unexpected args: %#v", script.Args)
	}

	where.ToEmpty()
	where.NotInGroup(
		[]string{"name", "age"},
		[][]any{{"user1", nil}},
	)
	assert(where, "")
}

func TestFilter_NotInGroupDiscardsNestedNilPointers(t *testing.T) {
	var age *int
	nestedAge := &age

	where := way.F().NotInGroup(
		[]string{"name", "age"},
		[][]any{
			{"discarded", nestedAge},
			{"retained", 18},
		},
	)
	script := where.ToSQL()

	assert(script, "( name, age ) NOT IN ( ( ?, ? ) )")
	if !reflect.DeepEqual(script.Args, []any{"retained", 18}) {
		t.Fatalf("unexpected args: %#v", script.Args)
	}
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

	where.ToEmpty()
	where.NotLike("name", NewSQL("other_name"))
	assert(where, "name NOT LIKE other_name")
}

func TestFilter_IsNotNull(t *testing.T) {
	where := way.F()
	where.IsNotNull("email")
	assert(where, "email IS NOT NULL")
}

func TestFilter_Keyword(t *testing.T) {
	where := way.F()
	where.GroupLike("keyword-value", "name", "username", "email")
	assert(where, "( name LIKE ? OR username LIKE ? OR email LIKE ? )")
}

func TestFilter_AllCompare(t *testing.T) {
	where := way.F()
	where.CompareAll(func(ck CompareKey) {
		ck.Equal(
			"id",
			way.Table("account").
				Select("id").
				Where(way.F().Between("age", 18, 25)).
				ToSelect(),
		)
	})
	assert(where, "id = ALL ( SELECT id FROM account WHERE ( age BETWEEN ? AND ? ) )")
}

func TestFilter_CompareDoesNotModifySubquery(t *testing.T) {
	subquery := NewSQL("SELECT id FROM account")
	where := way.F()
	where.CompareAll(func(ck CompareKey) {
		ck.Equal(cst.Id, subquery)
	})

	assert(where, "id = ALL ( SELECT id FROM account )")
	if subquery.Prepare != "SELECT id FROM account" {
		t.Fatalf("subquery was modified: %q", subquery.Prepare)
	}
}

func TestFilter_ToSQLCopiesArgs(t *testing.T) {
	where := way.F().Equal(cst.Id, 1)
	script := where.ToSQL()
	script.Args[0] = 2

	cloned := where.ToSQL()
	if !reflect.DeepEqual(cloned.Args, []any{1}) {
		t.Fatalf("filter args were modified: %#v", cloned.Args)
	}
}

func TestFilter_CustomMakerPreservesLogicalBoundary(t *testing.T) {
	where := way.F().
		And(NewSQL("a = ? OR b = ?", 1, 2)).
		Equal("c", 3)
	script := where.ToSQL()

	assert(script, "( ( a = ? OR b = ? ) AND c = ? )")
	if !reflect.DeepEqual(script.Args, []any{1, 2, 3}) {
		t.Fatalf("unexpected args: %#v", script.Args)
	}

	partiallyParenthesized := way.F().
		And(NewSQL("(a = ?) OR b = ?", 1, 2)).
		Equal("c", 3).
		ToSQL()
	assert(partiallyParenthesized, "( ( (a = ?) OR b = ? ) AND c = ? )")

	negated := way.F().
		And(NewSQL("a = ? OR b = ?", 1, 2)).
		Not().
		ToSQL()
	assert(negated, "NOT ( ( a = ? OR b = ? ) )")
}

func TestFilter_TypedNilMaker(t *testing.T) {
	var maker *nilUnsafeFilterMaker

	where := way.F()
	where.And(maker)
	where.Equal(cst.Id, maker)
	where.Exists(maker)
	where.Like("name", maker)
	if !where.IsEmpty() {
		t.Fatalf("unexpected filter: %q", where.ToSQL().Prepare)
	}
}

func TestFilter_PreservesPointerDriverValuer(t *testing.T) {
	value := &pointerFilterValuer{value: "token"}
	where := way.F().
		Equal("equal_value", value).
		Between("range_value", value, value).
		Like("like_value", value)
	script := where.ToSQL()

	assert(script, "( equal_value = ? AND range_value BETWEEN ? AND ? AND like_value LIKE ? )")
	if len(script.Args) != 4 {
		t.Fatalf("unexpected args: %#v", script.Args)
	}
	for i, arg := range script.Args {
		if arg != value {
			t.Fatalf("arg %d lost driver.Valuer identity: %#v", i, arg)
		}
	}
	converted, err := driver.DefaultParameterConverter.ConvertValue(script.Args[0])
	if err != nil {
		t.Fatalf("convert driver.Valuer: %v", err)
	}
	if converted != "encoded:token" {
		t.Fatalf("unexpected converted value: %#v", converted)
	}

	where.ToEmpty()
	where.In("in_value", value)
	inSQL := where.ToSQL()
	assert(inSQL, "in_value = ?")
	if len(inSQL.Args) != 1 || inSQL.Args[0] != value {
		t.Fatalf("single-value IN lost driver.Valuer identity: %#v", inSQL.Args)
	}
}

func TestFilter_DiscardsNullDriverValuerFromNotIn(t *testing.T) {
	value := &pointerFilterValuer{null: true}
	where := way.F().NotIn(cst.Id, 1, value, 2)
	script := where.ToSQL()

	assert(script, "id NOT IN ( ?, ? )")
	if !reflect.DeepEqual(script.Args, []any{1, 2}) {
		t.Fatalf("unexpected args: %#v", script.Args)
	}
}

func TestFilter_AnyCompare(t *testing.T) {
	where := way.F()
	where.CompareAny(func(ck CompareKey) {
		ck.GreaterThan(
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

func TestStringFilter(t *testing.T) {
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

	way.NewStringFilter(where).
		IntBetween(createdAt, &createdAtValue).
		Int64Between(updatedAt, nil).
		Int64Between(updatedAt, &updatedAtValue).
		Float64Between(salary, &salaryValue).
		StringBetween(name, &nameValue)
	assert(where, "( created_at BETWEEN ? AND ? AND updated_at BETWEEN ? AND ? AND salary BETWEEN ? AND ? AND name BETWEEN ? AND ? )")

	where.ToEmpty()
	way.NewStringFilter(where).
		IntIn(id, &idValue).
		Int64In(id, &idValue).
		StringIn(id, &idValue)
	assert(where, "( id IN ( ?, ?, ? ) AND id IN ( ?, ?, ? ) AND id IN ( ?, ?, ? ) )")

	where.ToEmpty()
	like := ""
	where.GroupLike(like, email, name, username)
	assert(where, "")

	like = "Jack"
	where.GroupLike(like, email, name, username)
	assert(where, "( email LIKE ? OR name LIKE ? OR username LIKE ? )")

	where.ToEmpty()
	like = "Jerry"
	where.GroupLikeSearch(like, email, name)
	assert(where, "( email LIKE ? OR name LIKE ? )")

	where.ToEmpty()
	like = ""
	where.GroupLikeSearch(like, email, name)
	assert(where, "")
}

func TestStringFilter_StringBetweenOpenBounds(t *testing.T) {
	leftValue := ",m"
	left := way.F()
	way.NewStringFilter(left).StringBetween("name", &leftValue)
	leftSQL := left.ToSQL()
	assert(leftSQL, "name <= ?")
	if !reflect.DeepEqual(leftSQL.Args, []any{"m"}) {
		t.Fatalf("unexpected left args: %#v", leftSQL.Args)
	}

	rightValue := "m,"
	right := way.F()
	way.NewStringFilter(right).StringBetween("name", &rightValue)
	rightSQL := right.ToSQL()
	assert(rightSQL, "name >= ?")
	if !reflect.DeepEqual(rightSQL.Args, []any{"m"}) {
		t.Fatalf("unexpected right args: %#v", rightSQL.Args)
	}
}

func TestStringFilter_IntRangeOn32Bit(t *testing.T) {
	if strconv.IntSize != 32 {
		t.Skip("32-bit only")
	}
	if _, err := string2any(reflect.Int, "2147483648"); err == nil {
		t.Fatal("expected an integer range error")
	}
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

func TestTimeFilter_UsesConfiguredLocation(t *testing.T) {
	originalLocal := time.Local
	time.Local = time.UTC
	defer func() {
		time.Local = originalLocal
	}()

	location := time.FixedZone("UTC+8", 8*60*60)
	now := time.Date(2026, 9, 1, 1, 30, 0, 0, location)
	where := way.F()
	way.NewTimeFilter(where).SetTime(now).Today("created_at")
	script := where.ToSQL()

	expected := []any{
		time.Date(2026, 9, 1, 0, 0, 0, 0, location).Unix(),
		now.Unix(),
	}
	if !reflect.DeepEqual(script.Args, expected) {
		t.Fatalf("unexpected time range: %#v", script.Args)
	}

	lastMonth := way.F()
	way.NewTimeFilter(lastMonth).SetTime(now).LastMonth("created_at")
	lastMonthSQL := lastMonth.ToSQL()
	expected = []any{
		time.Date(2026, 8, 1, 0, 0, 0, 0, location).Unix(),
		time.Date(2026, 9, 1, 0, 0, 0, 0, location).Unix() - 1,
	}
	if !reflect.DeepEqual(lastMonthSQL.Args, expected) {
		t.Fatalf("unexpected last month range: %#v", lastMonthSQL.Args)
	}
}

func TestTimeFilter_PreservesOffsetDuringDSTFallback(t *testing.T) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load location: %v", err)
	}
	// Construct the second 01:30 occurrence explicitly through UTC.
	now := time.Date(2025, 11, 2, 6, 30, 30, 0, time.UTC).In(location)

	minute := way.F()
	way.NewTimeFilter(minute).SetTime(now).LastMinutes("created_at", 1)
	minuteSQL := minute.ToSQL()
	minuteExpected := []any{
		time.Date(2025, 11, 2, 6, 30, 0, 0, time.UTC).Unix(),
		now.Unix(),
	}
	if !reflect.DeepEqual(minuteSQL.Args, minuteExpected) {
		t.Fatalf("unexpected minute range: %#v", minuteSQL.Args)
	}

	hour := way.F()
	way.NewTimeFilter(hour).SetTime(now).LastHours("created_at", 1)
	hourSQL := hour.ToSQL()
	hourExpected := []any{
		time.Date(2025, 11, 2, 6, 0, 0, 0, time.UTC).Unix(),
		now.Unix(),
	}
	if !reflect.DeepEqual(hourSQL.Args, hourExpected) {
		t.Fatalf("unexpected hour range: %#v", hourSQL.Args)
	}
}

func TestTimeFilter_LastQuarters(t *testing.T) {
	location := time.FixedZone("UTC+8", 8*60*60)
	now := time.Date(2026, 8, 31, 1, 30, 0, 0, location)
	where := way.F()
	way.NewTimeFilter(where).SetTime(now).LastQuarters("created_at", 2)
	script := where.ToSQL()

	expected := []any{
		time.Date(2026, 4, 1, 0, 0, 0, 0, location).Unix(),
		now.Unix(),
	}
	if !reflect.DeepEqual(script.Args, expected) {
		t.Fatalf("unexpected quarter range: %#v", script.Args)
	}

	tooMany := way.F()
	way.NewTimeFilter(tooMany).SetTime(now).LastQuarters("created_at", maxTimeFilterQuarters+1)
	if !tooMany.IsEmpty() {
		t.Fatalf("unexpected filter: %q", tooMany.ToSQL().Prepare)
	}
}

func TestTimeFilter_RejectsOverflowingUnits(t *testing.T) {
	where := way.F()
	filter := way.NewTimeFilter(where)
	filter.LastDays("created_at", maxTimeFilterDays+1)
	filter.LastMonths("created_at", maxTimeFilterMonths+1)
	if !where.IsEmpty() {
		t.Fatalf("unexpected filter: %q", where.ToSQL().Prepare)
	}

	if strconv.IntSize != 64 {
		t.Skip("64-bit only")
	}

	filter.LastMinutes("created_at", math.MaxInt)
	filter.LastHours("created_at", math.MaxInt)
	if !where.IsEmpty() {
		t.Fatalf("unexpected filter: %q", where.ToSQL().Prepare)
	}
}
