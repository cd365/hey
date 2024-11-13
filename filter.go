package hey

import (
	"reflect"
	"strings"
	"sync"
)

// toInterfaceSlice Convert any type of slice to []interface{}.
func toInterfaceSlice[T interface{}](slice []T) []interface{} {
	result := make([]interface{}, len(slice))
	for i, v := range slice {
		result[i] = v
	}
	return result
}

// argsCompatible Compatibility parameter.
// var args []T
// Calling Method A: argsCompatible(args...) (T type must be interface{}).
// Calling Method B: argsCompatible(args) (T type can be interface{}, int, int64, string ...).
func argsCompatible(args ...interface{}) []interface{} {
	if len(args) != 1 {
		return args
	}
	switch v := args[0].(type) {
	case []interface{}:
		return argsCompatible(v...)
	case []string:
		return toInterfaceSlice(v)
	case []int:
		return toInterfaceSlice(v)
	case []int8:
		return toInterfaceSlice(v)
	case []int16:
		return toInterfaceSlice(v)
	case []int32:
		return toInterfaceSlice(v)
	case []int64:
		return toInterfaceSlice(v)
	case []uint:
		return toInterfaceSlice(v)
	case []uint8: // []byte
		return args
	case []uint16:
		return toInterfaceSlice(v)
	case []uint32:
		return toInterfaceSlice(v)
	case []uint64:
		return toInterfaceSlice(v)
	case []bool:
		return toInterfaceSlice(v)
	case []float32:
		return toInterfaceSlice(v)
	case []float64:
		return toInterfaceSlice(v)
	case []*string:
		return toInterfaceSlice(v)
	case []*int:
		return toInterfaceSlice(v)
	case []*int8:
		return toInterfaceSlice(v)
	case []*int16:
		return toInterfaceSlice(v)
	case []*int32:
		return toInterfaceSlice(v)
	case []*int64:
		return toInterfaceSlice(v)
	case []*uint:
		return toInterfaceSlice(v)
	case []*uint8:
		return toInterfaceSlice(v)
	case []*uint16:
		return toInterfaceSlice(v)
	case []*uint32:
		return toInterfaceSlice(v)
	case []*uint64:
		return toInterfaceSlice(v)
	case []*bool:
		return toInterfaceSlice(v)
	case []*float32:
		return toInterfaceSlice(v)
	case []*float64:
		return toInterfaceSlice(v)
	default:
		rt := reflect.TypeOf(args[0])
		if rt.Kind() != reflect.Slice {
			return args
		}
		// expand slice members when there is only one slice type value.
		rv := reflect.ValueOf(args[0])
		count := rv.Len()
		result := make([]interface{}, 0, count)
		for i := 0; i < count; i++ {
			result = append(result, rv.Index(i).Interface())
		}
		return result
	}
}

func filterSqlExpr(column string, compare string) string {
	if column == EmptyString {
		return EmptyString
	}
	return ConcatString(column, SqlSpace, compare, SqlSpace, SqlPlaceholder)
}

func filterEqual(column string) string {
	return filterSqlExpr(column, SqlEqual)
}

func filterNotEqual(column string) string {
	return filterSqlExpr(column, SqlNotEqual)
}

func filterGreaterThan(column string) string {
	return filterSqlExpr(column, SqlGreaterThan)
}

func filterGreaterThanEqual(column string) string {
	return filterSqlExpr(column, SqlGreaterThanEqual)
}

func filterLessThan(column string) string {
	return filterSqlExpr(column, SqlLessThan)
}

func filterLessThanEqual(column string) string {
	return filterSqlExpr(column, SqlLessThanEqual)
}

func filterIn(column string, values []interface{}, not bool) (expr string, args []interface{}) {
	if column == EmptyString || values == nil {
		return
	}
	values = argsCompatible(values...)
	length := len(values)
	if length == 0 {
		return
	}
	values = RemoveDuplicate(values...)
	length = len(values)
	if length == 1 {
		if not {
			expr = filterNotEqual(column)
		} else {
			expr = filterEqual(column)
		}
		args = []interface{}{values[0]}
		return
	}
	args = values
	length2 := length * 2
	result := make([]string, 0, length2)
	for i := 0; i < length; i++ {
		if i == 0 {
			result = append(result, SqlPlaceholder)
			continue
		}
		result = append(result, ", ", SqlPlaceholder)
	}
	tmp := make([]string, 0, len(result)+7)
	tmp = append(tmp, column)
	if not {
		tmp = append(tmp, " NOT")
	}
	tmp = append(tmp, " IN ", SqlLeftSmallBracket, SqlSpace)
	tmp = append(tmp, result...)
	tmp = append(tmp, SqlSpace, SqlRightSmallBracket)
	expr = ConcatString(tmp...)
	return
}

func filterInSql(column string, prepare string, args []interface{}, not bool) (expr string, param []interface{}) {
	if column == EmptyString || prepare == EmptyString {
		return
	}
	expr = column
	if not {
		expr = ConcatString(expr, " NOT")
	}
	expr = ConcatString(expr, " IN ", SqlLeftSmallBracket, SqlSpace, prepare, SqlSpace, SqlRightSmallBracket)
	param = args
	return
}

func filterInColsFields(columns ...string) string {
	return ConcatString(SqlLeftSmallBracket, SqlSpace, strings.Join(columns, ", "), SqlSpace, SqlRightSmallBracket)
}

func filterInCols(columns []string, values [][]interface{}, not bool) (expr string, args []interface{}) {
	count := len(columns)
	if count == 0 {
		return
	}
	length := len(values)
	if length == 0 {
		return
	}
	for i := 0; i < length; i++ {
		if len(values[i]) != count {
			args = nil
			return
		}
		args = append(args, values[i][:]...)
	}
	oneGroup := make([]string, count)
	for i := 0; i < count; i++ {
		oneGroup[i] = SqlPlaceholder
	}
	oneGroupString := ConcatString(SqlLeftSmallBracket, SqlSpace, strings.Join(oneGroup, ", "), SqlSpace, SqlRightSmallBracket)
	valueGroup := make([]string, length)
	for i := 0; i < length; i++ {
		valueGroup[i] = oneGroupString
	}
	tmp := make([]string, 0, 8)
	tmp = append(tmp, filterInColsFields(columns...))
	if not {
		tmp = append(tmp, " NOT")
	}
	tmp = append(tmp, " IN ", SqlLeftSmallBracket, SqlSpace)
	tmp = append(tmp, strings.Join(valueGroup, ", "))
	tmp = append(tmp, SqlSpace, SqlRightSmallBracket)
	expr = ConcatString(tmp...)
	return
}

func filterInColsSql(columns []string, prepare string, args []interface{}, not bool) (expr string, param []interface{}) {
	count := len(columns)
	if count == 0 || prepare == EmptyString {
		return
	}
	tmp := make([]string, 0, 8)
	tmp = append(tmp, filterInColsFields(columns...))
	if not {
		tmp = append(tmp, " NOT")
	}
	tmp = append(tmp, " IN ", SqlLeftSmallBracket, SqlSpace)
	tmp = append(tmp, prepare)
	tmp = append(tmp, SqlSpace, SqlRightSmallBracket)
	expr = ConcatString(tmp...)
	param = args
	return
}

func filterExists(prepare string, args []interface{}, not bool) (expr string, param []interface{}) {
	if prepare == EmptyString {
		return
	}
	exists := "EXISTS"
	if not {
		exists = ConcatString("NOT ", exists)
	}
	expr = ConcatString(exists, SqlSpace, SqlLeftSmallBracket, SqlSpace, prepare, SqlSpace, SqlRightSmallBracket)
	param = args
	return
}

func filterBetween(column string, not bool) (expr string) {
	if column == EmptyString {
		return
	}
	expr = column
	if not {
		expr = ConcatString(expr, " NOT")
	}
	expr = ConcatString(expr, " BETWEEN ", SqlPlaceholder, " AND ", SqlPlaceholder)
	return
}

func filterLike(column string, not bool) (expr string) {
	if column == EmptyString {
		return
	}
	expr = column
	if not {
		expr = ConcatString(expr, " NOT")
	}
	expr = ConcatString(expr, " LIKE ", SqlPlaceholder)
	return
}

func filterIsNull(column string, not bool) (expr string) {
	if column == EmptyString {
		return
	}
	expr = ConcatString(column, " IS")
	if not {
		expr = ConcatString(expr, " NOT")
	}
	expr = ConcatString(expr, " NULL")
	return
}

// Filter Implement SQL statement condition filtering.
type Filter interface {
	// SQL Generate conditional filtering SQL statements and their parameters.
	SQL() (string, []interface{})

	// Clean Clear the existing conditional filtering of the current object.
	Clean() Filter

	// IsEmpty Is the current object an empty object?
	IsEmpty() bool

	// Not Negate the result of the current conditional filter object. Multiple negations are allowed.
	Not() Filter

	// And Use logical operator `AND` to combine custom conditions.
	And(expr string, args ...interface{}) Filter

	// Or Use logical operator `OR` to combine custom conditions.
	Or(expr string, args ...interface{}) Filter

	// Group Add a new condition group, which is connected by the `AND` logical operator by default.
	Group(group func(g Filter)) Filter

	// OrGroup Add a new condition group, which is connected by the `OR` logical operator by default.
	OrGroup(group func(g Filter)) Filter

	// Use Implement import a set of conditional filter objects into the current object.
	Use(fs ...Filter) Filter

	// New Create a new conditional filter object based on a set of conditional filter objects.
	New(fs ...Filter) Filter

	// GreaterThan Implement conditional filtering: column > value .
	GreaterThan(column string, value interface{}) Filter

	// GreaterThanEqual Implement conditional filtering: column >= value .
	GreaterThanEqual(column string, value interface{}) Filter

	// LessThan Implement conditional filtering: column < value .
	LessThan(column string, value interface{}) Filter

	// LessThanEqual Implement conditional filtering: column <= value .
	LessThanEqual(column string, value interface{}) Filter

	// Equal Implement conditional filtering: column = value .
	Equal(column string, value interface{}) Filter

	// Between Implement conditional filtering: column BETWEEN value1 AND value2 .
	Between(column string, start interface{}, end interface{}) Filter

	// In Implement conditional filtering: column IN ( value1, value2, value3... ) .
	In(column string, values ...interface{}) Filter

	// InSql Implement conditional filtering: column IN ( subquery ) .
	InSql(column string, prepare string, args ...interface{}) Filter

	// InCols Implement conditional filtering: ( column1, column2, column3... ) IN ( ( value1, value2, value3... ), ( value21, value22, value23... )... ) .
	InCols(columns []string, values ...[]interface{}) Filter

	// InColsSql Implement conditional filtering: ( column1, column2, column3... ) IN ( subquery ) .
	InColsSql(columns []string, prepare string, args ...interface{}) Filter

	// Exists Implement conditional filtering: EXISTS ( subquery ) .
	Exists(prepare string, args ...interface{}) Filter

	// Like Implement conditional filtering: column LIKE value .
	Like(column string, value interface{}) Filter

	// IsNull Implement conditional filtering: column IS NULL .
	IsNull(column string) Filter

	// InGet Implement conditional filtering: column IN ( subquery ) .
	InGet(column string, get *Get) Filter

	// InColsGet Implement conditional filtering: ( column1, column2, column3... ) IN ( subquery ) .
	InColsGet(columns []string, get *Get) Filter

	// ExistsGet Implement conditional filtering: EXISTS ( subquery ) .
	ExistsGet(get *Get) Filter

	// NotEqual Implement conditional filtering: column <> value .
	NotEqual(column string, value interface{}) Filter

	// NotBetween Implement conditional filtering: column NOT BETWEEN value1 AND value2 .
	NotBetween(column string, start interface{}, end interface{}) Filter

	// NotIn Implement conditional filtering: column NOT IN ( value1, value2, value3... ) .
	NotIn(column string, values ...interface{}) Filter

	// NotInCols Implement conditional filtering: ( column1, column2, column3... ) NOT IN ( ( value1, value2, value3... ), ( value21, value22, value23... )... ) .
	NotInCols(columns []string, values ...[]interface{}) Filter

	// NotLike Implement conditional filtering: column NOT LIKE value .
	NotLike(column string, value interface{}) Filter

	// IsNotNull Implement conditional filtering: column IS NOT NULL .
	IsNotNull(column string) Filter

	// You might be thinking why there is no method with the prefix `Or` defined to implement methods like OrEqual, OrLike, OrIn ...
	// 1. Considering that most of the OR is not used frequently in the business development process.
	// 2. If the business really needs to use it, you can use the OrGroup method: OrGroup(func(g Filter) { g.Equal("column", 1) }) .
}

// filter Implementing interface Filter.
type filter struct {
	not     bool
	num     int
	prepare *strings.Builder
	args    []interface{}
}

// filterNew New a Filter.
func filterNew() *filter {
	return &filter{
		prepare: &strings.Builder{},
	}
}

// F New a Filter.
func F() Filter {
	return filterNew()
}

var filters = &sync.Pool{
	New: func() interface{} {
		return filterNew()
	},
}

func GetFilter() Filter {
	return filters.Get().(*filter)
}

func PutFilter(f Filter) {
	f.Clean()
	filters.Put(f)
}

func (s *filter) SQL() (string, []interface{}) {
	if s.num == 0 {
		return EmptyString, nil
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	if s.not {
		b.WriteString(SqlNot)
		b.WriteString(SqlSpace)
	}
	if s.num > 1 {
		b.WriteString(SqlLeftSmallBracket)
		b.WriteString(SqlSpace)
		b.WriteString(s.prepare.String())
		b.WriteString(SqlSpace)
		b.WriteString(SqlRightSmallBracket)
	} else {
		b.WriteString(s.prepare.String())
	}
	return b.String(), s.args[:]
}

func (s *filter) clean() {
	s.not = false
	s.num = 0
	s.prepare.Reset()
	s.args = nil
}

func (s *filter) Clean() Filter {
	s.clean()
	return s
}

func (s *filter) IsEmpty() bool {
	return s.num == 0
}

func (s *filter) Not() Filter {
	s.not = !s.not
	return s
}

func (s *filter) add(logic string, expr string, args ...interface{}) *filter {
	if expr == EmptyString {
		return s
	}
	if s.num == 0 {
		s.prepare.WriteString(expr)
		s.args = args[:]
		s.num++
		return s
	}
	s.prepare.WriteString(ConcatString(SqlSpace, logic, SqlSpace, expr))
	s.args = append(s.args, args[:]...)
	s.num++
	return s
}

func (s *filter) addGroup(logic string, group func(g Filter)) *filter {
	if group == nil {
		return s
	}
	groups := GetFilter()
	defer PutFilter(groups)
	group(groups)
	if groups.IsEmpty() {
		return s
	}
	expr, args := groups.SQL()
	s.add(logic, expr, args...)
	return s
}

func (s *filter) And(expr string, args ...interface{}) Filter {
	return s.add(SqlAnd, expr, args...)
}

func (s *filter) Or(expr string, args ...interface{}) Filter {
	return s.add(SqlOr, expr, args...)
}

func (s *filter) Group(group func(g Filter)) Filter {
	return s.addGroup(SqlAnd, group)
}

func (s *filter) OrGroup(group func(g Filter)) Filter {
	return s.addGroup(SqlOr, group)
}

func (s *filter) Use(fs ...Filter) Filter {
	groups := GetFilter()
	defer PutFilter(groups)
	for _, tmp := range fs {
		if tmp == nil || tmp.IsEmpty() {
			continue
		}
		expr, param := tmp.SQL()
		groups.And(expr, param...)
	}
	prepare, args := groups.SQL()
	return s.And(prepare, args...)
}

func (s *filter) New(fs ...Filter) Filter {
	return filterNew().Use(fs...)
}

func (s *filter) GreaterThan(column string, value interface{}) Filter {
	return s.add(SqlAnd, filterGreaterThan(column), value)
}

func (s *filter) GreaterThanEqual(column string, value interface{}) Filter {
	return s.add(SqlAnd, filterGreaterThanEqual(column), value)
}

func (s *filter) LessThan(column string, value interface{}) Filter {
	return s.add(SqlAnd, filterLessThan(column), value)
}

func (s *filter) LessThanEqual(column string, value interface{}) Filter {
	return s.add(SqlAnd, filterLessThanEqual(column), value)
}

func (s *filter) Equal(column string, value interface{}) Filter {
	return s.add(SqlAnd, filterEqual(column), value)
}

func (s *filter) Between(column string, start interface{}, end interface{}) Filter {
	return s.add(SqlAnd, filterBetween(column, false), start, end)
}

func (s *filter) In(column string, values ...interface{}) Filter {
	expr, args := filterIn(column, values, false)
	return s.add(SqlAnd, expr, args...)
}

func (s *filter) InSql(column string, prepare string, args ...interface{}) Filter {
	expr, param := filterInSql(column, prepare, args, false)
	return s.add(SqlAnd, expr, param...)
}

func (s *filter) InCols(columns []string, values ...[]interface{}) Filter {
	expr, param := filterInCols(columns, values, false)
	return s.add(SqlAnd, expr, param...)
}

func (s *filter) InColsSql(columns []string, prepare string, args ...interface{}) Filter {
	expr, param := filterInColsSql(columns, prepare, args, false)
	return s.add(SqlAnd, expr, param...)
}

func (s *filter) Exists(prepare string, args ...interface{}) Filter {
	expr, param := filterExists(prepare, args, false)
	return s.add(SqlAnd, expr, param...)
}

func (s *filter) Like(column string, value interface{}) Filter {
	return s.add(SqlAnd, filterLike(column, false), value)
}

func (s *filter) IsNull(column string) Filter {
	return s.add(SqlAnd, filterIsNull(column, false))
}

func (s *filter) InGet(column string, get *Get) Filter {
	if get == nil {
		return s
	}
	expr, args := get.SQL()
	return s.InSql(column, expr, args...)
}

func (s *filter) InColsGet(columns []string, get *Get) Filter {
	if get == nil {
		return s
	}
	expr, args := get.SQL()
	return s.InColsSql(columns, expr, args...)
}

func (s *filter) ExistsGet(get *Get) Filter {
	if get == nil {
		return s
	}
	expr, args := get.SQL()
	return s.Exists(expr, args...)
}

func (s *filter) NotEqual(column string, value interface{}) Filter {
	return s.add(SqlAnd, filterNotEqual(column), value)
}

func (s *filter) NotBetween(column string, start interface{}, end interface{}) Filter {
	return s.add(SqlAnd, filterBetween(column, true), start, end)
}

func (s *filter) NotIn(column string, values ...interface{}) Filter {
	expr, args := filterIn(column, values, true)
	return s.add(SqlAnd, expr, args...)
}

func (s *filter) NotInCols(columns []string, values ...[]interface{}) Filter {
	expr, args := filterInCols(columns, values, true)
	return s.add(SqlAnd, expr, args...)
}

func (s *filter) NotLike(column string, value interface{}) Filter {
	return s.add(SqlAnd, filterLike(column, true), value)
}

func (s *filter) IsNotNull(column string) Filter {
	return s.add(SqlAnd, filterIsNull(column, true))
}

func buildFilterAll(f Filter, column string, logic string, subquery *Get) {
	if f == nil || column == EmptyString || logic == EmptyString || subquery == nil {
		return
	}
	expr, args := subquery.SQL()
	if expr == EmptyString {
		return
	}
	expr = ConcatString(column, SqlSpace, logic, SqlSpace, SqlAll, SqlSpace, SqlLeftSmallBracket, SqlSpace, expr, SqlSpace, SqlRightSmallBracket)
	f.And(expr, args...)
}

// EqualAll There are few practical application scenarios because all values are required to be equal.
func EqualAll(f Filter, column string, subquery *Get) {
	buildFilterAll(f, column, SqlEqual, subquery)
}

// NotEqualAll Implement the filter condition: column <> ALL ( subquery ) .
func NotEqualAll(f Filter, column string, subquery *Get) {
	buildFilterAll(f, column, SqlNotEqual, subquery)
}

// GreaterThanAll Implement the filter condition: column > ALL ( subquery ) .
func GreaterThanAll(f Filter, column string, subquery *Get) {
	buildFilterAll(f, column, SqlGreaterThan, subquery)
}

// GreaterThanEqualAll Implement the filter condition: column >= ALL ( subquery ) .
func GreaterThanEqualAll(f Filter, column string, subquery *Get) {
	buildFilterAll(f, column, SqlGreaterThanEqual, subquery)
}

// LessThanAll Implement the filter condition: column < ALL ( subquery ) .
func LessThanAll(f Filter, column string, subquery *Get) {
	buildFilterAll(f, column, SqlLessThan, subquery)
}

// LessThanEqualAll Implement the filter condition: column <= ALL ( subquery ) .
func LessThanEqualAll(f Filter, column string, subquery *Get) {
	buildFilterAll(f, column, SqlLessThanEqual, subquery)
}

func buildFilterAny(f Filter, column string, logic string, subquery *Get) {
	if f == nil || column == EmptyString || logic == EmptyString || subquery == nil {
		return
	}
	expr, args := subquery.SQL()
	if expr == EmptyString {
		return
	}
	expr = ConcatString(column, SqlSpace, logic, SqlSpace, SqlAny, SqlSpace, SqlLeftSmallBracket, SqlSpace, expr, SqlSpace, SqlRightSmallBracket)
	f.And(expr, args...)
}

// EqualAny Implement the filter condition: column = ANY ( subquery ) .
func EqualAny(f Filter, column string, subquery *Get) {
	buildFilterAny(f, column, SqlEqual, subquery)
}

// NotEqualAny Implement the filter condition: column <> ANY ( subquery ) .
func NotEqualAny(f Filter, column string, subquery *Get) {
	buildFilterAny(f, column, SqlNotEqual, subquery)
}

// GreaterThanAny Implement the filter condition: column > ANY ( subquery ) .
func GreaterThanAny(f Filter, column string, subquery *Get) {
	buildFilterAny(f, column, SqlGreaterThan, subquery)
}

// GreaterThanEqualAny Implement the filter condition: column >= ANY ( subquery ) .
func GreaterThanEqualAny(f Filter, column string, subquery *Get) {
	buildFilterAny(f, column, SqlGreaterThanEqual, subquery)
}

// LessThanAny Implement the filter condition: column < ANY ( subquery ) .
func LessThanAny(f Filter, column string, subquery *Get) {
	buildFilterAny(f, column, SqlLessThan, subquery)
}

// LessThanEqualAny Implement the filter condition: column <= ANY ( subquery ) .
func LessThanEqualAny(f Filter, column string, subquery *Get) {
	buildFilterAny(f, column, SqlLessThanEqual, subquery)
}
