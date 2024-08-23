package hey

import (
	"reflect"
	"strings"
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

func filterGreater(column string) string {
	return filterSqlExpr(column, SqlGreater)
}

func filterGreaterEqual(column string) string {
	return filterSqlExpr(column, SqlGreaterEqual)
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
	tmp := make([]string, 0, length2+3)
	tmp = append(tmp, column)
	if not {
		tmp = append(tmp, " NOT")
	}
	tmp = append(tmp, " IN ( ")
	tmp = append(tmp, result...)
	tmp = append(tmp, " )")
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
	expr = ConcatString(expr, " IN ( ", prepare, " )")
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

	tmp := make([]string, 0, 5)
	tmp = append(tmp, filterInColsFields(columns...))
	if not {
		tmp = append(tmp, " NOT")
	}
	tmp = append(tmp, " IN ( ")
	tmp = append(tmp, strings.Join(valueGroup, ", "))
	tmp = append(tmp, " )")
	expr = ConcatString(tmp...)
	return
}

func filterInColsSql(columns []string, prepare string, args []interface{}, not bool) (expr string, param []interface{}) {
	count := len(columns)
	if count == 0 || prepare == EmptyString {
		return
	}
	tmp := make([]string, 0, 5)
	tmp = append(tmp, filterInColsFields(columns...))
	if not {
		tmp = append(tmp, " NOT")
	}
	tmp = append(tmp, " IN ( ")
	tmp = append(tmp, prepare)
	tmp = append(tmp, " )")
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
	expr = ConcatString(exists, " ( ", prepare, " )")
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

type Filter interface {
	Copy(filter ...Filter) Filter
	IsEmpty() bool

	And(expr string, args ...interface{}) Filter
	Filter(filters ...Filter) Filter
	Group(group func(filter Filter)) Filter
	Equal(column string, value interface{}) Filter
	Greater(column string, value interface{}) Filter
	GreaterEqual(column string, value interface{}) Filter
	LessThan(column string, value interface{}) Filter
	LessThanEqual(column string, value interface{}) Filter
	Between(column string, start interface{}, end interface{}) Filter
	In(column string, values ...interface{}) Filter
	InSql(column string, prepare string, args []interface{}) Filter
	InCols(columns []string, values ...[]interface{}) Filter
	InColsSql(columns []string, prepare string, args []interface{}) Filter
	Exists(prepare string, args []interface{}) Filter
	Like(column string, value interface{}) Filter
	IsNull(column string) Filter
	NotEqual(column string, value interface{}) Filter
	NotBetween(column string, start interface{}, end interface{}) Filter
	NotIn(column string, values ...interface{}) Filter
	NotInSql(column string, prepare string, args []interface{}) Filter
	NotInCols(columns []string, values ...[]interface{}) Filter
	NotInColsSql(columns []string, prepare string, args []interface{}) Filter
	NotExists(prepare string, args []interface{}) Filter
	NotLike(column string, value interface{}) Filter
	IsNotNull(column string) Filter

	Or(expr string, args ...interface{}) Filter
	OrFilter(filters ...Filter) Filter
	OrGroup(group func(filter Filter)) Filter
	OrEqual(column string, value interface{}) Filter
	OrGreater(column string, value interface{}) Filter
	OrGreaterEqual(column string, value interface{}) Filter
	OrLessThan(column string, value interface{}) Filter
	OrLessThanEqual(column string, value interface{}) Filter
	OrBetween(column string, start interface{}, end interface{}) Filter
	OrIn(column string, values ...interface{}) Filter
	OrInSql(column string, prepare string, args []interface{}) Filter
	OrInCols(columns []string, values ...[]interface{}) Filter
	OrInColsSql(columns []string, prepare string, args []interface{}) Filter
	OrExists(prepare string, args []interface{}) Filter
	OrLike(column string, value interface{}) Filter
	OrIsNull(column string) Filter
	OrNotEqual(column string, value interface{}) Filter
	OrNotBetween(column string, start interface{}, end interface{}) Filter
	OrNotIn(column string, values ...interface{}) Filter
	OrNotInSql(column string, prepare string, args []interface{}) Filter
	OrNotInCols(columns []string, values ...[]interface{}) Filter
	OrNotInColsSql(columns []string, prepare string, args []interface{}) Filter
	OrNotExists(prepare string, args []interface{}) Filter
	OrNotLike(column string, value interface{}) Filter
	OrIsNotNull(column string) Filter

	SQL() (prepare string, args []interface{})
}

type filter struct {
	prepare *strings.Builder
	args    []interface{}
	num     int
	numOr   int
}

func (s *filter) Copy(filter ...Filter) Filter {
	return F().Filter(filter...)
}

func (s *filter) IsEmpty() bool {
	return s.num == 0
}

func (s *filter) add(logic string, expr string, args ...interface{}) Filter {
	if expr == EmptyString {
		return s
	}
	if s.num == 0 {
		s.prepare.WriteString(expr)
		s.args = args
		s.num++
		return s
	}
	s.prepare.WriteString(ConcatString(SqlSpace, logic, SqlSpace, expr))
	s.args = append(s.args, args...)
	s.num++
	if logic == SqlOr {
		s.numOr++
	}
	return s
}

// And Parameter 'args' is the compatibility parameter.
func (s *filter) And(expr string, args ...interface{}) Filter {
	return s.add(SqlAnd, expr, argsCompatible(args...)...)
}

func (s *filter) Filter(filters ...Filter) Filter {
	for _, f := range filters {
		if f == nil || f.IsEmpty() {
			continue
		}
		expr, args := f.SQL()
		s.add(SqlAnd, expr, args...)
	}
	return s
}

func (s *filter) addGroup(logic string, group func(filter Filter)) Filter {
	if group == nil {
		return s
	}
	tmp := F()
	group(tmp)
	if tmp.IsEmpty() {
		return s
	}
	expr, args := tmp.SQL()
	return s.add(logic, expr, args...)
}

func (s *filter) Group(group func(filter Filter)) Filter {
	return s.addGroup(SqlAnd, group)
}

func (s *filter) Equal(column string, value interface{}) Filter {
	return s.add(SqlAnd, filterEqual(column), value)
}

func (s *filter) Greater(column string, value interface{}) Filter {
	return s.add(SqlAnd, filterGreater(column), value)
}

func (s *filter) GreaterEqual(column string, value interface{}) Filter {
	return s.add(SqlAnd, filterGreaterEqual(column), value)
}

func (s *filter) LessThan(column string, value interface{}) Filter {
	return s.add(SqlAnd, filterLessThan(column), value)
}

func (s *filter) LessThanEqual(column string, value interface{}) Filter {
	return s.add(SqlAnd, filterLessThanEqual(column), value)
}

func (s *filter) Between(column string, start interface{}, end interface{}) Filter {
	return s.add(SqlAnd, filterBetween(column, false), start, end)
}

// In Parameter 'values' is the compatibility parameter.
func (s *filter) In(column string, values ...interface{}) Filter {
	expr, args := filterIn(column, values, false)
	return s.add(SqlAnd, expr, args...)
}

func (s *filter) InSql(column string, prepare string, args []interface{}) Filter {
	expr, list := filterInSql(column, prepare, args, false)
	return s.add(SqlAnd, expr, list...)
}

func (s *filter) InCols(columns []string, values ...[]interface{}) Filter {
	expr, list := filterInCols(columns, values, false)
	return s.add(SqlAnd, expr, list...)
}

func (s *filter) InColsSql(columns []string, prepare string, args []interface{}) Filter {
	expr, list := filterInColsSql(columns, prepare, args, false)
	return s.add(SqlAnd, expr, list...)
}

func (s *filter) Exists(prepare string, args []interface{}) Filter {
	expr, list := filterExists(prepare, args, false)
	return s.add(SqlAnd, expr, list...)
}

func (s *filter) Like(column string, value interface{}) Filter {
	return s.add(SqlAnd, filterLike(column, false), value)
}

func (s *filter) IsNull(column string) Filter {
	return s.add(SqlAnd, filterIsNull(column, false))
}

func (s *filter) NotEqual(column string, value interface{}) Filter {
	return s.add(SqlAnd, filterNotEqual(column), value)
}

func (s *filter) NotBetween(column string, start interface{}, end interface{}) Filter {
	return s.add(SqlAnd, filterBetween(column, true), start, end)
}

// NotIn Parameter 'values' is the compatibility parameter.
func (s *filter) NotIn(column string, values ...interface{}) Filter {
	expr, args := filterIn(column, values, true)
	return s.add(SqlAnd, expr, args...)
}

func (s *filter) NotInSql(column string, prepare string, args []interface{}) Filter {
	expr, list := filterInSql(column, prepare, args, true)
	return s.add(SqlAnd, expr, list...)
}

func (s *filter) NotInCols(columns []string, values ...[]interface{}) Filter {
	expr, args := filterInCols(columns, values, true)
	return s.add(SqlAnd, expr, args...)
}

func (s *filter) NotInColsSql(columns []string, prepare string, args []interface{}) Filter {
	expr, list := filterInColsSql(columns, prepare, args, true)
	return s.add(SqlAnd, expr, list...)
}

func (s *filter) NotExists(prepare string, args []interface{}) Filter {
	expr, list := filterExists(prepare, args, true)
	return s.add(SqlAnd, expr, list...)
}

func (s *filter) NotLike(column string, value interface{}) Filter {
	return s.add(SqlAnd, filterLike(column, true), value)
}

func (s *filter) IsNotNull(column string) Filter {
	return s.add(SqlAnd, filterIsNull(column, true))
}

// Or Parameter 'args' is the compatibility parameter.
func (s *filter) Or(expr string, args ...interface{}) Filter {
	return s.add(SqlOr, expr, argsCompatible(args...)...)
}

func (s *filter) OrFilter(filters ...Filter) Filter {
	for _, f := range filters {
		if f == nil || f.IsEmpty() {
			continue
		}
		expr, args := f.SQL()
		s.add(SqlOr, expr, args...)
	}
	return s
}

func (s *filter) OrGroup(group func(filter Filter)) Filter {
	return s.addGroup(SqlOr, group)
}

func (s *filter) OrEqual(column string, value interface{}) Filter {
	return s.add(SqlOr, filterEqual(column), value)
}

func (s *filter) OrGreater(column string, value interface{}) Filter {
	return s.add(SqlOr, filterGreater(column), value)
}

func (s *filter) OrGreaterEqual(column string, value interface{}) Filter {
	return s.add(SqlOr, filterGreaterEqual(column), value)
}

func (s *filter) OrLessThan(column string, value interface{}) Filter {
	return s.add(SqlOr, filterLessThan(column), value)
}

func (s *filter) OrLessThanEqual(column string, value interface{}) Filter {
	return s.add(SqlOr, filterLessThanEqual(column), value)
}

func (s *filter) OrBetween(column string, start interface{}, end interface{}) Filter {
	return s.add(SqlOr, filterBetween(column, false), start, end)
}

// OrIn Parameter 'values' is the compatibility parameter.
func (s *filter) OrIn(column string, values ...interface{}) Filter {
	expr, args := filterIn(column, values, false)
	return s.add(SqlOr, expr, args...)
}

func (s *filter) OrInSql(column string, prepare string, args []interface{}) Filter {
	expr, list := filterInSql(column, prepare, args, false)
	return s.add(SqlOr, expr, list...)
}

func (s *filter) OrInCols(columns []string, values ...[]interface{}) Filter {
	expr, args := filterInCols(columns, values, false)
	return s.add(SqlOr, expr, args...)
}

func (s *filter) OrInColsSql(columns []string, prepare string, args []interface{}) Filter {
	expr, list := filterInColsSql(columns, prepare, args, false)
	return s.add(SqlOr, expr, list...)
}

func (s *filter) OrExists(prepare string, args []interface{}) Filter {
	expr, list := filterExists(prepare, args, false)
	return s.add(SqlOr, expr, list...)
}

func (s *filter) OrLike(column string, value interface{}) Filter {
	return s.add(SqlOr, filterLike(column, false), value)
}

func (s *filter) OrIsNull(column string) Filter {
	return s.add(SqlOr, filterIsNull(column, false))
}

func (s *filter) OrNotEqual(column string, value interface{}) Filter {
	return s.add(SqlOr, filterNotEqual(column), value)
}

func (s *filter) OrNotBetween(column string, start interface{}, end interface{}) Filter {
	return s.add(SqlOr, filterBetween(column, true), start, end)
}

// OrNotIn Parameter 'values' is the compatibility parameter.
func (s *filter) OrNotIn(column string, values ...interface{}) Filter {
	expr, args := filterIn(column, values, true)
	return s.add(SqlOr, expr, args...)
}

func (s *filter) OrNotInSql(column string, prepare string, args []interface{}) Filter {
	expr, list := filterInSql(column, prepare, args, true)
	return s.add(SqlOr, expr, list...)
}

func (s *filter) OrNotInCols(columns []string, values ...[]interface{}) Filter {
	expr, args := filterInCols(columns, values, true)
	return s.add(SqlOr, expr, args...)
}

func (s *filter) OrNotInColsSql(columns []string, prepare string, args []interface{}) Filter {
	expr, list := filterInColsSql(columns, prepare, args, true)
	return s.add(SqlOr, expr, list...)
}

func (s *filter) OrNotExists(prepare string, args []interface{}) Filter {
	expr, list := filterExists(prepare, args, true)
	return s.add(SqlOr, expr, list...)
}

func (s *filter) OrNotLike(column string, value interface{}) Filter {
	return s.add(SqlOr, filterLike(column, true), value)
}

func (s *filter) OrIsNotNull(column string) Filter {
	return s.add(SqlOr, filterIsNull(column, true))
}

func (s *filter) SQL() (prepare string, args []interface{}) {
	if s.num > 1 && s.numOr > 0 {
		return ConcatString(SqlLeftSmallBracket, SqlSpace, s.prepare.String(), SqlSpace, SqlRightSmallBracket), s.args
	}
	return s.prepare.String(), s.args
}

func F() Filter {
	return &filter{
		prepare: &strings.Builder{},
	}
}

// InGet Convenient for Filter.InSql.
func InGet(column string, get *Get) (field string, prepare string, args []interface{}) {
	field = column
	prepare, args = get.SQL()
	return
}

// InColsGet Convenient for Filter.InColsSql.
func InColsGet(columns []string, get *Get) (fields []string, prepare string, args []interface{}) {
	fields = columns
	prepare, args = get.SQL()
	return
}

// ExistsGet Convenient for Filter.Exists.
func ExistsGet(get *Get) (prepare string, args []interface{}) {
	prepare, args = get.SQL()
	return
}

// buildFilterAll Implement the filter condition: column $logic ALL ( subquery ). $logic: =, <>, >, >=, <, <=.
func buildFilterAll(column string, logic string, subquery *Get) (expr string, args []interface{}) {
	if column == EmptyString || logic == EmptyString || subquery == nil {
		return
	}
	expr, args = subquery.SQL()
	if expr == EmptyString {
		args = nil
		return
	}
	expr = ConcatString(column, SqlSpace, logic, SqlSpace, SqlAll, SqlSpace, SqlLeftSmallBracket, SqlSpace, expr, SqlSpace, SqlRightSmallBracket)
	return
}

// EqualAll There are few practical application scenarios because all values are required to be equal.
func EqualAll(column string, subquery *Get) (expr string, args []interface{}) {
	return buildFilterAll(column, SqlEqual, subquery)
}

func NotEqualAll(column string, subquery *Get) (expr string, args []interface{}) {
	return buildFilterAll(column, SqlNotEqual, subquery)
}

func GreaterAll(column string, subquery *Get) (expr string, args []interface{}) {
	return buildFilterAll(column, SqlGreater, subquery)
}

func GreaterEqualAll(column string, subquery *Get) (expr string, args []interface{}) {
	return buildFilterAll(column, SqlGreaterEqual, subquery)
}

func LessThanAll(column string, subquery *Get) (expr string, args []interface{}) {
	return buildFilterAll(column, SqlLessThan, subquery)
}

func LessThanEqualAll(column string, subquery *Get) (expr string, args []interface{}) {
	return buildFilterAll(column, SqlLessThanEqual, subquery)
}

// buildFilterAny Implement the filter condition: column $logic ANY ( subquery ). $logic: =, <>, >, >=, <, <=.
func buildFilterAny(column string, logic string, subquery *Get) (expr string, args []interface{}) {
	if column == EmptyString || logic == EmptyString || subquery == nil {
		return
	}
	expr, args = subquery.SQL()
	if expr == EmptyString {
		args = nil
		return
	}
	expr = ConcatString(column, SqlSpace, logic, SqlSpace, SqlAny, SqlSpace, SqlLeftSmallBracket, SqlSpace, expr, SqlSpace, SqlRightSmallBracket)
	return
}

func EqualAny(column string, subquery *Get) (expr string, args []interface{}) {
	return buildFilterAny(column, SqlEqual, subquery)
}

func NotEqualAny(column string, subquery *Get) (expr string, args []interface{}) {
	return buildFilterAny(column, SqlNotEqual, subquery)
}

func GreaterAny(column string, subquery *Get) (expr string, args []interface{}) {
	return buildFilterAny(column, SqlGreater, subquery)
}

func GreaterEqualAny(column string, subquery *Get) (expr string, args []interface{}) {
	return buildFilterAny(column, SqlGreaterEqual, subquery)
}

func LessThanAny(column string, subquery *Get) (expr string, args []interface{}) {
	return buildFilterAny(column, SqlLessThan, subquery)
}

func LessThanEqualAny(column string, subquery *Get) (expr string, args []interface{}) {
	return buildFilterAny(column, SqlLessThanEqual, subquery)
}
