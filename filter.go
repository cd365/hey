package hey

import (
	"reflect"
	"strings"
)

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

func filterInValues(values ...interface{}) []interface{} {
	length := len(values)
	if length != 1 {
		return values
	}
	rt := reflect.TypeOf(values[0])
	if rt.Kind() != reflect.Slice {
		return values
	}
	// expand slice members when there is only one slice type value
	rv := reflect.ValueOf(values[0])
	count := rv.Len()
	result := make([]interface{}, 0, count)
	for i := 0; i < count; i++ {
		result = append(result, rv.Index(i).Interface())
	}
	return result
}

func filterIn(column string, values []interface{}, not bool) (expr string, args []interface{}) {
	if column == EmptyString || values == nil {
		return
	}
	values = filterInValues(values...)
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

func filterInGet(column string, fc func() (prepare string, args []interface{}), not bool) (expr string, args []interface{}) {
	if column == EmptyString || fc == nil {
		return
	}
	prepare := EmptyString
	prepare, args = fc()
	if prepare == EmptyString {
		return
	}
	expr = column
	if not {
		expr = ConcatString(expr, " NOT")
	}
	expr = ConcatString(expr, " IN ( ", prepare, " )")
	return
}

func filterInColsFields(columns ...string) string {
	return ConcatString("( ", strings.Join(columns, ", "), " )")
}

func filterInCols(columns []string, values [][]interface{}, not bool) (expr string, args []interface{}) {
	count := len(columns)
	if count == 0 || len(values) == 0 {
		return
	}
	length := len(values)
	if length == 0 {
		return
	}
	for i := 0; i < length; i++ {
		if len(values[i]) != count {
			return
		}
		args = append(args, values[i][:]...)
	}

	oneGroup := make([]string, count)
	for i := 0; i < count; i++ {
		oneGroup[i] = SqlPlaceholder
	}
	oneGroupString := ConcatString("( ", strings.Join(oneGroup, ", "), " )")
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

func filterInColsGet(columns []string, fc func() (prepare string, args []interface{}), not bool) (expr string, args []interface{}) {
	count := len(columns)
	if count == 0 || fc == nil {
		return
	}
	prepare := EmptyString
	prepare, args = fc()
	if prepare == EmptyString {
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
	return
}

func filterExists(fc func() (prepare string, args []interface{}), not bool) (expr string, args []interface{}) {
	if fc == nil {
		return
	}
	prepare, param := fc()
	if prepare == EmptyString {
		return
	}
	exists := "EXISTS"
	if not {
		exists = ConcatString("NOT ", exists)
	}
	expr = ConcatString(exists, " ( ", prepare, " )")
	args = param
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
	And(prepare string, args ...interface{}) Filter
	Filter(filters ...Filter) Filter
	Group(group func(filter Filter)) Filter
	Equal(column string, value interface{}) Filter
	Greater(column string, value interface{}) Filter
	GreaterEqual(column string, value interface{}) Filter
	LessThan(column string, value interface{}) Filter
	LessThanEqual(column string, value interface{}) Filter
	Between(column string, start interface{}, end interface{}) Filter
	In(column string, values ...interface{}) Filter
	InQuery(column string, fc func() (prepare string, args []interface{})) Filter
	InGet(column string, cmd Commander) Filter
	InCols(columns []string, values ...[]interface{}) Filter
	InColsQuery(columns []string, fc func() (prepare string, args []interface{})) Filter
	InColsGet(columns []string, cmd Commander) Filter
	Exists(fc func() (prepare string, args []interface{})) Filter
	Like(column string, value interface{}) Filter
	IsNull(column string) Filter
	NotEqual(column string, value interface{}) Filter
	NotBetween(column string, start interface{}, end interface{}) Filter
	NotIn(column string, values ...interface{}) Filter
	NotInQuery(column string, fc func() (prepare string, args []interface{})) Filter
	NotInGet(column string, cmd Commander) Filter
	NotInCols(columns []string, values ...[]interface{}) Filter
	NotInColsQuery(columns []string, fc func() (prepare string, args []interface{})) Filter
	NotInColsGet(columns []string, cmd Commander) Filter
	NotExists(fc func() (prepare string, args []interface{})) Filter
	NotLike(column string, value interface{}) Filter
	IsNotNull(column string) Filter
	Or(prepare string, args ...interface{}) Filter
	OrFilter(filters ...Filter) Filter
	OrGroup(group func(filter Filter)) Filter
	OrEqual(column string, value interface{}) Filter
	OrGreater(column string, value interface{}) Filter
	OrGreaterEqual(column string, value interface{}) Filter
	OrLessThan(column string, value interface{}) Filter
	OrLessThanEqual(column string, value interface{}) Filter
	OrBetween(column string, start interface{}, end interface{}) Filter
	OrIn(column string, values ...interface{}) Filter
	OrInQuery(column string, fc func() (prepare string, args []interface{})) Filter
	OrInGet(column string, cmd Commander) Filter
	OrInCols(columns []string, values ...[]interface{}) Filter
	OrInColsQuery(columns []string, fc func() (prepare string, args []interface{})) Filter
	OrInColsGet(columns []string, cmd Commander) Filter
	OrExists(fc func() (prepare string, args []interface{})) Filter
	OrLike(column string, value interface{}) Filter
	OrIsNull(column string) Filter
	OrNotEqual(column string, value interface{}) Filter
	OrNotBetween(column string, start interface{}, end interface{}) Filter
	OrNotIn(column string, values ...interface{}) Filter
	OrNotInQuery(column string, fc func() (prepare string, args []interface{})) Filter
	OrNotInGet(column string, cmd Commander) Filter
	OrNotInCols(columns []string, values ...[]interface{}) Filter
	OrNotInColsQuery(columns []string, fc func() (prepare string, args []interface{})) Filter
	OrNotInColsGet(columns []string, cmd Commander) Filter
	OrNotExists(fc func() (prepare string, args []interface{})) Filter
	OrNotLike(column string, value interface{}) Filter
	OrIsNotNull(column string) Filter
	SQL() (prepare string, args []interface{})
}

type filter struct {
	prepare *strings.Builder
	args    []interface{}
	num     int
}

func (s *filter) Copy(filter ...Filter) Filter {
	return F().Filter(filter...)
}

func (s *filter) add(logic string, expr string, args []interface{}) Filter {
	if expr == EmptyString {
		return s
	}
	if s.prepare.Len() == 0 {
		s.prepare.WriteString(expr)
		s.args = args
		s.num = 1
		return s
	}
	s.prepare.WriteString(ConcatString(SqlSpace, logic, SqlSpace, expr))
	s.args = append(s.args, args...)
	s.num++
	return s
}

func (s *filter) And(expr string, args ...interface{}) Filter {
	return s.add(SqlAnd, expr, args)
}

func (s *filter) andSlice(expr string, args []interface{}) Filter {
	return s.And(expr, args...)
}

func (s *filter) orSlice(expr string, args []interface{}) Filter {
	return s.Or(expr, args...)
}

func (s *filter) Filter(filters ...Filter) Filter {
	for _, f := range filters {
		if f == nil {
			continue
		}
		prepare, args := f.SQL()
		if prepare == EmptyString {
			continue
		}
		s.And(prepare, args...)
	}
	return s
}

func (s *filter) addGroup(logic string, group func(filter Filter)) Filter {
	if group == nil {
		return s
	}
	newFilter := F()
	group(newFilter)
	expr, args := newFilter.SQL()
	if expr == EmptyString {
		return s
	}
	return s.add(logic, expr, args)
}

func (s *filter) Group(group func(filter Filter)) Filter {
	return s.addGroup(SqlAnd, group)
}

func (s *filter) Equal(column string, value interface{}) Filter {
	return s.And(filterEqual(column), value)
}

func (s *filter) Greater(column string, value interface{}) Filter {
	return s.And(filterGreater(column), value)
}

func (s *filter) GreaterEqual(column string, value interface{}) Filter {
	return s.And(filterGreaterEqual(column), value)
}

func (s *filter) LessThan(column string, value interface{}) Filter {
	return s.And(filterLessThan(column), value)
}

func (s *filter) LessThanEqual(column string, value interface{}) Filter {
	return s.And(filterLessThanEqual(column), value)
}

func (s *filter) Between(column string, start interface{}, end interface{}) Filter {
	return s.And(filterBetween(column, false), start, end)
}

func (s *filter) In(column string, values ...interface{}) Filter {
	return s.andSlice(filterIn(column, values, false))
}

func (s *filter) InQuery(column string, fc func() (prepare string, args []interface{})) Filter {
	return s.andSlice(filterInGet(column, fc, false))
}

func (s *filter) InGet(column string, cmd Commander) Filter {
	return s.InQuery(column, func() (prepare string, args []interface{}) { return cmd.SQL() })
}

func (s *filter) InCols(columns []string, values ...[]interface{}) Filter {
	return s.andSlice(filterInCols(columns, values, false))
}

func (s *filter) InColsQuery(columns []string, fc func() (prepare string, args []interface{})) Filter {
	return s.andSlice(filterInColsGet(columns, fc, false))
}

func (s *filter) InColsGet(columns []string, cmd Commander) Filter {
	return s.InColsQuery(columns, func() (prepare string, args []interface{}) { return cmd.SQL() })
}

func (s *filter) Exists(fc func() (prepare string, args []interface{})) Filter {
	return s.andSlice(filterExists(fc, false))
}

func (s *filter) Like(column string, value interface{}) Filter {
	return s.And(filterLike(column, false), value)
}

func (s *filter) IsNull(column string) Filter {
	return s.And(filterIsNull(column, false))
}

func (s *filter) NotEqual(column string, value interface{}) Filter {
	return s.And(filterNotEqual(column), value)
}

func (s *filter) NotBetween(column string, start interface{}, end interface{}) Filter {
	return s.And(filterBetween(column, true), start, end)
}

func (s *filter) NotIn(column string, values ...interface{}) Filter {
	return s.andSlice(filterIn(column, values, true))
}

func (s *filter) NotInQuery(column string, fc func() (prepare string, args []interface{})) Filter {
	return s.andSlice(filterInGet(column, fc, true))
}

func (s *filter) NotInGet(column string, cmd Commander) Filter {
	return s.NotInQuery(column, func() (prepare string, args []interface{}) { return cmd.SQL() })
}

func (s *filter) NotInCols(columns []string, values ...[]interface{}) Filter {
	return s.andSlice(filterInCols(columns, values, true))
}

func (s *filter) NotInColsQuery(columns []string, fc func() (prepare string, args []interface{})) Filter {
	return s.andSlice(filterInColsGet(columns, fc, true))
}

func (s *filter) NotInColsGet(columns []string, cmd Commander) Filter {
	return s.NotInColsQuery(columns, func() (prepare string, args []interface{}) { return cmd.SQL() })
}

func (s *filter) NotExists(fc func() (prepare string, args []interface{})) Filter {
	return s.andSlice(filterExists(fc, true))
}

func (s *filter) NotLike(column string, value interface{}) Filter {
	return s.And(filterLike(column, true), value)
}

func (s *filter) IsNotNull(column string) Filter {
	return s.And(filterIsNull(column, true))
}

func (s *filter) Or(expr string, args ...interface{}) Filter {
	return s.add(SqlOr, expr, args)
}

func (s *filter) OrFilter(filters ...Filter) Filter {
	for _, f := range filters {
		if f == nil {
			continue
		}
		prepare, args := f.SQL()
		if prepare == EmptyString {
			continue
		}
		s.Or(prepare, args...)
	}
	return s
}

func (s *filter) OrGroup(group func(filter Filter)) Filter {
	return s.addGroup(SqlOr, group)
}

func (s *filter) OrEqual(column string, value interface{}) Filter {
	return s.Or(filterEqual(column), value)
}

func (s *filter) OrGreater(column string, value interface{}) Filter {
	return s.Or(filterGreater(column), value)
}

func (s *filter) OrGreaterEqual(column string, value interface{}) Filter {
	return s.Or(filterGreaterEqual(column), value)
}

func (s *filter) OrLessThan(column string, value interface{}) Filter {
	return s.Or(filterLessThan(column), value)
}

func (s *filter) OrLessThanEqual(column string, value interface{}) Filter {
	return s.Or(filterLessThanEqual(column), value)
}

func (s *filter) OrBetween(column string, start interface{}, end interface{}) Filter {
	return s.Or(filterBetween(column, false), start, end)
}

func (s *filter) OrIn(column string, values ...interface{}) Filter {
	return s.orSlice(filterIn(column, values, false))
}

func (s *filter) OrInQuery(column string, fc func() (prepare string, args []interface{})) Filter {
	return s.orSlice(filterInGet(column, fc, false))
}

func (s *filter) OrInGet(column string, cmd Commander) Filter {
	return s.OrInQuery(column, func() (prepare string, args []interface{}) { return cmd.SQL() })
}

func (s *filter) OrInCols(columns []string, values ...[]interface{}) Filter {
	return s.orSlice(filterInCols(columns, values, false))
}

func (s *filter) OrInColsQuery(columns []string, fc func() (prepare string, args []interface{})) Filter {
	return s.orSlice(filterInColsGet(columns, fc, false))
}

func (s *filter) OrInColsGet(columns []string, cmd Commander) Filter {
	return s.OrInColsQuery(columns, func() (prepare string, args []interface{}) { return cmd.SQL() })
}

func (s *filter) OrExists(fc func() (prepare string, args []interface{})) Filter {
	return s.orSlice(filterExists(fc, false))
}

func (s *filter) OrLike(column string, value interface{}) Filter {
	return s.Or(filterLike(column, false), value)
}

func (s *filter) OrIsNull(column string) Filter {
	return s.Or(filterIsNull(column, false))
}

func (s *filter) OrNotEqual(column string, value interface{}) Filter {
	return s.Or(filterNotEqual(column), value)
}

func (s *filter) OrNotBetween(column string, start interface{}, end interface{}) Filter {
	return s.Or(filterBetween(column, true), start, end)
}

func (s *filter) OrNotIn(column string, values ...interface{}) Filter {
	return s.orSlice(filterIn(column, values, true))
}

func (s *filter) OrNotInQuery(column string, fc func() (prepare string, args []interface{})) Filter {
	return s.orSlice(filterInGet(column, fc, true))
}

func (s *filter) OrNotInGet(column string, cmd Commander) Filter {
	return s.OrNotInQuery(column, func() (prepare string, args []interface{}) { return cmd.SQL() })
}

func (s *filter) OrNotInCols(columns []string, values ...[]interface{}) Filter {
	return s.orSlice(filterInCols(columns, values, true))
}

func (s *filter) OrNotInColsQuery(columns []string, fc func() (prepare string, args []interface{})) Filter {
	return s.orSlice(filterInColsGet(columns, fc, true))
}

func (s *filter) OrNotInColsGet(columns []string, cmd Commander) Filter {
	return s.OrNotInColsQuery(columns, func() (prepare string, args []interface{}) { return cmd.SQL() })
}

func (s *filter) OrNotExists(fc func() (prepare string, args []interface{})) Filter {
	return s.orSlice(filterExists(fc, true))
}

func (s *filter) OrNotLike(column string, value interface{}) Filter {
	return s.Or(filterLike(column, true), value)
}

func (s *filter) OrIsNotNull(column string) Filter {
	return s.Or(filterIsNull(column, true))
}

func (s *filter) SQL() (prepare string, args []interface{}) {
	if s.num > 1 {
		return ConcatString("( ", s.prepare.String(), " )"), s.args
	}
	return s.prepare.String(), s.args
}

func F() Filter {
	return &filter{
		prepare: &strings.Builder{},
	}
}
