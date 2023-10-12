package hey

import (
	"strings"
)

const (
	Placeholder = "?"
)

const (
	filterCompareEqual         = "="
	filterCompareNotEqual      = "<>"
	filterCompareMoreThan      = ">"
	filterCompareMoreThanEqual = ">="
	filterCompareLessThan      = "<"
	filterCompareLessThanEqual = "<="
)

const (
	filterLogicAnd = "AND"
	filterLogicOr  = "OR"
)

type Preparer interface {
	// SQL get the prepare sql statement and parameter list
	SQL() (prepare string, args []interface{})
}

func filterCompareExpr(column string, compare string) (expr string) {
	if column == "" {
		return
	}
	expr = ConcatString(column, " ", compare, " ", Placeholder)
	return
}

func filterEqual(column string) string {
	return filterCompareExpr(column, filterCompareEqual)
}

func filterNotEqual(column string) string {
	return filterCompareExpr(column, filterCompareNotEqual)
}

func filterMoreThan(column string) string {
	return filterCompareExpr(column, filterCompareMoreThan)
}

func filterMoreThanEqual(column string) string {
	return filterCompareExpr(column, filterCompareMoreThanEqual)
}

func filterLessThan(column string) string {
	return filterCompareExpr(column, filterCompareLessThan)
}

func filterLessThanEqual(column string) string {
	return filterCompareExpr(column, filterCompareLessThanEqual)
}

func filterIn(column string, values []interface{}, not bool) (expr string, args []interface{}) {
	if column == "" || values == nil {
		return
	}
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
			result = append(result, Placeholder)
			continue
		}
		result = append(result, ", ", Placeholder)
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
	if column == "" || fc == nil {
		return
	}
	prepare := ""
	prepare, args = fc()
	if prepare == "" {
		return
	}
	expr = column
	if not {
		expr = ConcatString(expr, " NOT")
	}
	expr = ConcatString(expr, " IN")
	expr = ConcatString(expr, " ( ", prepare, " )")
	return
}

func filterInInt(values []int) (args []interface{}) {
	length := len(values)
	if length == 0 {
		return
	}
	args = make([]interface{}, length)
	for i := 0; i < length; i++ {
		args[i] = values[i]
	}
	return
}

func filterInInt64(values []int64) (args []interface{}) {
	length := len(values)
	if length == 0 {
		return
	}
	args = make([]interface{}, length)
	for i := 0; i < length; i++ {
		args[i] = values[i]
	}
	return
}

func filterInString(values []string) (args []interface{}) {
	length := len(values)
	if length == 0 {
		return
	}
	args = make([]interface{}, length)
	for i := 0; i < length; i++ {
		args[i] = values[i]
	}
	return
}

func inCols(columns ...string) string {
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
		oneGroup[i] = "?"
	}
	oneGroupString := ConcatString("( ", strings.Join(oneGroup, ", "), " )")
	valueGroup := make([]string, length)
	for i := 0; i < length; i++ {
		valueGroup[i] = oneGroupString
	}

	tmp := make([]string, 0, 5)
	tmp = append(tmp, inCols(columns...))
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
	prepare := ""
	prepare, args = fc()
	if prepare == "" {
		return
	}
	tmp := make([]string, 0, 5)
	tmp = append(tmp, inCols(columns...))
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
	if prepare == "" {
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
	if column == "" {
		return
	}
	expr = column
	if not {
		expr = ConcatString(expr, " NOT")
	}
	expr = ConcatString(expr, " BETWEEN ", Placeholder, " AND ", Placeholder)
	return
}

func filterLike(column string, not bool) (expr string) {
	if column == "" {
		return
	}
	expr = column
	if not {
		expr = ConcatString(expr, " NOT")
	}
	expr = ConcatString(expr, " LIKE ", Placeholder)
	return
}

func filterIsNull(column string, not bool) (expr string) {
	if column == "" {
		return
	}
	expr = ConcatString(column, " IS")
	if not {
		expr = ConcatString(expr, " NOT")
	}
	expr = ConcatString(expr, " NULL")
	return
}

// Indexing may be abandoned when filtering data:
// 1. It is not recommended to directly call methods with Not, Or, Null keywords;
// 2. Be careful when calling methods with the Like keyword;

type Filter interface {
	Copy(filter ...Filter) Filter
	And(prepare string, args ...interface{}) Filter
	Filter(filters ...Filter) Filter
	Group(group func(filter Filter)) Filter
	Equal(column string, value interface{}) Filter
	NotEqual(column string, value interface{}) Filter
	MoreThan(column string, value interface{}) Filter
	MoreThanEqual(column string, value interface{}) Filter
	LessThan(column string, value interface{}) Filter
	LessThanEqual(column string, value interface{}) Filter
	Between(column string, start interface{}, end interface{}) Filter
	In(column string, values ...interface{}) Filter
	InQuery(column string, fc func() (prepare string, args []interface{})) Filter
	InGet(column string, preparer Preparer) Filter
	InInt(column string, values []int) Filter
	InInt64(column string, values []int64) Filter
	InString(column string, values []string) Filter
	InCols(columns []string, values ...[]interface{}) Filter
	InColsQuery(columns []string, fc func() (prepare string, args []interface{})) Filter
	InColsGet(columns []string, preparer Preparer) Filter
	Exists(fc func() (prepare string, args []interface{})) Filter
	Like(column string, value interface{}) Filter
	IsNull(column string) Filter
	NotBetween(column string, start interface{}, end interface{}) Filter
	NotIn(column string, values ...interface{}) Filter
	NotInQuery(column string, fc func() (prepare string, args []interface{})) Filter
	NotInGet(column string, preparer Preparer) Filter
	NotInInt(column string, values []int) Filter
	NotInInt64(column string, values []int64) Filter
	NotInString(column string, values []string) Filter
	NotInCols(columns []string, values ...[]interface{}) Filter
	NotInColsQuery(columns []string, fc func() (prepare string, args []interface{})) Filter
	NotInColsGet(columns []string, preparer Preparer) Filter
	NotExists(fc func() (prepare string, args []interface{})) Filter
	NotLike(column string, value interface{}) Filter
	IsNotNull(column string) Filter
	Or(prepare string, args ...interface{}) Filter
	OrFilter(filters ...Filter) Filter
	OrGroup(group func(filter Filter)) Filter
	OrEqual(column string, value interface{}) Filter
	OrNotEqual(column string, value interface{}) Filter
	OrMoreThan(column string, value interface{}) Filter
	OrMoreThanEqual(column string, value interface{}) Filter
	OrLessThan(column string, value interface{}) Filter
	OrLessThanEqual(column string, value interface{}) Filter
	OrBetween(column string, start interface{}, end interface{}) Filter
	OrIn(column string, values ...interface{}) Filter
	OrInQuery(column string, fc func() (prepare string, args []interface{})) Filter
	OrInGet(column string, preparer Preparer) Filter
	OrInInt(column string, values []int) Filter
	OrInInt64(column string, values []int64) Filter
	OrInString(column string, values []string) Filter
	OrInCols(columns []string, values ...[]interface{}) Filter
	OrInColsQuery(columns []string, fc func() (prepare string, args []interface{})) Filter
	OrInColsGet(columns []string, preparer Preparer) Filter
	OrExists(fc func() (prepare string, args []interface{})) Filter
	OrLike(column string, value interface{}) Filter
	OrIsNull(column string) Filter
	OrNotBetween(column string, start interface{}, end interface{}) Filter
	OrNotIn(column string, values ...interface{}) Filter
	OrNotInQuery(column string, fc func() (prepare string, args []interface{})) Filter
	OrNotInGet(column string, preparer Preparer) Filter
	OrNotInInt(column string, values []int) Filter
	OrNotInInt64(column string, values []int64) Filter
	OrNotInString(column string, values []string) Filter
	OrNotInCols(columns []string, values ...[]interface{}) Filter
	OrNotInColsQuery(columns []string, fc func() (prepare string, args []interface{})) Filter
	OrNotInColsGet(columns []string, preparer Preparer) Filter
	OrNotExists(fc func() (prepare string, args []interface{})) Filter
	OrNotLike(column string, value interface{}) Filter
	OrIsNotNull(column string) Filter
	SQL() (prepare string, args []interface{})
}

type filter struct {
	prepare *strings.Builder
	args    []interface{}
}

func (s *filter) Copy(filter ...Filter) Filter {
	return NewFilter().Filter(filter...)
}

func (s *filter) add(logic string, expr string, args []interface{}) Filter {
	if expr == "" {
		return s
	}
	if s.prepare.Len() == 0 {
		s.prepare.WriteString(expr)
		s.args = args
		return s
	}
	s.prepare.WriteString(ConcatString(" ", logic, " ", expr))
	s.args = append(s.args, args...)
	return s
}

func (s *filter) And(expr string, args ...interface{}) Filter {
	return s.add(filterLogicAnd, expr, args)
}

func (s *filter) Or(expr string, args ...interface{}) Filter {
	return s.add(filterLogicOr, expr, args)
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
		if prepare == "" {
			continue
		}
		s.And(prepare, args...)
	}
	return s
}

func (s *filter) OrFilter(filters ...Filter) Filter {
	for _, f := range filters {
		if f == nil {
			continue
		}
		prepare, args := f.SQL()
		if prepare == "" {
			continue
		}
		s.Or(prepare, args...)
	}
	return s
}

func (s *filter) addGroup(logic string, group func(filter Filter)) Filter {
	if group == nil {
		return s
	}
	newFilter := NewFilter()
	group(newFilter)
	expr, args := newFilter.SQL()
	if expr == "" {
		return s
	}
	expr = ConcatString("( ", expr, " )")
	return s.add(logic, expr, args)
}

func (s *filter) Group(group func(filter Filter)) Filter {
	return s.addGroup(filterLogicAnd, group)
}

func (s *filter) OrGroup(group func(filter Filter)) Filter {
	return s.addGroup(filterLogicOr, group)
}

func (s *filter) Equal(column string, value interface{}) Filter {
	return s.And(filterEqual(column), value)
}

func (s *filter) NotEqual(column string, value interface{}) Filter {
	return s.And(filterNotEqual(column), value)
}

func (s *filter) MoreThan(column string, value interface{}) Filter {
	return s.And(filterMoreThan(column), value)
}

func (s *filter) MoreThanEqual(column string, value interface{}) Filter {
	return s.And(filterMoreThanEqual(column), value)
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

func (s *filter) InGet(column string, preparer Preparer) Filter {
	return s.InQuery(column, func() (prepare string, args []interface{}) { return preparer.SQL() })
}

func (s *filter) InInt(column string, values []int) Filter {
	return s.In(column, filterInInt(values)...)
}

func (s *filter) InInt64(column string, values []int64) Filter {
	return s.In(column, filterInInt64(values)...)
}

func (s *filter) InString(column string, values []string) Filter {
	return s.In(column, filterInString(values)...)
}

func (s *filter) InCols(columns []string, values ...[]interface{}) Filter {
	return s.andSlice(filterInCols(columns, values, false))
}

func (s *filter) InColsQuery(columns []string, fc func() (prepare string, args []interface{})) Filter {
	return s.andSlice(filterInColsGet(columns, fc, false))
}

func (s *filter) InColsGet(columns []string, preparer Preparer) Filter {
	return s.InColsQuery(columns, func() (prepare string, args []interface{}) { return preparer.SQL() })
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

func (s *filter) NotBetween(column string, start interface{}, end interface{}) Filter {
	return s.And(filterBetween(column, true), start, end)
}

func (s *filter) NotIn(column string, values ...interface{}) Filter {
	return s.andSlice(filterIn(column, values, true))
}

func (s *filter) NotInQuery(column string, fc func() (prepare string, args []interface{})) Filter {
	return s.andSlice(filterInGet(column, fc, true))
}

func (s *filter) NotInGet(column string, preparer Preparer) Filter {
	return s.NotInQuery(column, func() (prepare string, args []interface{}) { return preparer.SQL() })
}

func (s *filter) NotInInt(column string, values []int) Filter {
	return s.NotIn(column, filterInInt(values)...)
}

func (s *filter) NotInInt64(column string, values []int64) Filter {
	return s.NotIn(column, filterInInt64(values)...)
}

func (s *filter) NotInString(column string, values []string) Filter {
	return s.NotIn(column, filterInString(values)...)
}

func (s *filter) NotInCols(columns []string, values ...[]interface{}) Filter {
	return s.andSlice(filterInCols(columns, values, true))
}

func (s *filter) NotInColsQuery(columns []string, fc func() (prepare string, args []interface{})) Filter {
	return s.andSlice(filterInColsGet(columns, fc, true))
}

func (s *filter) NotInColsGet(columns []string, preparer Preparer) Filter {
	return s.NotInColsQuery(columns, func() (prepare string, args []interface{}) { return preparer.SQL() })
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

func (s *filter) OrEqual(column string, value interface{}) Filter {
	return s.Or(filterEqual(column), value)
}

func (s *filter) OrNotEqual(column string, value interface{}) Filter {
	return s.Or(filterNotEqual(column), value)
}

func (s *filter) OrMoreThan(column string, value interface{}) Filter {
	return s.Or(filterMoreThan(column), value)
}

func (s *filter) OrMoreThanEqual(column string, value interface{}) Filter {
	return s.Or(filterMoreThanEqual(column), value)
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

func (s *filter) OrInGet(column string, preparer Preparer) Filter {
	return s.OrInQuery(column, func() (prepare string, args []interface{}) { return preparer.SQL() })
}

func (s *filter) OrInInt(column string, values []int) Filter {
	return s.OrIn(column, filterInInt(values)...)
}

func (s *filter) OrInInt64(column string, values []int64) Filter {
	return s.OrIn(column, filterInInt64(values)...)
}

func (s *filter) OrInString(column string, values []string) Filter {
	return s.OrIn(column, filterInString(values)...)
}

func (s *filter) OrInCols(columns []string, values ...[]interface{}) Filter {
	return s.orSlice(filterInCols(columns, values, false))
}

func (s *filter) OrInColsQuery(columns []string, fc func() (prepare string, args []interface{})) Filter {
	return s.orSlice(filterInColsGet(columns, fc, false))
}

func (s *filter) OrInColsGet(columns []string, preparer Preparer) Filter {
	return s.OrInColsQuery(columns, func() (prepare string, args []interface{}) { return preparer.SQL() })
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

func (s *filter) OrNotBetween(column string, start interface{}, end interface{}) Filter {
	return s.Or(filterBetween(column, true), start, end)
}

func (s *filter) OrNotIn(column string, values ...interface{}) Filter {
	return s.orSlice(filterIn(column, values, true))
}

func (s *filter) OrNotInQuery(column string, fc func() (prepare string, args []interface{})) Filter {
	return s.orSlice(filterInGet(column, fc, true))
}

func (s *filter) OrNotInGet(column string, preparer Preparer) Filter {
	return s.OrNotInQuery(column, func() (prepare string, args []interface{}) { return preparer.SQL() })
}

func (s *filter) OrNotInInt(column string, values []int) Filter {
	return s.OrNotIn(column, filterInInt(values)...)
}

func (s *filter) OrNotInInt64(column string, values []int64) Filter {
	return s.OrNotIn(column, filterInInt64(values)...)
}

func (s *filter) OrNotInString(column string, values []string) Filter {
	return s.OrNotIn(column, filterInString(values)...)
}

func (s *filter) OrNotInCols(columns []string, values ...[]interface{}) Filter {
	return s.orSlice(filterInCols(columns, values, true))
}

func (s *filter) OrNotInColsQuery(columns []string, fc func() (prepare string, args []interface{})) Filter {
	return s.orSlice(filterInColsGet(columns, fc, true))
}

func (s *filter) OrNotInColsGet(columns []string, preparer Preparer) Filter {
	return s.OrNotInColsQuery(columns, func() (prepare string, args []interface{}) { return preparer.SQL() })
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
	return s.prepare.String(), s.args
}

func NewFilter() Filter {
	return &filter{
		prepare: &strings.Builder{},
	}
}
