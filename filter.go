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
	if column == "" {
		return
	}
	values = RemoveDuplicate(values...)
	length := len(values)
	if length == 0 {
		return
	}
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

type Filter interface {
	And(prepare string, args ...interface{}) Filter
	Or(prepare string, args ...interface{}) Filter
	Filter(filters ...Filter) Filter
	OrFilter(filters ...Filter) Filter
	Group(group func(filter Filter)) Filter
	OrGroup(group func(filter Filter)) Filter
	Equal(column string, value interface{}) Filter
	NotEqual(column string, value interface{}) Filter
	MoreThan(column string, value interface{}) Filter
	MoreThanEqual(column string, value interface{}) Filter
	LessThan(column string, value interface{}) Filter
	LessThanEqual(column string, value interface{}) Filter
	In(column string, values ...interface{}) Filter
	InSQL(column string, prepare string, args ...interface{}) Filter
	InGet(column string, get *Get) Filter
	InInt(column string, values []int) Filter
	InInt64(column string, values []int64) Filter
	InString(column string, values []string) Filter
	NotIn(column string, values ...interface{}) Filter
	NotInSQL(column string, prepare string, args ...interface{}) Filter
	NotInGet(column string, get *Get) Filter
	NotInInt(column string, values []int) Filter
	NotInInt64(column string, values []int64) Filter
	NotInString(column string, values []string) Filter
	Between(column string, start interface{}, end interface{}) Filter
	NotBetween(column string, start interface{}, end interface{}) Filter
	Like(column string, value interface{}) Filter
	NotLike(column string, value interface{}) Filter
	IsNull(column string) Filter
	IsNotNull(column string) Filter
	OrEqual(column string, value interface{}) Filter
	OrNotEqual(column string, value interface{}) Filter
	OrMoreThan(column string, value interface{}) Filter
	OrMoreThanEqual(column string, value interface{}) Filter
	OrLessThan(column string, value interface{}) Filter
	OrLessThanEqual(column string, value interface{}) Filter
	OrIn(column string, values ...interface{}) Filter
	OrInSQL(column string, prepare string, args ...interface{}) Filter
	OrInGet(column string, get *Get) Filter
	OrInInt(column string, values []int) Filter
	OrInInt64(column string, values []int64) Filter
	OrInString(column string, values []string) Filter
	OrNotIn(column string, values ...interface{}) Filter
	OrNotInSQL(column string, prepare string, args ...interface{}) Filter
	OrNotInGet(column string, get *Get) Filter
	OrNotInInt(column string, values []int) Filter
	OrNotInInt64(column string, values []int64) Filter
	OrNotInString(column string, values []string) Filter
	OrBetween(column string, start interface{}, end interface{}) Filter
	OrNotBetween(column string, start interface{}, end interface{}) Filter
	OrLike(column string, value interface{}) Filter
	OrNotLike(column string, value interface{}) Filter
	OrIsNull(column string) Filter
	OrIsNotNull(column string) Filter
	SQL() (prepare string, args []interface{})
}

type filter struct {
	prepare *strings.Builder
	args    []interface{}
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

func (s *filter) In(column string, values ...interface{}) Filter {
	expr, args := filterIn(column, values, false)
	return s.And(expr, args...)
}

func (s *filter) InSQL(column string, prepare string, args ...interface{}) Filter {
	if column == "" || prepare == "" {
		return s
	}
	expr := ConcatString(column, " IN ( ", prepare, " )")
	return s.And(expr, args...)
}

func (s *filter) InGet(column string, get *Get) Filter {
	if get == nil {
		return s
	}
	prepare, args := get.SQL()
	return s.InSQL(column, prepare, args...)
}

func (s *filter) InInt(column string, values []int) Filter {
	length := len(values)
	if length == 0 {
		return s
	}
	args := make([]interface{}, length)
	for i := 0; i < length; i++ {
		args[i] = values[i]
	}
	return s.In(column, args...)
}

func (s *filter) InInt64(column string, values []int64) Filter {
	length := len(values)
	if length == 0 {
		return s
	}
	args := make([]interface{}, length)
	for i := 0; i < length; i++ {
		args[i] = values[i]
	}
	return s.In(column, args...)
}

func (s *filter) InString(column string, values []string) Filter {
	length := len(values)
	if length == 0 {
		return s
	}
	args := make([]interface{}, length)
	for i := 0; i < length; i++ {
		args[i] = values[i]
	}
	return s.In(column, args...)
}

func (s *filter) NotIn(column string, values ...interface{}) Filter {
	expr, args := filterIn(column, values, true)
	return s.And(expr, args...)
}

func (s *filter) NotInSQL(column string, prepare string, args ...interface{}) Filter {
	if column == "" || prepare == "" {
		return s
	}
	expr := ConcatString(column, " NOT IN ( ", prepare, " )")
	return s.And(expr, args...)
}

func (s *filter) NotInGet(column string, get *Get) Filter {
	if get == nil {
		return s
	}
	prepare, args := get.SQL()
	return s.NotInSQL(column, prepare, args...)
}

func (s *filter) NotInInt(column string, values []int) Filter {
	length := len(values)
	if length == 0 {
		return s
	}
	args := make([]interface{}, length)
	for i := 0; i < length; i++ {
		args[i] = values[i]
	}
	return s.NotIn(column, args...)
}

func (s *filter) NotInInt64(column string, values []int64) Filter {
	length := len(values)
	if length == 0 {
		return s
	}
	args := make([]interface{}, length)
	for i := 0; i < length; i++ {
		args[i] = values[i]
	}
	return s.NotIn(column, args...)
}

func (s *filter) NotInString(column string, values []string) Filter {
	length := len(values)
	if length == 0 {
		return s
	}
	args := make([]interface{}, length)
	for i := 0; i < length; i++ {
		args[i] = values[i]
	}
	return s.NotIn(column, args...)
}

func (s *filter) Like(column string, value interface{}) Filter {
	return s.And(filterLike(column, false), value)
}

func (s *filter) NotLike(column string, value interface{}) Filter {
	return s.And(filterLike(column, true), value)
}

func (s *filter) IsNull(column string) Filter {
	return s.And(filterIsNull(column, false))
}

func (s *filter) IsNotNull(column string) Filter {
	return s.And(filterIsNull(column, true))
}

func (s *filter) Between(column string, start interface{}, end interface{}) Filter {
	return s.And(filterBetween(column, false), start, end)
}

func (s *filter) NotBetween(column string, start interface{}, end interface{}) Filter {
	return s.And(filterBetween(column, true), start, end)
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

func (s *filter) OrIn(column string, values ...interface{}) Filter {
	expr, args := filterIn(column, values, false)
	return s.Or(expr, args...)
}

func (s *filter) OrInSQL(column string, prepare string, args ...interface{}) Filter {
	if column == "" || prepare == "" {
		return s
	}
	expr := ConcatString(column, " IN ( ", prepare, " )")
	return s.Or(expr, args...)
}

func (s *filter) OrInGet(column string, get *Get) Filter {
	if get == nil {
		return s
	}
	prepare, args := get.SQL()
	return s.OrInSQL(column, prepare, args...)
}

func (s *filter) OrInInt(column string, values []int) Filter {
	length := len(values)
	if length == 0 {
		return s
	}
	args := make([]interface{}, length)
	for i := 0; i < length; i++ {
		args[i] = values[i]
	}
	return s.OrIn(column, args...)
}

func (s *filter) OrInInt64(column string, values []int64) Filter {
	length := len(values)
	if length == 0 {
		return s
	}
	args := make([]interface{}, length)
	for i := 0; i < length; i++ {
		args[i] = values[i]
	}
	return s.OrIn(column, args...)
}

func (s *filter) OrInString(column string, values []string) Filter {
	length := len(values)
	if length == 0 {
		return s
	}
	args := make([]interface{}, length)
	for i := 0; i < length; i++ {
		args[i] = values[i]
	}
	return s.OrIn(column, args...)
}

func (s *filter) OrNotIn(column string, values ...interface{}) Filter {
	expr, args := filterIn(column, values, true)
	return s.Or(expr, args...)
}

func (s *filter) OrNotInSQL(column string, prepare string, args ...interface{}) Filter {
	if column == "" || prepare == "" {
		return s
	}
	expr := ConcatString(column, " NOT IN ( ", prepare, " )")
	return s.Or(expr, args...)
}

func (s *filter) OrNotInGet(column string, get *Get) Filter {
	if get == nil {
		return s
	}
	prepare, args := get.SQL()
	return s.OrNotInSQL(column, prepare, args...)
}

func (s *filter) OrNotInInt(column string, values []int) Filter {
	length := len(values)
	if length == 0 {
		return s
	}
	args := make([]interface{}, length)
	for i := 0; i < length; i++ {
		args[i] = values[i]
	}
	return s.OrNotIn(column, args...)
}

func (s *filter) OrNotInInt64(column string, values []int64) Filter {
	length := len(values)
	if length == 0 {
		return s
	}
	args := make([]interface{}, length)
	for i := 0; i < length; i++ {
		args[i] = values[i]
	}
	return s.OrNotIn(column, args...)
}

func (s *filter) OrNotInString(column string, values []string) Filter {
	length := len(values)
	if length == 0 {
		return s
	}
	args := make([]interface{}, length)
	for i := 0; i < length; i++ {
		args[i] = values[i]
	}
	return s.OrNotIn(column, args...)
}

func (s *filter) OrLike(column string, value interface{}) Filter {
	return s.Or(filterLike(column, false), value)
}

func (s *filter) OrNotLike(column string, value interface{}) Filter {
	return s.Or(filterLike(column, true), value)
}

func (s *filter) OrIsNull(column string) Filter {
	return s.Or(filterIsNull(column, false))
}

func (s *filter) OrIsNotNull(column string) Filter {
	return s.Or(filterIsNull(column, true))
}

func (s *filter) OrBetween(column string, start interface{}, end interface{}) Filter {
	return s.Or(filterBetween(column, false), start, end)
}

func (s *filter) OrNotBetween(column string, start interface{}, end interface{}) Filter {
	return s.Or(filterBetween(column, true), start, end)
}

func (s *filter) SQL() (prepare string, args []interface{}) {
	return s.prepare.String(), s.args
}

func NewFilter() Filter {
	return &filter{
		prepare: &strings.Builder{},
	}
}
