package hey

import (
	"fmt"
	"strings"
)

type filterCompare string

const (
	filterCompareEqual         filterCompare = "="
	filterCompareNotEqual      filterCompare = "<>"
	filterCompareMoreThan      filterCompare = ">"
	filterCompareMoreThanEqual filterCompare = ">="
	filterCompareLessThan      filterCompare = "<"
	filterCompareLessThanEqual filterCompare = "<="
)

type filterLogic string

const (
	filterLogicAnd filterLogic = "AND"
	filterLogicOr  filterLogic = "OR"
)

func filterCompareExpr(column string, compare filterCompare) string {
	if column == "" {
		return ""
	}
	return fmt.Sprintf("%s %s %s", column, compare, Placeholder)
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

func filterIn(column string, values []interface{}, in bool) (expr string, args []interface{}) {
	if column == "" {
		return
	}
	args = RemoveDuplicate(values...)
	length := len(args)
	if length == 0 {
		return
	}
	if length == 1 {
		if in {
			expr = filterEqual(column)
		} else {
			expr = filterNotEqual(column)
		}
		return
	}
	result := make([]string, length)
	for i := 0; i < length; i++ {
		result[i] = Placeholder
	}
	tmp := strings.Join(result, ", ")
	if in {
		expr = fmt.Sprintf("%s IN ( %s )", column, tmp)
	} else {
		expr = fmt.Sprintf("%s NOT IN ( %s )", column, tmp)
	}
	return
}

func filterBetween(column string) string {
	if column == "" {
		return ""
	}
	return fmt.Sprintf("%s BETWEEN %s AND %s", column, Placeholder, Placeholder)
}

func filterNotBetween(column string) string {
	if column == "" {
		return ""
	}
	return fmt.Sprintf("%s NOT BETWEEN %s AND %s", column, Placeholder, Placeholder)
}

func filterLike(column string) string {
	if column == "" {
		return ""
	}
	return fmt.Sprintf("%s LIKE %s", column, Placeholder)
}

func filterNotLike(column string) string {
	if column == "" {
		return ""
	}
	return fmt.Sprintf("%s NOT LIKE %s", column, Placeholder)
}

func filterIsNull(column string) string {
	if column == "" {
		return ""
	}
	return fmt.Sprintf("%s IS NULL", column)
}

func filterIsNotNull(column string) string {
	if column == "" {
		return ""
	}
	return fmt.Sprintf("%s IS NOT NULL", column)
}

type Filter interface {
	And(prepare string, args ...interface{}) Filter
	Or(prepare string, args ...interface{}) Filter
	Group(group func(filter Filter) Filter) Filter
	OrGroup(group func(filter Filter) Filter) Filter
	Equal(column string, value interface{}) Filter
	NotEqual(column string, value interface{}) Filter
	MoreThan(column string, value interface{}) Filter
	MoreThanEqual(column string, value interface{}) Filter
	LessThan(column string, value interface{}) Filter
	LessThanEqual(column string, value interface{}) Filter
	In(column string, values ...interface{}) Filter
	NotIn(column string, values ...interface{}) Filter
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
	OrNotIn(column string, values ...interface{}) Filter
	OrBetween(column string, start interface{}, end interface{}) Filter
	OrNotBetween(column string, start interface{}, end interface{}) Filter
	OrLike(column string, value interface{}) Filter
	OrNotLike(column string, value interface{}) Filter
	OrIsNull(column string) Filter
	OrIsNotNull(column string) Filter
	Clear() Filter
	Result() (prepare string, args []interface{})
}

type filter struct {
	prepare string
	args    []interface{}
}

func (s *filter) add(logic filterLogic, expr string, args []interface{}) Filter {
	if expr == "" {
		return s
	}
	if s.prepare == "" {
		s.prepare = expr
		s.args = args
		return s
	}
	s.prepare = fmt.Sprintf("%s %s %s", s.prepare, logic, expr)
	s.args = append(s.args, args...)
	return s
}

func (s *filter) And(expr string, args ...interface{}) Filter {
	return s.add(filterLogicAnd, expr, args)
}

func (s *filter) Or(expr string, args ...interface{}) Filter {
	return s.add(filterLogicOr, expr, args)
}

func (s *filter) addGroup(logic filterLogic, group func(filter Filter) Filter) Filter {
	if group == nil {
		return s
	}
	expr, args := group(NewFilter()).Result()
	if expr == "" {
		return s
	}
	expr = fmt.Sprintf("( %s )", expr)
	return s.add(logic, expr, args)
}

func (s *filter) Group(group func(filter Filter) Filter) Filter {
	return s.addGroup(filterLogicAnd, group)
}

func (s *filter) OrGroup(group func(filter Filter) Filter) Filter {
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
	expr, args := filterIn(column, values, true)
	return s.And(expr, args...)
}

func (s *filter) NotIn(column string, values ...interface{}) Filter {
	expr, args := filterIn(column, values, false)
	return s.And(expr, args...)
}

func (s *filter) Like(column string, value interface{}) Filter {
	return s.And(filterLike(column), value)
}

func (s *filter) NotLike(column string, value interface{}) Filter {
	return s.And(filterNotLike(column), value)
}

func (s *filter) IsNull(column string) Filter {
	return s.And(filterIsNull(column))
}

func (s *filter) IsNotNull(column string) Filter {
	return s.And(filterIsNotNull(column))
}

func (s *filter) Between(column string, start interface{}, end interface{}) Filter {
	return s.And(filterBetween(column), start, end)
}

func (s *filter) NotBetween(column string, start interface{}, end interface{}) Filter {
	return s.And(filterNotBetween(column), start, end)
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
	expr, args := filterIn(column, values, true)
	return s.Or(expr, args...)
}

func (s *filter) OrNotIn(column string, values ...interface{}) Filter {
	expr, args := filterIn(column, values, false)
	return s.Or(expr, args...)
}

func (s *filter) OrLike(column string, value interface{}) Filter {
	return s.Or(filterLike(column), value)
}

func (s *filter) OrNotLike(column string, value interface{}) Filter {
	return s.Or(filterNotLike(column), value)
}

func (s *filter) OrIsNull(column string) Filter {
	return s.Or(filterIsNull(column))
}

func (s *filter) OrIsNotNull(column string) Filter {
	return s.Or(filterIsNotNull(column))
}

func (s *filter) OrBetween(column string, start interface{}, end interface{}) Filter {
	return s.Or(filterBetween(column), start, end)
}

func (s *filter) OrNotBetween(column string, start interface{}, end interface{}) Filter {
	return s.Or(filterNotBetween(column), start, end)
}

func (s *filter) Clear() Filter {
	s.prepare, s.args = "", nil
	return s
}

func (s *filter) Result() (string, []interface{}) {
	return s.prepare, s.args
}

func NewFilter() Filter {
	return &filter{}
}

func FilterMerge(or bool, filters ...Filter) (result Filter) {
	result = NewFilter()
	for _, f := range filters {
		if f == nil {
			continue
		}
		prepare, args := f.Result()
		if or {
			result.Or(prepare, args...)
		} else {
			result.And(prepare, args...)
		}
	}
	return
}
