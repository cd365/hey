package hey

import (
	"fmt"
)

func _MakeIn(length int) (result string) {
	for i := 0; i < length; i++ {
		if i == 0 {
			result = Placeholder
		} else {
			result = fmt.Sprintf("%s, %s", result, Placeholder)
		}
	}
	return
}

func _Equal(column string) string {
	if column == "" {
		return ""
	}
	return fmt.Sprintf("%s = %s", column, Placeholder)
}

func _NotEqual(column string) string {
	if column == "" {
		return ""
	}
	return fmt.Sprintf("%s <> %s", column, Placeholder)
}

func _GreaterThan(column string) string {
	if column == "" {
		return ""
	}
	return fmt.Sprintf("%s > %s", column, Placeholder)
}

func _GreaterThanEqual(column string) string {
	if column == "" {
		return ""
	}
	return fmt.Sprintf("%s >= %s", column, Placeholder)
}

func _LessThan(column string) string {
	if column == "" {
		return ""
	}
	return fmt.Sprintf("%s < %s", column, Placeholder)
}

func _LessThanEqual(column string) string {
	if column == "" {
		return ""
	}
	return fmt.Sprintf("%s <= %s", column, Placeholder)
}

func _In(column string, length int) string {
	if column == "" || length <= 0 {
		return ""
	}
	if length == 1 {
		return _Equal(column)
	}
	return fmt.Sprintf("%s IN ( %s )", column, _MakeIn(length))
}

func _NotIn(column string, length int) string {
	if column == "" || length <= 0 {
		return ""
	}
	if length == 1 {
		return _NotEqual(column)
	}
	return fmt.Sprintf("%s NOT IN ( %s )", column, _MakeIn(length))
}

func _Like(column string) string {
	if column == "" {
		return ""
	}
	return fmt.Sprintf("%s LIKE %s", column, Placeholder)
}

func _NotLike(column string) string {
	if column == "" {
		return ""
	}
	return fmt.Sprintf("%s NOT LIKE %s", column, Placeholder)
}

func _IsNull(column string) string {
	if column == "" {
		return ""
	}
	return fmt.Sprintf("%s IS NULL", column)
}

func _IsNotNull(column string) string {
	if column == "" {
		return ""
	}
	return fmt.Sprintf("%s IS NOT NULL", column)
}

func _Between(column string) string {
	if column == "" {
		return ""
	}
	return fmt.Sprintf("%s BETWEEN %s AND %s", column, Placeholder, Placeholder)
}

func _NotBetween(column string) string {
	if column == "" {
		return ""
	}
	return fmt.Sprintf("%s NOT BETWEEN %s AND %s", column, Placeholder, Placeholder)
}

type Filter interface {
	And(prepare string, args ...interface{}) Filter
	Or(prepare string, args ...interface{}) Filter
	Equal(column string, value interface{}) Filter
	NotEqual(column string, value interface{}) Filter
	GreaterThan(column string, value interface{}) Filter
	GreaterThanEqual(column string, value interface{}) Filter
	LessThan(column string, value interface{}) Filter
	LessThanEqual(column string, value interface{}) Filter
	In(column string, values ...interface{}) Filter
	NotIn(column string, values ...interface{}) Filter
	Like(column string, value interface{}) Filter
	NotLike(column string, value interface{}) Filter
	IsNull(column string) Filter
	IsNotNull(column string) Filter
	Between(column string, start interface{}, end interface{}) Filter
	NotBetween(column string, start interface{}, end interface{}) Filter
	OrEqual(column string, value interface{}) Filter
	OrNotEqual(column string, value interface{}) Filter
	OrGreaterThan(column string, value interface{}) Filter
	OrGreaterThanEqual(column string, value interface{}) Filter
	OrLessThan(column string, value interface{}) Filter
	OrLessThanEqual(column string, value interface{}) Filter
	OrIn(column string, values ...interface{}) Filter
	OrNotIn(column string, values ...interface{}) Filter
	OrLike(column string, value interface{}) Filter
	OrNotLike(column string, value interface{}) Filter
	OrIsNull(column string) Filter
	OrIsNotNull(column string) Filter
	OrBetween(column string, start interface{}, end interface{}) Filter
	OrNotBetween(column string, start interface{}, end interface{}) Filter
	Group(group func(filter Filter) Filter) Filter
	OrGroup(group func(filter Filter) Filter) Filter
	Clear() Filter
	Result() (prepare string, args []interface{})
}

type filter struct {
	prepare string
	args    []interface{}
}

func (s *filter) And(prepare string, args ...interface{}) Filter {
	if prepare == "" {
		return s
	}
	if s.prepare == "" {
		s.prepare = prepare
		s.args = args
	} else {
		s.prepare = fmt.Sprintf("%s AND %s", s.prepare, prepare)
		s.args = append(s.args, args...)
	}
	return s
}

func (s *filter) Or(prepare string, args ...interface{}) Filter {
	if prepare == "" {
		return s
	}
	if s.prepare == "" {
		s.prepare = prepare
		s.args = args
	} else {
		s.prepare = fmt.Sprintf("%s OR %s", s.prepare, prepare)
		s.args = append(s.args, args...)
	}
	return s
}

func (s *filter) Equal(column string, value interface{}) Filter {
	return s.And(_Equal(column), value)
}

func (s *filter) NotEqual(column string, value interface{}) Filter {
	return s.And(_NotEqual(column), value)
}

func (s *filter) GreaterThan(column string, value interface{}) Filter {
	return s.And(_GreaterThan(column), value)
}

func (s *filter) GreaterThanEqual(column string, value interface{}) Filter {
	return s.And(_GreaterThanEqual(column), value)
}

func (s *filter) LessThan(column string, value interface{}) Filter {
	return s.And(_LessThan(column), value)
}

func (s *filter) LessThanEqual(column string, value interface{}) Filter {
	return s.And(_LessThanEqual(column), value)
}

func (s *filter) In(column string, values ...interface{}) Filter {
	values = RemoveDuplicate(values...)
	return s.And(_In(column, len(values)), values...)
}

func (s *filter) NotIn(column string, values ...interface{}) Filter {
	values = RemoveDuplicate(values...)
	return s.And(_NotIn(column, len(values)), values...)
}

func (s *filter) Like(column string, value interface{}) Filter {
	return s.And(_Like(column), value)
}

func (s *filter) NotLike(column string, value interface{}) Filter {
	return s.And(_NotLike(column), value)
}

func (s *filter) IsNull(column string) Filter {
	return s.And(_IsNull(column))
}

func (s *filter) IsNotNull(column string) Filter {
	return s.And(_IsNotNull(column))
}

func (s *filter) Between(column string, start interface{}, end interface{}) Filter {
	return s.And(_Between(column), start, end)
}

func (s *filter) NotBetween(column string, start interface{}, end interface{}) Filter {
	return s.And(_NotBetween(column), start, end)
}

func (s *filter) OrEqual(column string, value interface{}) Filter {
	return s.Or(_Equal(column), value)
}

func (s *filter) OrNotEqual(column string, value interface{}) Filter {
	return s.Or(_NotEqual(column), value)
}

func (s *filter) OrGreaterThan(column string, value interface{}) Filter {
	return s.Or(_GreaterThan(column), value)
}

func (s *filter) OrGreaterThanEqual(column string, value interface{}) Filter {
	return s.Or(_GreaterThanEqual(column), value)
}

func (s *filter) OrLessThan(column string, value interface{}) Filter {
	return s.Or(_LessThan(column), value)
}

func (s *filter) OrLessThanEqual(column string, value interface{}) Filter {
	return s.Or(_LessThanEqual(column), value)
}

func (s *filter) OrIn(column string, values ...interface{}) Filter {
	values = RemoveDuplicate(values...)
	return s.Or(_In(column, len(values)), values...)
}

func (s *filter) OrNotIn(column string, values ...interface{}) Filter {
	values = RemoveDuplicate(values...)
	return s.Or(_NotIn(column, len(values)), values...)
}

func (s *filter) OrLike(column string, value interface{}) Filter {
	return s.Or(_Like(column), value)
}

func (s *filter) OrNotLike(column string, value interface{}) Filter {
	return s.Or(_NotLike(column), value)
}

func (s *filter) OrIsNull(column string) Filter {
	return s.Or(_IsNull(column))
}

func (s *filter) OrIsNotNull(column string) Filter {
	return s.Or(_IsNotNull(column))
}

func (s *filter) OrBetween(column string, start interface{}, end interface{}) Filter {
	return s.Or(_Between(column), start, end)
}

func (s *filter) OrNotBetween(column string, start interface{}, end interface{}) Filter {
	return s.Or(_NotBetween(column), start, end)
}

func (s *filter) Group(group func(filter Filter) Filter) Filter {
	if group == nil {
		return s
	}
	prepare, args := group(&filter{}).Result()
	if prepare != "" {
		prepare = fmt.Sprintf("( %s )", prepare)
	}
	return s.And(prepare, args...)
}

func (s *filter) OrGroup(group func(filter Filter) Filter) Filter {
	if group == nil {
		return s
	}
	prepare, args := group(&filter{}).Result()
	if prepare != "" {
		prepare = fmt.Sprintf("( %s )", prepare)
	}
	return s.Or(prepare, args...)
}

func (s *filter) Clear() Filter {
	s.prepare = ""
	s.args = nil
	return s
}

func (s *filter) Result() (prepare string, args []interface{}) {
	prepare = s.prepare
	args = s.args
	return
}

func NewFilter() Filter {
	return &filter{}
}
