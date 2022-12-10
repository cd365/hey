package hey

import (
	"fmt"
)

type JoinType string

const (
	JoinInner JoinType = "INNER"
	JoinLeft           = "LEFT"
	JoinRight          = "RIGHT"
	JoinFull           = "FULL"
)

type Joiner interface {
	Alias(alias string) Joiner
	Assoc(assoc string) Joiner
	Field(field string) Joiner
	Filter(filter Filter) Joiner
	Query(field ...string) Joiner
	Table() string
	Using() string
	Column(column string) string
	Result() (prepare string, args []interface{})
	Select() []string
}

type Join struct {
	style  JoinType
	table  string
	alias  string
	assoc  string
	field  string
	filter Filter
	query  []string
	using  string
}

func (s *Join) Alias(alias string) Joiner {
	if alias != "" {
		s.alias = alias
		s.using = alias
	}
	return s
}

func (s *Join) Assoc(assoc string) Joiner {
	s.assoc = assoc
	return s
}

func (s *Join) Field(field string) Joiner {
	s.field = field
	return s
}

func (s *Join) Filter(filter Filter) Joiner {
	s.filter = filter
	return s
}

func (s *Join) Query(query ...string) Joiner {
	s.query = query
	return s
}

func (s *Join) Table() string {
	return s.table
}

func (s *Join) Using() string {
	return s.using
}

func (s *Join) Column(column string) string {
	return fmt.Sprintf("%s.%s", s.using, column)
}

func (s *Join) Result() (prepare string, args []interface{}) {
	if s.table == "" || s.assoc == "" {
		return
	}
	prepare = fmt.Sprintf("%s JOIN %s", s.style, s.table)
	if s.alias != "" {
		prepare = fmt.Sprintf("%s AS %s", prepare, s.alias)
	}
	prepare = fmt.Sprintf("%s ON %s = %s", prepare, s.assoc, s.field)
	if s.filter != nil {
		key, val := s.filter.Result()
		if key != "" {
			prepare = fmt.Sprintf("%s AND %s", prepare, key)
			args = val
		}
	}
	return
}

func (s *Join) Select() (result []string) {
	length := len(s.query)
	result = make([]string, length)
	result = make([]string, len(s.query))
	for i := 0; i < length; i++ {
		result[i] = s.Column(s.query[i])
	}
	return
}

func initJoin(style JoinType, table string) Joiner {
	return &Join{
		style: style,
		table: table,
		using: table,
	}
}

func InnerJoin(table string) Joiner {
	return initJoin(JoinInner, table)
}

func LeftJoin(table string) Joiner {
	return initJoin(JoinLeft, table)
}

func RightJoin(table string) Joiner {
	return initJoin(JoinRight, table)
}

func FullJoin(table string) Joiner {
	return initJoin(JoinFull, table)
}
