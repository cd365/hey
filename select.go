package hey

import (
	"bytes"
	"fmt"
	"strings"
)

type Table struct {
	Table string
	Alias string
	Args  []interface{}
}

func (s *Table) Result() (table string) {
	table = s.Table
	if s.Alias != "" {
		table = fmt.Sprintf("%s AS %s", table, s.Alias)
	}
	return
}

type JoinType string

const (
	InnerJoin = "INNER JOIN"
	LeftJoin  = "LEFT JOIN"
	RightJoin = "RIGHT JOIN"
	FullJoin  = "FULL JOIN"
)

type Join struct {
	Model JoinType // one of INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN
	Table string   // table name
	Alias string   // table alias name
	On    string   // join on
	And   Filter   // join on and ...
}

func (s *Join) Result() (result string, args []interface{}) {
	if s.Table == "" || s.On == "" {
		return
	}
	result = fmt.Sprintf("%s %s", s.Model, s.Table)
	if s.Alias != "" {
		result = fmt.Sprintf("%s AS %s", result, s.Alias)
	}
	result = fmt.Sprintf("%s ON %s", result, s.On)
	if s.And != nil {
		key, val := s.And.Result()
		if key != "" {
			result = fmt.Sprintf("%s AND %s", result, key)
			args = val
		}
	}
	return
}

type Order struct {
	Order []string
}

func (s *Order) Asc(columns ...string) *Order {
	for _, column := range columns {
		if column == "" {
			continue
		}
		s.Order = append(s.Order, fmt.Sprintf("%s ASC", column))
	}
	return s
}

func (s *Order) Desc(columns ...string) *Order {
	for _, column := range columns {
		if column == "" {
			continue
		}
		s.Order = append(s.Order, fmt.Sprintf("%s DESC", column))
	}
	return s
}

func (s *Order) Result() string {
	return strings.Join(s.Order, ", ")
}

type Selector struct {
	_table  *Table
	_where  Filter
	_field  []string
	_join   []*Join
	_group  []string
	_having Filter
	_order  *Order
	_limit  int64
	_offset int64
}

func NewSelector() *Selector {
	return &Selector{}
}

func (s *Selector) Clear() {
	s._table = nil
	s._field = nil
	s._join = nil
	s._where = nil
	s._group = nil
	s._having = nil
	s._order = nil
	s._limit = 0
	s._offset = 0
}

func (s *Selector) Table(table string, args ...interface{}) *Selector {
	if s._table == nil {
		s._table = &Table{}
	}
	s._table.Table = table
	s._table.Args = args
	return s
}

func (s *Selector) Field(field ...string) *Selector {
	s._field = field
	return s
}

func (s *Selector) FieldValue() []string {
	return s._field
}

func (s *Selector) Where(where Filter) *Selector {
	s._where = where
	return s
}

func (s *Selector) Alias(alias string) *Selector {
	if s._table == nil {
		s._table = &Table{}
	}
	s._table.Alias = alias
	return s
}

func (s *Selector) join(model JoinType, table string, alias string, on string, and ...Filter) *Selector {
	if model == "" || table == "" || on == "" {
		return s
	}
	if s._join == nil {
		s._join = make([]*Join, 0)
	}
	with := NewFilter()
	length := len(and)
	var prepare string
	var args []interface{}
	for i := 0; i < length; i++ {
		prepare, args = and[i].Result()
		with.And(prepare, args)
	}
	s._join = append(s._join, &Join{
		Model: model,
		Table: table,
		Alias: alias,
		On:    on,
		And:   with,
	})
	return s
}

func (s *Selector) Join(join *Join) *Selector {
	if join != nil {
		s.join(join.Model, join.Table, join.Alias, join.On, join.And)
	}
	return s
}

func (s *Selector) InnerJoin(table string, alias string, on string, filter ...Filter) *Selector {
	return s.join(InnerJoin, table, alias, on, filter...)
}

func (s *Selector) LeftJoin(table string, alias string, on string, filter ...Filter) *Selector {
	return s.join(LeftJoin, table, alias, on, filter...)
}

func (s *Selector) RightJoin(table string, alias string, on string, filter ...Filter) *Selector {
	return s.join(RightJoin, table, alias, on, filter...)
}

func (s *Selector) FullJoin(table string, alias string, on string, filter ...Filter) *Selector {
	return s.join(FullJoin, table, alias, on, filter...)
}

func (s *Selector) Group(group ...string) *Selector {
	s._group = group
	return s
}

func (s *Selector) Having(having Filter) *Selector {
	s._having = having
	return s
}

func (s *Selector) Asc(columns ...string) *Selector {
	if s._order == nil {
		s._order = &Order{}
	}
	s._order.Asc(columns...)
	return s
}

func (s *Selector) Desc(columns ...string) *Selector {
	if s._order == nil {
		s._order = &Order{}
	}
	s._order.Desc(columns...)
	return s
}

func (s *Selector) Limit(limit int64) *Selector {
	s._limit = limit
	return s
}

func (s *Selector) Offset(offset int64) *Selector {
	s._offset = offset
	return s
}

func (s *Selector) SqlSelect() (prepare string, args []interface{}) {
	buf := &bytes.Buffer{}
	fields := "*"
	field := s._field
	length := len(field)
	if length > 0 {
		fields = strings.Join(field, ", ")
	}
	buf.WriteString(fmt.Sprintf("SELECT %s FROM %s", fields, s._table.Result()))
	if len(s._table.Args) > 0 {
		args = append(args, s._table.Args...)
	}
	length = len(s._join)
	for i := 0; i < length; i++ {
		key, val := s._join[i].Result()
		buf.WriteString(fmt.Sprintf(" %s", key))
		args = append(args, val...)
	}
	if s._where != nil {
		key, val := s._where.Result()
		if key != "" {
			buf.WriteString(fmt.Sprintf(" WHERE %s", key))
			args = append(args, val...)
		}
	}
	length = len(s._group)
	if length > 0 {
		groupBys := strings.Join(s._group, ", ")
		buf.WriteString(fmt.Sprintf(" GROUP BY %s", groupBys))
	}
	if s._having != nil {
		key, val := s._having.Result()
		if key != "" {
			buf.WriteString(fmt.Sprintf(" HAVING %s", key))
			args = append(args, val...)
		}
	}
	if s._order != nil {
		buf.WriteString(fmt.Sprintf(" ORDER BY %s", s._order.Result()))
	}
	if s._limit > 0 {
		buf.WriteString(fmt.Sprintf(" LIMIT %d", s._limit))
		if s._offset > 0 {
			buf.WriteString(fmt.Sprintf(" OFFSET %d", s._offset))
		}
	}
	prepare = buf.String()
	return
}

func (s *Selector) SqlCount() (prepare string, args []interface{}) {
	buf := &bytes.Buffer{}
	fields := "COUNT(*)"
	field := s._field
	length := len(field)
	if length > 0 {
		fields = field[0]
	}
	buf.WriteString(fmt.Sprintf("SELECT %s FROM %s", fields, s._table.Result()))
	if len(s._table.Args) > 0 {
		args = append(args, s._table.Args...)
	}
	length = len(s._join)
	for i := 0; i < length; i++ {
		key, val := s._join[i].Result()
		buf.WriteString(fmt.Sprintf(" %s", key))
		args = append(args, val...)
	}
	if s._where != nil {
		key, val := s._where.Result()
		if key != "" {
			buf.WriteString(fmt.Sprintf(" WHERE %s", key))
		}
		args = append(args, val...)
	}
	prepare = buf.String()
	return
}
