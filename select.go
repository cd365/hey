package hey

import (
	"bytes"
	"fmt"
	"strings"
)

type Table struct {
	Table string        // query table string
	Alias string        // alias name of single table name
	Args  []interface{} // args of table, if table is sql statement
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

type Select struct {
	table  *Table
	field  []string
	where  Filter
	join   []*Join
	group  []string
	having Filter
	order  *Order
	limit  int64
	offset int64
}

func NewSelect() *Select {
	return &Select{}
}

func (s *Select) Table(table string, args ...interface{}) *Select {
	if s.table == nil {
		s.table = &Table{}
	}
	s.table.Table = table
	s.table.Args = args
	return s
}

func (s *Select) Field(field ...string) *Select {
	s.field = field
	return s
}

func (s *Select) Where(where Filter) *Select {
	s.where = where
	return s
}

func (s *Select) Alias(alias string) *Select {
	if s.table == nil {
		s.table = &Table{}
	}
	s.table.Alias = alias
	return s
}

func (s *Select) joins(model JoinType, table string, alias string, on string, and ...Filter) *Select {
	if model == "" || table == "" || on == "" {
		return s
	}
	if s.join == nil {
		s.join = make([]*Join, 0)
	}
	logic := NewFilter()
	length := len(and)
	var prepare string
	var args []interface{}
	for i := 0; i < length; i++ {
		prepare, args = and[i].Result()
		logic.And(prepare, args...)
	}
	s.join = append(s.join, &Join{
		Model: model,
		Table: table,
		Alias: alias,
		On:    on,
		And:   logic,
	})
	return s
}

func (s *Select) InnerJoin(table string, alias string, on string, filter ...Filter) *Select {
	return s.joins(InnerJoin, table, alias, on, filter...)
}

func (s *Select) LeftJoin(table string, alias string, on string, filter ...Filter) *Select {
	return s.joins(LeftJoin, table, alias, on, filter...)
}

func (s *Select) RightJoin(table string, alias string, on string, filter ...Filter) *Select {
	return s.joins(RightJoin, table, alias, on, filter...)
}

func (s *Select) FullJoin(table string, alias string, on string, filter ...Filter) *Select {
	return s.joins(FullJoin, table, alias, on, filter...)
}

func (s *Select) Group(group ...string) *Select {
	s.group = group
	return s
}

func (s *Select) Having(having Filter) *Select {
	s.having = having
	return s
}

func (s *Select) Asc(columns ...string) *Select {
	if s.order == nil {
		s.order = &Order{}
	}
	s.order.Asc(columns...)
	return s
}

func (s *Select) Desc(columns ...string) *Select {
	if s.order == nil {
		s.order = &Order{}
	}
	s.order.Desc(columns...)
	return s
}

func (s *Select) Limit(limit int64) *Select {
	s.limit = limit
	return s
}

func (s *Select) Offset(offset int64) *Select {
	s.offset = offset
	return s
}

func (s *Select) SqlSelect() (prepare string, args []interface{}) {
	buf := &bytes.Buffer{}
	columns := "*"
	field := s.field
	length := len(field)
	if length > 0 {
		columns = strings.Join(field, ", ")
	}
	buf.WriteString(fmt.Sprintf("SELECT %s FROM %s", columns, s.table.Result()))
	if s.table.Args != nil {
		args = append(args, s.table.Args...)
	}
	length = len(s.join)
	for i := 0; i < length; i++ {
		key, val := s.join[i].Result()
		buf.WriteString(fmt.Sprintf(" %s", key))
		args = append(args, val...)
	}
	if s.where != nil {
		key, val := s.where.Result()
		if key != "" {
			buf.WriteString(fmt.Sprintf(" WHERE %s", key))
			args = append(args, val...)
		}
	}
	length = len(s.group)
	if length > 0 {
		groupBys := strings.Join(s.group, ", ")
		buf.WriteString(fmt.Sprintf(" GROUP BY %s", groupBys))
	}
	if s.having != nil {
		key, val := s.having.Result()
		if key != "" {
			buf.WriteString(fmt.Sprintf(" HAVING %s", key))
			args = append(args, val...)
		}
	}
	if s.order != nil {
		buf.WriteString(fmt.Sprintf(" ORDER BY %s", s.order.Result()))
	}
	if s.limit > 0 {
		buf.WriteString(fmt.Sprintf(" LIMIT %d", s.limit))
		if s.offset > 0 {
			buf.WriteString(fmt.Sprintf(" OFFSET %d", s.offset))
		}
	}
	prepare = buf.String()
	return
}

func (s *Select) SqlCount(count ...string) (prepare string, args []interface{}) {
	column := "COUNT(*)"
	length := len(count)
	for i := length - 1; i >= 0; i-- {
		if count[i] != "" {
			column = count[i]
			break
		}
	}
	buf := &bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("SELECT %s FROM %s", column, s.table.Result()))
	if len(s.table.Args) > 0 {
		args = append(args, s.table.Args...)
	}
	length = len(s.join)
	for i := 0; i < length; i++ {
		key, val := s.join[i].Result()
		buf.WriteString(fmt.Sprintf(" %s", key))
		args = append(args, val...)
	}
	if s.where != nil {
		key, val := s.where.Result()
		if key != "" {
			buf.WriteString(fmt.Sprintf(" WHERE %s", key))
		}
		args = append(args, val...)
	}
	prepare = buf.String()
	return
}
