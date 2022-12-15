package hey

import (
	"bytes"
	"fmt"
	"strings"
)

func Alias(name string, alias ...string) string {
	for i := len(alias) - 1; i >= 0; i-- {
		if alias[i] != "" {
			return fmt.Sprintf("%s AS %s", name, alias[i])
		}
	}
	return name
}

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

type Order struct {
	Order []string
}

func (s *Order) Asc(columns ...string) *Order {
	for _, column := range columns {
		if column != "" {
			s.Order = append(s.Order, fmt.Sprintf("%s ASC", column))
		}
	}
	return s
}

func (s *Order) Desc(columns ...string) *Order {
	for _, column := range columns {
		if column != "" {
			s.Order = append(s.Order, fmt.Sprintf("%s DESC", column))
		}
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
	join   []Joiner
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

func (s *Select) InnerJoin(table string, args ...interface{}) Joiner {
	return InnerJoin(table, args...)
}

func (s *Select) LeftJoin(table string, args ...interface{}) Joiner {
	return LeftJoin(table, args...)
}

func (s *Select) RightJoin(table string, args ...interface{}) Joiner {
	return RightJoin(table, args...)
}

func (s *Select) FullJoin(table string, args ...interface{}) Joiner {
	return FullJoin(table, args...)
}

func (s *Select) Join(join ...Joiner) *Select {
	s.join = join
	return s
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
	args = append(args, s.table.Args...)
	length = len(s.join)
	for i := 0; i < length; i++ {
		key, val := s.join[i].Result()
		if key != "" {
			buf.WriteString(fmt.Sprintf(" %s", key))
			args = append(args, val...)
		}
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
	args = append(args, s.table.Args...)
	length = len(s.join)
	for i := 0; i < length; i++ {
		key, val := s.join[i].Result()
		if key != "" {
			buf.WriteString(fmt.Sprintf(" %s", key))
			args = append(args, val...)
		}
	}
	if s.where != nil {
		key, val := s.where.Result()
		if key != "" {
			buf.WriteString(fmt.Sprintf(" WHERE %s", key))
			args = append(args, val...)
		}
	}
	prepare = buf.String()
	return
}
