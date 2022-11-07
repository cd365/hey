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
	SqlTable   *Table
	SqlWhere   Filter
	SqlField   []string
	SqlJoin    []*Join
	SqlGroupBy []string
	SqlHaving  Filter
	SqlOrder   *Order
	SqlLimit   int64
	SqlOffset  int64
}

func NewSelector() *Selector {
	return &Selector{}
}

func (s *Selector) Clear() {
	s.SqlTable = nil
	s.SqlField = nil
	s.SqlJoin = nil
	s.SqlWhere = nil
	s.SqlGroupBy = nil
	s.SqlHaving = nil
	s.SqlOrder = nil
	s.SqlLimit = 0
	s.SqlOffset = 0
}

func (s *Selector) Table(table string, args ...interface{}) *Selector {
	if s.SqlTable == nil {
		s.SqlTable = &Table{}
	}
	s.SqlTable.Table = table
	s.SqlTable.Args = args
	return s
}

func (s *Selector) Field(field ...string) *Selector {
	s.SqlField = field
	return s
}

func (s *Selector) Where(where Filter) *Selector {
	s.SqlWhere = where
	return s
}

func (s *Selector) Alias(alias string) *Selector {
	if s.SqlTable == nil {
		s.SqlTable = &Table{}
	}
	s.SqlTable.Alias = alias
	return s
}

func (s *Selector) join(model JoinType, table string, alias string, on string, and ...Filter) *Selector {
	if model == "" || table == "" || on == "" {
		return s
	}
	if s.SqlJoin == nil {
		s.SqlJoin = make([]*Join, 0)
	}
	with := NewFilter()
	length := len(and)
	var prepare string
	var args []interface{}
	for i := 0; i < length; i++ {
		prepare, args = and[i].Result()
		with.And(prepare, args)
	}
	s.SqlJoin = append(s.SqlJoin, &Join{
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

func (s *Selector) GroupBy(groupBy ...string) *Selector {
	s.SqlGroupBy = groupBy
	return s
}

func (s *Selector) Having(having Filter) *Selector {
	s.SqlHaving = having
	return s
}

func (s *Selector) Asc(columns ...string) *Selector {
	if s.SqlOrder == nil {
		s.SqlOrder = &Order{}
	}
	s.SqlOrder.Asc(columns...)
	return s
}

func (s *Selector) Desc(columns ...string) *Selector {
	if s.SqlOrder == nil {
		s.SqlOrder = &Order{}
	}
	s.SqlOrder.Desc(columns...)
	return s
}

func (s *Selector) Limit(limit int64) *Selector {
	s.SqlLimit = limit
	return s
}

func (s *Selector) Offset(offset int64) *Selector {
	s.SqlOffset = offset
	return s
}

func (s *Selector) SqlSelect() (prepare string, args []interface{}) {
	buf := &bytes.Buffer{}
	fields := "*"
	field := s.SqlField
	length := len(field)
	if length > 0 {
		fields = strings.Join(field, ", ")
	}
	buf.WriteString(fmt.Sprintf("SELECT %s FROM %s", fields, s.SqlTable.Result()))
	if len(s.SqlTable.Args) > 0 {
		args = append(args, s.SqlTable.Args...)
	}
	length = len(s.SqlJoin)
	for i := 0; i < length; i++ {
		key, val := s.SqlJoin[i].Result()
		buf.WriteString(fmt.Sprintf(" %s", key))
		args = append(args, val...)
	}
	if s.SqlWhere != nil {
		key, val := s.SqlWhere.Result()
		if key != "" {
			buf.WriteString(fmt.Sprintf(" WHERE %s", key))
			args = append(args, val...)
		}
	}
	length = len(s.SqlGroupBy)
	if length > 0 {
		groupBys := strings.Join(s.SqlGroupBy, ", ")
		buf.WriteString(fmt.Sprintf(" GROUP BY %s", groupBys))
	}
	if s.SqlHaving != nil {
		key, val := s.SqlHaving.Result()
		if key != "" {
			buf.WriteString(fmt.Sprintf(" HAVING %s", key))
			args = append(args, val...)
		}
	}
	if s.SqlOrder != nil {
		buf.WriteString(fmt.Sprintf(" ORDER BY %s", s.SqlOrder.Result()))
	}
	if s.SqlLimit > 0 {
		buf.WriteString(fmt.Sprintf(" LIMIT %d", s.SqlLimit))
		if s.SqlOffset > 0 {
			buf.WriteString(fmt.Sprintf(" OFFSET %d", s.SqlOffset))
		}
	}
	prepare = buf.String()
	return
}

func (s *Selector) SqlCount() (prepare string, args []interface{}) {
	buf := &bytes.Buffer{}
	fields := "COUNT(*)"
	field := s.SqlField
	length := len(field)
	if length > 0 {
		fields = field[0]
	}
	buf.WriteString(fmt.Sprintf("SELECT %s FROM %s", fields, s.SqlTable.Result()))
	if len(s.SqlTable.Args) > 0 {
		args = append(args, s.SqlTable.Args...)
	}
	length = len(s.SqlJoin)
	for i := 0; i < length; i++ {
		key, val := s.SqlJoin[i].Result()
		buf.WriteString(fmt.Sprintf(" %s", key))
		args = append(args, val...)
	}
	if s.SqlWhere != nil {
		key, val := s.SqlWhere.Result()
		if key != "" {
			buf.WriteString(fmt.Sprintf(" WHERE %s", key))
		}
		args = append(args, val...)
	}
	prepare = buf.String()
	return
}
