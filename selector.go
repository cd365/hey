package hey

import (
	"fmt"
	"strings"
)

type JoinType string

const (
	JoinMaster JoinType = "MASTER"
	JoinInner  JoinType = "INNER"
	JoinLeft   JoinType = "LEFT"
	JoinRight  JoinType = "RIGHT"
	JoinFull   JoinType = "FULL"
)

type Joiner interface {
	// As set current table name as alias-name
	As(as string) Joiner

	// Alias get current table name as alias-name
	Alias() string

	// Asa set current table name as <a>
	Asa() Joiner

	// Asb set current table name as <b>
	Asb() Joiner

	// Asc set current table name as <c>
	Asc() Joiner

	// Asd set current table name as <d>
	Asd() Joiner

	// Ase set current table name as <e>
	Ase() Joiner

	// Asf set current table name as <f>
	Asf() Joiner

	// Table get current table name
	Table() string

	// Name get the name used by the current table, aliases take precedence
	Name() string

	// Query set the connection query finally needs to query the field list of this table
	Query(field ...string) Joiner

	// QueryFields get the connection query finally needs to query the field list of this table
	QueryFields() []string

	// Field the table's field full name value
	Field(field string) string

	// On the criteria for joining the current table query
	On(on string, filter ...Filter) Joiner

	// OnEqual the criteria for joining the current table query, left = right
	OnEqual(left string, right string, filter ...Filter) Joiner

	// Result the current table joins the final sql script and parameters generated by the query
	Result() (prepare string, args []interface{})
}

type Join struct {
	joinType       JoinType      // join type
	tableName      string        // table name | SQL statement
	tableAliasName string        // table alias name
	joinOn         string        // join condition
	tableArgs      []interface{} // if table value is a SQL statement
	queryField     []string      // the connection query finally needs to query the field list of this table
	joinOnFilter   Filter        // used for condition after on
}

func (s *Join) As(as string) Joiner {
	s.tableAliasName = as
	return s
}

func (s *Join) Alias() string {
	return s.tableAliasName
}

func (s *Join) Asa() Joiner {
	s.As("a")
	return s
}

func (s *Join) Asb() Joiner {
	s.As("b")
	return s
}

func (s *Join) Asc() Joiner {
	s.As("c")
	return s
}

func (s *Join) Asd() Joiner {
	s.As("d")
	return s
}

func (s *Join) Ase() Joiner {
	s.As("e")
	return s
}

func (s *Join) Asf() Joiner {
	s.As("f")
	return s
}

func (s *Join) Table() string {
	return s.tableName
}

func (s *Join) Name() string {
	if s.tableAliasName != "" {
		return s.tableAliasName
	}
	return s.tableName
}

func (s *Join) prefix() string {
	return fmt.Sprintf("%s.", s.Name())
}

func (s *Join) Query(field ...string) Joiner {
	prefix := s.prefix()
	for k, v := range field {
		if !strings.Contains(v, prefix) {
			field[k] = prefix + v
		}
	}
	s.queryField = field
	return s
}

func (s *Join) QueryFields() []string {
	return s.queryField
}

func (s *Join) Field(field string) string {
	return s.prefix() + field
}

func (s *Join) On(joinOn string, joinOnFilter ...Filter) Joiner {
	s.joinOn = joinOn
	s.joinOnFilter = FilterMerge(false, joinOnFilter...)
	return s
}

func (s *Join) OnEqual(left string, right string, filter ...Filter) Joiner {
	return s.On(fmt.Sprintf("%s = %s", left, right), filter...)
}

func (s *Join) Result() (prepare string, args []interface{}) {
	if s.joinType == JoinMaster || s.tableName == "" || s.joinOn == "" {
		return
	}
	buf := sqlBuilder("")
	buf.WriteString(string(s.joinType))
	buf.WriteString(" JOIN ")
	buf.WriteString(s.tableName)
	args = s.tableArgs
	if s.tableAliasName != "" {
		buf.WriteString(" AS ")
		buf.WriteString(s.tableAliasName)
	}
	buf.WriteString(" ON ")
	buf.WriteString(s.joinOn)
	if s.joinOnFilter != nil {
		key, val := s.joinOnFilter.Result()
		if key != "" {
			buf.WriteString(" AND ")
			buf.WriteString(key)
			args = append(args, val...)
		}
	}
	prepare = buf.String()
	return
}

func initJoin(joinType JoinType, table string, args ...interface{}) Joiner {
	return &Join{
		joinType:  joinType,
		tableName: table,
		tableArgs: args,
	}
}

func MasterJoin(table string, args ...interface{}) Joiner {
	return initJoin(JoinMaster, table, args...)
}

func InnerJoin(table string, args ...interface{}) Joiner {
	return initJoin(JoinInner, table, args...)
}

func LeftJoin(table string, args ...interface{}) Joiner {
	return initJoin(JoinLeft, table, args...)
}

func RightJoin(table string, args ...interface{}) Joiner {
	return initJoin(JoinRight, table, args...)
}

func FullJoin(table string, args ...interface{}) Joiner {
	return initJoin(JoinFull, table, args...)
}

type Order struct {
	Order []string
}

func NewOrder() *Order {
	return &Order{}
}

func (s *Order) Asc(columns ...string) *Order {
	for _, column := range columns {
		if column != "" {
			s.Order = append(s.Order, column+" ASC")
		}
	}
	return s
}

func (s *Order) Desc(columns ...string) *Order {
	for _, column := range columns {
		if column != "" {
			s.Order = append(s.Order, column+" DESC")
		}
	}
	return s
}

func (s *Order) Result() string {
	return strings.Join(s.Order, ", ")
}

func Alias(prepare string, alias ...string) string {
	for i := len(alias) - 1; i >= 0; i-- {
		if alias[i] != "" {
			return prepare + " AS " + alias[i]
		}
	}
	return prepare
}

func FieldMerge(fields ...[]string) (result []string) {
	length := 0
	for _, field := range fields {
		length += len(field)
	}
	result = make([]string, 0, length)
	for _, field := range fields {
		result = append(result, field...)
	}
	return
}

func SubQuery(prepare string) string {
	return "( " + prepare + " )"
}
