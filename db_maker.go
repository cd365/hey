package hey

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
)

// ToEmpty Sets the property value of an object to empty value.
type ToEmpty interface {
	// ToEmpty Sets the property value of an object to empty value.
	ToEmpty()
}

// SQLComment Constructing SQL statement comments.
type SQLComment interface {
	Maker

	ToEmpty

	// Comment Add comment.
	Comment(comment string) SQLComment
}

type sqlComment struct {
	comment []string
}

func newSqlComment() *sqlComment {
	return &sqlComment{
		comment: make([]string, 0, 1<<1),
	}
}

func (s *sqlComment) ToEmpty() {
	s.comment = nil
}

func (s *sqlComment) Comment(comment string) SQLComment {
	if comment == StrEmpty {
		return s
	}
	s.comment = append(s.comment, comment)
	return s
}

func (s *sqlComment) ToSQL() *SQL {
	if len(s.comment) == 0 {
		return NewEmptySQL()
	}
	return NewSQL(Strings("/*", strings.Join(s.comment, StrComma), "*/"))
}

/*
-- CTE
WITH [RECURSIVE]
	cte_name1 [(column_name11, column_name12, ...)] AS ( SELECT ... )
	[, cte_name2 [(column_name21, column_name22, ...)] AS ( SELECT ... ) ]
SELECT ... FROM cte_name1 ...

-- EXAMPLE RECURSIVE CTE:
-- postgres
WITH RECURSIVE sss AS (
	SELECT id, pid, name, 1 AS level, name::TEXT AS path FROM employee WHERE pid = 0
	UNION ALL
	SELECT e.id, e.pid, e.name, f.level + 1, f.path || ' -> ' || e.name FROM employee e INNER JOIN sss f ON e.pid = f.id
)
SELECT * FROM sss ORDER BY level ASC, id DESC
*/

// SQLWith CTE: Common Table Expression.
type SQLWith interface {
	Maker

	ToEmpty

	// Recursive Recursion or cancellation of recursion.
	Recursive() SQLWith

	// Set Setting common table expression.
	Set(alias string, maker Maker, columns ...string) SQLWith

	// Del Removing common table expression.
	Del(alias string) SQLWith
}

type sqlWith struct {
	column map[string][]string

	prepare map[string]Maker

	alias []string

	recursive bool
}

func newSqlWith() *sqlWith {
	return &sqlWith{
		column:  make(map[string][]string, 1<<1),
		prepare: make(map[string]Maker, 1<<1),
		alias:   make([]string, 0, 1<<1),
	}
}

func (s *sqlWith) ToEmpty() {
	s.column = make(map[string][]string, 1<<1)
	s.prepare = make(map[string]Maker, 1<<1)
	s.alias = make([]string, 0, 1<<1)
	s.recursive = false
}

func (s *sqlWith) IsEmpty() bool {
	return len(s.alias) == 0
}

func (s *sqlWith) ToSQL() *SQL {
	if s.IsEmpty() {
		return NewSQL(StrEmpty)
	}
	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)
	b.WriteString(StrWith)
	b.WriteString(StrSpace)
	if s.recursive {
		b.WriteString(StrRecursive)
		b.WriteString(StrSpace)
	}
	result := NewEmptySQL()
	for index, alias := range s.alias {
		if index > 0 {
			b.WriteString(StrCommaSpace)
		}
		script := s.prepare[alias]
		b.WriteString(alias)
		b.WriteString(StrSpace)
		if columns := s.column[alias]; len(columns) > 0 {
			// Displays the column alias that defines the CTE, overwriting the original column name of the query result.
			b.WriteString(StrLeftSmallBracket)
			b.WriteString(StrSpace)
			b.WriteString(strings.Join(columns, StrCommaSpace))
			b.WriteString(StrSpace)
			b.WriteString(StrRightSmallBracket)
			b.WriteString(StrSpace)
		}
		b.WriteString(Strings(StrAs, StrSpace, StrLeftSmallBracket, StrSpace))
		tmp := script.ToSQL()
		b.WriteString(tmp.Prepare)
		b.WriteString(Strings(StrSpace, StrRightSmallBracket))
		result.Args = append(result.Args, tmp.Args...)
	}
	result.Prepare = b.String()
	return result
}

func (s *sqlWith) Recursive() SQLWith {
	s.recursive = !s.recursive
	return s
}

func (s *sqlWith) Set(alias string, maker Maker, columns ...string) SQLWith {
	if alias == StrEmpty || maker == nil || maker.ToSQL().IsEmpty() {
		return s
	}
	if _, ok := s.prepare[alias]; !ok {
		s.alias = append(s.alias, alias)
	}
	s.column[alias] = columns
	s.prepare[alias] = maker
	return s
}

func (s *sqlWith) Del(alias string) SQLWith {
	if alias == StrEmpty {
		return s
	}
	if _, ok := s.prepare[alias]; !ok {
		return s
	}
	keeps := make([]string, 0, len(s.alias))
	for _, tmp := range s.alias {
		if tmp != alias {
			keeps = append(keeps, tmp)
		}
	}
	s.alias = keeps
	delete(s.column, alias)
	delete(s.prepare, alias)
	return s
}

// SQLSelect Build the query column set.
type SQLSelect interface {
	Maker

	ToEmpty

	IsEmpty() bool

	// Distinct DISTINCT column1, column2, column3 ...
	Distinct() SQLSelect

	// Add Put Maker to the query list.
	Add(maker Maker) SQLSelect

	// Del Delete some columns from the query list. If not specified, delete all.
	Del(columns ...string) SQLSelect

	// Has Does the column exist in the query list?
	Has(column string) bool

	// Len Query list length.
	Len() int

	// Get Query list and its corresponding column parameter list.
	Get() (columns []string, args map[int][]any)

	// Set Query list and its corresponding column parameter list.
	Set(columns []string, args map[int][]any) SQLSelect

	// Select Add one or more query lists. If no parameter is provided, all existing query lists will be deleted.
	Select(columns ...any) SQLSelect
}

type sqlSelect struct {
	columnsMap map[string]int

	columnsArgs map[int][]any

	way *Way

	columns []string

	// distinct Allows multiple columns to be deduplicated, such as: DISTINCT column1, column2, column3 ...
	distinct bool
}

func newSqlSelect(way *Way) *sqlSelect {
	return &sqlSelect{
		columnsMap:  make(map[string]int, 1<<5),
		columnsArgs: make(map[int][]any, 1<<5),
		way:         way,
		columns:     make([]string, 0, 1<<5),
	}
}

func (s *sqlSelect) ToEmpty() {
	s.columns = make([]string, 0, 1<<5)
	s.columnsMap = make(map[string]int, 1<<5)
	s.columnsArgs = make(map[int][]any)
	s.distinct = false
}

func (s *sqlSelect) IsEmpty() bool {
	return len(s.columns) == 0
}

func (s *sqlSelect) ToSQL() *SQL {
	length := len(s.columns)
	if length == 0 {
		prepare := StrStar
		if s.distinct {
			prepare = Strings(StrDistinct, StrSpace, prepare)
		}
		return NewSQL(prepare)
	}
	script := NewSQL(StrEmpty)
	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)
	if s.distinct {
		b.WriteString(StrDistinct)
		b.WriteString(StrSpace)
	}
	columns := make([]string, 0, length)
	for i := range length {
		args, ok := s.columnsArgs[i]
		if !ok {
			continue
		}
		columns = append(columns, s.columns[i])
		script.Args = append(script.Args, args...)
	}
	b.WriteString(strings.Join(s.way.ReplaceAll(columns), StrCommaSpace))
	script.Prepare = b.String()
	return script
}

func (s *sqlSelect) Distinct() SQLSelect {
	s.distinct = !s.distinct
	return s
}

func (s *sqlSelect) Has(column string) bool {
	_, ok := s.columnsMap[column]
	return ok
}

func (s *sqlSelect) add(column string, args ...any) *sqlSelect {
	if column == StrEmpty {
		return s
	}
	index, ok := s.columnsMap[column]
	if ok {
		s.columnsArgs[index] = args
		return s
	}
	index = len(s.columns)
	s.columns = append(s.columns, column)
	s.columnsMap[column] = index
	s.columnsArgs[index] = args
	return s
}

func (s *sqlSelect) Add(maker Maker) SQLSelect {
	if maker == nil {
		return s
	}
	script := maker.ToSQL()
	if script.IsEmpty() {
		return s
	}
	return s.add(script.Prepare, script.Args...)
}

func (s *sqlSelect) AddAll(columns ...string) SQLSelect {
	index := len(s.columns)
	for _, column := range columns {
		if column == StrEmpty {
			continue
		}
		if _, ok := s.columnsMap[column]; ok {
			continue
		}
		s.columns = append(s.columns, column)
		s.columnsMap[column] = index
		s.columnsArgs[index] = nil
		index++
	}
	return s
}

func (s *sqlSelect) Del(columns ...string) SQLSelect {
	if columns == nil {
		s.ToEmpty()
		return s
	}
	deletes := make(map[int]*struct{}, len(columns))
	for _, column := range columns {
		if column == StrEmpty {
			continue
		}
		index, ok := s.columnsMap[column]
		if !ok {
			continue
		}
		deletes[index] = nil
	}
	length := len(s.columns)
	result := make([]string, 0, length)
	for index, column := range s.columns {
		if _, ok := deletes[index]; ok {
			delete(s.columnsMap, column)
			delete(s.columnsArgs, index)
		} else {
			result = append(result, column)
		}
	}
	s.columns = result
	return s
}

func (s *sqlSelect) Len() int {
	return len(s.columns)
}

func (s *sqlSelect) Get() ([]string, map[int][]any) {
	return s.columns, s.columnsArgs
}

func (s *sqlSelect) Set(columns []string, args map[int][]any) SQLSelect {
	columnsMap := make(map[string]int, len(columns))
	for i, column := range columns {
		columnsMap[column] = i
		if _, ok := args[i]; !ok {
			args[i] = nil
		}
	}
	s.columns, s.columnsMap, s.columnsArgs = columns, columnsMap, args
	return s
}

func (s *sqlSelect) use(columns ...SQLSelect) SQLSelect {
	length := len(columns)
	for i := range length {
		tmp := columns[i]
		if tmp == nil {
			continue
		}
		cols, args := tmp.Get()
		for index, value := range cols {
			s.add(value, args[index]...)
		}
	}
	return s
}

func (s *sqlSelect) Select(columns ...any) SQLSelect {
	if len(columns) == 0 {
		return s.Del()
	}
	for _, value := range columns {
		switch v := value.(type) {
		case string:
			s.add(v)
		case []string:
			for _, column := range v {
				s.add(column)
			}
		case *SQL:
			s.Add(v)
		case []*SQL:
			for _, w := range v {
				s.Add(w)
			}
		case SQLSelect:
			s.use(v)
		case []SQLSelect:
			s.use(v...)
		case Maker:
			s.Add(v)
		case []Maker:
			for _, maker := range v {
				s.Add(maker)
			}
		default:
		}
	}
	return s
}

// SQLJoinOn Construct the connection query conditions.
type SQLJoinOn interface {
	Maker

	// Equal Use equal value JOIN ON condition.
	Equal(table1alias string, table1column string, table2alias string, table2column string) SQLJoinOn

	// On Append custom conditions to the ON statement or use custom conditions on the ON statement to associate tables.
	On(on func(f Filter)) SQLJoinOn

	// Using Use USING instead of ON.
	Using(columns ...string) SQLJoinOn
}

type sqlJoinOn struct {
	way *Way

	on Filter

	usings []string
}

func newSQLJoinOn(way *Way) *sqlJoinOn {
	return &sqlJoinOn{
		way: way,
		on:  way.F(),
	}
}

func (s *sqlJoinOn) Equal(table1alias string, table1column string, table2alias string, table2column string) SQLJoinOn {
	s.on.And(JoinSQLSpace(Prefix(table1alias, table1column), StrEqual, Prefix(table2alias, table2column)))
	return s
}

func (s *sqlJoinOn) On(on func(f Filter)) SQLJoinOn {
	if on != nil {
		on(s.on)
	}
	return s
}

func (s *sqlJoinOn) Using(columns ...string) SQLJoinOn {
	columns = DiscardDuplicate(func(tmp string) bool { return tmp == StrEmpty }, columns...)
	if len(columns) > 0 {
		s.usings = columns
	}
	return s
}

func (s *sqlJoinOn) ToSQL() *SQL {
	// JOIN ON
	if s.on != nil && !s.on.IsEmpty() {
		script := s.on.ToSQL()
		script.Prepare = Strings(StrOn, StrSpace, script.Prepare)
		return script
	}
	// JOIN USING
	if length := len(s.usings); length > 0 {
		using := make([]string, 0, length)
		for _, column := range s.usings {
			if column != StrEmpty {
				using = append(using, column)
			}
		}
		if len(using) > 0 {
			using = s.way.ReplaceAll(using)
			prepare := Strings(StrUsing, StrSpace, StrLeftSmallBracket, StrSpace, strings.Join(using, StrCommaSpace), StrSpace, StrRightSmallBracket)
			return NewSQL(prepare)
		}
	}
	return NewEmptySQL()
}

type SQLJoinAssoc func(table1alias string, table2alias string) SQLJoinOn

type sqlJoinSchema struct {
	joinTable SQLAlias

	condition Maker

	joinType string
}

// SQLJoin Build a join query.
type SQLJoin interface {
	Maker

	ToEmpty

	// GetTable Get join query the main table.
	GetTable() SQLAlias

	// SetTable Set join query the main table.
	SetTable(table SQLAlias) SQLJoin

	// Table Create a table for join query.
	Table(table any, alias string) SQLAlias

	// On Set the join query conditions.
	On(on func(on SQLJoinOn, table1alias string, table2alias string)) SQLJoinAssoc

	// Using The conditions for the join query use USING.
	Using(columns ...string) SQLJoinAssoc

	// OnEqual The join query conditions uses table1.column = table2.column.
	OnEqual(table1column string, table2column string) SQLJoinAssoc

	// Join Use the join type to set the table join relationship, if the table1 value is nil, use the main table.
	Join(joinType string, table1 SQLAlias, table2 SQLAlias, on SQLJoinAssoc) SQLJoin

	// InnerJoin Set the table join relationship, if the table1 value is nil, use the main table.
	InnerJoin(table1 SQLAlias, table2 SQLAlias, on SQLJoinAssoc) SQLJoin

	// LeftJoin Set the table join relationship, if the table1 value is nil, use the main table.
	LeftJoin(table1 SQLAlias, table2 SQLAlias, on SQLJoinAssoc) SQLJoin

	// RightJoin Set the table join relationship, if the table1 value is nil, use the main table.
	RightJoin(table1 SQLAlias, table2 SQLAlias, on SQLJoinAssoc) SQLJoin

	// Select Set the query column list.
	Select(columns ...any) SQLJoin

	// Prefix Set column prefix with the table name or table alias.
	Prefix(prefix SQLAlias, column string, aliases ...string) string

	// PrefixSelect Add a column list to the query based on the table alias or table name prefix.
	PrefixSelect(prefix SQLAlias, columns ...string) SQLJoin
}

type sqlJoin struct {
	table SQLAlias

	selects *sqlSelect

	way *Way

	joins []*sqlJoinSchema
}

func newSqlJoin(way *Way) *sqlJoin {
	tmp := &sqlJoin{
		way:   way,
		joins: make([]*sqlJoinSchema, 0, 1<<1),
	}
	return tmp
}

func (s *sqlJoin) ToEmpty() {
	s.joins = make([]*sqlJoinSchema, 0, 1<<1)
	s.selects.ToEmpty()
	s.table = nil
}

func (s *sqlJoin) GetTable() SQLAlias {
	return s.table
}

func (s *sqlJoin) SetTable(table SQLAlias) SQLJoin {
	s.table = table
	return s
}

func (s *sqlJoin) ToSQL() *SQL {
	if s.table == nil || s.table.IsEmpty() {
		return NewEmptySQL()
	}
	script := NewEmptySQL()
	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)
	for index, tmp := range s.joins {
		if tmp == nil {
			continue
		}
		if index > 0 {
			b.WriteString(StrSpace)
		}
		b.WriteString(tmp.joinType)
		right := tmp.joinTable.ToSQL()
		b.WriteString(StrSpace)
		b.WriteString(right.Prepare)
		script.Args = append(script.Args, right.Args...)
		if tmp.condition != nil {
			if on := tmp.condition.ToSQL(); on != nil && !on.IsEmpty() {
				b.WriteString(StrSpace)
				b.WriteString(on.Prepare)
				script.Args = append(script.Args, on.Args...)
			}
		}
	}
	script.Prepare = b.String()
	return script
}

func (s *sqlJoin) Table(table any, alias string) SQLAlias {
	return s.way.getTable(table).SetAlias(alias)
}

// On For `... JOIN ON ...`
func (s *sqlJoin) On(on func(on SQLJoinOn, table1alias string, table2alias string)) SQLJoinAssoc {
	return func(table1alias string, table2alias string) SQLJoinOn {
		return newSQLJoinOn(s.way).On(func(o Filter) {
			newAssoc := newSQLJoinOn(s.way)
			on(newAssoc, table1alias, table2alias)
			newAssoc.On(func(f Filter) { o.Use(f) })
		})
	}
}

// Using For `... JOIN USING ...`
func (s *sqlJoin) Using(columns ...string) SQLJoinAssoc {
	return func(alias1 string, alias2 string) SQLJoinOn {
		return newSQLJoinOn(s.way).Using(columns...)
	}
}

// OnEqual For `... JOIN ON ... = ... [...]`
func (s *sqlJoin) OnEqual(table1column string, table2column string) SQLJoinAssoc {
	if table1column == StrEmpty || table2column == StrEmpty {
		return nil
	}
	return func(alias1 string, alias2 string) SQLJoinOn {
		return newSQLJoinOn(s.way).On(func(f Filter) {
			f.CompareEqual(Prefix(alias1, table1column), Prefix(alias2, table2column))
		})
	}
}

func (s *sqlJoin) Join(joinType string, table1 SQLAlias, table2 SQLAlias, on SQLJoinAssoc) SQLJoin {
	if joinType == StrEmpty {
		joinType = StrJoinInner
	}
	if table1 == nil || table1.ToSQL().IsEmpty() {
		table1 = s.table
	}
	if table2 == nil || table2.ToSQL().IsEmpty() {
		return s
	}
	join := &sqlJoinSchema{
		joinType:  joinType,
		joinTable: table2,
	}
	if on != nil {
		join.condition = on(table1.GetAlias(), table2.GetAlias())
	}
	s.joins = append(s.joins, join)
	return s
}

func (s *sqlJoin) InnerJoin(table1 SQLAlias, table2 SQLAlias, on SQLJoinAssoc) SQLJoin {
	return s.Join(StrJoinInner, table1, table2, on)
}

func (s *sqlJoin) LeftJoin(table1 SQLAlias, table2 SQLAlias, on SQLJoinAssoc) SQLJoin {
	return s.Join(StrJoinLeft, table1, table2, on)
}

func (s *sqlJoin) RightJoin(table1 SQLAlias, table2 SQLAlias, on SQLJoinAssoc) SQLJoin {
	return s.Join(StrJoinRight, table1, table2, on)
}

func (s *sqlJoin) Select(columns ...any) SQLJoin {
	s.selects.Select(columns...)
	return s
}

func (s *sqlJoin) prefixColumnAll(prefix SQLAlias, columns []string) []string {
	if prefix == nil {
		return columns
	}
	alias := prefix.GetAlias()
	if alias == StrEmpty {
		alias = prefix.GetSQL().Prepare
	}
	return s.way.T().SetAlias(alias).ColumnAll(columns...)
}

func (s *sqlJoin) Prefix(prefix SQLAlias, column string, aliases ...string) string {
	return s.way.Alias(s.prefixColumnAll(prefix, []string{column})[0], aliases...).ToSQL().Prepare
}

func (s *sqlJoin) PrefixSelect(prefix SQLAlias, columns ...string) SQLJoin {
	return s.Select(s.prefixColumnAll(prefix, columns))
}

// SQLGroupBy Build GROUP BY statements.
type SQLGroupBy interface {
	Maker

	ToEmpty

	// Group Set the grouping column, allowing string, []string, *SQL, []*SQL, Maker, []Maker.
	Group(group ...any) SQLGroupBy

	// Having Set the conditions filter HAVING statement after GROUP BY.
	Having(having func(having Filter)) SQLGroupBy
}

type sqlGroupBy struct {
	having Filter

	groupColumnsMap map[string]int

	way *Way

	groupColumnsArgs map[string][]any

	groupColumns []string
}

func newSqlGroupBy(way *Way) *sqlGroupBy {
	return &sqlGroupBy{
		having:           way.F(),
		groupColumnsMap:  make(map[string]int, 1<<1),
		way:              way,
		groupColumnsArgs: make(map[string][]any, 1<<1),
		groupColumns:     make([]string, 0, 1<<1),
	}
}

func (s *sqlGroupBy) ToEmpty() {
	s.having = s.way.F()
	s.groupColumns = make([]string, 0, 1<<1)
	s.groupColumnsMap = make(map[string]int, 1<<1)
	s.groupColumnsArgs = make(map[string][]any, 1<<1)
}

func (s *sqlGroupBy) IsEmpty() bool {
	return len(s.groupColumns) == 0
}

func (s *sqlGroupBy) ToSQL() *SQL {
	script := NewEmptySQL()
	if s.IsEmpty() {
		return script
	}
	groupBy := strings.Join(s.way.ReplaceAll(s.groupColumns), StrCommaSpace)
	groupByArgs := make([]any, 0)
	for _, column := range s.groupColumns {
		groupByArgs = append(groupByArgs, s.groupColumnsArgs[column]...)
	}
	script.Prepare = Strings(StrGroupBy, StrSpace, groupBy)
	script.Args = groupByArgs
	if s.having == nil || s.having.IsEmpty() {
		return script
	}
	return JoinSQLSpace(script, StrHaving, ParcelFilter(s.having))
}

func (s *sqlGroupBy) add(script *SQL) *sqlGroupBy {
	if script == nil || script.IsEmpty() {
		return s
	}
	column, args := strings.TrimSpace(script.Prepare), script.Args
	if _, ok := s.groupColumnsMap[column]; ok {
		return s
	}
	index := len(s.groupColumns)
	s.groupColumns = append(s.groupColumns, column)
	s.groupColumnsMap[column] = index
	s.groupColumnsArgs[column] = args
	return s
}

func (s *sqlGroupBy) Group(group ...any) SQLGroupBy {
	for _, value := range group {
		switch v := value.(type) {
		case string:
			s.add(NewSQL(v))
		case []string:
			for _, w := range v {
				s.add(NewSQL(w))
			}
		case *SQL:
			s.add(v)
		case []*SQL:
			for _, w := range v {
				s.add(w)
			}
		case Maker:
			if v != nil {
				s.add(v.ToSQL())
			}
		case []Maker:
			for _, w := range v {
				if w != nil {
					s.add(w.ToSQL())
				}
			}
		default:
		}
	}
	return s
}

func (s *sqlGroupBy) Having(having func(having Filter)) SQLGroupBy {
	if having != nil {
		having(s.having)
	}
	return s
}

// SQLOrderBy Build ORDER BY statements.
type SQLOrderBy interface {
	Maker

	ToEmpty

	// Asc Build column1 ASC, column2 ASC, column3 ASC...
	Asc(columns ...string) SQLOrderBy

	// Desc Build column1 DESC, column2 DESC, column3 DESC...
	Desc(columns ...string) SQLOrderBy
}

type sqlOrderBy struct {
	orderMap map[string]int

	way *Way

	orderBy []string
}

func newSqlOrderBy(way *Way) *sqlOrderBy {
	return &sqlOrderBy{
		orderMap: make(map[string]int, 1<<1),
		way:      way,
		orderBy:  make([]string, 0, 1<<1),
	}
}

func (s *sqlOrderBy) ToEmpty() {
	s.orderBy = make([]string, 0, 1<<1)
	s.orderMap = make(map[string]int, 1<<1)
}

func (s *sqlOrderBy) IsEmpty() bool {
	return len(s.orderBy) == 0
}

func (s *sqlOrderBy) ToSQL() *SQL {
	script := NewSQL(StrEmpty)
	if s.IsEmpty() {
		return script
	}
	script.Prepare = Strings(Strings(StrOrderBy, StrSpace, strings.Join(s.orderBy, StrCommaSpace)))
	return script
}

func (s *sqlOrderBy) add(category string, columns ...string) SQLOrderBy {
	if category == StrEmpty {
		return s
	}
	index := len(s.orderBy)
	for _, column := range columns {
		if column == StrEmpty {
			continue
		}
		if _, ok := s.orderMap[column]; ok {
			continue
		}
		s.orderMap[column] = index
		index++
		order := s.way.Replace(column)
		order = Strings(order, StrSpace, category)
		s.orderBy = append(s.orderBy, order)
	}
	return s
}

func (s *sqlOrderBy) Asc(columns ...string) SQLOrderBy {
	return s.add(StrAsc, columns...)
}

func (s *sqlOrderBy) Desc(columns ...string) SQLOrderBy {
	return s.add(StrDesc, columns...)
}

// SQLLimit Build LIMIT n[ OFFSET m] statements.
type SQLLimit interface {
	Maker

	ToEmpty

	// Limit SQL LIMIT.
	Limit(limit int64) SQLLimit

	// Offset SQL OFFSET.
	Offset(offset int64) SQLLimit

	// Page SQL LIMIT and OFFSET.
	Page(page int64, limit ...int64) SQLLimit
}

type sqlLimit struct {
	limit *int64

	offset *int64
}

func newSqlLimit() *sqlLimit {
	return &sqlLimit{}
}

func (s *sqlLimit) ToEmpty() {
	s.limit = nil
	s.offset = nil
}

func (s *sqlLimit) IsEmpty() bool {
	return s.limit == nil
}

func (s *sqlLimit) ToSQL() *SQL {
	script := NewSQL(StrEmpty)
	if s.IsEmpty() {
		return script
	}
	makers := make([]any, 0, 1<<2)
	makers = append(makers, any2sql(StrLimit))
	makers = append(makers, any2sql(*s.limit))
	if s.offset != nil && *s.offset >= 0 {
		makers = append(makers, any2sql(StrOffset))
		makers = append(makers, any2sql(*s.offset))
	}
	return JoinSQLSpace(makers...)
}

func (s *sqlLimit) Limit(limit int64) SQLLimit {
	if limit > 0 {
		s.limit = &limit
	}
	return s
}

func (s *sqlLimit) Offset(offset int64) SQLLimit {
	if offset > 0 {
		s.offset = &offset
	}
	return s
}

func (s *sqlLimit) Page(page int64, limit ...int64) SQLLimit {
	if page <= 0 {
		return s
	}
	for i := len(limit) - 1; i >= 0; i-- {
		if limit[i] > 0 {
			s.Limit(limit[i]).Offset((page - 1) * limit[i])
			break
		}
	}
	return s
}

// Limiter limit and offset.
type Limiter interface {
	GetLimit() int64

	GetOffset() int64
}

// SQLValues Build INSERT-VALUES statements.
type SQLValues interface {
	Maker

	ToEmpty

	IsEmpty() bool

	// Subquery The inserted data is a subquery.
	Subquery(subquery Maker) SQLValues

	// Values The inserted data of VALUES.
	Values(values ...[]any) SQLValues
}

type sqlValues struct {
	subquery Maker

	values [][]any
}

func newSqlValues() *sqlValues {
	return &sqlValues{
		values: make([][]any, 1),
	}
}

func (s *sqlValues) ToEmpty() {
	s.subquery = nil
	s.values = make([][]any, 1)
}

func (s *sqlValues) IsEmpty() bool {
	return s.subquery == nil && (len(s.values) == 0 || len(s.values[0]) == 0)
}

func (s *sqlValues) valuesToSQL(values [][]any) *SQL {
	script := NewEmptySQL()
	count := len(values)
	if count == 0 {
		return script
	}
	length := len(values[0])
	if length == 0 {
		return script
	}
	line := make([]string, length)
	script.Args = make([]any, 0, count*length)
	for i := range length {
		line[i] = StrPlaceholder
	}
	value := ParcelPrepare(strings.Join(line, StrCommaSpace))
	rows := make([]string, count)
	for i := range count {
		script.Args = append(script.Args, values[i]...)
		rows[i] = value
	}
	script.Prepare = strings.Join(rows, StrCommaSpace)
	return script
}

func (s *sqlValues) ToSQL() *SQL {
	if s.subquery != nil {
		return s.subquery.ToSQL()
	}
	return s.valuesToSQL(s.values)
}

func (s *sqlValues) Subquery(subquery Maker) SQLValues {
	if subquery == nil {
		return s
	}
	if script := subquery.ToSQL(); script == nil || script.IsEmpty() {
		return s
	}
	s.subquery = subquery
	return s
}

func (s *sqlValues) Values(values ...[]any) SQLValues {
	s.values = values
	return s
}

// SQLReturning Build INSERT INTO xxx RETURNING xxx
type SQLReturning interface {
	Maker

	ToEmpty

	// Prepare When constructing a SQL statement that insert a row of data and return the id,
	// you may need to adjust the SQL statement, such as adding `RETURNING id` to the end of the insert statement.
	Prepare(prepare func(tmp *SQL)) SQLReturning

	// Returning Set the RETURNING statement to return one or more columns.
	Returning(columns ...string) SQLReturning

	// Execute When constructing a SQL statement that inserts a row of data and returns the id,
	// get the id value of the inserted row (this may vary depending on the database driver)
	Execute(execute func(ctx context.Context, stmt *Stmt, args ...any) (id int64, err error)) SQLReturning
}

type sqlReturning struct {
	way *Way

	insert *SQL

	prepare func(tmp *SQL)

	execute func(ctx context.Context, stmt *Stmt, args ...any) (id int64, err error)
}

func newReturning(way *Way, insert *SQL) *sqlReturning {
	return &sqlReturning{
		way:    way,
		insert: insert,
	}
}

func (s *sqlReturning) ToEmpty() {
	s.insert = NewEmptySQL()
	s.prepare = nil
	s.execute = nil
}

// Prepare You may need to modify the SQL statement to be executed.
func (s *sqlReturning) Prepare(prepare func(tmp *SQL)) SQLReturning {
	s.prepare = prepare
	return s
}

// Returning Set the RETURNING statement to return one or more columns.
func (s *sqlReturning) Returning(columns ...string) SQLReturning {
	columns = ArrayDiscard(columns, func(k int, v string) bool { return strings.TrimSpace(v) == StrEmpty })
	if len(columns) == 0 {
		columns = []string{StrStar}
	}
	return s.Prepare(func(tmp *SQL) {
		tmp.Prepare = JoinSQLSpace(tmp.Prepare, StrReturning, JoinSQLCommaSpace(AnyAny(columns)...)).Prepare
	})
}

// ToSQL Make SQL.
func (s *sqlReturning) ToSQL() *SQL {
	result := s.insert.Copy()
	if prepare := s.prepare; prepare != nil {
		prepare(result)
	}
	return result
}

// Execute Customize the method to return the sequence value of inserted data.
func (s *sqlReturning) Execute(execute func(ctx context.Context, stmt *Stmt, args ...any) (id int64, err error)) SQLReturning {
	s.execute = execute
	return s
}

// SQLUpdateSet Build UPDATE-SET statements.
type SQLUpdateSet interface {
	Maker

	ToEmpty

	Len() int

	// Forbid Set a list of columns that cannot be updated.
	Forbid(columns ...string) SQLUpdateSet

	// GetForbid Get a list of columns that are prohibited from updating.
	GetForbid() []string

	// Select Set columns that only allow updates, not including defaults.
	Select(columns ...string) SQLUpdateSet

	// Set Update column assignment.
	Set(column string, value any) SQLUpdateSet

	// Decr Update column decrement.
	Decr(column string, decr any) SQLUpdateSet

	// Incr Update column increment.
	Incr(column string, incr any) SQLUpdateSet

	// SetMap Update column assignment by map.
	SetMap(columnValue map[string]any) SQLUpdateSet

	// SetSlice Update column assignment by slice.
	SetSlice(columns []string, values []any) SQLUpdateSet

	// Update Parse the given update data and assign the update value.
	Update(update any) SQLUpdateSet

	// Compare Compare struct assignment update.
	Compare(old, new any, except ...string) SQLUpdateSet

	// Default Set the default columns that need to be updated, such as update timestamp.
	Default(column string, value any) SQLUpdateSet

	// Remove Delete a column-value.
	Remove(columns ...string) SQLUpdateSet

	// Assign Assigning values through other column.
	Assign(dst string, src string) SQLUpdateSet

	// GetUpdate Get a list of existing updates.
	GetUpdate() ([]string, [][]any)

	// SetUpdate Delete the existing update list and set the update list.
	SetUpdate(updates []string, params [][]any) SQLUpdateSet
}

type sqlUpdateSet struct {
	forbidSet map[string]*struct{}

	exists map[string][]string // column => expression lists

	onlyAllow map[string]*struct{} // Set columns that only allow updates.

	updateMap map[string]int

	way *Way

	defaults *sqlUpdateSet

	updateExpr []string

	updateArgs [][]any
}

func (s *sqlUpdateSet) init() {
	s.forbidSet = make(map[string]*struct{}, 1<<3)
	s.exists = make(map[string][]string, 1<<3)
	s.updateMap = make(map[string]int, 1<<3)
	s.updateExpr = make([]string, 0, 1<<3)
	s.updateArgs = make([][]any, 0, 1<<3)
}

func (s *sqlUpdateSet) toEmpty() {
	s.forbidSet = make(map[string]*struct{}, 1<<3)
	s.exists = make(map[string][]string, 1<<3)
	s.onlyAllow = nil
	s.updateMap = make(map[string]int, 1<<3)
	s.updateExpr = make([]string, 0, 1<<3)
	s.updateArgs = make([][]any, 0, 1<<3)
}

func newSqlUpdateSet(way *Way) *sqlUpdateSet {
	result := &sqlUpdateSet{
		way: way,
	}
	defaults := &sqlUpdateSet{
		way: way,
	}
	result.init()
	defaults.init()
	result.defaults = defaults
	return result
}

func (s *sqlUpdateSet) ToEmpty() {
	s.toEmpty()
	s.defaults.toEmpty()
}

func (s *sqlUpdateSet) IsEmpty() bool {
	return len(s.updateExpr) == 0
}

func (s *sqlUpdateSet) ToSQL() *SQL {
	script := NewEmptySQL()
	length := len(s.updateExpr)
	if length == 0 {
		return script
	}

	updates := make([]string, length)
	copy(updates, s.updateExpr)
	params := make([][]any, length)
	copy(params, s.updateArgs)

	defaultUpdates := s.defaults.updateExpr
	if len(defaultUpdates) > 0 {
		for index, defaultUpdate := range defaultUpdates {
			if _, ok := s.updateMap[defaultUpdate]; !ok {
				updates = append(updates, defaultUpdate)
				params = append(params, s.defaults.updateArgs[index])
			}
		}
	}

	script.Prepare = strings.Join(updates, StrCommaSpace)
	for _, tmp := range params {
		script.Args = append(script.Args, tmp...)
	}
	return script
}

func (s *sqlUpdateSet) beautifyExpr(update string) string {
	update = strings.TrimSpace(update)
	for strings.Contains(update, "  ") {
		update = strings.ReplaceAll(update, "  ", StrSpace)
	}
	return update
}

func (s *sqlUpdateSet) exprArgs(value *SQL) SQLUpdateSet {
	if value == nil || value.IsEmpty() {
		return s
	}
	update := s.beautifyExpr(value.Prepare)
	if update == StrEmpty {
		return s
	}
	index, ok := s.updateMap[update]
	if ok {
		s.updateExpr[index], s.updateArgs[index] = update, value.Args
		return s
	}
	s.updateMap[update] = len(s.updateExpr)
	s.updateExpr = append(s.updateExpr, update)
	s.updateArgs = append(s.updateArgs, value.Args)
	return s
}

func (s *sqlUpdateSet) Len() int {
	return len(s.updateExpr)
}

func (s *sqlUpdateSet) Forbid(columns ...string) SQLUpdateSet {
	for _, column := range columns {
		s.forbidSet[column] = nil
		if s.defaults != nil {
			s.defaults.forbidSet[column] = nil
		}
	}
	return s
}

func (s *sqlUpdateSet) GetForbid() []string {
	columns := make([]string, 0, len(s.forbidSet))
	for column := range s.forbidSet {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return columns
}

func (s *sqlUpdateSet) Select(columns ...string) SQLUpdateSet {
	onlyAllow := make(map[string]*struct{}, len(columns))
	for _, column := range columns {
		onlyAllow[column] = nil
	}
	if s.onlyAllow == nil {
		s.onlyAllow = make(map[string]*struct{}, 1<<3)
	}
	maps.Copy(s.onlyAllow, onlyAllow)
	return s
}

func (s *sqlUpdateSet) columnUpdate(column string, script *SQL) SQLUpdateSet {
	if s.onlyAllow != nil {
		if _, ok := s.onlyAllow[column]; !ok {
			return s
		}
	}
	s.exists[column] = append(s.exists[column], script.Prepare)
	return s.exprArgs(script)
}

func (s *sqlUpdateSet) Set(column string, value any) SQLUpdateSet {
	if _, ok := s.forbidSet[column]; ok {
		return s
	}
	script := NewSQL(fmt.Sprintf("%s = %s", s.way.Replace(column), StrPlaceholder), value)
	return s.columnUpdate(column, script)
}

func (s *sqlUpdateSet) Decr(column string, decrement any) SQLUpdateSet {
	if _, ok := s.forbidSet[column]; ok {
		return s
	}
	replace := s.way.Replace(column)
	script := NewSQL(fmt.Sprintf("%s = %s - %s", replace, replace, StrPlaceholder), decrement)
	return s.columnUpdate(column, script)
}

func (s *sqlUpdateSet) Incr(column string, increment any) SQLUpdateSet {
	if _, ok := s.forbidSet[column]; ok {
		return s
	}
	replace := s.way.Replace(column)
	script := NewSQL(fmt.Sprintf("%s = %s + %s", replace, replace, StrPlaceholder), increment)
	return s.columnUpdate(column, script)
}

func (s *sqlUpdateSet) SetMap(columnValue map[string]any) SQLUpdateSet {
	columns := make([]string, 0, len(columnValue))
	for column := range columnValue {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	for column := range columnValue {
		s.Set(column, columnValue[column])
	}
	return s
}

// SetSlice SET column = value by slice, require len(columns) == len(values).
func (s *sqlUpdateSet) SetSlice(columns []string, values []any) SQLUpdateSet {
	for index, column := range columns {
		s.Set(column, values[index])
	}
	return s
}

// Update Value of update should be one of anyStruct, *anyStruct, map[string]any.
func (s *sqlUpdateSet) Update(update any) SQLUpdateSet {
	if update == nil {
		return s
	}
	if columnValue, ok := update.(map[string]any); ok {
		columns := make([]string, 0, len(columnValue))
		for column := range columnValue {
			columns = append(columns, column)
		}
		sort.Strings(columns)
		for _, column := range columns {
			s.Set(column, columnValue[column])
		}
		return s
	}
	if tmp, ok := update.(*SQL); ok {
		return s.exprArgs(tmp)
	}
	if tmp, ok := update.(Maker); ok {
		return s.exprArgs(tmp.ToSQL())
	}
	return s.SetSlice(StructModify(update, s.way.cfg.ScanTag))
}

// Compare For compare old and new to automatically calculate the need to update columns.
func (s *sqlUpdateSet) Compare(old, new any, except ...string) SQLUpdateSet {
	return s.SetSlice(StructUpdate(old, new, s.way.cfg.ScanTag, except...))
}

// Default Set the default columns that need to be updated, such as update timestamp.
func (s *sqlUpdateSet) Default(column string, value any) SQLUpdateSet {
	column = strings.TrimSpace(column)
	if column == StrEmpty {
		return s
	}
	if _, ok := s.forbidSet[column]; ok {
		return s
	}
	script := NewSQL(fmt.Sprintf("%s = %s", s.way.Replace(column), StrPlaceholder), value)
	if _, ok := s.updateMap[script.Prepare]; ok {
		return s
	}
	s.defaults.columnUpdate(column, script)
	return s
}

// Remove Delete a column-value.
func (s *sqlUpdateSet) Remove(columns ...string) SQLUpdateSet {
	s.Forbid(columns...)
	removes := make(map[string]*struct{}, 1<<3)
	for _, column := range columns {
		if tmp, ok := s.exists[column]; ok {
			removes = MergeAssoc(removes, ArrayToAssoc(tmp, func(v string) (string, *struct{}) { return v, nil }))
		}
	}
	dropExpr := make(map[string]*struct{}, 1<<3)
	dropArgs := make(map[int]*struct{}, 1<<3)
	for index, value := range s.updateExpr {
		if _, ok := removes[value]; ok {
			dropExpr[value] = nil
			dropArgs[index] = nil
		}
	}
	updateExpr := ArrayDiscard(s.updateExpr, func(k int, v string) bool {
		_, ok := dropExpr[v]
		return ok
	})

	updateArgs := ArrayDiscard(s.updateArgs, func(k int, v []any) bool {
		_, ok := dropArgs[k]
		return ok
	})
	updateMap := AssocDiscard(s.updateMap, func(k string, v int) bool {
		_, ok := dropExpr[k]
		return ok
	})
	s.updateExpr, s.updateArgs, s.updateMap = updateExpr, updateArgs, updateMap
	if s.defaults != nil {
		s.defaults.Remove(columns...)
	}
	return s
}

// Assign Assigning values through other column; [a.]dst_column_name = [b.]src_column_name
func (s *sqlUpdateSet) Assign(dst string, src string) SQLUpdateSet {
	return s.Update(JoinSQLSpace(s.way.Replace(dst), StrEqual, s.way.Replace(src)))
}

func (s *sqlUpdateSet) GetUpdate() ([]string, [][]any) {
	return s.updateExpr, s.updateArgs
}

func (s *sqlUpdateSet) SetUpdate(updates []string, params [][]any) SQLUpdateSet {
	s.ToEmpty()
	for index, value := range updates {
		script := NewSQL(value, params[index]...)
		s.exprArgs(script)
	}
	return s
}

// SQLOnConflictUpdateSet Implement the following SQL statement:
// INSERT INTO ... ON CONFLICT ( column_a[, column_b, column_c...] ) DO UPDATE SET column1 = EXCLUDED.column1, column2 = EXCLUDED.column2, column3 = EXCLUDED.column3, column4 = 'fixed value' ...
type SQLOnConflictUpdateSet interface {
	SQLUpdateSet

	// Excluded Construct the update expression column1 = EXCLUDED.column1, column2 = EXCLUDED.column2, column3 = EXCLUDED.column3 ...
	// This is how the `new` data is accessed that causes the conflict.
	Excluded(columns ...string) SQLOnConflictUpdateSet
}

type sqlOnConflictUpdateSet struct {
	SQLUpdateSet

	way *Way
}

func newSqlOnConflictUpdateSet(way *Way) SQLOnConflictUpdateSet {
	tmp := &sqlOnConflictUpdateSet{
		way: way,
	}
	tmp.SQLUpdateSet = newSqlUpdateSet(way)
	return tmp
}

func (s *sqlOnConflictUpdateSet) Excluded(columns ...string) SQLOnConflictUpdateSet {
	for _, column := range columns {
		tmp := s.way.Replace(column)
		s.Update(NewSQL(Strings(tmp, StrSpace, StrEqual, StrSpace, StrExcluded, StrPoint, tmp)))
	}
	return s
}

// SQLOnConflict Implement the following SQL statement:
// INSERT INTO ... ON CONFLICT (column_a[, column_b, column_c...]) DO NOTHING /* If a conflict occurs, the insert operation is ignored. */
// INSERT INTO ... ON CONFLICT (column_a[, column_b, column_c...]) DO UPDATE SET column1 = EXCLUDED.column1, column2 = EXCLUDED.column2, column3 = EXCLUDED.column3, column4 = 'fixed value' ... /* If a conflict occurs, the existing row is updated with the new value */
type SQLOnConflict interface {
	Maker

	ToEmpty

	// OnConflict The column causing the conflict, such as a unique key or primary key, which can be a single column or multiple columns.
	OnConflict(onConflicts ...string) SQLOnConflict

	// Do The SQL statement that needs to be executed when a data conflict occurs. By default, nothing is done.
	Do(maker Maker) SQLOnConflict

	// DoUpdateSet SQL update statements executed when data conflicts occur.
	DoUpdateSet(fc func(u SQLOnConflictUpdateSet)) SQLOnConflict
}

type sqlOnConflict struct {
	onConflictsDoUpdateSet SQLOnConflictUpdateSet

	way *Way

	insert Maker

	onConflictsDo Maker

	onConflicts []string
}

func newSqlOnConflict(way *Way, insert Maker) *sqlOnConflict {
	return &sqlOnConflict{
		way:    way,
		insert: insert,
	}
}

func (s *sqlOnConflict) ToEmpty() {
	s.onConflictsDoUpdateSet = nil
	s.insert = nil
	s.onConflictsDo = nil
	s.onConflicts = make([]string, 0, 1<<1)
}

func (s *sqlOnConflict) OnConflict(onConflicts ...string) SQLOnConflict {
	s.onConflicts = onConflicts
	return s
}

func (s *sqlOnConflict) Do(maker Maker) SQLOnConflict {
	s.onConflictsDo = maker
	return s
}

func (s *sqlOnConflict) DoUpdateSet(fc func(u SQLOnConflictUpdateSet)) SQLOnConflict {
	tmp := s.onConflictsDoUpdateSet
	if tmp == nil {
		s.onConflictsDoUpdateSet = newSqlOnConflictUpdateSet(s.way)
		tmp = s.onConflictsDoUpdateSet
	}
	fc(tmp)
	return s
}

func (s *sqlOnConflict) ToSQL() *SQL {
	script := NewSQL(StrEmpty)
	if s.insert == nil || s.insert.ToSQL().IsEmpty() || len(s.onConflicts) == 0 {
		return script
	}
	insert := s.insert.ToSQL()
	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)
	b.WriteString(insert.Prepare)
	script.Args = append(script.Args, insert.Args...)
	b.WriteString(Strings(StrSpace, StrOn, StrSpace, StrConflict, StrSpace))
	b.WriteString(ParcelPrepare(strings.Join(s.way.ReplaceAll(s.onConflicts), StrCommaSpace)))
	b.WriteString(StrSpace)
	b.WriteString(StrDo)
	b.WriteString(StrSpace)
	prepare, args := StrNothing, make([]any, 0)
	if onConflictsDo := s.onConflictsDo; onConflictsDo != nil {
		if tmp := onConflictsDo.ToSQL(); tmp != nil && !tmp.IsEmpty() {
			prepare, args = tmp.Prepare, tmp.Args[:]
		}
	}
	if prepare == StrNothing && s.onConflictsDoUpdateSet != nil && s.onConflictsDoUpdateSet.Len() > 0 {
		update := s.onConflictsDoUpdateSet.ToSQL()
		if update != nil && !update.IsEmpty() {
			b1 := poolGetStringBuilder()
			defer poolPutStringBuilder(b1)
			b.WriteString(Strings(StrUpdate, StrSpace, StrSet, StrSpace))
			b1.WriteString(update.Prepare)
			prepare, args = b1.String(), update.Args[:]
		}
	}
	b.WriteString(prepare)
	script.Args = append(script.Args, args...)
	script.Prepare = b.String()
	return script
}

// SQLInsert Build INSERT statements.
type SQLInsert interface {
	Maker

	ToEmpty

	// Table Insert data into the target table.
	Table(table Maker) SQLInsert

	// Forbid When inserting data, it is forbidden to set certain columns, such as: auto-increment id.
	Forbid(columns ...string) SQLInsert

	// GetForbid Get a list of columns that have been prohibited from insertion.
	GetForbid() []string

	// Select Set the columns to allow inserts only, not including defaults.
	Select(columns ...string) SQLInsert

	// Column Set the inserted column list. An empty value will delete the set field list.
	Column(columns ...string) SQLInsert

	// Values Set the list of values to be inserted.
	Values(values ...[]any) SQLInsert

	// Subquery Use the query result as the values of the insert statement.
	Subquery(subquery Maker) SQLInsert

	// ColumnValue Set a single column and value.
	ColumnValue(column string, value any) SQLInsert

	// Create Parses the given insert data and sets the insert data.
	Create(create any) SQLInsert

	// Default Set the default column for inserted data, such as the creation timestamp.
	Default(column string, value any) SQLInsert

	// Remove Delete a column-value.
	Remove(columns ...string) SQLInsert

	// Returning Insert a piece of data and get the auto-increment value.
	Returning(fc func(r SQLReturning)) SQLInsert

	// GetColumn Get the list of inserted columns that have been set.
	GetColumn(excludes ...string) []string

	// OnConflict When inserting data, set the execution logic when there is a conflict.
	OnConflict(fc func(o SQLOnConflict)) SQLInsert
}

type sqlInsert struct {
	forbidSet map[string]*struct{}

	way *Way

	table *SQL

	onlyAllow map[string]*struct{} // Set the columns to allow inserts only.

	columns *sqlSelect

	values *sqlValues

	returning *sqlReturning

	onConflict *sqlOnConflict

	defaults *sqlInsert
}

func (s *sqlInsert) init() {
	s.forbidSet = make(map[string]*struct{}, 1<<3)
	s.table = NewEmptySQL()
	s.columns = newSqlSelect(s.way)
	s.values = newSqlValues()
	s.returning = newReturning(s.way, NewEmptySQL())
	s.onConflict = newSqlOnConflict(s.way, NewEmptySQL())
}

func (s *sqlInsert) toEmpty() {
	s.forbidSet = make(map[string]*struct{}, 1<<3)
	s.onlyAllow = nil
	s.columns.ToEmpty()
	s.values.ToEmpty()
	s.returning.ToEmpty()
	s.onConflict.ToEmpty()
}

func newSqlInsert(way *Way) *sqlInsert {
	result := &sqlInsert{
		way: way,
	}
	defaults := &sqlInsert{
		way: way,
	}
	result.init()
	defaults.init()
	result.defaults = defaults
	return result
}

func (s *sqlInsert) ToEmpty() {
	s.toEmpty()
	s.defaults.toEmpty()
}

func (s *sqlInsert) ToSQL() *SQL {
	if s.table.IsEmpty() {
		return NewEmptySQL()
	}
	makers := []any{NewSQL(StrInsert), NewSQL(StrInto), s.table}

	columns1, params1 := s.columns.Get()
	values1 := make([][]any, len(s.values.values))
	copy(values1, s.values.values)

	columns, values := make([]string, len(columns1)), make([][]any, len(values1))
	copy(columns, columns1)
	copy(values, values1)
	params := make(map[int][]any, len(params1))
	maps.Copy(params, params1)
	if len(columns) > 0 {
		if len(values) > 0 && len(s.defaults.values.values) == 1 {
			// add default columns and values.
			defaultColumns, defaultParams := s.defaults.columns.Get()
			defaultValuesSlice := s.defaults.values.values[0]
			defaultValues := make([]any, len(defaultValuesSlice))
			copy(defaultValues, defaultValuesSlice)
			defaultColumnsLength, defaultValuesLength := len(defaultColumns), len(defaultValues)
			if defaultColumnsLength > 0 && defaultValuesLength > 0 && defaultColumnsLength == len(defaultValues) {
				had := make(map[string]*struct{}, len(columns))
				for _, column := range columns {
					had[column] = nil
				}
				for index, column := range defaultColumns {
					if _, ok := had[column]; ok {
						continue
					}
					next := len(columns)
					columns = append(columns, column)
					if len(defaultParams[index]) > 0 {
						params[next] = defaultParams[index]
					}
					for i := range values {
						values[i] = append(values[i], defaultValues[index])
					}
				}
			}
		}
		makers = append(makers, NewSQL(ParcelPrepare(strings.Join(columns, StrCommaSpace))))
	}

	ok := false

	subquery := s.values.subquery
	if subquery != nil {
		if script := subquery.ToSQL(); !script.IsEmpty() {
			makers = append(makers, script)
			ok = true
		}
	}

	if !ok {
		if len(values) > 0 {
			makers = append(makers, NewSQL(StrValues))
			makers = append(makers, s.values.valuesToSQL(values))
			ok = true
		}
	}

	if !ok {
		return NewEmptySQL()
	}

	if s.returning != nil && s.returning.execute != nil {
		s.returning.insert = JoinSQLSpace(makers...)
		if script := s.returning.ToSQL(); !script.IsEmpty() {
			return script
		}
	}

	if s.onConflict != nil && len(s.onConflict.onConflicts) > 0 {
		s.onConflict.insert = JoinSQLSpace(makers...)
		if script := s.onConflict.ToSQL(); !script.IsEmpty() {
			return script
		}
	}

	return JoinSQLSpace(makers...)
}

func (s *sqlInsert) Table(table Maker) SQLInsert {
	if table == nil {
		return s
	}
	script := table.ToSQL()
	if script.IsEmpty() {
		return s
	}
	s.table = script
	return s
}

func (s *sqlInsert) Forbid(columns ...string) SQLInsert {
	for _, column := range columns {
		s.forbidSet[column] = nil
		s.defaults.forbidSet[column] = nil
	}
	return s
}

func (s *sqlInsert) GetForbid() []string {
	columns := make([]string, 0, len(s.forbidSet))
	for column := range s.forbidSet {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return columns
}

func (s *sqlInsert) Select(columns ...string) SQLInsert {
	onlyAllow := make(map[string]*struct{}, len(columns))
	for _, column := range columns {
		onlyAllow[column] = nil
	}
	if s.onlyAllow == nil {
		s.onlyAllow = make(map[string]*struct{}, 1<<3)
	}
	maps.Copy(s.onlyAllow, onlyAllow)
	return s
}

func (s *sqlInsert) Column(columns ...string) SQLInsert {
	if len(columns) == 0 {
		return s
	}
	s.columns.ToEmpty() // Clear previous columns.
	s.columns.Select(columns)
	return s
}

func (s *sqlInsert) Values(values ...[]any) SQLInsert {
	if len(values) == 0 {
		return s
	}
	s.values.Values(values...)
	return s
}

func (s *sqlInsert) Subquery(subquery Maker) SQLInsert {
	s.values.Subquery(subquery)
	return s
}

func (s *sqlInsert) ColumnValue(column string, value any) SQLInsert {
	if _, ok := s.forbidSet[column]; ok {
		return s
	}
	if s.columns.Has(column) {
		return s
	}
	if s.onlyAllow != nil {
		if _, ok := s.onlyAllow[column]; !ok {
			return s
		}
	}
	s.columns.AddAll(column)
	for index := range s.values.values {
		s.values.values[index] = append(s.values.values[index], value)
	}
	return s
}

// Create value of creation should be one of struct{}, *struct{}, map[string]any, []struct, []*struct{}, *[]struct{}, *[]*struct{}.
func (s *sqlInsert) Create(create any) SQLInsert {
	if columnValue, ok := create.(map[string]any); ok {
		length := len(columnValue)
		if length == 0 {
			return s
		}
		columns := make([]string, 0, length)
		for column := range columnValue {
			if _, ok := s.forbidSet[column]; ok {
				continue
			}
			columns = append(columns, column)
		}
		sort.Strings(columns)
		for _, column := range columns {
			s.ColumnValue(column, columnValue[column])
		}
		return s
	}
	columns, values := StructInsert(create, s.way.cfg.ScanTag, nil, nil)
	removes := make(map[int]*struct{}, len(columns))
	for index, column := range columns {
		if _, ok := s.forbidSet[column]; ok {
			removes[index] = nil
		}
	}
	if len(removes) > 0 {
		columns = ArrayDiscard(columns, func(k int, v string) bool {
			_, ok := removes[k]
			return ok
		})
		for index, value := range values {
			values[index] = ArrayDiscard(value, func(k int, v any) bool {
				_, ok := removes[k]
				return ok
			})
		}
	}
	if s.onlyAllow != nil {
		indexes := make(map[int]*struct{}, 1<<3)
		for index, column := range columns {
			if _, ok := s.onlyAllow[column]; ok {
				indexes[index] = nil
			}
		}
		columns = ArrayDiscard(columns, func(k int, v string) bool {
			_, ok := indexes[k]
			return ok
		})
		for index, value := range values {
			values[index] = ArrayDiscard(value, func(k int, v any) bool {
				_, ok := indexes[k]
				return ok
			})
		}
	}
	return s.Column(columns...).Values(values...)
}

func (s *sqlInsert) Default(column string, value any) SQLInsert {
	s.defaults.ColumnValue(column, value)
	return s
}

func (s *sqlInsert) Remove(columns ...string) SQLInsert {
	fields, params := s.columns.Get()
	values := s.values.values
	length1, length2 := len(fields), len(values)
	if length1 == 0 || length2 == 0 {
		return s
	}
	ok := true
	for _, value := range values {
		if length1 != len(value) {
			ok = false
			break
		}
	}
	if !ok {
		return s
	}

	removes := make(map[string]*struct{}, len(columns))
	for _, column := range columns {
		removes[column] = nil
	}
	assoc := make(map[int]*struct{}, length1)
	for index, field := range fields {
		if _, ok = removes[field]; ok {
			assoc[index] = nil
		}
	}
	if len(assoc) == 0 {
		return s
	}
	fields = ArrayDiscard(fields, func(k int, v string) bool {
		if _, ok = assoc[k]; ok {
			delete(params, k)
		}
		return ok
	})
	s.columns.Del().Set(fields, params)
	for index, value := range values {
		s.values.values[index] = ArrayDiscard(value, func(k int, v any) bool {
			_, ok = assoc[k]
			return ok
		})
	}
	if s.defaults != nil {
		s.defaults.Remove(columns...)
	}
	return s
}

func (s *sqlInsert) Returning(fc func(r SQLReturning)) SQLInsert {
	if s.returning == nil {
		s.returning = newReturning(s.way, NewEmptySQL())
	}
	fc(s.returning)
	return s
}

func (s *sqlInsert) GetColumn(excludes ...string) []string {
	lengths := len(excludes)
	columns, _ := s.columns.Get()
	if lengths == 0 {
		return columns
	}
	discard := ArrayToAssoc(excludes, func(v string) (string, *struct{}) { return v, nil })
	columns = ArrayDiscard(columns, func(k int, v string) bool {
		_, ok := discard[v]
		return ok
	})
	return columns
}

func (s *sqlInsert) OnConflict(fc func(o SQLOnConflict)) SQLInsert {
	fc(s.onConflict)
	return s
}

/* CASE [xxx] WHEN x THEN X [WHEN xx THEN XX] [ELSE xxx] END [AS xxx] */

// SQLString Convert a go string to a sql string.
func SQLString(value string) string {
	return fmt.Sprintf("'%s'", value)
}

// SQLCase Implementing SQL CASE.
type SQLCase interface {
	Maker

	// Alias Set alias name.
	Alias(alias string) SQLCase

	// Case SQL CASE.
	Case(value any) SQLCase

	// WhenThen Add WHEN xxx THEN xxx.
	WhenThen(when, then any) SQLCase

	// Else SQL CASE xxx ELSE xxx.
	Else(value any) SQLCase
}

type sqlCase struct {
	sqlCase *SQL // CASE value, value is optional.

	sqlElse *SQL // ELSE value, value is optional.

	way *Way

	alias string // Alias-name for CASE , value is optional.

	whenThen []*SQL // WHEN xxx THEN xxx [WHEN xxx THEN xxx] ...
}

func NewSQLCase(way *Way) SQLCase {
	return &sqlCase{
		way: way,
	}
}

func (s *Way) Case() SQLCase {
	return NewSQLCase(s)
}

// ToSQL Build CASE statement.
func (s *sqlCase) ToSQL() *SQL {
	script := NewSQL(StrEmpty)
	if len(s.whenThen) == 0 {
		return script
	}
	whenThen := JoinSQLSpace(AnyAny(s.whenThen)...).ToSQL()
	if whenThen.IsEmpty() {
		return script
	}
	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)
	b.WriteString(StrCase)
	if tmp := s.sqlCase; tmp != nil && !tmp.IsEmpty() {
		b.WriteString(StrSpace)
		b.WriteString(tmp.Prepare)
		script.Args = append(script.Args, tmp.Args...)
	}
	b.WriteString(StrSpace)
	b.WriteString(whenThen.Prepare)
	script.Args = append(script.Args, whenThen.Args...)
	if tmp := s.sqlElse; tmp != nil && !tmp.IsEmpty() {
		b.WriteString(StrSpace)
		b.WriteString(StrElse)
		b.WriteString(StrSpace)
		b.WriteString(tmp.Prepare)
		script.Args = append(script.Args, tmp.Args...)
	}
	b.WriteString(StrSpace)
	b.WriteString(StrEnd)
	script.Prepare = b.String()
	return newSqlAlias(script).v(s.way).SetAlias(s.alias).ToSQL()
}

// Alias Set the alias for the entire CASE.
func (s *sqlCase) Alias(alias string) SQLCase {
	s.alias = alias
	return s
}

func handleCaseEmptyString(script *SQL) *SQL {
	if script == nil {
		return NewSQL(StrNull)
	}
	if prepare := SQLString(StrEmpty); script.Prepare == StrEmpty {
		script.Prepare, script.Args = prepare, nil
	}
	return script
}

func (s *sqlCase) Case(value any) SQLCase {
	s.sqlCase = handleCaseEmptyString(nil1any2sql(value))
	return s
}

func (s *sqlCase) WhenThen(when, then any) SQLCase {
	s.whenThen = append(s.whenThen, JoinSQLSpace(StrWhen, handleCaseEmptyString(nil1any2sql(when)), StrThen, handleCaseEmptyString(nil1any2sql(then))))
	return s
}

func (s *sqlCase) Else(value any) SQLCase {
	s.sqlElse = handleCaseEmptyString(nil1any2sql(value))
	return s
}
