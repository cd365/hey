// Construct SQL clause

package hey

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"regexp"
	"sort"
	"strings"

	"github.com/cd365/hey/v7/cst"
)

// ToEmpty Sets the property value of an object to empty value.
type ToEmpty interface {
	// ToEmpty Sets the property value of an object to empty value.
	ToEmpty()
}

// SQLLabel Constructing SQL statement labels.
type SQLLabel interface {
	Maker

	ToEmpty

	// Labels Add custom labels.
	Labels(labels ...string) SQLLabel
}

type sqlLabel struct {
	labelsMap map[string]*struct{}

	// separator Set separator string between multiple labels.
	separator string

	labels []string
}

func newSQLLabel(way *Way) SQLLabel {
	if way == nil {
		panic(pin)
	}
	separator := way.cfg.LabelsSeparator
	if separator == cst.Empty {
		separator = cst.Comma
	}
	return &sqlLabel{
		labelsMap: make(map[string]*struct{}, 1),
		labels:    make([]string, 0, 1),
		separator: separator,
	}
}

func (s *sqlLabel) ToEmpty() {
	s.labelsMap = make(map[string]*struct{}, 1)
	s.labels = make([]string, 0, 1)
}

func (s *sqlLabel) Labels(labels ...string) SQLLabel {
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if label == cst.Empty {
			continue
		}
		if _, ok := s.labelsMap[label]; ok {
			continue
		}
		s.labels = append(s.labels, label)
		s.labelsMap[label] = nil
	}
	return s
}

func (s *sqlLabel) ToSQL() *SQL {
	if len(s.labels) == 0 {
		return NewEmptySQL()
	}
	return NewSQL(BlockComment(strings.Join(s.labels, s.separator)))
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

func newSQLWith(way *Way) SQLWith {
	if way == nil {
		panic(pin)
	}
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
		return NewSQL(cst.Empty)
	}
	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)
	b.WriteString(cst.WITH)
	b.WriteString(cst.Space)
	if s.recursive {
		b.WriteString(cst.RECURSIVE)
		b.WriteString(cst.Space)
	}
	result := NewEmptySQL()
	for index, alias := range s.alias {
		prepare := s.prepare[alias]
		if prepare == nil {
			continue
		}

		script := prepare.ToSQL()
		if script == nil || script.IsEmpty() {
			continue
		}

		script.Prepare = strings.TrimSpace(script.Prepare)
		if script.Prepare == cst.Empty {
			continue
		}

		if index > 0 {
			b.WriteString(cst.CommaSpace)
		}

		b.WriteString(alias)
		b.WriteString(cst.Space)

		if columns := s.column[alias]; len(columns) > 0 {
			// Displays the column alias that defines the CTE, overwriting the original column name of the query result.
			b.WriteString(cst.LeftParenthesis)
			b.WriteString(cst.Space)
			b.WriteString(strings.Join(columns, cst.CommaSpace))
			b.WriteString(cst.Space)
			b.WriteString(cst.RightParenthesis)
			b.WriteString(cst.Space)
		}

		b.WriteString(JoinString(cst.AS, cst.Space))
		b.WriteString(ParcelPrepare(script.Prepare))
		result.Args = append(result.Args, script.Args...)
	}
	result.Prepare = b.String()
	return result
}

func (s *sqlWith) Recursive() SQLWith {
	s.recursive = !s.recursive
	return s
}

func (s *sqlWith) Set(alias string, maker Maker, columns ...string) SQLWith {
	if alias == cst.Empty || maker == nil || maker.ToSQL().IsEmpty() {
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
	if alias == cst.Empty {
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

func newSQLSelect(way *Way) SQLSelect {
	if way == nil {
		panic(pin)
	}
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
		prepare := cst.Asterisk
		if s.distinct {
			prepare = JoinString(cst.DISTINCT, cst.Space, prepare)
		}
		return NewSQL(prepare)
	}
	script := NewSQL(cst.Empty)
	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)
	if s.distinct {
		b.WriteString(cst.DISTINCT)
		b.WriteString(cst.Space)
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
	b.WriteString(strings.Join(s.way.ReplaceAll(columns), cst.CommaSpace))
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
	if column == cst.Empty {
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

func (s *sqlSelect) Del(columns ...string) SQLSelect {
	if columns == nil {
		if s.IsEmpty() || !s.distinct {
			s.ToEmpty()
		}
		return s
	}
	deletes := make(map[int]*struct{}, len(columns))
	for _, column := range columns {
		if column == cst.Empty {
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
			// Example: SELECT 1 ...
			s.Add(AnyToSQL(value))
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

func newSQLJoinOn(way *Way) SQLJoinOn {
	if way == nil {
		panic(pin)
	}
	return &sqlJoinOn{
		way: way,
		on:  way.F(),
	}
}

func (s *sqlJoinOn) Equal(table1alias string, table1column string, table2alias string, table2column string) SQLJoinOn {
	s.on.And(JoinSQLSpace(Prefix(s.way.Replace(table1alias), s.way.Replace(table1column)), cst.Equal, Prefix(s.way.Replace(table2alias), s.way.Replace(table2column))))
	return s
}

func (s *sqlJoinOn) On(on func(f Filter)) SQLJoinOn {
	if on != nil {
		on(s.on)
	}
	return s
}

func (s *sqlJoinOn) Using(columns ...string) SQLJoinOn {
	columns = DiscardDuplicate(func(tmp string) bool { return tmp == cst.Empty }, columns...)
	if len(columns) > 0 {
		s.usings = columns
	}
	return s
}

func (s *sqlJoinOn) ToSQL() *SQL {
	// JOIN ON
	if s.on != nil && !s.on.IsEmpty() {
		script := s.on.ToSQL()
		script.Prepare = JoinString(cst.ON, cst.Space, script.Prepare)
		return script
	}
	// JOIN USING
	if length := len(s.usings); length > 0 {
		using := make([]string, 0, length)
		for _, column := range s.usings {
			if column != cst.Empty {
				using = append(using, column)
			}
		}
		if len(using) > 0 {
			using = s.way.ReplaceAll(using)
			prepare := JoinString(cst.USING, cst.Space, cst.LeftParenthesis, cst.Space, strings.Join(using, cst.CommaSpace), cst.Space, cst.RightParenthesis)
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

	// GetMaster Get join query the master table.
	GetMaster() SQLAlias

	// SetMaster Set join query the master table.
	SetMaster(table SQLAlias) SQLJoin

	// NewTable Create a table for join query.
	NewTable(table any, alias string) SQLAlias

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

	// TableColumn Create a table name prefix for the query column.
	TableColumn(table SQLAlias, column string, aliases ...string) string

	// TableColumns Add table name prefix to the query column list values.
	TableColumns(table SQLAlias, columns ...string) []string
}

type sqlJoin struct {
	table SQLAlias

	query SQLSelect

	way *Way

	joins []*sqlJoinSchema
}

func newSQLJoin(way *Way, query SQLSelect) SQLJoin {
	if way == nil {
		panic(pin)
	}
	tmp := &sqlJoin{
		way:   way,
		joins: make([]*sqlJoinSchema, 0, 1<<1),
		query: query,
	}
	return tmp
}

func (s *sqlJoin) ToEmpty() {
	s.joins = make([]*sqlJoinSchema, 0, 1<<1)
	s.query.ToEmpty()
	s.table = nil
}

func (s *sqlJoin) GetMaster() SQLAlias {
	return s.table
}

func (s *sqlJoin) SetMaster(table SQLAlias) SQLJoin {
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
			b.WriteString(cst.Space)
		}
		b.WriteString(tmp.joinType)
		right := tmp.joinTable.ToSQL()
		b.WriteString(cst.Space)
		b.WriteString(right.Prepare)
		script.Args = append(script.Args, right.Args...)
		if tmp.condition != nil {
			if on := tmp.condition.ToSQL(); on != nil && !on.IsEmpty() {
				b.WriteString(cst.Space)
				b.WriteString(on.Prepare)
				script.Args = append(script.Args, on.Args...)
			}
		}
	}
	script.Prepare = b.String()
	return script
}

func (s *sqlJoin) NewTable(table any, alias string) SQLAlias {
	return s.way.cfg.NewSQLTable(s.way, table).SetAlias(alias)
}

func (s *sqlJoin) joinOn() SQLJoinOn {
	return s.way.cfg.NewSQLJoinOn(s.way)
}

// On For `... JOIN ON ...`
func (s *sqlJoin) On(on func(on SQLJoinOn, table1alias string, table2alias string)) SQLJoinAssoc {
	return func(table1alias string, table2alias string) SQLJoinOn {
		joinOn := s.joinOn()
		on(joinOn, table1alias, table2alias)
		return joinOn
	}
}

// Using For `... JOIN USING ...`
func (s *sqlJoin) Using(columns ...string) SQLJoinAssoc {
	return func(alias1 string, alias2 string) SQLJoinOn {
		return s.joinOn().Using(columns...)
	}
}

// OnEqual For `... JOIN ON ... = ... [...]`
func (s *sqlJoin) OnEqual(table1column string, table2column string) SQLJoinAssoc {
	if table1column == cst.Empty || table2column == cst.Empty {
		return nil
	}
	return func(alias1 string, alias2 string) SQLJoinOn {
		return s.joinOn().Equal(alias1, table1column, alias2, table2column)
	}
}

func (s *sqlJoin) Join(joinType string, table1 SQLAlias, table2 SQLAlias, on SQLJoinAssoc) SQLJoin {
	if table2 == nil || table2.ToSQL().IsEmpty() {
		return s
	}
	if joinType == cst.Empty {
		joinType = JoinString(cst.INNER, cst.Space, cst.JOIN)
	}
	if table1 == nil || table1.ToSQL().IsEmpty() {
		table1 = s.table
	}
	join := &sqlJoinSchema{
		joinType:  joinType,
		joinTable: table2,
	}
	if on != nil {
		alias1 := table1.GetAlias()
		alias2 := table2.GetAlias()
		if alias1 == cst.Empty {
			// Use the default table name when the alias is empty; in this case,
			// the table name should be the original table name or the CTE alias.
			alias1 = table1.GetSQL().Prepare
		}
		if alias2 == cst.Empty {
			// Use the default table name when the alias is empty; in this case,
			// the table name should be the original table name or the CTE alias.
			alias2 = table2.GetSQL().Prepare
		}
		join.condition = on(alias1, alias2)
	}
	s.joins = append(s.joins, join)
	return s
}

func (s *sqlJoin) InnerJoin(table1 SQLAlias, table2 SQLAlias, on SQLJoinAssoc) SQLJoin {
	return s.Join(JoinString(cst.INNER, cst.Space, cst.JOIN), table1, table2, on)
}

func (s *sqlJoin) LeftJoin(table1 SQLAlias, table2 SQLAlias, on SQLJoinAssoc) SQLJoin {
	return s.Join(JoinString(cst.LEFT, cst.Space, cst.JOIN), table1, table2, on)
}

func (s *sqlJoin) RightJoin(table1 SQLAlias, table2 SQLAlias, on SQLJoinAssoc) SQLJoin {
	return s.Join(JoinString(cst.RIGHT, cst.Space, cst.JOIN), table1, table2, on)
}

func (s *sqlJoin) Select(columns ...any) SQLJoin {
	s.query.Select(columns...)
	return s
}

func (s *sqlJoin) tableColumns(table SQLAlias, columns []string) []string {
	if table == nil {
		return columns
	}
	return s.way.T(table.GetAlias()).ColumnAll(columns...)
}

func (s *sqlJoin) TableColumn(table SQLAlias, column string, aliases ...string) string {
	return s.way.Alias(s.tableColumns(table, []string{column})[0], aliases...).ToSQL().Prepare
}

func (s *sqlJoin) TableColumns(table SQLAlias, columns ...string) []string {
	return s.tableColumns(table, columns)
}

type SQLWindow interface {
	Maker

	ToEmpty

	// Set Setting window expression.
	Set(alias string, maker func(o SQLWindowFuncOver)) SQLWindow

	// Del Removing window expression.
	Del(alias string) SQLWindow
}

type sqlWindow struct {
	way *Way

	prepare map[string]Maker

	alias []string
}

func newSQLWindow(way *Way) SQLWindow {
	if way == nil {
		panic(pin)
	}
	return &sqlWindow{
		way:     way,
		prepare: make(map[string]Maker, 1<<1),
		alias:   make([]string, 0, 1<<1),
	}
}

func (s *sqlWindow) ToEmpty() {
	s.prepare = make(map[string]Maker, 1<<1)
	s.alias = make([]string, 0, 1<<1)
}

func (s *sqlWindow) IsEmpty() bool {
	return len(s.alias) == 0
}

func (s *sqlWindow) ToSQL() *SQL {
	if s.IsEmpty() {
		return NewSQL(cst.Empty)
	}

	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)
	b.WriteString(cst.WINDOW)
	b.WriteString(cst.Space)
	result := NewEmptySQL()
	for index, alias := range s.alias {
		prepare := s.prepare[alias]
		if prepare == nil {
			continue
		}

		script := prepare.ToSQL()
		if script == nil || script.IsEmpty() {
			continue
		}

		script.Prepare = strings.TrimSpace(script.Prepare)
		if script.Prepare == cst.Empty {
			continue
		}

		if index > 0 {
			b.WriteString(cst.CommaSpace)
		}

		b.WriteString(alias)
		b.WriteString(cst.Space)

		notLeftParenthesis := script.Prepare[0] != cst.LeftParenthesis[0]
		b.WriteString(JoinString(cst.AS, cst.Space))
		if notLeftParenthesis {
			b.WriteString(JoinString(cst.LeftParenthesis, cst.Space))
		}
		b.WriteString(script.Prepare)
		if notLeftParenthesis {
			b.WriteString(JoinString(cst.Space, cst.RightParenthesis))
		}
		result.Args = append(result.Args, script.Args...)
	}
	result.Prepare = b.String()
	return result
}

func (s *sqlWindow) Set(alias string, maker func(o SQLWindowFuncOver)) SQLWindow {
	if alias == cst.Empty || maker == nil {
		return s
	}
	windowFuncOver := s.way.cfg.NewSQLWindowFuncOver(s.way)
	maker(windowFuncOver)
	script := windowFuncOver.ToSQL()
	if script == nil || script.IsEmpty() {
		return s
	}
	if _, ok := s.prepare[alias]; !ok {
		s.alias = append(s.alias, alias)
	}
	s.prepare[alias] = script
	return s
}

func (s *sqlWindow) Del(alias string) SQLWindow {
	if alias == cst.Empty {
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
	delete(s.prepare, alias)
	return s
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

func newSQLGroupBy(way *Way) SQLGroupBy {
	if way == nil {
		panic(pin)
	}
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
	groupBy := strings.Join(s.way.ReplaceAll(s.groupColumns), cst.CommaSpace)
	groupByArgs := make([]any, 0)
	for _, column := range s.groupColumns {
		groupByArgs = append(groupByArgs, s.groupColumnsArgs[column]...)
	}
	script.Prepare = JoinString(cst.GROUP, cst.Space, cst.BY, cst.Space, groupBy)
	script.Args = groupByArgs
	if s.having == nil || s.having.IsEmpty() {
		return script
	}
	return JoinSQLSpace(script, cst.HAVING, parcelSingleFilter(s.having))
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

	// Num Number of sorted columns used.
	Num() int

	// IsEmpty Is there a list of sorting columns?
	IsEmpty() bool

	// Allow the list of columns that can be used for sorting.
	Allow(columns ...string) SQLOrderBy

	// Replace The column name that may be used.
	Replace(dst string, src string) SQLOrderBy

	// Asc Build column1 ASC, column2 ASC, column3 ASC...
	Asc(columns ...string) SQLOrderBy

	// Desc Build column1 DESC, column2 DESC, column3 DESC...
	Desc(columns ...string) SQLOrderBy

	// OrderString Automatically call sorting based on the sort string format.
	OrderString(order *string) SQLOrderBy
}
type sqlOrderBy struct {
	allow map[string]*struct{}

	replace map[string]string

	orderMap map[string]int

	way *Way

	orderBy []string
}

func newSQLOrderBy(way *Way) SQLOrderBy {
	if way == nil {
		panic(pin)
	}
	return &sqlOrderBy{
		orderMap: make(map[string]int, 1<<1),
		way:      way,
		orderBy:  make([]string, 0, 1<<1),
	}
}

func (s *sqlOrderBy) ToEmpty() {
	s.allow, s.replace = nil, nil
	s.orderBy = make([]string, 0, 1<<1)
	s.orderMap = make(map[string]int, 1<<1)
}

func (s *sqlOrderBy) IsEmpty() bool {
	return len(s.orderBy) == 0
}

func (s *sqlOrderBy) ToSQL() *SQL {
	script := NewSQL(cst.Empty)
	if s.IsEmpty() {
		return script
	}
	script.Prepare = JoinString(cst.ORDER, cst.Space, cst.BY, cst.Space, strings.Join(s.orderBy, cst.CommaSpace))
	return script
}

func (s *sqlOrderBy) Num() int {
	return len(s.orderBy)
}

func (s *sqlOrderBy) Allow(columns ...string) SQLOrderBy {
	columns = DiscardDuplicate(func(column string) bool {
		if column == cst.Empty || strings.TrimSpace(column) == cst.Empty {
			return true
		}
		return false
	}, columns...)
	length := len(columns)
	if length == 0 {
		return s
	}
	if s.allow == nil {
		s.allow = make(map[string]*struct{}, length)
	}
	for _, column := range columns {
		s.allow[column] = nil
	}
	return s
}

func (s *sqlOrderBy) Replace(dst string, src string) SQLOrderBy {
	dst, src = strings.TrimSpace(dst), strings.TrimSpace(src)
	if dst == cst.Empty || src == cst.Empty {
		return s
	}
	if s.replace == nil {
		s.replace = make(map[string]string, 1)
	}
	s.replace[src] = dst
	return s
}

func (s *sqlOrderBy) add(category string, columns ...string) SQLOrderBy {
	if category == cst.Empty {
		return s
	}
	columns = DiscardDuplicate(func(column string) bool {
		if column == cst.Empty || strings.TrimSpace(column) == cst.Empty {
			return true
		}
		return false
	}, columns...)
	index := len(s.orderBy)
	for _, column := range columns {
		if s.replace != nil {
			replace, ok := s.replace[column]
			if ok && replace != cst.Empty {
				column = replace
			}
		}
		if s.allow != nil {
			if _, ok := s.allow[column]; !ok {
				continue
			}
		}
		if _, ok := s.orderMap[column]; ok {
			continue
		}
		s.orderMap[column] = index
		index++
		order := s.way.Replace(column)
		order = JoinString(order, cst.Space, category)
		s.orderBy = append(s.orderBy, order)
	}
	return s
}

func (s *sqlOrderBy) Asc(columns ...string) SQLOrderBy {
	return s.add(cst.ASC, columns...)
}

func (s *sqlOrderBy) Desc(columns ...string) SQLOrderBy {
	return s.add(cst.DESC, columns...)
}

// orderStringRegexp `column_name_first:a,column_name_second:d` => `column_name_first ASC, column_name_second DESC`
var orderStringRegexp = regexp.MustCompile(`^([a-zA-Z][a-zA-Z0-9_]*([.][a-zA-Z][a-zA-Z0-9_]*)*):([ad])$`)

func (s *sqlOrderBy) OrderString(order *string) SQLOrderBy {
	if order == nil || *order == cst.Empty {
		return s
	}
	orders := strings.Split(*order, cst.Comma)
	for _, v := range orders {
		if len(v) > 32 {
			continue
		}
		match := orderStringRegexp.FindAllStringSubmatch(strings.TrimSpace(v), -1)
		length := len(match)
		if length != 1 {
			continue
		}
		matched := match[0]
		length = len(matched) // the length should be 4.
		if length < 4 || matched[3] == cst.Empty {
			continue
		}
		column := matched[1]
		if matched[3][0] == 97 {
			s.Asc(column)
			continue
		}
		if matched[3][0] == 100 {
			s.Desc(column)
			continue
		}
	}
	return s
}

// SQLLimit Build LIMIT n[ OFFSET m] or OFFSET m ROWS FETCH NEXT n ROWS ONLY statements.
type SQLLimit interface {
	Maker

	ToEmpty

	IsEmpty() bool

	// Limit SQL LIMIT.
	Limit(limit int64) SQLLimit

	// Offset SQL OFFSET.
	Offset(offset int64) SQLLimit

	// Page SQL LIMIT and OFFSET.
	Page(page int64, pageSize ...int64) SQLLimit

	// DirectLimit Directly use the effective limit value, unaffected by the configured maximum.
	DirectLimit(limit int64) SQLLimit

	// DirectOffset Directly use the effective offset value, unaffected by the configured maximum.
	DirectOffset(offset int64) SQLLimit

	// DirectPage Directly use the effective limit and offset value, unaffected by the configured maximum.
	DirectPage(page int64, pageSize ...int64) SQLLimit
}

// sqlLimit Implement `LIMIT n[ OFFSET m]` SQL statements.
type sqlLimit struct {
	way *Way

	limit *int64

	offset *int64
}

func newSQLLimit(way *Way) SQLLimit {
	if way == nil {
		panic(pin)
	}
	return &sqlLimit{
		way: way,
	}
}

func (s *sqlLimit) ToEmpty() {
	s.limit = nil
	s.offset = nil
}

func (s *sqlLimit) IsEmpty() bool {
	return s.limit == nil
}

func (s *sqlLimit) ToSQL() *SQL {
	script := NewSQL(cst.Empty)
	if s.IsEmpty() {
		return script
	}
	makers := make([]any, 0, 1<<2)
	makers = append(makers, AnyToSQL(cst.LIMIT))
	makers = append(makers, AnyToSQL(*s.limit))
	if s.offset != nil && *s.offset >= 0 {
		makers = append(makers, AnyToSQL(cst.OFFSET))
		makers = append(makers, AnyToSQL(*s.offset))
	}
	return JoinSQLSpace(makers...)
}

func (s *sqlLimit) limitValue(direct bool, limit int64) *sqlLimit {
	if limit > 0 {
		if !direct && s.way.cfg.MaxLimit > 0 && limit > s.way.cfg.MaxLimit {
			limit = s.way.cfg.MaxLimit
		}
		s.limit = &limit
	}
	return s
}

func (s *sqlLimit) offsetValue(direct bool, offset int64) *sqlLimit {
	if offset >= 0 {
		if !direct && s.way.cfg.MaxOffset > 0 && offset > s.way.cfg.MaxOffset {
			offset = s.way.cfg.MaxOffset
		}
		s.offset = &offset
	}
	return s
}

func (s *sqlLimit) pageValue(direct bool, page int64, pageSize ...int64) *sqlLimit {
	if page <= 0 {
		return s
	}
	limit := s.way.cfg.DefaultPageSize
	if s.limit != nil {
		limit = *s.limit
	}
	for i := len(pageSize) - 1; i >= 0; i-- {
		if pageSize[i] > 0 {
			limit = pageSize[i]
			break
		}
	}
	offset := (page - 1) * limit
	return s.limitValue(direct, limit).offsetValue(direct, offset)
}

func (s *sqlLimit) Limit(limit int64) SQLLimit {
	return s.limitValue(false, limit)
}

func (s *sqlLimit) Offset(offset int64) SQLLimit {
	return s.offsetValue(false, offset)
}

func (s *sqlLimit) Page(page int64, pageSize ...int64) SQLLimit {
	return s.pageValue(false, page, pageSize...)
}

func (s *sqlLimit) DirectLimit(limit int64) SQLLimit {
	return s.limitValue(true, limit)
}

func (s *sqlLimit) DirectOffset(offset int64) SQLLimit {
	return s.offsetValue(true, offset)
}

func (s *sqlLimit) DirectPage(page int64, pageSize ...int64) SQLLimit {
	return s.pageValue(true, page, pageSize...)
}

// offsetRowsFetchNextRowsOnly Implement `OFFSET m ROWS FETCH NEXT n ROWS ONLY` SQL statements.
type offsetRowsFetchNextRowsOnly struct {
	*sqlLimit
}

func NewOffsetRowsFetchNextRowsOnly(way *Way) SQLLimit {
	return &offsetRowsFetchNextRowsOnly{
		sqlLimit: newSQLLimit(way).(*sqlLimit),
	}
}

func (s *offsetRowsFetchNextRowsOnly) ToSQL() *SQL {
	if s.limit == nil || *s.limit <= 0 {
		return NewEmptySQL()
	}
	limit := *s.limit
	makers := make([]any, 0, 1<<3)
	offset := int64(0)
	if s.offset != nil && *s.offset >= 0 {
		offset = *s.offset
	}
	makers = append(
		makers,
		AnyToSQL(cst.OFFSET),
		AnyToSQL(offset),
		AnyToSQL(cst.ROWS),
		AnyToSQL(cst.FETCH),
		AnyToSQL(cst.NEXT),
		AnyToSQL(limit),
		AnyToSQL(cst.ROWS),
		AnyToSQL(cst.ONLY),
	)
	return JoinSQLSpace(makers...)
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

	// GetSubquery Get subquery value.
	GetSubquery() Maker

	// SetSubquery The inserted data is a subquery.
	SetSubquery(subquery Maker) SQLValues

	// GetValues Get values value.
	GetValues() [][]any

	// SetValues The inserted data of VALUES.
	SetValues(values ...[]any) SQLValues

	// ValuesToSQL Values to *SQL.
	ValuesToSQL(values [][]any) *SQL
}

type sqlValues struct {
	subquery Maker

	values [][]any
}

func newSQLValues(way *Way) SQLValues {
	if way == nil {
		panic(pin)
	}
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

func (s *sqlValues) ToSQL() *SQL {
	if s.subquery != nil {
		return s.subquery.ToSQL()
	}
	return s.ValuesToSQL(s.values)
}

func (s *sqlValues) GetSubquery() Maker {
	return s.subquery
}

func (s *sqlValues) SetSubquery(subquery Maker) SQLValues {
	if subquery == nil {
		return s
	}
	if script := subquery.ToSQL(); script == nil || script.IsEmpty() {
		return s
	}
	s.subquery = subquery
	return s
}

func (s *sqlValues) GetValues() [][]any {
	return s.values
}

func (s *sqlValues) SetValues(values ...[]any) SQLValues {
	s.values = values
	return s
}

func (s *sqlValues) ValuesToSQL(values [][]any) *SQL {
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
		line[i] = cst.Placeholder
	}
	value := ParcelPrepare(strings.Join(line, cst.CommaSpace))
	rows := make([]string, count)
	for i := range count {
		script.Args = append(script.Args, values[i]...)
		rows[i] = value
	}
	script.Prepare = strings.Join(rows, cst.CommaSpace)
	return script
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

	// LastInsertId The driver returns the id value of the inserted data.
	LastInsertId() func(ctx context.Context, stmt *Stmt, args ...any) (lastInsertId int64, err error)

	// QueryRowScan Return values from QueryRow scan for inserted data.
	QueryRowScan(dest ...any) func(ctx context.Context, stmt *Stmt, args ...any) (int64, error)

	// RowsAffected Returns the number of rows affected directly.
	RowsAffected() func(ctx context.Context, stmt *Stmt, args ...any) (rowsAffected int64, err error)

	// GetExecute Get execute function value.
	GetExecute() func(ctx context.Context, stmt *Stmt, args ...any) (id int64, err error)

	// SetExecute When constructing a SQL statement that inserts a row of data and returns the id,
	// get the id value of the inserted row (this may vary depending on the database driver)
	SetExecute(execute func(ctx context.Context, stmt *Stmt, args ...any) (id int64, err error)) SQLReturning

	// GetInsert Get insert Maker.
	GetInsert() Maker

	// SetInsert Set insert Maker.
	SetInsert(script Maker) SQLReturning
}

type sqlReturning struct {
	way *Way

	insert Maker

	prepare func(tmp *SQL)

	execute func(ctx context.Context, stmt *Stmt, args ...any) (id int64, err error)
}

func newSQLReturning(way *Way, insert Maker) SQLReturning {
	if way == nil {
		panic(pin)
	}
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
	columns = ArrayDiscard(columns, func(k int, v string) bool { return strings.TrimSpace(v) == cst.Empty })
	if len(columns) == 0 {
		columns = []string{cst.Asterisk}
	}
	return s.Prepare(func(tmp *SQL) {
		tmp.Prepare = JoinSQLSpace(tmp.Prepare, cst.RETURNING, JoinSQLCommaSpace(AnyAny(columns)...)).Prepare
	})
}

// ToSQL Make SQL.
func (s *sqlReturning) ToSQL() *SQL {
	if s.insert == nil {
		return NewEmptySQL()
	}
	script := s.insert.ToSQL()
	if script == nil || script.IsEmpty() {
		return NewEmptySQL()
	}
	result := script.Clone()
	if prepare := s.prepare; prepare != nil {
		prepare(result)
	}
	return result
}

// LastInsertId The driver returns the id value of the inserted data.
func (s *sqlReturning) LastInsertId() func(ctx context.Context, stmt *Stmt, args ...any) (lastInsertId int64, err error) {
	return func(ctx context.Context, stmt *Stmt, args ...any) (int64, error) {
		result, err := stmt.Exec(ctx, args...)
		if err != nil {
			return 0, err
		}
		return result.LastInsertId()
	}
}

// QueryRowScan Return values from QueryRow scan for inserted data.
func (s *sqlReturning) QueryRowScan(dest ...any) func(ctx context.Context, stmt *Stmt, args ...any) (int64, error) {
	return func(ctx context.Context, stmt *Stmt, args ...any) (id int64, err error) {
		err = stmt.QueryRow(ctx, func(row *sql.Row) error {
			if len(dest) > 0 {
				return row.Scan(dest...)
			}
			return row.Scan(&id)
		}, args...)
		return
	}
}

// RowsAffected Returns the number of rows affected directly.
func (s *sqlReturning) RowsAffected() func(ctx context.Context, stmt *Stmt, args ...any) (rowsAffected int64, err error) {
	return func(ctx context.Context, stmt *Stmt, args ...any) (int64, error) {
		return stmt.Execute(ctx, args...)
	}
}

func (s *sqlReturning) GetExecute() func(ctx context.Context, stmt *Stmt, args ...any) (id int64, err error) {
	return s.execute
}

// SetExecute Customize the method to return the sequence value of inserted data.
func (s *sqlReturning) SetExecute(execute func(ctx context.Context, stmt *Stmt, args ...any) (id int64, err error)) SQLReturning {
	s.execute = execute
	return s
}

func (s *sqlReturning) GetInsert() Maker {
	return s.insert
}

func (s *sqlReturning) SetInsert(script Maker) SQLReturning {
	s.insert = script
	return s
}

// SQLUpdateSet Build UPDATE-SET statements.
type SQLUpdateSet interface {
	Maker

	ToEmpty

	IsEmpty() bool

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

	// Update Parse the given update data and assign the update value.
	Update(update any) SQLUpdateSet

	// Compare Compare struct assignment update.
	Compare(old, new any, except ...string) SQLUpdateSet

	// Default Set the default columns that need to be updated, such as update timestamp.
	Default(column string, value any) SQLUpdateSet

	// Remove Delete a column-value.
	Remove(columns ...string) SQLUpdateSet

	// Assign Assigning values through other column, null, empty string, subquery ...
	Assign(dst string, src string) SQLUpdateSet

	// GetUpdate Get a list of existing updates.
	GetUpdate() ([]string, [][]any)

	// SetUpdate Delete the existing update list and set the update list.
	SetUpdate(updates []string, params [][]any) SQLUpdateSet
}

type sqlUpdateSet struct {
	forbidSet map[string]*struct{}

	exists map[string]string // Existing update column, map[column-name]column-update-expression

	onlyAllow map[string]*struct{} // Set columns that only allow updates.

	updateMap map[string]int

	way *Way

	defaults *sqlUpdateSet

	updateExpr []string

	updateArgs [][]any
}

func (s *sqlUpdateSet) init() {
	s.forbidSet = make(map[string]*struct{}, 1<<3)
	s.exists = make(map[string]string, 1<<3)
	s.updateMap = make(map[string]int, 1<<3)
	s.updateExpr = make([]string, 0, 1<<3)
	s.updateArgs = make([][]any, 0, 1<<3)
}

func (s *sqlUpdateSet) toEmpty() {
	s.forbidSet = make(map[string]*struct{}, 1<<3)
	s.exists = make(map[string]string, 1<<3)
	s.onlyAllow = nil
	s.updateMap = make(map[string]int, 1<<3)
	s.updateExpr = make([]string, 0, 1<<3)
	s.updateArgs = make([][]any, 0, 1<<3)
}

func newSQLUpdateSet(way *Way) SQLUpdateSet {
	if way == nil {
		panic(pin)
	}
	result := &sqlUpdateSet{
		way: way,
	}
	defaults := &sqlUpdateSet{
		way: way,
	}
	result.init()
	defaults.init()
	result.defaults = defaults
	forbid := way.cfg.UpdateForbidColumn
	if len(forbid) > 0 {
		result.Forbid(forbid...)
	}
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

	script.Prepare = strings.Join(updates, cst.CommaSpace)
	for _, tmp := range params {
		script.Args = append(script.Args, tmp...)
	}
	return script
}

func (s *sqlUpdateSet) beautifyExpr(update string) string {
	update = strings.TrimSpace(update)
	for strings.Contains(update, "  ") {
		update = strings.ReplaceAll(update, "  ", cst.Space)
	}
	return update
}

func (s *sqlUpdateSet) exprArgs(value *SQL) SQLUpdateSet {
	if value == nil || value.IsEmpty() {
		return s
	}
	update := s.beautifyExpr(value.Prepare)
	if update == cst.Empty {
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
		if column != cst.Empty {
			onlyAllow[column] = nil
		}
	}
	length := len(onlyAllow)
	if length == 0 {
		return s
	}
	if s.onlyAllow == nil {
		s.onlyAllow = make(map[string]*struct{}, length)
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
	if _, ok := s.exists[column]; ok {
		return s
	}
	s.exists[column] = script.Prepare
	return s.exprArgs(script)
}

func (s *sqlUpdateSet) Set(column string, value any) SQLUpdateSet {
	if _, ok := s.forbidSet[column]; ok {
		return s
	}
	if value == nil {
		script := JoinSQLSpace(s.way.Replace(column), cst.Equal, cst.Placeholder)
		script.Args = append(script.Args, nil)
		return s.columnUpdate(column, script)
	}
	values := make([]any, 0, 1)
	update := make([]any, 0, 3)
	update = append(update, s.way.Replace(column), cst.Equal)
	switch tmp := value.(type) {
	case *SQL:
		update = append(update, ParcelSQL(tmp))
	case Maker:
		update = append(update, ParcelSQL(tmp.ToSQL()))
	default:
		update = append(update, cst.Placeholder)
		values = append(values, value)
	}
	script := JoinSQLSpace(update...)
	if len(values) > 0 {
		script.Args = append(script.Args, values...)
	}
	return s.columnUpdate(column, script)
}

func (s *sqlUpdateSet) Decr(column string, decrement any) SQLUpdateSet {
	if _, ok := s.forbidSet[column]; ok {
		return s
	}
	replace := s.way.Replace(column)
	script := NewSQL(fmt.Sprintf("%s %s %s %s %s", replace, cst.Equal, replace, cst.Minus, cst.Placeholder), decrement)
	return s.columnUpdate(column, script)
}

func (s *sqlUpdateSet) Incr(column string, increment any) SQLUpdateSet {
	if _, ok := s.forbidSet[column]; ok {
		return s
	}
	replace := s.way.Replace(column)
	script := NewSQL(fmt.Sprintf("%s %s %s %s %s", replace, cst.Equal, replace, cst.Plus, cst.Placeholder), increment)
	return s.columnUpdate(column, script)
}

// batchSet SET column = value by slice, require len(columns) == len(values).
func (s *sqlUpdateSet) batchSet(columns []string, values []any) SQLUpdateSet {
	if len(columns) != len(values) {
		return s
	}
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
	if tmp, ok := update.(*SQL); ok {
		return s.exprArgs(tmp)
	}
	if tmp, ok := update.(Maker); ok {
		return s.exprArgs(tmp.ToSQL())
	}
	columns, values := ObjectModify(update, s.way.cfg.ScanTag)
	return s.batchSet(columns, values)
}

// Compare For compare old and new to automatically calculate the need to update columns.
func (s *sqlUpdateSet) Compare(old, new any, except ...string) SQLUpdateSet {
	columns, values := StructUpdate(old, new, s.way.cfg.ScanTag, except...)
	return s.batchSet(columns, values)
}

// Default Set the default columns that need to be updated, such as update timestamp.
func (s *sqlUpdateSet) Default(column string, value any) SQLUpdateSet {
	column = strings.TrimSpace(column)
	if column == cst.Empty {
		return s
	}
	if _, ok := s.forbidSet[column]; ok {
		return s
	}
	script := NewSQL(fmt.Sprintf("%s = %s", s.way.Replace(column), cst.Placeholder), value)
	if _, ok := s.updateMap[script.Prepare]; ok {
		return s
	}
	s.defaults.columnUpdate(column, script)
	return s
}

// Remove Delete a column-value.
func (s *sqlUpdateSet) Remove(columns ...string) SQLUpdateSet {
	removes := make(map[string]*struct{})
	for _, column := range columns {
		if value, ok := s.exists[column]; ok {
			removes[value] = nil
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
	updateMap := MapDiscard(s.updateMap, func(k string, v int) bool {
		_, ok := dropExpr[k]
		return ok
	})
	updateExists := MapDiscard(s.exists, func(k string, v string) bool {
		_, ok := dropExpr[v]
		return ok
	})
	s.updateExpr, s.updateArgs, s.updateMap, s.exists = updateExpr, updateArgs, updateMap, updateExists
	if s.defaults != nil {
		s.defaults.Remove(columns...)
	}
	return s
}

// Assign Assigning values through other column, null, empty string, subquery ...
func (s *sqlUpdateSet) Assign(dst string, src string) SQLUpdateSet {
	if _, ok := s.forbidSet[dst]; ok {
		return s
	}
	scripts := make([]any, 0, 3)
	scripts = append(scripts, s.way.Replace(dst), cst.Equal)
	if src == cst.Empty {
		src = SQLString(src)
	}
	scripts = append(scripts, AnyToSQL(src))
	return s.columnUpdate(dst, JoinSQLSpace(scripts...))
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

func newSQLOnConflictUpdateSet(way *Way) SQLOnConflictUpdateSet {
	if way == nil {
		panic(pin)
	}
	tmp := &sqlOnConflictUpdateSet{
		way: way,
	}
	tmp.SQLUpdateSet = way.cfg.NewSQLUpdateSet(way)
	return tmp
}

func (s *sqlOnConflictUpdateSet) Excluded(columns ...string) SQLOnConflictUpdateSet {
	for _, column := range columns {
		tmp := s.way.Replace(column)
		s.Update(NewSQL(JoinString(tmp, cst.Space, cst.Equal, cst.Space, cst.EXCLUDED, cst.Point, tmp)))
	}
	return s
}

// SQLOnConflict Implement the following SQL statement:
// INSERT INTO ... ON CONFLICT (column_a[, column_b, column_c...]) DO NOTHING /* If a conflict occurs, the insert operation is ignored. */
// INSERT INTO ... ON CONFLICT (column_a[, column_b, column_c...]) DO UPDATE SET column1 = EXCLUDED.column1, column2 = EXCLUDED.column2, column3 = EXCLUDED.column3, column4 = 'fixed value' ... /* If a conflict occurs, the existing row is updated with the new value */
type SQLOnConflict interface {
	Maker

	ToEmpty

	// GetOnConflict Get ON CONFLICT columns.
	GetOnConflict() []string

	// SetOnConflict The column causing the conflict, such as a unique key or primary key, which can be a single column or multiple columns.
	SetOnConflict(onConflicts ...string) SQLOnConflict

	// Do The SQL statement that needs to be executed when a data conflict occurs. By default, nothing is done.
	Do(maker Maker) SQLOnConflict

	// DoUpdateSet SQL update statements executed when data conflicts occur.
	DoUpdateSet(fx func(u SQLOnConflictUpdateSet)) SQLOnConflict

	// GetInsert Get insert Maker.
	GetInsert() Maker

	// SetInsert Set insert Maker.
	SetInsert(script Maker) SQLOnConflict
}

type sqlOnConflict struct {
	onConflictsDoUpdateSet SQLOnConflictUpdateSet

	way *Way

	insert Maker

	onConflictsDo Maker

	onConflicts []string
}

func newSQLOnConflict(way *Way, insert Maker) SQLOnConflict {
	if way == nil {
		panic(pin)
	}
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

func (s *sqlOnConflict) SetOnConflict(onConflicts ...string) SQLOnConflict {
	s.onConflicts = onConflicts
	return s
}

func (s *sqlOnConflict) GetOnConflict() []string {
	return s.onConflicts
}

func (s *sqlOnConflict) Do(maker Maker) SQLOnConflict {
	s.onConflictsDo = maker
	return s
}

func (s *sqlOnConflict) DoUpdateSet(fx func(u SQLOnConflictUpdateSet)) SQLOnConflict {
	tmp := s.onConflictsDoUpdateSet
	if tmp == nil {
		s.onConflictsDoUpdateSet = s.way.cfg.NewSQLOnConflictUpdateSet(s.way)
		tmp = s.onConflictsDoUpdateSet
	}
	fx(tmp)
	return s
}

func (s *sqlOnConflict) ToSQL() *SQL {
	script := NewSQL(cst.Empty)
	if s.insert == nil || s.insert.ToSQL().IsEmpty() || len(s.onConflicts) == 0 {
		return script
	}
	insert := s.insert.ToSQL()
	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)
	b.WriteString(insert.Prepare)
	script.Args = append(script.Args, insert.Args...)
	b.WriteString(JoinString(cst.Space, cst.ON, cst.Space, cst.CONFLICT, cst.Space))
	b.WriteString(ParcelPrepare(strings.Join(s.way.ReplaceAll(s.onConflicts), cst.CommaSpace)))
	b.WriteString(cst.Space)
	b.WriteString(cst.DO)
	b.WriteString(cst.Space)
	prepare, args := cst.NOTHING, make([]any, 0)
	if onConflictsDo := s.onConflictsDo; onConflictsDo != nil {
		if tmp := onConflictsDo.ToSQL(); tmp != nil && !tmp.IsEmpty() {
			prepare, args = tmp.Prepare, tmp.Args[:]
		}
	}
	if prepare == cst.NOTHING && s.onConflictsDoUpdateSet != nil && s.onConflictsDoUpdateSet.Len() > 0 {
		update := s.onConflictsDoUpdateSet.ToSQL()
		if update != nil && !update.IsEmpty() {
			b1 := poolGetStringBuilder()
			defer poolPutStringBuilder(b1)
			b.WriteString(JoinString(cst.UPDATE, cst.Space, cst.SET, cst.Space))
			b1.WriteString(update.Prepare)
			prepare, args = b1.String(), update.Args[:]
		}
	}
	b.WriteString(prepare)
	script.Args = append(script.Args, args...)
	script.Prepare = b.String()
	return script
}

func (s *sqlOnConflict) GetInsert() Maker {
	return s.insert
}

func (s *sqlOnConflict) SetInsert(script Maker) SQLOnConflict {
	s.insert = script
	return s
}

// SQLInsert Build INSERT statements.
type SQLInsert interface {
	Maker

	ToEmpty

	// Table Insert data into the target table.
	Table(table Maker) SQLInsert

	// TableIsValid Report whether the current table name is valid.
	TableIsValid() bool

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

	// SetSubquery Use the query result as the values of the insert statement.
	SetSubquery(subquery Maker) SQLInsert

	// ColumnValue Set a single column and value.
	ColumnValue(column string, value any) SQLInsert

	// ReturningId Insert a single record and get the id of the inserted data.
	ReturningId() SQLInsert

	// Create Parses the given insert data and sets the insert data.
	Create(create any) SQLInsert

	// Default Set the default column for inserted data, such as the creation timestamp.
	Default(column string, value any) SQLInsert

	// Remove Delete a column-value.
	Remove(columns ...string) SQLInsert

	// GetReturning Get returning value.
	GetReturning() SQLReturning

	// Returning Insert a piece of data and get the auto-increment value.
	Returning(fx func(r SQLReturning)) SQLInsert

	// GetColumn Get the list of inserted columns that have been set.
	GetColumn(excludes ...string) []string

	// OnConflict When inserting data, set the execution logic when there is a conflict.
	OnConflict(fx func(o SQLOnConflict)) SQLInsert
}

type sqlInsert struct {
	forbidSet map[string]*struct{}

	way *Way

	table *SQL

	onlyAllow map[string]*struct{} // Set the columns to allow inserts only.

	columns SQLSelect

	values SQLValues

	returning SQLReturning

	onConflict SQLOnConflict

	defaults *sqlInsert
}

func (s *sqlInsert) init() {
	s.forbidSet = make(map[string]*struct{}, 1<<3)
	s.table = NewEmptySQL()
	s.columns = s.way.cfg.NewSQLSelect(s.way)
	s.values = s.way.cfg.NewSQLValues(s.way)
	s.returning = s.way.cfg.NewSQLReturning(s.way, NewEmptySQL())
	s.onConflict = s.way.cfg.NewSQLOnConflict(s.way, NewEmptySQL())
}

func (s *sqlInsert) toEmpty() {
	s.forbidSet = make(map[string]*struct{}, 1<<3)
	s.onlyAllow = nil
	s.columns.ToEmpty()
	s.values.ToEmpty()
	s.returning.ToEmpty()
	s.onConflict.ToEmpty()
}

func newSQLInsert(way *Way) SQLInsert {
	if way == nil {
		panic(pin)
	}
	result := &sqlInsert{
		way: way,
	}
	defaults := &sqlInsert{
		way: way,
	}
	result.init()
	defaults.init()
	result.defaults = defaults
	forbid := way.cfg.InsertForbidColumn
	if len(forbid) > 0 {
		result.Forbid(forbid...)
	}
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
	makers := []any{NewSQL(cst.INSERT), NewSQL(cst.INTO), s.table}

	columns1, params1 := s.columns.Get()
	values := s.values.GetValues()
	values1 := make([][]any, len(values))
	copy(values1, values)

	columns, values := make([]string, len(columns1)), make([][]any, len(values1))
	copy(columns, columns1)
	copy(values, values1)
	params := make(map[int][]any, len(params1))
	maps.Copy(params, params1)
	if len(columns) > 0 {
		defaultsValues := s.defaults.values.GetValues()
		if len(values) > 0 && len(defaultsValues) == 1 {
			// add default columns and values.
			defaultColumns, defaultParams := s.defaults.columns.Get()
			defaultValuesSlice := defaultsValues[0]
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
		makers = append(makers, NewSQL(ParcelPrepare(strings.Join(s.way.ReplaceAll(columns), cst.CommaSpace))))
	}

	ok := false

	subquery := s.values.GetSubquery()
	if subquery != nil {
		if script := subquery.ToSQL(); !script.IsEmpty() {
			makers = append(makers, script)
			ok = true
		}
	}

	if !ok {
		if len(values) > 0 {
			makers = append(makers, NewSQL(cst.VALUES))
			makers = append(makers, s.values.ValuesToSQL(values))
			ok = true
		}
	}

	if !ok {
		return NewEmptySQL()
	}

	if s.returning != nil {
		execute := s.returning.GetExecute()
		if execute != nil {
			s.returning.SetInsert(JoinSQLSpace(makers...))
			if script := s.returning.ToSQL(); !script.IsEmpty() {
				return script
			}
		}
	}

	if s.onConflict != nil {
		onConflicts := s.onConflict.GetOnConflict()
		if len(onConflicts) > 0 {
			s.onConflict.SetInsert(JoinSQLSpace(makers...))
			if script := s.onConflict.ToSQL(); !script.IsEmpty() {
				return script
			}
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

func (s *sqlInsert) TableIsValid() bool {
	if s.table == nil || s.table.IsEmpty() {
		return false
	}
	return true
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
	s.values.SetValues(values...)
	return s
}

func (s *sqlInsert) SetSubquery(subquery Maker) SQLInsert {
	s.values.SetSubquery(subquery)
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
	s.columns.Add(NewSQL(column))
	values := s.values.GetValues()
	for index := range values {
		values[index] = append(values[index], value)
	}
	s.values.SetValues(values...)
	return s
}

func (s *sqlInsert) ReturningId() SQLInsert {
	subquery := s.values.GetSubquery()
	if subquery != nil {
		return s
	}
	length := len(s.values.GetValues())
	if length != 1 {
		return s
	}
	s.Returning(s.way.cfg.Manual.InsertOneReturningId)
	return s
}

// Create value of creation should be one of struct{}, *struct{}, map[string]any, []struct, []*struct{}, *[]struct{}, *[]*struct{}.
func (s *sqlInsert) Create(create any) SQLInsert {
	forbid := MapToArray(s.forbidSet, func(k string, v *struct{}) string { return k })
	onlyAllow := MapToArray(s.onlyAllow, func(k string, v *struct{}) string { return k })
	columns, values, category := ObjectInsert(create, s.way.cfg.ScanTag, forbid, onlyAllow)
	if category == CategoryInsertOne {
		defer s.ReturningId()
	}
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
			return !ok
		})
		for index, value := range values {
			values[index] = ArrayDiscard(value, func(k int, v any) bool {
				_, ok := indexes[k]
				return !ok
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
	values := s.values.GetValues()
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
		values[index] = ArrayDiscard(value, func(k int, v any) bool {
			_, ok = assoc[k]
			return ok
		})
	}
	s.values.SetValues(values...)
	if s.defaults != nil {
		s.defaults.Remove(columns...)
	}
	return s
}

func (s *sqlInsert) GetReturning() SQLReturning {
	return s.returning
}

func (s *sqlInsert) Returning(fx func(r SQLReturning)) SQLInsert {
	if fx == nil {
		return s
	}
	if s.returning == nil {
		s.returning = s.way.cfg.NewSQLReturning(s.way, NewEmptySQL())
	}
	fx(s.returning)
	return s
}

func (s *sqlInsert) GetColumn(excludes ...string) []string {
	lengths := len(excludes)
	columns, _ := s.columns.Get()
	if lengths == 0 {
		return columns
	}
	discard := ArrayToMap(excludes, func(v string) (string, *struct{}) { return v, nil })
	columns = ArrayDiscard(columns, func(k int, v string) bool {
		_, ok := discard[v]
		return ok
	})
	return columns
}

func (s *sqlInsert) OnConflict(fx func(o SQLOnConflict)) SQLInsert {
	fx(s.onConflict)
	return s
}

// SQLString Convert a go string to a SQL string.
func SQLString(value string) string {
	return JoinString(cst.SingleQuotationMark, value, cst.SingleQuotationMark)
}

func nullAnyToSQL(i any) *SQL {
	if i == nil {
		return AnyToSQL(cst.NULL)
	}
	return AnyToSQL(i)
}

/* CASE [xxx] WHEN x THEN X [WHEN xx THEN XX] [ELSE xxx] END [AS xxx] */

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
	if way == nil {
		panic(pin)
	}
	return &sqlCase{
		way: way,
	}
}

func (s *Way) Case() SQLCase {
	return s.cfg.NewSQLCase(s)
}

func (s *sqlCase) ToSQL() *SQL {
	script := NewSQL(cst.Empty)
	if len(s.whenThen) == 0 {
		return script
	}

	whenThen := JoinSQLSpace(AnyAny(s.whenThen)...).ToSQL()
	if whenThen.IsEmpty() {
		return script
	}

	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)
	b.WriteString(cst.CASE)
	if tmp := s.sqlCase; tmp != nil && !tmp.IsEmpty() {
		b.WriteString(cst.Space)
		b.WriteString(tmp.Prepare)
		script.Args = append(script.Args, tmp.Args...)
	}
	b.WriteString(cst.Space)
	b.WriteString(whenThen.Prepare)
	script.Args = append(script.Args, whenThen.Args...)
	if tmp := s.sqlElse; tmp != nil && !tmp.IsEmpty() {
		b.WriteString(cst.Space)
		b.WriteString(cst.ELSE)
		b.WriteString(cst.Space)
		b.WriteString(tmp.Prepare)
		script.Args = append(script.Args, tmp.Args...)
	}
	b.WriteString(cst.Space)
	b.WriteString(cst.END)
	script.Prepare = b.String()
	return newSqlAlias(script).w(s.way).SetAlias(s.alias).ToSQL()
}

func (s *sqlCase) Alias(alias string) SQLCase {
	s.alias = alias
	return s
}

func handleCaseEmptyString(script *SQL) *SQL {
	if script.Prepare == cst.Empty {
		script.Prepare, script.Args = SQLString(cst.Empty), nil
	}
	return script
}

func (s *sqlCase) Case(value any) SQLCase {
	s.sqlCase = handleCaseEmptyString(nullAnyToSQL(value))
	return s
}

func (s *sqlCase) WhenThen(when, then any) SQLCase {
	s.whenThen = append(
		s.whenThen,
		JoinSQLSpace(
			cst.WHEN,
			handleCaseEmptyString(nullAnyToSQL(when)),
			cst.THEN,
			handleCaseEmptyString(nullAnyToSQL(then)),
		),
	)
	return s
}

func (s *sqlCase) Else(value any) SQLCase {
	s.sqlElse = handleCaseEmptyString(nullAnyToSQL(value))
	return s
}
