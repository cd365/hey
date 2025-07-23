package hey

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// SQLTable Used to construct expressions that can use table aliases and their corresponding parameter lists.
type SQLTable interface {
	IsEmpty

	Cmder

	// Alias Setting aliases for script statements.
	Alias(alias string) SQLTable

	// GetAlias Getting aliases for script statements.
	GetAlias() string
}

type sqlTable struct {
	cmder Cmder
	alias string
}

func (s *sqlTable) IsEmpty() bool {
	return IsEmptyCmder(s.cmder)
}

func (s *sqlTable) Cmd() (prepare string, args []any) {
	if s.IsEmpty() {
		return
	}
	prepare, args = s.cmder.Cmd()
	if s.alias != EmptyString {
		prepare = SqlAlias(prepare, s.alias)
	}
	return
}

func (s *sqlTable) Alias(alias string) SQLTable {
	// Allow setting empty values.
	s.alias = alias
	return s
}

func (s *sqlTable) GetAlias() string {
	return s.alias
}

func NewSQLTable(prepare string, args []any) SQLTable {
	return &sqlTable{
		cmder: NewCmder(prepare, args),
	}
}

func NewSQLTableGet(alias string, get *Get) SQLTable {
	return NewSQLTable(ParcelCmder(get).Cmd()).Alias(alias)
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
	IsEmpty

	Cmder

	// Recursive Recursion or cancellation of recursion.
	Recursive() SQLWith

	// Set Setting common table expression.
	Set(alias string, cmder Cmder, columns ...string) SQLWith

	// Del Removing common table expression.
	Del(alias string) SQLWith
}

type sqlWith struct {
	recursive bool
	alias     []string
	column    map[string][]string
	prepare   map[string]Cmder
}

func NewSQLWith() SQLWith {
	return &sqlWith{
		alias:   make([]string, 0, 2),
		column:  make(map[string][]string, 2),
		prepare: make(map[string]Cmder, 2),
	}
}

func (s *sqlWith) IsEmpty() bool {
	return len(s.alias) == 0
}

func (s *sqlWith) Cmd() (prepare string, args []any) {
	if s.IsEmpty() {
		return
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(SqlWith)
	b.WriteString(SqlSpace)
	if s.recursive {
		b.WriteString("RECURSIVE")
		b.WriteString(SqlSpace)
	}
	var param []any
	for index, alias := range s.alias {
		if index > 0 {
			b.WriteString(SqlConcat)
		}
		script := s.prepare[alias]
		b.WriteString(alias)
		b.WriteString(SqlSpace)
		if columns := s.column[alias]; len(columns) > 0 {
			// Displays the column alias that defines the CTE, overwriting the original column name of the query result.
			b.WriteString(SqlLeftSmallBracket)
			b.WriteString(SqlSpace)
			b.WriteString(strings.Join(columns, SqlConcat))
			b.WriteString(SqlSpace)
			b.WriteString(SqlRightSmallBracket)
			b.WriteString(SqlSpace)
		}
		b.WriteString(ConcatString(SqlAs, SqlSpace, SqlLeftSmallBracket, SqlSpace))
		prepare, param = script.Cmd()
		b.WriteString(prepare)
		b.WriteString(ConcatString(SqlSpace, SqlRightSmallBracket))
		args = append(args, param...)
	}
	prepare = b.String()
	return
}

func (s *sqlWith) Recursive() SQLWith {
	s.recursive = !s.recursive
	return s
}

func (s *sqlWith) Set(alias string, cmder Cmder, columns ...string) SQLWith {
	if alias == EmptyString || IsEmptyCmder(cmder) {
		return s
	}
	if _, ok := s.prepare[alias]; !ok {
		s.alias = append(s.alias, alias)
	}
	s.column[alias] = columns
	s.prepare[alias] = cmder
	return s
}

func (s *sqlWith) Del(alias string) SQLWith {
	if alias == EmptyString {
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

// SQLSelect Used to build the list of columns to be queried.
type SQLSelect interface {
	IsEmpty

	Cmder

	Index(column string) int

	Exists(column string) bool

	Add(column string, args ...any) SQLSelect

	AddAll(columns ...string) SQLSelect

	AddCmder(cmder Cmder) SQLSelect

	DelAll(columns ...string) SQLSelect

	Len() int

	Get() ([]string, map[int][]any)

	Set(columns []string, columnsArgs map[int][]any) SQLSelect

	Use(sqlSelect ...SQLSelect) SQLSelect

	// Queried Get all columns of the query results.
	Queried(excepts ...string) []string
}

type sqlSelect struct {
	columns     []string
	columnsMap  map[string]int
	columnsArgs map[int][]any

	way *Way
}

func NewSQLSelect(way *Way) SQLSelect {
	return &sqlSelect{
		columns:     make([]string, 0, 16),
		columnsMap:  make(map[string]int, 16),
		columnsArgs: make(map[int][]any),
		way:         way,
	}
}

func (s *sqlSelect) IsEmpty() bool {
	return len(s.columns) == 0
}

func (s *sqlSelect) Cmd() (prepare string, args []any) {
	length := len(s.columns)
	if length == 0 {
		return SqlStar, nil
	}
	columns := make([]string, 0, length)
	for i := range length {
		tmpArgs, ok := s.columnsArgs[i]
		if !ok {
			continue
		}
		columns = append(columns, s.columns[i])
		if tmpArgs != nil {
			args = append(args, tmpArgs...)
		}
	}
	prepare = strings.Join(s.way.Replaces(columns), SqlConcat)
	return
}

func (s *sqlSelect) Index(column string) int {
	index, ok := s.columnsMap[column]
	if !ok {
		return -1
	}
	return index
}

func (s *sqlSelect) Exists(column string) bool {
	return s.Index(column) >= 0
}

func (s *sqlSelect) Add(column string, args ...any) SQLSelect {
	if column == EmptyString {
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

func (s *sqlSelect) AddAll(columns ...string) SQLSelect {
	index := len(s.columns)
	for _, column := range columns {
		if column == EmptyString {
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

func (s *sqlSelect) AddCmder(cmder Cmder) SQLSelect {
	if cmder != nil {
		if prepare, args := cmder.Cmd(); prepare == EmptyString {
			return s.Add(prepare, args...)
		}
	}
	return s
}

func (s *sqlSelect) DelAll(columns ...string) SQLSelect {
	if columns == nil {
		s.columns = make([]string, 0, 16)
		s.columnsMap = make(map[string]int, 16)
		s.columnsArgs = make(map[int][]any)
		return s
	}
	deleteIndex := make(map[int]*struct{}, len(columns))
	for _, column := range columns {
		if column == EmptyString {
			continue
		}
		index, ok := s.columnsMap[column]
		if !ok {
			continue
		}
		deleteIndex[index] = &struct{}{}
	}
	length := len(s.columns)
	result := make([]string, 0, length)
	for index, column := range s.columns {
		if _, ok := deleteIndex[index]; ok {
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

func (s *sqlSelect) Set(columns []string, columnsArgs map[int][]any) SQLSelect {
	columnsMap := make(map[string]int, len(columns))
	for i, column := range columns {
		columnsMap[column] = i
		if _, ok := columnsArgs[i]; !ok {
			columnsArgs[i] = nil
		}
	}
	s.columns, s.columnsMap, s.columnsArgs = columns, columnsMap, columnsArgs
	return s
}

func (s *sqlSelect) Use(sqlSelect ...SQLSelect) SQLSelect {
	length := len(sqlSelect)
	for i := range length {
		tmp := sqlSelect[i]
		if tmp == nil {
			continue
		}
		cols, args := tmp.Get()
		for index, value := range cols {
			s.Add(value, args[index]...)
		}
	}
	return s
}

// Queried Get all columns of the query results.
func (s *sqlSelect) Queried(excepts ...string) []string {
	star := []string{SqlStar}
	cols := s.columns[:]
	length := len(cols)
	if length == 0 {
		return star
	}
	lists := make([]string, 0, length)
	for i := range length {
		column := strings.TrimSpace(cols[i])
		index := strings.LastIndex(column, SqlSpace)
		if index >= 0 {
			column = column[index+1:]
			if strings.Contains(column, SqlRightSmallBracket) {
				return star
			}
			lists = append(lists, column)
		} else {
			column = strings.TrimSuffix(column, SqlPoint)
			index = strings.LastIndex(column, SqlPoint)
			if index >= 0 {
				lists = append(lists, column[index+1:])
			} else {
				lists = append(lists, column)
			}
		}
	}
	totals := len(excepts)
	if totals == 0 {
		return lists
	}
	result := make([]string, 0, length)
	exists := make(map[string]*struct{}, totals)
	for _, except := range excepts {
		except = strings.TrimSpace(except)
		except = strings.TrimSuffix(except, SqlPoint)
		if index := strings.LastIndex(except, SqlPoint); index >= 0 {
			except = except[index+1:]
		}
		exists[except] = &struct{}{}
	}
	for _, column := range lists {
		if _, ok := exists[column]; !ok {
			result = append(result, column)
		}
	}
	return result
}

type SQLJoinOn interface {
	Cmder

	// Equal Use equal value JOIN ON condition.
	Equal(leftAlias string, leftColumn string, rightAlias string, rightColumn string) SQLJoinOn

	// Filter Append custom conditions to the ON statement or use custom conditions on the ON statement to associate tables.
	Filter(fc func(f Filter)) SQLJoinOn

	// Using Use USING instead of ON.
	Using(using ...string) SQLJoinOn
}

type sqlJoinOn struct {
	way   *Way
	on    Filter
	using []string
}

func (s *sqlJoinOn) Equal(leftAlias string, leftColumn string, rightAlias string, rightColumn string) SQLJoinOn {
	s.on.And(ConcatString(SqlPrefix(leftAlias, leftColumn), SqlSpace, SqlEqual, SqlSpace, SqlPrefix(rightAlias, rightColumn)))
	return s
}

func (s *sqlJoinOn) Filter(fc func(f Filter)) SQLJoinOn {
	if fc != nil {
		fc(s.on)
	}
	return s
}

func (s *sqlJoinOn) Using(using ...string) SQLJoinOn {
	using = DiscardDuplicate(func(tmp string) bool { return tmp == EmptyString }, using...)
	if len(using) > 0 {
		s.using = using
	}
	return s
}

func (s *sqlJoinOn) Cmd() (prepare string, args []any) {
	// JOIN ON
	if s.on != nil && s.on.Num() > 0 {
		prepare, args = s.on.Cmd()
		prepare = ConcatString(SqlOn, SqlSpace, prepare)
		return
	}

	// JOIN USING
	if length := len(s.using); length > 0 {
		using := make([]string, 0, length)
		for _, column := range s.using {
			if column != EmptyString {
				using = append(using, column)
			}
		}
		if len(using) > 0 {
			using = s.way.Replaces(using)
			prepare = ConcatString(SqlUsing, SqlSpace, SqlLeftSmallBracket, SqlSpace, strings.Join(using, SqlConcat), SqlSpace, SqlRightSmallBracket)
			return
		}
	}

	return
}

func newSQLJoinOn(way *Way) SQLJoinOn {
	return &sqlJoinOn{
		way: way,
		on:  way.F(),
	}
}

// SQLJoinAssoc Constructing conditions for join queries.
type SQLJoinAssoc func(leftAlias string, rightAlias string) SQLJoinOn

type sqlJoinSchema struct {
	joinType   string
	rightTable SQLTable
	condition  Cmder
}

// SQLJoin Constructing multi-table join queries.
type SQLJoin interface {
	Cmder

	GetMaster() SQLTable

	SetMaster(master SQLTable) SQLJoin

	NewTable(table string, alias string, args ...any) SQLTable

	NewSubquery(subquery Cmder, alias string) SQLTable

	On(onList ...func(o SQLJoinOn, leftAlias string, rightAlias string)) SQLJoinAssoc

	Using(columns ...string) SQLJoinAssoc

	OnEqual(leftColumn string, rightColumn string) SQLJoinAssoc

	Join(joinTypeString string, leftTable SQLTable, rightTable SQLTable, on SQLJoinAssoc) SQLJoin

	InnerJoin(leftTable SQLTable, rightTable SQLTable, on SQLJoinAssoc) SQLJoin

	LeftJoin(leftTable SQLTable, rightTable SQLTable, on SQLJoinAssoc) SQLJoin

	RightJoin(leftTable SQLTable, rightTable SQLTable, on SQLJoinAssoc) SQLJoin

	// Queries Get query columns.
	Queries() SQLSelect

	// TableColumn Build *TableColumn based on SQLTable.
	TableColumn(table SQLTable) *TableColumn

	// TableSelect Add the queried column list based on the table's alias prefix.
	TableSelect(table SQLTable, columns ...string) []string

	// TableSelectAliases Add the queried column list based on the table's alias prefix, support setting the query column alias.
	TableSelectAliases(table SQLTable, aliases map[string]string, columns ...string) []string

	// SelectGroupsColumns Add the queried column list based on the table's alias prefix.
	SelectGroupsColumns(columns ...[]string) SQLJoin

	// SelectTableColumnAlias Batch set multiple columns of the specified table and set aliases for all columns.
	SelectTableColumnAlias(table SQLTable, columnAndColumnAlias ...string) SQLJoin
}

type sqlJoin struct {
	master    SQLTable
	joins     []*sqlJoinSchema
	sqlSelect SQLSelect

	way *Way
}

func NewSQLJoin(way *Way) SQLJoin {
	tmp := &sqlJoin{
		joins:     make([]*sqlJoinSchema, 0, 2),
		sqlSelect: NewSQLSelect(way),
		way:       way,
	}
	return tmp
}

func (s *sqlJoin) GetMaster() SQLTable {
	return s.master
}

func (s *sqlJoin) SetMaster(master SQLTable) SQLJoin {
	if master != nil && !master.IsEmpty() {
		s.master = master
	}
	return s
}

func (s *sqlJoin) Cmd() (prepare string, args []any) {
	columns, params := s.Queries().Cmd()
	if params != nil {
		args = append(args, params...)
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(ConcatString(SqlSelect, SqlSpace))
	b.WriteString(columns)
	b.WriteString(ConcatString(SqlSpace, SqlFrom, SqlSpace))
	prepare, params = s.master.Cmd()
	b.WriteString(prepare)
	b.WriteString(SqlSpace)
	args = append(args, params...)
	for index, tmp := range s.joins {
		if tmp == nil {
			continue
		}
		if index > 0 {
			b.WriteString(SqlSpace)
		}
		b.WriteString(tmp.joinType)
		prepare, params = tmp.rightTable.Cmd()
		b.WriteString(SqlSpace)
		b.WriteString(prepare)
		args = append(args, params...)
		if tmp.condition != nil {
			prepare, params = tmp.condition.Cmd()
			if prepare != EmptyString {
				b.WriteString(SqlSpace)
				b.WriteString(prepare)
				if params != nil {
					args = append(args, params...)
				}
			}
		}
	}
	prepare = b.String()
	return
}

func (s *sqlJoin) NewTable(table string, alias string, args ...any) SQLTable {
	return NewSQLTable(table, args).Alias(alias)
}

func (s *sqlJoin) NewSubquery(subquery Cmder, alias string) SQLTable {
	prepare, args := ParcelCmder(subquery).Cmd()
	return s.NewTable(prepare, alias, args...)
}

// On For `... JOIN ON ...`
func (s *sqlJoin) On(onList ...func(o SQLJoinOn, leftAlias string, rightAlias string)) SQLJoinAssoc {
	return func(leftAlias string, rightAlias string) SQLJoinOn {
		return newSQLJoinOn(s.way).Filter(func(o Filter) {
			for _, tmp := range onList {
				if tmp != nil {
					newAssoc := newSQLJoinOn(s.way)
					tmp(newAssoc, leftAlias, rightAlias)
					newAssoc.Filter(func(f Filter) { o.Use(f) })
				}
			}
		})
	}
}

// Using For `... JOIN USING ...`
func (s *sqlJoin) Using(columns ...string) SQLJoinAssoc {
	return func(leftAlias string, rightAlias string) SQLJoinOn {
		return newSQLJoinOn(s.way).Using(columns...)
	}
}

// OnEqual For `... JOIN ON ... = ... [...]`
func (s *sqlJoin) OnEqual(leftColumn string, rightColumn string) SQLJoinAssoc {
	if leftColumn == EmptyString || rightColumn == EmptyString {
		return nil
	}
	return func(leftAlias string, rightAlias string) SQLJoinOn {
		return newSQLJoinOn(s.way).Filter(func(f Filter) {
			f.CompareEqual(SqlPrefix(leftAlias, leftColumn), SqlPrefix(rightAlias, rightColumn))
		})
	}
}

func (s *sqlJoin) Join(joinTypeString string, leftTable SQLTable, rightTable SQLTable, on SQLJoinAssoc) SQLJoin {
	if joinTypeString == EmptyString {
		joinTypeString = SqlJoinInner
	}
	if leftTable == nil || leftTable.IsEmpty() {
		leftTable = s.master
	}
	if rightTable == nil || rightTable.IsEmpty() {
		return s
	}
	join := &sqlJoinSchema{
		joinType:   joinTypeString,
		rightTable: rightTable,
	}
	if on != nil {
		join.condition = on(leftTable.GetAlias(), rightTable.GetAlias())
	}
	s.joins = append(s.joins, join)
	return s
}

func (s *sqlJoin) InnerJoin(leftTable SQLTable, rightTable SQLTable, on SQLJoinAssoc) SQLJoin {
	return s.Join(SqlJoinInner, leftTable, rightTable, on)
}

func (s *sqlJoin) LeftJoin(leftTable SQLTable, rightTable SQLTable, on SQLJoinAssoc) SQLJoin {
	return s.Join(SqlJoinLeft, leftTable, rightTable, on)
}

func (s *sqlJoin) RightJoin(leftTable SQLTable, rightTable SQLTable, on SQLJoinAssoc) SQLJoin {
	return s.Join(SqlJoinRight, leftTable, rightTable, on)
}

func (s *sqlJoin) Queries() SQLSelect {
	return s.sqlSelect
}

func (s *sqlJoin) TableColumn(table SQLTable) *TableColumn {
	result := s.way.T()
	if table == nil {
		return result
	}
	if alias := table.GetAlias(); alias != EmptyString {
		result.SetAlias(alias)
	}
	return result
}

func (s *sqlJoin) selectTableColumnOptionalColumnAlias(table SQLTable, aliases map[string]string, columns ...string) []string {
	change := s.TableColumn(table)
	result := make([]string, len(columns))
	for index, column := range columns {
		alias, ok := aliases[column]
		if !ok || alias == EmptyString {
			alias = columns[index]
		}
		result[index] = change.Column(column, alias)
	}
	return result
}

func (s *sqlJoin) TableSelect(table SQLTable, columns ...string) []string {
	return s.selectTableColumnOptionalColumnAlias(table, nil, columns...)
}

func (s *sqlJoin) TableSelectAliases(table SQLTable, aliases map[string]string, columns ...string) []string {
	return s.selectTableColumnOptionalColumnAlias(table, aliases, columns...)
}

func (s *sqlJoin) SelectGroupsColumns(columns ...[]string) SQLJoin {
	groups := make([]string, 0, 32)
	for _, values := range columns {
		groups = append(groups, values...)
	}
	s.sqlSelect.AddAll(groups...)
	return s
}

func (s *sqlJoin) SelectTableColumnAlias(table SQLTable, columnAndColumnAlias ...string) SQLJoin {
	length := len(columnAndColumnAlias)
	if length == 0 || length&1 == 1 {
		return s
	}
	tmp := s.TableColumn(table)
	for i := 0; i < length; i += 2 {
		if columnAndColumnAlias[i] != EmptyString && columnAndColumnAlias[i+1] != EmptyString {
			s.sqlSelect.Add(tmp.Column(columnAndColumnAlias[i], columnAndColumnAlias[i+1]))
		}
	}
	return s
}

// SQLGroupBy Constructing query groups.
type SQLGroupBy interface {
	IsEmpty

	Cmder

	Column(columns ...string) SQLGroupBy

	Group(prepare string, args ...any) SQLGroupBy

	GroupCmder(cmder Cmder) SQLGroupBy

	Having(having func(having Filter)) SQLGroupBy
}

type sqlGroupBy struct {
	group     string
	groupArgs []any

	groupColumns    []string
	groupColumnsMap map[string]int

	having Filter

	way *Way
}

func (s *sqlGroupBy) IsEmpty() bool {
	return len(s.groupColumns) == 0 && s.group == EmptyString
}

func (s *sqlGroupBy) Cmd() (prepare string, args []any) {
	groupBy := EmptyString
	if s.group != EmptyString {
		groupBy = s.group
		args = append(args, s.groupArgs...)
	}
	if group := strings.Join(s.way.Replaces(s.groupColumns), SqlConcat); group != EmptyString {
		if groupBy == EmptyString {
			groupBy = group
		} else {
			groupBy = ConcatString(groupBy, SqlConcat, group)
		}
	}

	if groupBy == EmptyString {
		args = nil
		return
	}

	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(ConcatString(SqlGroupBy, SqlSpace, groupBy))

	if !s.having.IsEmpty() {
		b.WriteString(ConcatString(SqlSpace, SqlHaving, SqlSpace))
		having, havingArgs := ParcelFilter(s.having).Cmd()
		b.WriteString(having)
		if havingArgs != nil {
			args = append(args, havingArgs...)
		}
	}
	prepare = b.String()
	return
}

func (s *sqlGroupBy) Group(prepare string, args ...any) SQLGroupBy {
	s.group, s.groupArgs = prepare, args
	return s
}

func (s *sqlGroupBy) GroupCmder(cmder Cmder) SQLGroupBy {
	if cmder == nil {
		return s
	}
	prepare, args := cmder.Cmd()
	if prepare != EmptyString {
		return s.Group(prepare, args...)
	}
	return s
}

func (s *sqlGroupBy) Column(columns ...string) SQLGroupBy {
	for _, column := range columns {
		if column == EmptyString {
			continue
		}
		if _, ok := s.groupColumnsMap[column]; ok {
			continue
		}
		s.groupColumnsMap[column] = len(s.groupColumns)
		s.groupColumns = append(s.groupColumns, column)
	}
	return s
}

func (s *sqlGroupBy) Having(having func(having Filter)) SQLGroupBy {
	if having != nil {
		having(s.having)
	}
	return s
}

func NewSQLGroupBy(way *Way) SQLGroupBy {
	return &sqlGroupBy{
		groupColumns:    make([]string, 0, 2),
		groupColumnsMap: make(map[string]int, 2),
		having:          way.F(),
		way:             way,
	}
}

// SQLOrderBy Constructing query orders.
type SQLOrderBy interface {
	IsEmpty

	Cmder

	Use(columns ...string) SQLOrderBy

	Asc(columns ...string) SQLOrderBy

	Desc(columns ...string) SQLOrderBy
}

type sqlOrderBy struct {
	allow    map[string]*struct{}
	orderBy  []string
	orderMap map[string]int
	way      *Way
}

func (s *sqlOrderBy) IsEmpty() bool {
	return len(s.orderBy) == 0
}

func (s *sqlOrderBy) Cmd() (prepare string, args []any) {
	if s.IsEmpty() {
		return
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(ConcatString(SqlOrderBy, SqlSpace))
	b.WriteString(strings.Join(s.orderBy, SqlConcat))
	prepare = b.String()
	return
}

func (s *sqlOrderBy) Use(columns ...string) SQLOrderBy {
	allow := make(map[string]*struct{}, len(columns))
	for _, column := range columns {
		if column != EmptyString {
			allow[column] = nil
		}
	}
	if len(allow) > 0 {
		s.allow = MergeAssoc(s.allow, allow)
	}
	return s
}

func (s *sqlOrderBy) add(category string, columns ...string) SQLOrderBy {
	if category == EmptyString {
		return s
	}
	allow := s.allow != nil
	index := len(s.orderBy)
	for _, column := range columns {
		if column == EmptyString {
			continue
		}
		if allow {
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
		order = ConcatString(order, SqlSpace, category)
		s.orderBy = append(s.orderBy, order)
	}
	return s
}

func (s *sqlOrderBy) Asc(columns ...string) SQLOrderBy {
	return s.add(SqlAsc, columns...)
}

func (s *sqlOrderBy) Desc(columns ...string) SQLOrderBy {
	return s.add(SqlDesc, columns...)
}

func NewSQLOrderBy(way *Way) SQLOrderBy {
	return &sqlOrderBy{
		orderBy:  make([]string, 0, 2),
		orderMap: make(map[string]int, 2),
		way:      way,
	}
}

// SQLLimit Constructing query limits.
type SQLLimit interface {
	IsEmpty

	Cmder

	Limit(limit int64) SQLLimit

	Offset(offset int64) SQLLimit

	Page(page int64, limit ...int64) SQLLimit
}

type sqlLimit struct {
	limit  *int64
	offset *int64
}

func (s *sqlLimit) IsEmpty() bool {
	return s.limit == nil
}

func (s *sqlLimit) Cmd() (prepare string, args []any) {
	if s.IsEmpty() {
		return
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(SqlLimit)
	b.WriteString(SqlSpace)
	limit := strconv.FormatInt(*s.limit, 10)
	b.WriteString(limit)
	if s.offset != nil && *s.offset >= 0 {
		b.WriteString(SqlSpace)
		b.WriteString(SqlOffset)
		b.WriteString(SqlSpace)
		offset := strconv.FormatInt(*s.offset, 10)
		b.WriteString(offset)
	}
	prepare = b.String()
	return
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

func NewSQLLimit() SQLLimit {
	return &sqlLimit{}
}

// SQLUpsertColumn Constructing insert columns.
type SQLUpsertColumn interface {
	IsEmpty

	Cmder

	Add(columns ...string) SQLUpsertColumn

	Del(columns ...string) SQLUpsertColumn

	DelUseIndex(indexes ...int) SQLUpsertColumn

	ColumnIndex(column string) int

	ColumnExists(column string) bool

	Len() int

	SetColumns(columns []string) SQLUpsertColumn

	GetColumns() []string

	GetColumnsMap() map[string]*struct{}
}

type sqlUpsertColumn struct {
	columns    []string
	columnsMap map[string]int
	way        *Way
}

func NewSQLUpsertColumn(way *Way) SQLUpsertColumn {
	return &sqlUpsertColumn{
		columns:    make([]string, 0, 16),
		columnsMap: make(map[string]int, 16),
		way:        way,
	}
}

func (s *sqlUpsertColumn) IsEmpty() bool {
	return len(s.columns) == 0
}

func (s *sqlUpsertColumn) Cmd() (prepare string, args []any) {
	if s.IsEmpty() {
		return
	}
	return ParcelPrepare(strings.Join(s.way.Replaces(s.columns), SqlConcat)), nil
}

func (s *sqlUpsertColumn) Add(columns ...string) SQLUpsertColumn {
	num := len(s.columns)
	for _, column := range columns {
		if column == EmptyString {
			continue
		}
		if _, ok := s.columnsMap[column]; ok {
			continue
		}
		s.columns = append(s.columns, column)
		s.columnsMap[column] = num
		num++
	}
	return s
}

func (s *sqlUpsertColumn) Del(columns ...string) SQLUpsertColumn {
	if columns == nil {
		s.columns = make([]string, 0, 16)
		s.columnsMap = make(map[string]int, 16)
		return s
	}
	deletedIndex := make(map[int]*struct{}, len(columns))
	for _, column := range columns {
		if column == EmptyString {
			continue
		}
		index, ok := s.columnsMap[column]
		if !ok {
			continue
		}
		deletedIndex[index] = &struct{}{}
	}
	length := len(s.columns)
	columns = make([]string, 0, length)
	columnsMap := make(map[string]int, length)
	num := 0
	for index, column := range s.columns {
		if _, ok := deletedIndex[index]; !ok {
			columns = append(columns, column)
			columnsMap[column] = num
			num++
		}
	}
	s.columns, s.columnsMap = columns, columnsMap
	return s
}

func (s *sqlUpsertColumn) DelUseIndex(indexes ...int) SQLUpsertColumn {
	length := len(s.columns)
	minIndex, maxIndex := 0, length
	if maxIndex == minIndex {
		return s
	}
	count := len(indexes)
	deletedIndex := make(map[int]*struct{}, count)
	for _, index := range indexes {
		if index >= minIndex && index < maxIndex {
			deletedIndex[index] = &struct{}{}
		}
	}
	if len(deletedIndex) == 0 {
		return s
	}
	columns := make([]string, 0, length)
	for index, column := range s.columns {
		if _, ok := deletedIndex[index]; ok {
			columns = append(columns, column)
		}
	}
	return s.Del(columns...)
}

func (s *sqlUpsertColumn) ColumnIndex(column string) int {
	index, ok := s.columnsMap[column]
	if !ok {
		return -1
	}
	return index
}

func (s *sqlUpsertColumn) ColumnExists(column string) bool {
	return s.ColumnIndex(column) >= 0
}

func (s *sqlUpsertColumn) SetColumns(columns []string) SQLUpsertColumn {
	return s.Del().Add(columns...)
}

func (s *sqlUpsertColumn) GetColumns() []string {
	return s.columns[:]
}

func (s *sqlUpsertColumn) GetColumnsMap() map[string]*struct{} {
	result := make(map[string]*struct{}, len(s.columns))
	for _, column := range s.GetColumns() {
		result[column] = &struct{}{}
	}
	return result
}

func (s *sqlUpsertColumn) Len() int {
	return len(s.columns)
}

// SQLInsertValue Constructing insert values.
type SQLInsertValue interface {
	IsEmpty

	Cmder

	SetSubquery(subquery Cmder) SQLInsertValue

	SetValues(values ...[]any) SQLInsertValue

	Set(index int, value any) SQLInsertValue

	Del(indexes ...int) SQLInsertValue

	LenValues() int

	GetValues() [][]any
}

type sqlInsertValue struct {
	subquery Cmder
	values   [][]any
}

func NewSQLInsertValue() SQLInsertValue {
	return &sqlInsertValue{}
}

func (s *sqlInsertValue) IsEmpty() bool {
	return s.subquery == nil && (len(s.values) == 0 || len(s.values[0]) == 0)
}

func (s *sqlInsertValue) Cmd() (prepare string, args []any) {
	if s.IsEmpty() {
		return
	}
	if s.subquery != nil {
		prepare, args = s.subquery.Cmd()
		return
	}
	count := len(s.values)
	if count == 0 {
		return
	}
	length := len(s.values[0])
	if length == 0 {
		return
	}
	line := make([]string, length)
	args = make([]any, 0, count*length)
	for i := range length {
		line[i] = SqlPlaceholder
	}
	value := ParcelPrepare(strings.Join(line, SqlConcat))
	rows := make([]string, count)
	for i := range count {
		args = append(args, s.values[i]...)
		rows[i] = value
	}
	prepare = strings.Join(rows, SqlConcat)
	return
}

func (s *sqlInsertValue) SetSubquery(subquery Cmder) SQLInsertValue {
	if subquery != nil {
		if prepare, _ := subquery.Cmd(); prepare == EmptyString {
			return s
		}
	}
	s.subquery = subquery
	return s
}

func (s *sqlInsertValue) SetValues(values ...[]any) SQLInsertValue {
	s.values = values
	return s
}

func (s *sqlInsertValue) Set(index int, value any) SQLInsertValue {
	if index < 0 {
		return s
	}
	if s.values == nil {
		s.values = make([][]any, 1)
	}
	for num, tmp := range s.values {
		length := len(tmp)
		if index > length {
			continue
		}
		if index == length {
			s.values[num] = append(s.values[num], value)
		} else {
			s.values[num][index] = value
		}
	}
	return s
}

func (s *sqlInsertValue) Del(indexes ...int) SQLInsertValue {
	if s.values == nil {
		return s
	}
	length := len(indexes)
	if length == 0 {
		return s
	}
	deletedIndex := make(map[int]*struct{}, length)
	for _, index := range indexes {
		if index < 0 {
			continue
		}
		deletedIndex[index] = &struct{}{}
	}
	length = len(deletedIndex)
	if length == 0 {
		return s
	}
	values := make([][]any, len(s.values))
	for index, value := range s.values {
		values[index] = make([]any, 0, len(value))
		for num, tmp := range value {
			if _, ok := deletedIndex[num]; !ok {
				values[index] = append(values[index], tmp)
			}
		}
	}
	s.values = values
	return s
}

func (s *sqlInsertValue) LenValues() int {
	return len(s.values)
}

func (s *sqlInsertValue) GetValues() [][]any {
	return s.values
}

// SQLUpdateSet Constructing update sets.
type SQLUpdateSet interface {
	IsEmpty

	Cmder

	Update(update string, args ...any) SQLUpdateSet

	Set(column string, value any) SQLUpdateSet

	Decr(column string, decr any) SQLUpdateSet

	Incr(column string, incr any) SQLUpdateSet

	SetMap(columnValue map[string]any) SQLUpdateSet

	SetSlice(columns []string, values []any) SQLUpdateSet

	Len() int

	GetUpdate() (updates []string, args [][]any)

	UpdateIndex(prepare string) int

	UpdateExists(prepare string) bool
}

type sqlUpdateSet struct {
	updateExpr []string
	updateArgs [][]any
	updateMap  map[string]int
	way        *Way
}

func NewSQLUpdateSet(way *Way) SQLUpdateSet {
	return &sqlUpdateSet{
		updateExpr: make([]string, 0, 8),
		updateArgs: make([][]any, 0, 8),
		updateMap:  make(map[string]int, 8),
		way:        way,
	}
}

func (s *sqlUpdateSet) IsEmpty() bool {
	return len(s.updateExpr) == 0
}

func (s *sqlUpdateSet) Cmd() (prepare string, args []any) {
	if s.IsEmpty() {
		return
	}
	prepare = strings.Join(s.updateExpr, SqlConcat)
	for _, tmp := range s.updateArgs {
		args = append(args, tmp...)
	}
	return
}

func (s *sqlUpdateSet) beautifyUpdate(update string) string {
	update = strings.TrimSpace(update)
	for strings.Contains(update, "  ") {
		update = strings.ReplaceAll(update, "  ", SqlSpace)
	}
	return update
}

func (s *sqlUpdateSet) Update(update string, args ...any) SQLUpdateSet {
	if update == EmptyString {
		return s
	}
	update = s.beautifyUpdate(update)
	if update == EmptyString {
		return s
	}
	index, ok := s.updateMap[update]
	if ok {
		s.updateExpr[index], s.updateArgs[index] = update, args
		return s
	}
	s.updateMap[update] = len(s.updateExpr)
	s.updateExpr = append(s.updateExpr, update)
	s.updateArgs = append(s.updateArgs, args)
	return s
}

func (s *sqlUpdateSet) Set(column string, value any) SQLUpdateSet {
	column = s.way.Replace(column)
	return s.Update(fmt.Sprintf("%s = %s", column, SqlPlaceholder), value)
}

func (s *sqlUpdateSet) Decr(column string, decrement any) SQLUpdateSet {
	column = s.way.Replace(column)
	return s.Update(fmt.Sprintf("%s = %s - %s", column, column, SqlPlaceholder), decrement)
}

func (s *sqlUpdateSet) Incr(column string, increment any) SQLUpdateSet {
	s.way.Replace(column)
	return s.Update(fmt.Sprintf("%s = %s + %s", column, column, SqlPlaceholder), increment)
}

func (s *sqlUpdateSet) SetMap(columnValue map[string]any) SQLUpdateSet {
	for column, value := range columnValue {
		s.Set(column, value)
	}
	return s
}

func (s *sqlUpdateSet) SetSlice(columns []string, values []any) SQLUpdateSet {
	for index, column := range columns {
		s.Set(column, values[index])
	}
	return s
}

func (s *sqlUpdateSet) Len() int {
	return len(s.updateExpr)
}

func (s *sqlUpdateSet) GetUpdate() ([]string, [][]any) {
	return s.updateExpr, s.updateArgs
}

func (s *sqlUpdateSet) UpdateIndex(update string) int {
	update = s.beautifyUpdate(update)
	index, ok := s.updateMap[update]
	if !ok {
		return -1
	}
	return index
}

func (s *sqlUpdateSet) UpdateExists(update string) bool {
	return s.UpdateIndex(update) >= 0
}

/**
 * sql identifier.
 **/

type TableColumn struct {
	alias string
	way   *Way
}

// Alias Get the alias name value.
func (s *TableColumn) Alias() string {
	return s.alias
}

// aliasesName Adjust alias name.
func (s *TableColumn) aliasName(alias ...string) string {
	return s.way.Replace(LastNotEmptyString(alias))
}

// SetAlias Set the alias name value.
func (s *TableColumn) SetAlias(alias string) *TableColumn {
	s.alias = alias
	return s
}

// Adjust Batch adjust columns.
func (s *TableColumn) Adjust(adjust func(column string) string, columns ...string) []string {
	if adjust != nil {
		for index, column := range columns {
			columns[index] = s.way.Replace(adjust(column))
		}
	}
	return columns
}

// ColumnAll Add table name prefix to column names in batches.
func (s *TableColumn) ColumnAll(columns ...string) []string {
	if s.alias == EmptyString {
		return s.way.Replaces(columns)
	}
	alias := s.way.Replace(s.alias)
	result := make([]string, len(columns))
	for index, column := range columns {
		result[index] = SqlPrefix(alias, s.way.Replace(column))
	}
	return result
}

// columnAlias Set an alias for the column.
// "column_name + alias_name" -> "column_name"
// "column_name + alias_name" -> "column_name AS alias_name"
func (s *TableColumn) columnAlias(column string, alias string) string {
	return SqlAlias(column, alias)
}

// Column Add table name prefix to single column name, allowing column alias to be set.
func (s *TableColumn) Column(column string, aliases ...string) string {
	return s.columnAlias(s.ColumnAll(column)[0], s.aliasName(aliases...))
}

// Sum SUM(column[, alias])
func (s *TableColumn) Sum(column string, aliases ...string) string {
	return s.columnAlias(fmt.Sprintf("SUM(%s)", s.Column(column)), s.aliasName(aliases...))
}

// Max MAX(column[, alias])
func (s *TableColumn) Max(column string, aliases ...string) string {
	return s.columnAlias(fmt.Sprintf("MAX(%s)", s.Column(column)), s.aliasName(aliases...))
}

// Min MIN(column[, alias])
func (s *TableColumn) Min(column string, aliases ...string) string {
	return s.columnAlias(fmt.Sprintf("MIN(%s)", s.Column(column)), s.aliasName(aliases...))
}

// Avg AVG(column[, alias])
func (s *TableColumn) Avg(column string, aliases ...string) string {
	return s.columnAlias(fmt.Sprintf("AVG(%s)", s.Column(column)), s.aliasName(aliases...))
}

// Count Example
// Count(): COUNT(*) AS `counts`
// Count("total"): COUNT(*) AS `total`
// Count("1", "total"): COUNT(1) AS `total`
// Count("id", "counts"): COUNT(`id`) AS `counts`
func (s *TableColumn) Count(counts ...string) string {
	count := "COUNT(*)"
	length := len(counts)
	if length == 0 {
		// using default expression: COUNT(*) AS `counts`
		return s.columnAlias(count, s.way.Replace(DefaultAliasNameCount))
	}
	if length == 1 && counts[0] != EmptyString {
		// only set alias name
		return s.columnAlias(count, s.way.Replace(counts[0]))
	}
	// set COUNT function parameters and alias name
	countAlias := s.way.Replace(DefaultAliasNameCount)
	column := false
	for i := range length {
		if counts[i] == EmptyString {
			continue
		}
		if column {
			countAlias = s.way.Replace(counts[i])
			break
		}
		count, column = fmt.Sprintf("COUNT(%s)", s.Column(counts[i])), true
	}
	return s.columnAlias(count, countAlias)
}

// aggregate Perform an aggregate function on the column and set a default value to replace NULL values.
func (s *TableColumn) aggregate(column string, defaultValue string, aliases ...string) string {
	return s.columnAlias(NullDefaultValue(s.Column(column), defaultValue), s.aliasName(aliases...))
}

// SUM COALESCE(SUM(column) ,0)[ AS column_alias_name]
func (s *TableColumn) SUM(column string, aliases ...string) string {
	return s.aggregate(s.Sum(column), "0", aliases...)
}

// MAX COALESCE(MAX(column) ,0)[ AS column_alias_name]
func (s *TableColumn) MAX(column string, aliases ...string) string {
	return s.aggregate(s.Max(column), "0", aliases...)
}

// MIN COALESCE(MIN(column) ,0)[ AS column_alias_name]
func (s *TableColumn) MIN(column string, aliases ...string) string {
	return s.aggregate(s.Min(column), "0", aliases...)
}

// AVG COALESCE(AVG(column) ,0)[ AS column_alias_name]
func (s *TableColumn) AVG(column string, aliases ...string) string {
	return s.aggregate(s.Avg(column), "0", aliases...)
}

func NewTableColumn(way *Way, aliases ...string) *TableColumn {
	tmp := &TableColumn{
		way: way,
	}
	tmp.alias = LastNotEmptyString(aliases)
	return tmp
}

/**
 * SQL window functions.
 **/

// WindowFunc SQL window function.
type WindowFunc struct {
	way *Way

	// withFunc The window function used.
	withFunc string

	// partition Setting up window partitions.
	partition []string

	// order Sorting data within a group.
	order []string

	// windowFrame Window frame clause. `ROWS` or `RANGE`.
	windowFrame string

	// alias Serial number column alias.
	alias string
}

// WithFunc Using custom function. for example: CUME_DIST(), PERCENT_RANK(), PERCENTILE_CONT(), PERCENTILE_DISC()...
func (s *WindowFunc) WithFunc(withFunc string) *WindowFunc {
	s.withFunc = withFunc
	return s
}

// RowNumber ROW_NUMBER() Assign a unique serial number to each row, in the order specified, starting with 1.
func (s *WindowFunc) RowNumber() *WindowFunc {
	return s.WithFunc("ROW_NUMBER()")
}

// Rank RANK() Assign a rank to each row, if there are duplicate values, the rank is skipped.
func (s *WindowFunc) Rank() *WindowFunc {
	return s.WithFunc("RANK()")
}

// DenseRank DENSE_RANK() Similar to RANK(), but does not skip rankings.
func (s *WindowFunc) DenseRank() *WindowFunc {
	return s.WithFunc("DENSE_RANK()")
}

// Ntile NTILE() Divide the rows in the window into n buckets and assign a bucket number to each row.
func (s *WindowFunc) Ntile(buckets int64) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("NTILE(%d)", buckets))
}

// Sum SUM() Returns the sum of all rows in the window.
func (s *WindowFunc) Sum(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("SUM(%s)", s.way.Replace(column)))
}

// Max MAX() Returns the maximum value within the window.
func (s *WindowFunc) Max(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("MAX(%s)", s.way.Replace(column)))
}

// Min MIN() Returns the minimum value within the window.
func (s *WindowFunc) Min(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("MIN(%s)", s.way.Replace(column)))
}

// Avg AVG() Returns the average of all rows in the window.
func (s *WindowFunc) Avg(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("AVG(%s)", s.way.Replace(column)))
}

// Count COUNT() Returns the number of rows in the window.
func (s *WindowFunc) Count(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("COUNT(%s)", s.way.Replace(column)))
}

// Lag LAG() Returns the value of the row before the current row.
func (s *WindowFunc) Lag(column string, offset int64, defaultValue any) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("LAG(%s, %d, %s)", s.way.Replace(column), offset, argValueToString(defaultValue)))
}

// Lead LEAD() Returns the value of a row after the current row.
func (s *WindowFunc) Lead(column string, offset int64, defaultValue any) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("LEAD(%s, %d, %s)", s.way.Replace(column), offset, argValueToString(defaultValue)))
}

// NthValue NTH_VALUE() The Nth value can be returned according to the specified order. This is very useful when you need to get data at a specific position.
func (s *WindowFunc) NthValue(column string, LineNumber int64) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("NTH_VALUE(%s, %d)", s.way.Replace(column), LineNumber))
}

// FirstValue FIRST_VALUE() Returns the value of the first row in the window.
func (s *WindowFunc) FirstValue(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("FIRST_VALUE(%s)", s.way.Replace(column)))
}

// LastValue LAST_VALUE() Returns the value of the last row in the window.
func (s *WindowFunc) LastValue(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("LAST_VALUE(%s)", s.way.Replace(column)))
}

// Partition The OVER clause defines window partitions so that the window function is calculated independently in each partition.
func (s *WindowFunc) Partition(column ...string) *WindowFunc {
	s.partition = append(s.partition, s.way.Replaces(column)...)
	return s
}

// Asc Define the sorting within the partition so that the window function is calculated in order.
func (s *WindowFunc) Asc(column string) *WindowFunc {
	s.order = append(s.order, fmt.Sprintf("%s %s", s.way.Replace(column), SqlAsc))
	return s
}

// Desc Define the sorting within the partition so that the window function is calculated in descending order.
func (s *WindowFunc) Desc(column string) *WindowFunc {
	s.order = append(s.order, fmt.Sprintf("%s %s", s.way.Replace(column), SqlDesc))
	return s
}

// WindowFrame Set custom window frame clause.
func (s *WindowFunc) WindowFrame(windowFrame string) *WindowFunc {
	s.windowFrame = windowFrame
	return s
}

// Alias Set the alias of the column that uses the window function.
func (s *WindowFunc) Alias(alias string) *WindowFunc {
	s.alias = alias
	return s
}

// Result Query column expressions.
func (s *WindowFunc) Result() string {
	if s.withFunc == EmptyString || s.partition == nil || s.order == nil || s.alias == EmptyString {
		panic("hey: the SQL window function parameters are incomplete")
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(s.withFunc)
	b.WriteString(ConcatString(SqlSpace, SqlOver, SqlSpace, SqlLeftSmallBracket, SqlSpace, SqlPartitionBy, SqlSpace))
	b.WriteString(strings.Join(s.partition, SqlConcat))
	b.WriteString(ConcatString(SqlSpace, SqlOrderBy, SqlSpace))
	b.WriteString(strings.Join(s.order, SqlConcat))
	if s.windowFrame != EmptyString {
		b.WriteString(SqlSpace)
		b.WriteString(s.windowFrame)
	}
	b.WriteString(ConcatString(SqlSpace, SqlRightSmallBracket, SqlSpace, SqlAs, SqlSpace))
	b.WriteString(s.alias)
	return b.String()
}

func NewWindowFunc(way *Way, aliases ...string) *WindowFunc {
	return &WindowFunc{
		way:   way,
		alias: LastNotEmptyString(aliases),
	}
}

// CmderQuery execute the built SQL statement and scan query results.
func CmderQuery(ctx context.Context, way *Way, cmder Cmder, query func(rows *sql.Rows) (err error)) error {
	prepare, args := cmder.Cmd()
	return way.QueryContext(ctx, query, prepare, args...)
}

// CmderGet execute the built SQL statement and scan query results.
func CmderGet(ctx context.Context, way *Way, cmder Cmder, result any) error {
	prepare, args := cmder.Cmd()
	return way.TakeAllContext(ctx, result, prepare, args...)
}

// CmderScanAll execute the built SQL statement and scan all from the query results.
func CmderScanAll(ctx context.Context, way *Way, cmder Cmder, custom func(rows *sql.Rows) error) error {
	return CmderQuery(ctx, way, cmder, func(rows *sql.Rows) error {
		return way.ScanAll(rows, custom)
	})
}

// CmderScanOne execute the built SQL statement and scan at most once from the query results.
func CmderScanOne(ctx context.Context, way *Way, cmder Cmder, dest ...any) error {
	return CmderQuery(ctx, way, cmder, func(rows *sql.Rows) error {
		return way.ScanOne(rows, dest...)
	})
}

// CmderViewMap execute the built SQL statement and scan all from the query results.
func CmderViewMap(ctx context.Context, way *Way, cmder Cmder) (result []map[string]any, err error) {
	err = CmderQuery(ctx, way, cmder, func(rows *sql.Rows) (err error) {
		result, err = ScanViewMap(rows)
		return
	})
	return
}

/* CASE [xxx] WHEN xxx THEN XXX [WHEN yyy THEN YYY] [ELSE xxx] END [AS xxx] */

func sqlCaseValueFirst(value any) (prepare string) {
	if value == nil {
		prepare = SqlNull
		return
	}
	switch val := value.(type) {
	case bool:
		prepare = fmt.Sprintf("%t", val)
	case float32:
		prepare = strconv.FormatFloat(float64(val), 'g', -1, 64)
	case float64:
		prepare = strconv.FormatFloat(val, 'g', -1, 64)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		prepare = fmt.Sprintf("%d", val)
	case string:
		prepare = val
	default:
		panic(fmt.Errorf("hey: unsupported type `%T`", val))
	}
	return
}

func sqlCaseValuesToCmder(values ...any) Cmder {
	length := len(values)
	if length == 0 {
		return nil
	}
	if tmp, ok := values[0].(Cmder); ok {
		return tmp
	}
	if length == 1 {
		return NewCmder(sqlCaseValueFirst(values[0]), nil)
	}
	first, ok := values[0].(string)
	if !ok {
		return nil
	}
	return NewCmder(sqlCaseValueFirst(first), values[1:])
}

func sqlCaseString(value string) string {
	return fmt.Sprintf("'%s'", value)
}

func sqlCaseParseCmder(cmder Cmder) (prepare string, args []interface{}) {
	if cmder == nil {
		prepare = SqlNull
		return
	}
	emptyString := sqlCaseString(EmptyString)
	prepare, args = cmder.Cmd()
	if prepare == EmptyString || prepare == emptyString {
		prepare = emptyString
		return
	}
	return
}

// SQLWhenThen Store multiple pairs of WHEN xxx THEN xxx.
type SQLWhenThen interface {
	Cmder

	String(value string) string

	When(values ...any) SQLWhenThen

	Then(values ...any) SQLWhenThen
}

type sqlWhenThen struct {
	when []Cmder
	then []Cmder
}

func (s *sqlWhenThen) String(value string) string {
	return sqlCaseString(value)
}

func (s *sqlWhenThen) Cmd() (prepare string, args []any) {
	b := getStringBuilder()
	defer putStringBuilder(b)
	length1, length2 := len(s.when), len(s.then)
	if length1 != length2 || length1 == 0 {
		return
	}
	for i := 0; i < length1; i++ {
		if i > 0 {
			b.WriteString(SqlSpace)
		}
		b.WriteString(SqlWhen)
		b.WriteString(SqlSpace)
		if s.when == nil {
			b.WriteString(SqlNull)
		} else {
			express, params := sqlCaseParseCmder(s.when[i])
			b.WriteString(express)
			args = append(args, params...)
		}
		b.WriteString(SqlSpace)
		b.WriteString(SqlThen)
		b.WriteString(SqlSpace)
		if s.then == nil {
			b.WriteString(SqlNull)
		} else {
			express, params := sqlCaseParseCmder(s.then[i])
			b.WriteString(express)
			args = append(args, params...)
		}
	}
	prepare = b.String()
	return
}

func (s *sqlWhenThen) When(values ...any) SQLWhenThen {
	s.when = append(s.when, sqlCaseValuesToCmder(values...))
	return s
}

func (s *sqlWhenThen) Then(values ...any) SQLWhenThen {
	s.then = append(s.then, sqlCaseValuesToCmder(values...))
	return s
}

// SQLCase Implementing SQL CASE.
type SQLCase interface {
	Cmder

	String(value string) string

	Alias(alias string) SQLCase

	Case(values ...any) SQLCase

	When(fc func(w SQLWhenThen)) SQLCase

	Else(values ...any) SQLCase
}

type sqlCase struct {
	alias   string      // alias name for CASE , value is optional
	cmdCase Cmder       // CASE value , value is optional
	cmdWhen SQLWhenThen // WHEN xxx THEN xxx [WHEN xxx THEN xxx] ...
	cmdElse Cmder       // ELSE value , value is optional

	way *Way
}

func (s *sqlCase) String(value string) string {
	return sqlCaseString(value)
}

func (s *sqlCase) Cmd() (prepare string, args []any) {
	if s.cmdWhen == nil {
		return
	}
	whenThenPrepare, whenThenArgs := s.cmdWhen.Cmd()
	if whenThenPrepare == EmptyString {
		return
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(SqlCase)
	if tmp := s.cmdCase; tmp != nil {
		b.WriteString(SqlSpace)
		express, params := sqlCaseParseCmder(tmp)
		b.WriteString(express)
		args = append(args, params...)
	}
	b.WriteString(SqlSpace)
	b.WriteString(whenThenPrepare)
	args = append(args, whenThenArgs...)
	if tmp := s.cmdElse; tmp != nil {
		b.WriteString(SqlSpace)
		b.WriteString(SqlElse)
		b.WriteString(SqlSpace)
		express, params := sqlCaseParseCmder(tmp)
		b.WriteString(express)
		args = append(args, params...)
	}
	b.WriteString(SqlSpace)
	b.WriteString(SqlEnd)
	if alias := s.alias; alias != EmptyString {
		b.WriteString(SqlSpace)
		b.WriteString(SqlAs)
		b.WriteString(SqlSpace)
		if s.way != nil {
			alias = s.way.Replace(alias)
		}
		b.WriteString(alias)
	}
	prepare = b.String()
	return
}

func (s *sqlCase) Alias(alias string) SQLCase {
	s.alias = alias
	return s
}

func (s *sqlCase) Case(values ...any) SQLCase {
	if tmp := sqlCaseValuesToCmder(values...); tmp != nil {
		s.cmdCase = tmp
	}
	return s
}

func (s *sqlCase) When(fc func(w SQLWhenThen)) SQLCase {
	if fc == nil {
		return s
	}
	if s.cmdWhen == nil {
		s.cmdWhen = &sqlWhenThen{}
	}
	fc(s.cmdWhen)
	return s
}

func (s *sqlCase) Else(values ...any) SQLCase {
	if tmp := sqlCaseValuesToCmder(values...); tmp != nil {
		s.cmdElse = tmp
	}
	return s
}

func NewSQLCase(way *Way) SQLCase {
	return &sqlCase{
		way: way,
	}
}
