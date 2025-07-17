package hey

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// TableCmder Used to construct expressions that can use table aliases and their corresponding parameter lists.
type TableCmder interface {
	IsEmpty

	Cmder

	// Alias Setting aliases for script statements.
	Alias(alias string) TableCmder

	// GetAlias Getting aliases for script statements.
	GetAlias() string
}

type tableCmder struct {
	cmder Cmder
	alias string
}

func (s *tableCmder) IsEmpty() bool {
	return IsEmptyCmder(s.cmder)
}

func (s *tableCmder) Cmd() (prepare string, args []any) {
	if s.IsEmpty() {
		return
	}
	prepare, args = s.cmder.Cmd()
	if s.alias != EmptyString {
		prepare = SqlAlias(prepare, s.alias)
	}
	return
}

func (s *tableCmder) Alias(alias string) TableCmder {
	// Allow setting empty values.
	s.alias = alias
	return s
}

func (s *tableCmder) GetAlias() string {
	return s.alias
}

func NewTableCmder(prepare string, args []any) TableCmder {
	return &tableCmder{
		cmder: NewCmder(prepare, args),
	}
}

func NewCmderGet(alias string, get *Get) TableCmder {
	return NewTableCmder(ParcelCmder(get).Cmd()).Alias(alias)
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

// QueryWith CTE: Common Table Expression.
type QueryWith interface {
	IsEmpty

	Cmder

	// Recursive Recursion or cancellation of recursion.
	Recursive() QueryWith

	// Set Setting common table expression.
	Set(alias string, cmder Cmder, columns ...string) QueryWith

	// Del Removing common table expression.
	Del(alias string) QueryWith
}

type queryWith struct {
	recursive bool
	alias     []string
	column    map[string][]string
	prepare   map[string]Cmder
}

func NewQueryWith() QueryWith {
	return &queryWith{
		alias:   make([]string, 0, 2),
		column:  make(map[string][]string, 2),
		prepare: make(map[string]Cmder, 2),
	}
}

func (s *queryWith) IsEmpty() bool {
	return len(s.alias) == 0
}

func (s *queryWith) Cmd() (prepare string, args []any) {
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

func (s *queryWith) Recursive() QueryWith {
	s.recursive = !s.recursive
	return s
}

func (s *queryWith) Set(alias string, cmder Cmder, columns ...string) QueryWith {
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

func (s *queryWith) Del(alias string) QueryWith {
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

// QueryColumns Used to build the list of columns to be queried.
type QueryColumns interface {
	IsEmpty

	Cmder

	Index(column string) int

	Exists(column string) bool

	Add(column string, args ...any) QueryColumns

	AddAll(columns ...string) QueryColumns

	DelAll(columns ...string) QueryColumns

	Len() int

	Get() ([]string, map[int][]any)

	Set(columns []string, columnsArgs map[int][]any) QueryColumns

	Use(queryColumns ...QueryColumns) QueryColumns

	// Queried Get all columns of the query results.
	Queried(excepts ...string) []string
}

type queryColumns struct {
	columns     []string
	columnsMap  map[string]int
	columnsArgs map[int][]any

	way *Way
}

func NewQueryColumns(way *Way) QueryColumns {
	return &queryColumns{
		columns:     make([]string, 0, 16),
		columnsMap:  make(map[string]int, 16),
		columnsArgs: make(map[int][]any),
		way:         way,
	}
}

func (s *queryColumns) IsEmpty() bool {
	return len(s.columns) == 0
}

func (s *queryColumns) Cmd() (prepare string, args []any) {
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

func (s *queryColumns) Index(column string) int {
	index, ok := s.columnsMap[column]
	if !ok {
		return -1
	}
	return index
}

func (s *queryColumns) Exists(column string) bool {
	return s.Index(column) >= 0
}

func (s *queryColumns) Add(column string, args ...any) QueryColumns {
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

func (s *queryColumns) AddAll(columns ...string) QueryColumns {
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

func (s *queryColumns) DelAll(columns ...string) QueryColumns {
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

func (s *queryColumns) Len() int {
	return len(s.columns)
}

func (s *queryColumns) Get() ([]string, map[int][]any) {
	return s.columns, s.columnsArgs
}

func (s *queryColumns) Set(columns []string, columnsArgs map[int][]any) QueryColumns {
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

func (s *queryColumns) Use(queryColumns ...QueryColumns) QueryColumns {
	length := len(queryColumns)
	for i := range length {
		tmp := queryColumns[i]
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
func (s *queryColumns) Queried(excepts ...string) []string {
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

type JoinOn interface {
	Cmder

	// Filter JOIN ON Condition.
	Filter() Filter

	// On Using custom JOIN ON conditions.
	On(on func(on Filter)) JoinOn

	// Equal Use equal value JOIN ON condition.
	Equal(leftAlias string, leftColumn string, rightAlias string, rightColumn string) JoinOn

	// Using Use USING instead of ON.
	Using(using ...string) JoinOn
}

type joinOn struct {
	way   *Way
	on    Filter
	using []string
}

func (s *joinOn) Filter() Filter {
	return s.on
}

func (s *joinOn) On(fc func(on Filter)) JoinOn {
	if fc != nil {
		fc(s.on)
	}
	return s
}

func (s *joinOn) Equal(leftAlias string, leftColumn string, rightAlias string, rightColumn string) JoinOn {
	s.on.And(ConcatString(SqlPrefix(leftAlias, leftColumn), SqlSpace, SqlEqual, SqlSpace, SqlPrefix(rightAlias, rightColumn)))
	return s
}

func (s *joinOn) Using(using ...string) JoinOn {
	using = DiscardDuplicate(func(tmp string) bool { return tmp == EmptyString }, using...)
	if len(using) > 0 {
		s.using = using
	}
	return s
}

func (s *joinOn) Cmd() (prepare string, args []any) {
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

func newJoinOn(way *Way) JoinOn {
	return &joinOn{
		way: way,
		on:  way.F(),
	}
}

// JoinFunc Constructing conditions for join queries.
type JoinFunc func(leftAlias string, rightAlias string) JoinOn

type queryJoinSchema struct {
	joinType   string
	rightTable TableCmder
	condition  Cmder
}

// QueryJoin Constructing multi-table join queries.
type QueryJoin interface {
	Cmder

	GetMaster() TableCmder

	SetMaster(master TableCmder) QueryJoin

	NewTable(table string, alias string, args ...any) TableCmder

	NewSubquery(subquery Cmder, alias string) TableCmder

	On(onList ...func(o JoinOn, leftAlias string, rightAlias string)) JoinFunc

	Using(columns ...string) JoinFunc

	OnEqual(leftColumn string, rightColumn string) JoinFunc

	Join(joinTypeString string, leftTable TableCmder, rightTable TableCmder, on JoinFunc) QueryJoin

	InnerJoin(leftTable TableCmder, rightTable TableCmder, on JoinFunc) QueryJoin

	LeftJoin(leftTable TableCmder, rightTable TableCmder, on JoinFunc) QueryJoin

	RightJoin(leftTable TableCmder, rightTable TableCmder, on JoinFunc) QueryJoin

	// Queries Get query columns.
	Queries() QueryColumns

	// TableColumn Build *TableColumn based on TableCmder.
	TableColumn(table TableCmder) *TableColumn

	// TableSelect Add the queried column list based on the table's alias prefix.
	TableSelect(table TableCmder, columns ...string) []string

	// TableSelectAliases Add the queried column list based on the table's alias prefix, support setting the query column alias.
	TableSelectAliases(table TableCmder, aliases map[string]string, columns ...string) []string

	// SelectGroupsColumns Add the queried column list based on the table's alias prefix.
	SelectGroupsColumns(columns ...[]string) QueryJoin

	// SelectTableColumnAlias Batch set multiple columns of the specified table and set aliases for all columns.
	SelectTableColumnAlias(table TableCmder, columnAndColumnAlias ...string) QueryJoin
}

type queryJoin struct {
	master       TableCmder
	joins        []*queryJoinSchema
	queryColumns QueryColumns

	way *Way
}

func NewQueryJoin(way *Way) QueryJoin {
	tmp := &queryJoin{
		joins:        make([]*queryJoinSchema, 0, 2),
		queryColumns: NewQueryColumns(way),
		way:          way,
	}
	return tmp
}

func (s *queryJoin) GetMaster() TableCmder {
	return s.master
}

func (s *queryJoin) SetMaster(master TableCmder) QueryJoin {
	if master != nil && !master.IsEmpty() {
		s.master = master
	}
	return s
}

func (s *queryJoin) Cmd() (prepare string, args []any) {
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

func (s *queryJoin) NewTable(table string, alias string, args ...any) TableCmder {
	return NewTableCmder(table, args).Alias(alias)
}

func (s *queryJoin) NewSubquery(subquery Cmder, alias string) TableCmder {
	prepare, args := ParcelCmder(subquery).Cmd()
	return s.NewTable(prepare, alias, args...)
}

// On For `... JOIN ON ...`
func (s *queryJoin) On(onList ...func(o JoinOn, leftAlias string, rightAlias string)) JoinFunc {
	return func(leftAlias string, rightAlias string) JoinOn {
		return newJoinOn(s.way).On(func(o Filter) {
			for _, tmp := range onList {
				if tmp != nil {
					newAssoc := newJoinOn(s.way)
					tmp(newAssoc, leftAlias, rightAlias)
					if object := newAssoc.Filter(); object.Num() > 0 {
						prepare, args := object.Cmd()
						o.And(prepare, args...)
					}
				}
			}
		})
	}
}

// Using For `... JOIN USING ...`
func (s *queryJoin) Using(columns ...string) JoinFunc {
	return func(leftAlias string, rightAlias string) JoinOn {
		return newJoinOn(s.way).Using(columns...)
	}
}

// OnEqual For `... JOIN ON ... = ... [...]`
func (s *queryJoin) OnEqual(leftColumn string, rightColumn string) JoinFunc {
	if leftColumn == EmptyString || rightColumn == EmptyString {
		return nil
	}
	return func(leftAlias string, rightAlias string) JoinOn {
		return newJoinOn(s.way).On(func(on Filter) {
			on.CompareEqual(SqlPrefix(leftAlias, leftColumn), SqlPrefix(rightAlias, rightColumn))
		})
	}
}

func (s *queryJoin) Join(joinTypeString string, leftTable TableCmder, rightTable TableCmder, on JoinFunc) QueryJoin {
	if joinTypeString == EmptyString {
		joinTypeString = SqlJoinInner
	}
	if leftTable == nil || leftTable.IsEmpty() {
		leftTable = s.master
	}
	if rightTable == nil || rightTable.IsEmpty() {
		return s
	}
	join := &queryJoinSchema{
		joinType:   joinTypeString,
		rightTable: rightTable,
	}
	if on != nil {
		join.condition = on(leftTable.GetAlias(), rightTable.GetAlias())
	}
	s.joins = append(s.joins, join)
	return s
}

func (s *queryJoin) InnerJoin(leftTable TableCmder, rightTable TableCmder, on JoinFunc) QueryJoin {
	return s.Join(SqlJoinInner, leftTable, rightTable, on)
}

func (s *queryJoin) LeftJoin(leftTable TableCmder, rightTable TableCmder, on JoinFunc) QueryJoin {
	return s.Join(SqlJoinLeft, leftTable, rightTable, on)
}

func (s *queryJoin) RightJoin(leftTable TableCmder, rightTable TableCmder, on JoinFunc) QueryJoin {
	return s.Join(SqlJoinRight, leftTable, rightTable, on)
}

func (s *queryJoin) Queries() QueryColumns {
	return s.queryColumns
}

func (s *queryJoin) TableColumn(table TableCmder) *TableColumn {
	result := s.way.T()
	if table == nil {
		return result
	}
	if alias := table.GetAlias(); alias != EmptyString {
		result.SetAlias(alias)
	}
	return result
}

func (s *queryJoin) selectTableColumnOptionalColumnAlias(table TableCmder, aliases map[string]string, columns ...string) []string {
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

func (s *queryJoin) TableSelect(table TableCmder, columns ...string) []string {
	return s.selectTableColumnOptionalColumnAlias(table, nil, columns...)
}

func (s *queryJoin) TableSelectAliases(table TableCmder, aliases map[string]string, columns ...string) []string {
	return s.selectTableColumnOptionalColumnAlias(table, aliases, columns...)
}

func (s *queryJoin) SelectGroupsColumns(columns ...[]string) QueryJoin {
	groups := make([]string, 0, 32)
	for _, values := range columns {
		groups = append(groups, values...)
	}
	s.queryColumns.AddAll(groups...)
	return s
}

func (s *queryJoin) SelectTableColumnAlias(table TableCmder, columnAndColumnAlias ...string) QueryJoin {
	length := len(columnAndColumnAlias)
	if length == 0 || length&1 == 1 {
		return s
	}
	tmp := s.TableColumn(table)
	for i := 0; i < length; i += 2 {
		if columnAndColumnAlias[i] != EmptyString && columnAndColumnAlias[i+1] != EmptyString {
			s.queryColumns.Add(tmp.Column(columnAndColumnAlias[i], columnAndColumnAlias[i+1]))
		}
	}
	return s
}

// QueryGroup Constructing query groups.
type QueryGroup interface {
	IsEmpty

	Cmder

	Group(columns ...string) QueryGroup

	Having(having func(having Filter)) QueryGroup
}

type queryGroup struct {
	group    []string
	groupMap map[string]int
	having   Filter
	way      *Way
}

func (s *queryGroup) IsEmpty() bool {
	return len(s.group) == 0
}

func (s *queryGroup) Cmd() (prepare string, args []any) {
	if s.IsEmpty() {
		return
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(ConcatString(SqlGroupBy, SqlSpace))
	b.WriteString(strings.Join(s.way.Replaces(s.group), SqlConcat))
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

func (s *queryGroup) Group(columns ...string) QueryGroup {
	for _, column := range columns {
		if column == EmptyString {
			continue
		}
		if _, ok := s.groupMap[column]; ok {
			continue
		}
		s.groupMap[column] = len(s.group)
		s.group = append(s.group, column)
	}
	return s
}

func (s *queryGroup) Having(having func(having Filter)) QueryGroup {
	if having != nil {
		having(s.having)
	}
	return s
}

func NewQueryGroup(way *Way) QueryGroup {
	return &queryGroup{
		group:    make([]string, 0, 2),
		groupMap: make(map[string]int, 2),
		having:   way.F(),
		way:      way,
	}
}

// QueryOrder Constructing query orders.
type QueryOrder interface {
	IsEmpty

	Cmder

	Asc(columns ...string) QueryOrder

	Desc(columns ...string) QueryOrder
}

type queryOrder struct {
	orderBy  []string
	orderMap map[string]int
	way      *Way
}

func (s *queryOrder) IsEmpty() bool {
	return len(s.orderBy) == 0
}

func (s *queryOrder) Cmd() (prepare string, args []any) {
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

func (s *queryOrder) Asc(columns ...string) QueryOrder {
	index := len(s.orderBy)
	for _, column := range columns {
		if column == EmptyString {
			continue
		}
		if _, ok := s.orderMap[column]; ok {
			continue
		}
		s.orderMap[column] = index
		index++
		order := s.way.Replace(column)
		order = fmt.Sprintf("%s %s", order, SqlAsc)
		s.orderBy = append(s.orderBy, order)
	}
	return s
}

func (s *queryOrder) Desc(columns ...string) QueryOrder {
	index := len(s.orderBy)
	for _, column := range columns {
		if column == EmptyString {
			continue
		}
		if _, ok := s.orderMap[column]; ok {
			continue
		}
		s.orderMap[column] = index
		index++
		order := s.way.Replace(column)
		order = fmt.Sprintf("%s %s", order, SqlDesc)
		s.orderBy = append(s.orderBy, order)
	}
	return s
}

func NewQueryOrder(way *Way) QueryOrder {
	return &queryOrder{
		orderBy:  make([]string, 0, 2),
		orderMap: make(map[string]int, 2),
		way:      way,
	}
}

// QueryLimit Constructing query limits.
type QueryLimit interface {
	IsEmpty

	Cmder

	Limit(limit int64) QueryLimit

	Offset(offset int64) QueryLimit

	Page(page int64, limit ...int64) QueryLimit
}

type queryLimit struct {
	limit  *int64
	offset *int64
}

func (s *queryLimit) IsEmpty() bool {
	return s.limit == nil
}

func (s *queryLimit) Cmd() (prepare string, args []any) {
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

func (s *queryLimit) Limit(limit int64) QueryLimit {
	if limit > 0 {
		s.limit = &limit
	}
	return s
}

func (s *queryLimit) Offset(offset int64) QueryLimit {
	if offset > 0 {
		s.offset = &offset
	}
	return s
}

func (s *queryLimit) Page(page int64, limit ...int64) QueryLimit {
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

func NewQueryLimit() QueryLimit {
	return &queryLimit{}
}

// UpsertColumns Constructing insert columns.
type UpsertColumns interface {
	IsEmpty

	Cmder

	Add(columns ...string) UpsertColumns

	Del(columns ...string) UpsertColumns

	DelUseIndex(indexes ...int) UpsertColumns

	ColumnIndex(column string) int

	ColumnExists(column string) bool

	Len() int

	SetColumns(columns []string) UpsertColumns

	GetColumns() []string

	GetColumnsMap() map[string]*struct{}
}

type upsertColumns struct {
	columns    []string
	columnsMap map[string]int
	way        *Way
}

func NewUpsertColumns(way *Way) UpsertColumns {
	return &upsertColumns{
		columns:    make([]string, 0, 16),
		columnsMap: make(map[string]int, 16),
		way:        way,
	}
}

func (s *upsertColumns) IsEmpty() bool {
	return len(s.columns) == 0
}

func (s *upsertColumns) Cmd() (prepare string, args []any) {
	if s.IsEmpty() {
		return
	}
	return ParcelPrepare(strings.Join(s.way.Replaces(s.columns), SqlConcat)), nil
}

func (s *upsertColumns) Add(columns ...string) UpsertColumns {
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

func (s *upsertColumns) Del(columns ...string) UpsertColumns {
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

func (s *upsertColumns) DelUseIndex(indexes ...int) UpsertColumns {
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

func (s *upsertColumns) ColumnIndex(column string) int {
	index, ok := s.columnsMap[column]
	if !ok {
		return -1
	}
	return index
}

func (s *upsertColumns) ColumnExists(column string) bool {
	return s.ColumnIndex(column) >= 0
}

func (s *upsertColumns) SetColumns(columns []string) UpsertColumns {
	return s.Del().Add(columns...)
}

func (s *upsertColumns) GetColumns() []string {
	return s.columns[:]
}

func (s *upsertColumns) GetColumnsMap() map[string]*struct{} {
	result := make(map[string]*struct{}, len(s.columns))
	for _, column := range s.GetColumns() {
		result[column] = &struct{}{}
	}
	return result
}

func (s *upsertColumns) Len() int {
	return len(s.columns)
}

// InsertValue Constructing insert values.
type InsertValue interface {
	IsEmpty

	Cmder

	SetSubquery(subquery Cmder) InsertValue

	SetValues(values ...[]any) InsertValue

	Set(index int, value any) InsertValue

	Del(indexes ...int) InsertValue

	LenValues() int

	GetValues() [][]any
}

type insertValue struct {
	subquery Cmder
	values   [][]any
}

func NewInsertValue() InsertValue {
	return &insertValue{}
}

func (s *insertValue) IsEmpty() bool {
	return s.subquery == nil && (len(s.values) == 0 || len(s.values[0]) == 0)
}

func (s *insertValue) Cmd() (prepare string, args []any) {
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

func (s *insertValue) SetSubquery(subquery Cmder) InsertValue {
	if subquery != nil {
		if prepare, _ := subquery.Cmd(); prepare == EmptyString {
			return s
		}
	}
	s.subquery = subquery
	return s
}

func (s *insertValue) SetValues(values ...[]any) InsertValue {
	s.values = values
	return s
}

func (s *insertValue) Set(index int, value any) InsertValue {
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

func (s *insertValue) Del(indexes ...int) InsertValue {
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

func (s *insertValue) LenValues() int {
	return len(s.values)
}

func (s *insertValue) GetValues() [][]any {
	return s.values
}

// UpdateSet Constructing update sets.
type UpdateSet interface {
	IsEmpty

	Cmder

	Update(update string, args ...any) UpdateSet

	Set(column string, value any) UpdateSet

	Decr(column string, decr any) UpdateSet

	Incr(column string, incr any) UpdateSet

	SetMap(columnValue map[string]any) UpdateSet

	SetSlice(columns []string, values []any) UpdateSet

	Len() int

	GetUpdate() (updates []string, args [][]any)

	UpdateIndex(prepare string) int

	UpdateExists(prepare string) bool
}

type updateSet struct {
	updateExpr []string
	updateArgs [][]any
	updateMap  map[string]int
	way        *Way
}

func NewUpdateSet(way *Way) UpdateSet {
	return &updateSet{
		updateExpr: make([]string, 0, 8),
		updateArgs: make([][]any, 0, 8),
		updateMap:  make(map[string]int, 8),
		way:        way,
	}
}

func (s *updateSet) IsEmpty() bool {
	return len(s.updateExpr) == 0
}

func (s *updateSet) Cmd() (prepare string, args []any) {
	if s.IsEmpty() {
		return
	}
	prepare = strings.Join(s.updateExpr, SqlConcat)
	for _, tmp := range s.updateArgs {
		args = append(args, tmp...)
	}
	return
}

func (s *updateSet) beautifyUpdate(update string) string {
	update = strings.TrimSpace(update)
	for strings.Contains(update, "  ") {
		update = strings.ReplaceAll(update, "  ", SqlSpace)
	}
	return update
}

func (s *updateSet) Update(update string, args ...any) UpdateSet {
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

func (s *updateSet) Set(column string, value any) UpdateSet {
	column = s.way.Replace(column)
	return s.Update(fmt.Sprintf("%s = %s", column, SqlPlaceholder), value)
}

func (s *updateSet) Decr(column string, decrement any) UpdateSet {
	column = s.way.Replace(column)
	return s.Update(fmt.Sprintf("%s = %s - %s", column, column, SqlPlaceholder), decrement)
}

func (s *updateSet) Incr(column string, increment any) UpdateSet {
	s.way.Replace(column)
	return s.Update(fmt.Sprintf("%s = %s + %s", column, column, SqlPlaceholder), increment)
}

func (s *updateSet) SetMap(columnValue map[string]any) UpdateSet {
	for column, value := range columnValue {
		s.Set(column, value)
	}
	return s
}

func (s *updateSet) SetSlice(columns []string, values []any) UpdateSet {
	for index, column := range columns {
		s.Set(column, values[index])
	}
	return s
}

func (s *updateSet) Len() int {
	return len(s.updateExpr)
}

func (s *updateSet) GetUpdate() ([]string, [][]any) {
	return s.updateExpr, s.updateArgs
}

func (s *updateSet) UpdateIndex(update string) int {
	update = s.beautifyUpdate(update)
	index, ok := s.updateMap[update]
	if !ok {
		return -1
	}
	return index
}

func (s *updateSet) UpdateExists(update string) bool {
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
