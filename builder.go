package hey

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

const (
	SqlAs       = "AS"
	SqlAsc      = "ASC"
	SqlDesc     = "DESC"
	SqlUnion    = "UNION"
	SqlUnionAll = "UNION ALL"
)

const (
	SqlJoinInner = "INNER JOIN"
	SqlJoinLeft  = "LEFT JOIN"
	SqlJoinRight = "RIGHT JOIN"
	SqlJoinFull  = "FULL JOIN"
)

// schema used to store basic information such as context.Context, *Way, SQL comment, table name.
type schema struct {
	ctx     context.Context
	way     *Way
	comment string
	table   string
}

// comment make SQL statement builder
func comment(schema *schema) (builder *strings.Builder) {
	builder = &strings.Builder{}
	schema.comment = strings.TrimSpace(schema.comment)
	if schema.comment == "" {
		return
	}
	builder.WriteString("/* ")
	builder.WriteString(schema.comment)
	builder.WriteString(" */")
	return
}

// newSchema new schema with *Way
func newSchema(way *Way) *schema {
	return &schema{
		ctx: context.Background(),
		way: way,
	}
}

// Del for DELETE
type Del struct {
	schema *schema
	where  Filter
}

// NewDel for DELETE
func NewDel(way *Way) *Del {
	return &Del{
		schema: newSchema(way),
	}
}

// Comment set comment
func (s *Del) Comment(comment string) *Del {
	s.schema.comment = comment
	return s
}

// Context set context
func (s *Del) Context(ctx context.Context) *Del {
	s.schema.ctx = ctx
	return s
}

// Table set table name
func (s *Del) Table(table string) *Del {
	s.schema.table = table
	return s
}

// Where set where
func (s *Del) Where(where Filter) *Del {
	s.where = where
	return s
}

// WhereIn set where
func (s *Del) WhereIn(column string, values ...interface{}) *Del {
	return s.Where(NewFilter().In(column, values...))
}

// WhereEqual set where
func (s *Del) WhereEqual(column string, values interface{}) *Del {
	return s.Where(NewFilter().Equal(column, values))
}

// SQL build SQL statement
func (s *Del) SQL() (prepare string, args []interface{}) {
	if s.schema.table == "" {
		return
	}
	buf := comment(s.schema)
	buf.WriteString("DELETE FROM ")
	buf.WriteString(s.schema.table)
	if s.where != nil {
		where, whereArgs := s.where.SQL()
		if where != "" {
			buf.WriteString(" WHERE ")
			buf.WriteString(where)
			args = whereArgs
		}
	}
	prepare = buf.String()
	return
}

// Del execute the built SQL statement
func (s *Del) Del() (int64, error) {
	prepare, args := s.SQL()
	return s.schema.way.ExecContext(s.schema.ctx, prepare, args...)
}

// Add for INSERT
type Add struct {
	schema   *schema
	column   []string
	values   [][]interface{}
	except   []string
	subQuery *SubQuery
}

// NewAdd for INSERT
func NewAdd(way *Way) *Add {
	return &Add{
		schema: newSchema(way),
	}
}

// Comment set comment
func (s *Add) Comment(comment string) *Add {
	s.schema.comment = comment
	return s
}

// Context set context
func (s *Add) Context(ctx context.Context) *Add {
	s.schema.ctx = ctx
	return s
}

// Table set table name
func (s *Add) Table(table string) *Add {
	s.schema.table = table
	return s
}

// Column set column
func (s *Add) Column(column ...string) *Add {
	s.column = column
	return s
}

// Values set values, insert one or more rows
func (s *Add) Values(values ...[]interface{}) *Add {
	s.values = values
	return s
}

// AppendValues append values, insert one or more rows
func (s *Add) AppendValues(values ...[]interface{}) *Add {
	s.values = append(s.values, values...)
	return s
}

// ColumnValues set column list and column values at the same time, insert one or more rows
func (s *Add) ColumnValues(column []string, values ...[]interface{}) *Add {
	s.column, s.values = column, values
	return s
}

// Except exclude some columns from insert, only valid for Create and Map methods
func (s *Add) Except(except ...string) *Add {
	s.except = append(s.except, except...)
	return s
}

// Create insert by struct, insert one or more rows
func (s *Add) Create(creates ...interface{}) *Add {
	length := len(creates)
	if length == 0 {
		return s
	}
	column, values := make([][]string, length), make([][]interface{}, length)
	count := 0
	for i := 0; i < length; i++ {
		column[i], values[i] = StructInsert(creates[i], s.schema.way.tag, s.except...)
		lc, lv := len(column[i]), len(values[i])
		if lc != lv {
			return s
		}
		if i == 0 {
			count = lc
			continue
		}
		if lc != count {
			return s
		}
	}
	return s.ColumnValues(column[0], values...)
}

// Map insert by map, insert one row
func (s *Add) Map(columnValue map[string]interface{}) *Add {
	length := len(columnValue)
	if length == 0 {
		return s
	}
	except := make(map[string]struct{})
	for c := range except {
		except[c] = struct{}{}
	}
	columns := make([]string, 0, length)
	for column := range columnValue {
		if _, ok := except[column]; ok {
			continue
		}
		columns = append(columns, column)
	}
	sort.Strings(columns)
	length = len(columns)
	values := make([]interface{}, length)
	for key, val := range columns {
		values[key] = columnValue[val]
	}
	return s.ColumnValues(columns, values)
}

// ValuesSubQuery values is a query SQL statement
func (s *Add) ValuesSubQuery(prepare string, args ...interface{}) *Add {
	s.subQuery = NewSubQuery(prepare, args)
	return s
}

// ValuesSubQueryGet values is a query SQL statement
func (s *Add) ValuesSubQueryGet(get *Get) *Add {
	if get == nil {
		return s
	}
	prepare, args := get.SQL()
	s.subQuery = NewSubQuery(prepare, args)
	return s
}

// SQL build SQL statement
func (s *Add) SQL() (prepare string, args []interface{}) {
	length := len(s.column)
	if length == 0 {
		return
	}
	if s.subQuery != nil {
		buf := comment(s.schema)
		buf.WriteString("INSERT INTO ")
		buf.WriteString(s.schema.table)
		if len(s.column) > 0 {
			buf.WriteString(" ( ")
			buf.WriteString(strings.Join(s.column, ", "))
			buf.WriteString(" )")
		}
		buf.WriteString(" ")
		qp, qa := s.subQuery.SQL()
		buf.WriteString(qp)
		prepare = buf.String()
		args = qa
		return
	}
	if s.schema.table == "" {
		return
	}
	place := make([]string, length)
	for i := 0; i < length; i++ {
		place[i] = Placeholder
	}
	length = len(s.values)
	if length == 0 {
		return
	}
	buf := comment(s.schema)
	buf.WriteString("INSERT INTO ")
	buf.WriteString(s.schema.table)
	buf.WriteString(" ( ")
	buf.WriteString(strings.Join(s.column, ", "))
	buf.WriteString(" ) ")
	buf.WriteString(" VALUES")
	for i := 0; i < length; i++ {
		if i != 0 {
			buf.WriteString(",")
		}
		buf.WriteString(" ( ")
		buf.WriteString(strings.Join(place, ", "))
		buf.WriteString(" )")
		args = append(args, s.values[i]...)
	}
	prepare = buf.String()
	return
}

// Add execute the built SQL statement
func (s *Add) Add() (int64, error) {
	prepare, args := s.SQL()
	return s.schema.way.ExecContext(s.schema.ctx, prepare, args...)
}

// modify set the column to be updated
type modify struct {
	expr string
	args []interface{}
}

// Mod for UPDATE
type Mod struct {
	schema *schema
	update map[string]*modify
	except map[string]struct{}
	where  Filter
}

// NewMod for UPDATE
func NewMod(way *Way) *Mod {
	return &Mod{
		schema: newSchema(way),
		update: make(map[string]*modify),
		except: make(map[string]struct{}),
	}
}

// Comment set comment
func (s *Mod) Comment(comment string) *Mod {
	s.schema.comment = comment
	return s
}

// Context set context
func (s *Mod) Context(ctx context.Context) *Mod {
	s.schema.ctx = ctx
	return s
}

// Table set table name
func (s *Mod) Table(table string) *Mod {
	s.schema.table = table
	return s
}

// Except exclude some columns from update
func (s *Mod) Except(except ...string) *Mod {
	length := len(except)
	for i := 0; i < length; i++ {
		s.except[except[i]] = struct{}{}
	}
	return s
}

// columnExpr build update column expressions and column values
func (s *Mod) columnExpr(column string, expr string, args ...interface{}) *Mod {
	if _, ok := s.except[column]; ok {
		return s
	}
	s.update[column] = &modify{
		expr: expr,
		args: args,
	}
	return s
}

// Set SET column = value
func (s *Mod) Set(column string, value interface{}) *Mod {
	return s.columnExpr(column, fmt.Sprintf("%s = %s", column, Placeholder), value)
}

// Incr SET column = column + value
func (s *Mod) Incr(column string, value interface{}) *Mod {
	return s.columnExpr(column, fmt.Sprintf("%s = %s + %s", column, column, Placeholder), value)
}

// Decr SET column = column - value
func (s *Mod) Decr(column string, value interface{}) *Mod {
	return s.columnExpr(column, fmt.Sprintf("%s = %s - %s", column, column, Placeholder), value)
}

// Map SET column = value by map
func (s *Mod) Map(columnValue map[string]interface{}) *Mod {
	for column, value := range columnValue {
		s.Set(column, value)
	}
	return s
}

// Slice SET column = value by slice, require len(column) = len(value)
func (s *Mod) Slice(column []string, value []interface{}) *Mod {
	len1, len2 := len(column), len(value)
	if len1 != len2 {
		return s
	}
	for i := 0; i < len1; i++ {
		s.Set(column[i], value[i])
	}
	return s
}

// Compare for compare origin and latest to automatically calculate the list of columns and corresponding values that need to be updated
func (s *Mod) Compare(origin interface{}, latest interface{}) *Mod {
	return s.Map(StructUpdate(origin, latest, s.schema.way.tag))
}

// Where set where
func (s *Mod) Where(where Filter) *Mod {
	s.where = where
	return s
}

// WhereIn set where
func (s *Mod) WhereIn(column string, values ...interface{}) *Mod {
	return s.Where(NewFilter().In(column, values...))
}

// WhereEqual set where
func (s *Mod) WhereEqual(column string, value interface{}) *Mod {
	return s.Where(NewFilter().Equal(column, value))
}

// SQL build SQL statement
func (s *Mod) SQL() (prepare string, args []interface{}) {
	if s.schema.table == "" {
		return
	}
	length := len(s.update)
	if length == 0 {
		return
	}
	columns := make([]string, 0, length)
	for column := range s.update {
		if _, ok := s.except[column]; ok {
			continue
		}
		columns = append(columns, column)
	}
	length = len(columns)
	sort.Strings(columns)
	field := make([]string, length)
	value := make([]interface{}, 0, length)
	for k, v := range columns {
		field[k] = s.update[v].expr
		value = append(value, s.update[v].args...)
	}
	args = value
	buf := comment(s.schema)
	buf.WriteString("UPDATE ")
	buf.WriteString(s.schema.table)
	buf.WriteString(" SET ")
	buf.WriteString(strings.Join(field, ", "))
	if s.where != nil {
		key, val := s.where.SQL()
		if key != "" {
			buf.WriteString(" WHERE ")
			buf.WriteString(key)
			args = append(args, val...)
		}
	}
	prepare = buf.String()
	return
}

// Mod execute the built SQL statement
func (s *Mod) Mod() (int64, error) {
	prepare, args := s.SQL()
	return s.schema.way.ExecContext(s.schema.ctx, prepare, args...)
}

type SubQuery struct {
	prepare string
	args    []interface{}
}

func NewSubQuery(prepare string, args ...interface{}) *SubQuery {
	return &SubQuery{
		prepare: prepare,
		args:    args,
	}
}

func (s *SubQuery) SQL() (prepare string, args []interface{}) {
	prepare, args = s.prepare, s.args
	return
}

// GetJoin join SQL statement
type GetJoin struct {
	joinType string    // join type
	table    string    // table name
	subQuery *SubQuery // table is sub query
	alias    *string   // query table alias name
	on       string    // conditions for join query
}

// NewGetJoin new join
func NewGetJoin(joinType string, table ...string) *GetJoin {
	getJoin := &GetJoin{
		joinType: joinType,
	}
	for i := len(table) - 1; i >= 0; i-- {
		if table[i] != "" {
			getJoin.Table(table[i])
			break
		}
	}
	return getJoin
}

// Table set table name
func (s *GetJoin) Table(table string) *GetJoin {
	s.table = table
	return s
}

// TableSubQuery table is a query SQL statement
func (s *GetJoin) TableSubQuery(prepare string, args ...interface{}) *GetJoin {
	s.subQuery = NewSubQuery(prepare, args...)
	return s
}

// TableSubQueryGet table is a query SQL statement
func (s *GetJoin) TableSubQueryGet(get *Get, alias ...string) *GetJoin {
	if get == nil {
		return s
	}
	prepare, args := get.SQL()
	s.subQuery = NewSubQuery(prepare, args...)
	for i := len(alias) - 1; i >= 0; i-- {
		if alias[i] != "" {
			s.TableAlias(alias[i])
			break
		}
	}
	return s
}

// TableAlias table alias name
func (s *GetJoin) TableAlias(alias string) *GetJoin {
	s.alias = &alias
	return s
}

// On join query condition
func (s *GetJoin) On(on string) *GetJoin {
	s.on = on
	return s
}

// OnEqual join query condition
func (s *GetJoin) OnEqual(left string, right string) *GetJoin {
	return s.On(fmt.Sprintf("%s = %s", left, right))
}

// SQL build SQL statement
func (s *GetJoin) SQL() (prepare string, args []interface{}) {
	str := &strings.Builder{}
	str.WriteString(s.joinType)
	if s.subQuery != nil && s.alias != nil && *s.alias != "" {
		str.WriteString(" ( ")
		subPrepare, subArgs := s.subQuery.SQL()
		str.WriteString(subPrepare)
		str.WriteString(" ) ")
		str.WriteString("AS ")
		str.WriteString(*s.alias)
		str.WriteString(" ON ")
		str.WriteString(s.on)
		prepare = str.String()
		args = append(args, subArgs...)
		return
	}
	if s.table != "" {
		str.WriteString(" ")
		str.WriteString(s.table)
		if s.alias != nil && *s.alias != "" {
			str.WriteString(" AS ")
			str.WriteString(*s.alias)
		}
		str.WriteString(" ON ")
		str.WriteString(s.on)
		prepare = str.String()
		return
	}
	return
}

// union SQL statement union
type union struct {
	unionType string
	prepare   string
	args      []interface{}
}

// Limiter limit and offset
type Limiter interface {
	GetLimit() int64
	GetOffset() int64
}

// Get for SELECT
type Get struct {
	schema   *schema           // query table
	column   []string          // query field list
	subQuery *SubQuery         // the query table is a sub query
	alias    *string           // set an alias for the queried table
	join     []*GetJoin        // join query
	where    Filter            // WHERE condition to filter data
	group    []string          // group query result
	having   Filter            // use HAVING to filter data after grouping
	union    []*union          // union query
	order    []string          // order query result
	orderMap map[string]string // ordered columns map list
	limit    *int64            // limit the number of query result
	offset   *int64            // query result offset
}

// NewGet for SELECT
func NewGet(way *Way) *Get {
	return &Get{
		schema:   newSchema(way),
		orderMap: map[string]string{},
	}
}

// Comment set comment
func (s *Get) Comment(comment string) *Get {
	s.schema.comment = comment
	return s
}

// Context set context
func (s *Get) Context(ctx context.Context) *Get {
	s.schema.ctx = ctx
	return s
}

// Table set table name
func (s *Get) Table(table string, alias ...string) *Get {
	s.schema.table = table
	for i := len(alias) - 1; i >= 0; i-- {
		if alias[i] != "" {
			s.TableAlias(alias[i])
			break
		}
	}
	return s
}

// TableSubQuery table is a query SQL statement
func (s *Get) TableSubQuery(prepare string, args ...interface{}) *Get {
	s.subQuery = NewSubQuery(prepare, args...)
	return s
}

// TableSubQueryGet table is a query SQL statement
func (s *Get) TableSubQueryGet(get *Get, alias ...string) *Get {
	if get == nil {
		return s
	}
	prepare, args := get.SQL()
	s.subQuery = NewSubQuery(prepare, args...)
	for i := len(alias) - 1; i >= 0; i-- {
		if alias[i] != "" {
			s.TableAlias(alias[i])
			break
		}
	}
	return s
}

// TableAlias table alias name
func (s *Get) TableAlias(alias string) *Get {
	s.alias = &alias
	return s
}

// Join for join one or more tables
func (s *Get) Join(joins ...*GetJoin) *Get {
	length := len(joins)
	for i := 0; i < length; i++ {
		if joins[i] == nil {
			continue
		}
		s.join = append(s.join, joins[i])
	}
	return s
}

// InnerJoin for inner join
func (s *Get) InnerJoin(table ...string) *GetJoin {
	return NewGetJoin(SqlJoinInner, table...)
}

// LeftJoin for left join
func (s *Get) LeftJoin(table ...string) *GetJoin {
	return NewGetJoin(SqlJoinLeft, table...)
}

// RightJoin for right join
func (s *Get) RightJoin(table ...string) *GetJoin {
	return NewGetJoin(SqlJoinRight, table...)
}

// FullJoin for full join
func (s *Get) FullJoin(table ...string) *GetJoin {
	return NewGetJoin(SqlJoinFull, table...)
}

// Where set where
func (s *Get) Where(where Filter) *Get {
	s.where = where
	return s
}

// WhereIn set where
func (s *Get) WhereIn(column string, values ...interface{}) *Get {
	return s.Where(NewFilter().In(column, values...))
}

// WhereEqual set where
func (s *Get) WhereEqual(column string, value interface{}) *Get {
	return s.Where(NewFilter().Equal(column, value))
}

// Group set group columns
func (s *Get) Group(group ...string) *Get {
	s.group = append(s.group, group...)
	return s
}

// Having set filter of group result
func (s *Get) Having(having Filter) *Get {
	s.having = having
	return s
}

// Union union query
func (s *Get) Union(prepare string, args ...interface{}) *Get {
	if prepare == "" {
		return s
	}
	s.union = append(s.union, &union{
		unionType: SqlUnion,
		prepare:   prepare,
		args:      args,
	})
	return s
}

// UnionGet union query
func (s *Get) UnionGet(get ...*Get) *Get {
	for _, g := range get {
		if g == nil {
			continue
		}
		prepare, args := g.SQL()
		s.Union(prepare, args...)
	}
	return s
}

// UnionAll union all query
func (s *Get) UnionAll(prepare string, args ...interface{}) *Get {
	if prepare == "" {
		return s
	}
	s.union = append(s.union, &union{
		unionType: SqlUnionAll,
		prepare:   prepare,
		args:      args,
	})
	return s
}

// UnionAllGet union all query
func (s *Get) UnionAllGet(get ...*Get) *Get {
	for _, g := range get {
		if g == nil {
			continue
		}
		prepare, args := g.SQL()
		s.UnionAll(prepare, args...)
	}
	return s
}

// Alias table or column set alias name
func (s *Get) Alias(name string, alias string) string {
	return fmt.Sprintf("%s %s %s", name, SqlAs, alias)
}

// Prefix table or column set prefix
func (s *Get) Prefix(prefix string, name string) string {
	return fmt.Sprintf("%s.%s", prefix, name)
}

// PrefixGroup table or column batch set prefix
func (s *Get) PrefixGroup(prefix string, items ...string) []string {
	for key, val := range items {
		items[key] = s.Prefix(prefix, val)
	}
	return items
}

// AddCol append the columns list of query
func (s *Get) AddCol(column ...string) *Get {
	s.column = append(s.column, column...)
	return s
}

// Column set the columns list of query
func (s *Get) Column(column ...string) *Get {
	s.column = column
	return s
}

// orderBy set order by column
func (s *Get) orderBy(column string, order string) *Get {
	if column == "" || (order != SqlAsc && order != SqlDesc) {
		return s
	}
	if _, ok := s.orderMap[column]; ok {
		return s
	}
	s.order = append(s.order, fmt.Sprintf("%s %s", column, order))
	s.orderMap[column] = order
	return s
}

// Asc set order by column ASC
func (s *Get) Asc(column string) *Get {
	return s.orderBy(column, SqlAsc)
}

// Desc set order by column Desc
func (s *Get) Desc(column string) *Get {
	return s.orderBy(column, SqlDesc)
}

var (
	// orderByRegexp `column_name_first#a,column_name_second#d` => `column_name_first ASC, column_name_second DESC`
	orderByRegexp = regexp.MustCompile(`^([A-Za-z][A-Za-z0-9_.]{1,31})#([ad])$`)
)

// OrderParam set the column sorting list in batches through regular expressions according to the request parameter value
func (s *Get) OrderParam(param string) *Get {
	for _, v := range strings.Split(param, ",") {
		match := orderByRegexp.FindAllStringSubmatch(strings.TrimSpace(v), -1)
		if len(match) != 1 || len(match[0]) != 3 {
			continue
		}
		if match[0][2][0] == 97 {
			s.Asc(match[0][1])
			continue
		}
		if match[0][2][0] == 100 {
			s.Desc(match[0][1])
		}
	}
	return s
}

// Limit set limit
func (s *Get) Limit(limit int64) *Get {
	if limit <= 0 {
		return s
	}
	s.limit = &limit
	return s
}

// Offset set offset
func (s *Get) Offset(offset int64) *Get {
	if offset < 0 {
		return s
	}
	s.offset = &offset
	return s
}

// Limiter set limit and offset at the same time
func (s *Get) Limiter(limiter Limiter) *Get {
	if limiter == nil {
		return s
	}
	return s.Limit(limiter.GetLimit()).Offset(limiter.GetOffset())
}

// sqlTable build query base table SQL statement
func (s *Get) sqlTable() (prepare string, args []interface{}) {
	if s.schema.table == "" && s.subQuery == nil {
		return
	}
	buf := comment(s.schema)
	buf.WriteString("SELECT ")
	if s.column == nil {
		buf.WriteString("*")
	} else {
		buf.WriteString(strings.Join(s.column, ", "))
	}
	buf.WriteString(" FROM ")
	if s.subQuery == nil {
		buf.WriteString(s.schema.table)
	} else {
		buf.WriteString("( ")
		subPrepare, subArgs := s.subQuery.SQL()
		buf.WriteString(subPrepare)
		buf.WriteString(" )")
		args = append(args, subArgs...)
	}
	if s.alias != nil && *s.alias != "" {
		buf.WriteString(" ")
		buf.WriteString("AS ")
		buf.WriteString(*s.alias)
	}
	if s.join != nil {
		length := len(s.join)
		for i := 0; i < length; i++ {
			if s.join[i] == nil {
				continue
			}
			joinPrepare, joinArgs := s.join[i].SQL()
			if joinPrepare != "" {
				buf.WriteString(" ")
				buf.WriteString(joinPrepare)
				args = append(args, joinArgs...)
			}
		}
	}
	if s.where != nil {
		where, whereArgs := s.where.SQL()
		if where != "" {
			buf.WriteString(" ")
			buf.WriteString("WHERE ")
			buf.WriteString(where)
			args = append(args, whereArgs...)
		}
	}
	if s.group != nil {
		if len(s.group) > 0 {
			buf.WriteString(" ")
			buf.WriteString("GROUP BY ")
			buf.WriteString(strings.Join(s.group, ", "))
			if s.having != nil {
				having, havingArgs := s.having.SQL()
				if having != "" {
					buf.WriteString(" ")
					buf.WriteString("HAVING ")
					buf.WriteString(having)
					args = append(args, havingArgs...)
				}
			}
		}
	}
	if s.union != nil {
		for _, u := range s.union {
			buf.WriteString(" ")
			buf.WriteString(u.unionType)
			buf.WriteString(" ( ")
			buf.WriteString(u.prepare)
			buf.WriteString(" )")
			args = append(args, u.args...)
		}
	}
	prepare = strings.TrimSpace(buf.String())
	return
}

// SQLCount build SQL statement for count
func (s *Get) SQLCount(columns ...string) (prepare string, args []interface{}) {
	if columns == nil {
		columns = []string{s.Alias("COUNT(*)", "count")}
	}
	if s.group != nil || s.union != nil {
		prepare, args = s.sqlTable()
		prepare, args = NewGet(s.schema.way).
			TableSubQuery(prepare, args...).
			TableAlias("count_table_rows").
			SQLCount(columns...)
		return
	}
	column := s.column // store selected columns
	s.column = columns // set count column
	prepare, args = s.sqlTable()
	s.column = column // restore origin selected columns
	return
}

// SQL build SQL statement
func (s *Get) SQL() (prepare string, args []interface{}) {
	prepare, args = s.sqlTable()
	if prepare == "" {
		return
	}
	buf := &strings.Builder{}
	buf.WriteString(prepare)
	if len(s.order) > 0 {
		buf.WriteString(" ")
		buf.WriteString("ORDER BY ")
		buf.WriteString(strings.Join(s.order, ", "))
	}
	if s.limit != nil {
		buf.WriteString(" ")
		buf.WriteString(fmt.Sprintf("LIMIT %d", *s.limit))
		if s.offset != nil {
			buf.WriteString(" ")
			buf.WriteString(fmt.Sprintf("OFFSET %d", *s.offset))
		}
	}
	prepare = strings.TrimSpace(buf.String())
	return
}

// Query execute the built SQL statement and scan query result
func (s *Get) Query(query func(rows *sql.Rows) (err error)) error {
	prepare, args := s.SQL()
	return s.schema.way.QueryContext(s.schema.ctx, query, prepare, args...)
}

// Count execute the built SQL statement and scan query result for count
func (s *Get) Count(column ...string) (count int64, err error) {
	prepare, args := s.SQLCount(column...)
	err = s.schema.way.QueryContext(s.schema.ctx, func(rows *sql.Rows) (err error) {
		if rows.Next() {
			err = rows.Scan(&count)
		}
		return
	}, prepare, args...)
	return
}

// Get execute the built SQL statement and scan query result
func (s *Get) Get(result interface{}) error {
	prepare, args := s.SQL()
	return s.schema.way.ScanAllContext(s.schema.ctx, result, prepare, args...)
}

// CountGet execute the built SQL statement and scan query result, count + get
func (s *Get) CountGet(result interface{}, countColumn ...string) (count int64, err error) {
	count, err = s.Count(countColumn...)
	if err != nil || count == 0 {
		return
	}
	err = s.Get(result)
	return
}
