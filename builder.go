package hey

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

const (
	SqlAs   = "AS"
	SqlAsc  = "ASC"
	SqlDesc = "DESC"
)

type JoinType string

const (
	SqlJoinInner JoinType = "INNER JOIN"
	SqlJoinLeft  JoinType = "LEFT JOIN"
	SqlJoinRight JoinType = "RIGHT JOIN"
	SqlJoinFull  JoinType = "FULL JOIN"
)

func comment(comment string) (builder *strings.Builder) {
	builder = &strings.Builder{}
	comment = strings.TrimSpace(comment)
	if comment == "" {
		return
	}
	builder.WriteString("/* ")
	builder.WriteString(comment)
	builder.WriteString(" */ ")
	return
}

type schema struct {
	ctx     context.Context // context
	way     *Way            // way
	comment string          // sql statement comment
	table   string          // table name
}

func newSchema(way *Way) *schema {
	return &schema{
		ctx: context.Background(),
		way: way,
	}
}

type Del struct {
	schema *schema
	where  Filter
}

func NewDel(way *Way) *Del {
	return &Del{
		schema: newSchema(way),
	}
}

func (s *Del) Comment(comment string) *Del {
	s.schema.comment = comment
	return s
}

func (s *Del) Context(ctx context.Context) *Del {
	s.schema.ctx = ctx
	return s
}

func (s *Del) Table(table string) *Del {
	s.schema.table = table
	return s
}

func (s *Del) Where(where Filter) *Del {
	s.where = where
	return s
}

func (s *Del) WhereFunc(where func(f Filter)) *Del {
	newFilter := NewFilter()
	where(newFilter)
	return s.Where(newFilter)
}

func (s *Del) WhereIn(column string, values ...interface{}) *Del {
	return s.Where(NewFilter().In(column, values...))
}

func (s *Del) WhereEqual(column string, values interface{}) *Del {
	return s.Where(NewFilter().Equal(column, values))
}

func (s *Del) SQL() (prepare string, args []interface{}) {
	if s.schema.table == "" {
		return
	}
	buf := comment(s.schema.comment)
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

func (s *Del) Del() (int64, error) {
	prepare, args := s.SQL()
	return s.schema.way.ExecContext(s.schema.ctx, prepare, args...)
}

type Add struct {
	schema   *schema
	column   []string
	values   [][]interface{}
	except   []string
	subQuery *SubQuery
}

func NewAdd(way *Way) *Add {
	return &Add{
		schema: newSchema(way),
	}
}

func (s *Add) Comment(comment string) *Add {
	s.schema.comment = comment
	return s
}

func (s *Add) Context(ctx context.Context) *Add {
	s.schema.ctx = ctx
	return s
}

func (s *Add) Table(table string) *Add {
	s.schema.table = table
	return s
}

func (s *Add) Column(column ...string) *Add {
	s.column = column
	return s
}

func (s *Add) Values(values ...[]interface{}) *Add {
	s.values = values
	return s
}

func (s *Add) AppendValues(values ...[]interface{}) *Add {
	s.values = append(s.values, values...)
	return s
}

func (s *Add) ColumnValues(column []string, values ...[]interface{}) *Add {
	s.column, s.values = column, values
	return s
}

func (s *Add) Except(except ...string) *Add {
	s.except = append(s.except, except...) // only valid for Create and Map methods
	return s
}

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

func (s *Add) ValuesSubQuery(prepare string, args ...interface{}) *Add {
	s.subQuery = NewSubQuery(prepare, args)
	return s
}

func (s *Add) ValuesSubQueryGet(get *Get) *Add {
	prepare, args := get.SQL()
	s.subQuery = NewSubQuery(prepare, args)
	return s
}

func (s *Add) SQL() (prepare string, args []interface{}) {
	length := len(s.column)
	if length == 0 {
		return
	}
	if s.subQuery != nil {
		buf := comment(s.schema.comment)
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
		place[i] = "?"
	}
	length = len(s.values)
	if length == 0 {
		return
	}
	buf := comment(s.schema.comment)
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

func (s *Add) Add() (int64, error) {
	prepare, args := s.SQL()
	return s.schema.way.ExecContext(s.schema.ctx, prepare, args...)
}

type modify struct {
	expr string
	args []interface{}
}

type Mod struct {
	schema *schema
	update map[string]*modify
	except map[string]struct{}
	where  Filter
}

func NewMod(way *Way) *Mod {
	return &Mod{
		schema: newSchema(way),
		update: make(map[string]*modify),
		except: make(map[string]struct{}),
	}
}

func (s *Mod) Comment(comment string) *Mod {
	s.schema.comment = comment
	return s
}

func (s *Mod) Context(ctx context.Context) *Mod {
	s.schema.ctx = ctx
	return s
}

func (s *Mod) Table(table string) *Mod {
	s.schema.table = table
	return s
}

func (s *Mod) Except(except ...string) *Mod {
	length := len(except)
	for i := 0; i < length; i++ {
		s.except[except[i]] = struct{}{}
	}
	return s
}

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

func (s *Mod) Set(column string, value interface{}) *Mod {
	return s.columnExpr(column, fmt.Sprintf("%s = %s", column, Placeholder), value)
}

func (s *Mod) Incr(column string, value interface{}) *Mod {
	return s.columnExpr(column, fmt.Sprintf("%s = %s + %s", column, column, Placeholder), value)
}

func (s *Mod) Decr(column string, value interface{}) *Mod {
	return s.columnExpr(column, fmt.Sprintf("%s = %s - %s", column, column, Placeholder), value)
}

func (s *Mod) Map(columnValue map[string]interface{}) *Mod {
	for column, value := range columnValue {
		s.Set(column, value)
	}
	return s
}

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

func (s *Mod) Compare(origin interface{}, latest interface{}) *Mod {
	return s.Map(StructUpdate(origin, latest, s.schema.way.tag))
}

func (s *Mod) Where(where Filter) *Mod {
	s.where = where
	return s
}

func (s *Mod) WhereFunc(where func(f Filter)) *Mod {
	newFilter := NewFilter()
	where(newFilter)
	return s.Where(newFilter)
}

func (s *Mod) WhereIn(column string, values ...interface{}) *Mod {
	return s.Where(NewFilter().In(column, values...))
}

func (s *Mod) WhereEqual(column string, value interface{}) *Mod {
	return s.Where(NewFilter().Equal(column, value))
}

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
	buf := comment(s.schema.comment)
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

type GetJoin struct {
	joinType JoinType  // join type
	table    string    // table name
	subQuery *SubQuery // table is sub query
	alias    *string   // query table alias name
	on       string    // conditions for join query
}

func NewGetJoin(joinType JoinType, table ...string) *GetJoin {
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

func (s *GetJoin) Table(table string) *GetJoin {
	s.table = table
	return s
}

func (s *GetJoin) TableSubQuery(prepare string, args ...interface{}) *GetJoin {
	s.subQuery = NewSubQuery(prepare, args...)
	return s
}

func (s *GetJoin) TableSubQueryGet(get *Get) *GetJoin {
	prepare, args := get.SQL()
	s.subQuery = NewSubQuery(prepare, args...)
	return s
}

func (s *GetJoin) TableAlias(alias string) *GetJoin {
	s.alias = &alias
	return s
}

func (s *GetJoin) On(on string) *GetJoin {
	s.on = on
	return s
}

func (s *GetJoin) OnEqual(left string, right string) *GetJoin {
	return s.On(fmt.Sprintf("%s = %s", left, right))
}

func (s *GetJoin) SQL() (prepare string, args []interface{}) {
	str := &strings.Builder{}
	str.WriteString(fmt.Sprintf("%s", s.joinType))
	str.WriteString(" ")
	if s.subQuery != nil && s.alias != nil && *s.alias != "" {
		str.WriteString("( ")
		subPrepare, subArgs := s.subQuery.SQL()
		str.WriteString(subPrepare)
		str.WriteString(" )")
		str.WriteString(" AS ")
		str.WriteString(*s.alias)
		str.WriteString(" ")
		str.WriteString("ON")
		str.WriteString(" ")
		str.WriteString(s.on)
		prepare = str.String()
		args = append(args, subArgs...)
		return
	}
	if s.table != "" {
		str.WriteString(s.table)
		str.WriteString(" ")
		if s.alias != nil && *s.alias != "" {
			str.WriteString("AS ")
			str.WriteString(*s.alias)
			str.WriteString(" ")
		}
		str.WriteString("ON")
		str.WriteString(" ")
		str.WriteString(s.on)
		prepare = str.String()
		return
	}
	return
}

type Get struct {
	schema   *schema    // query table
	column   []string   // query field list
	subQuery *SubQuery  // the query table is a sub query
	alias    *string    // set an alias for the queried table
	join     []*GetJoin // join query
	where    Filter     // WHERE condition to filter data
	group    []string   // group query results
	having   Filter     // use HAVING to filter data after grouping
	order    []string   // sort query results
	limit    *int64     // limit the number of query results
	offset   *int64     // query results offset
}

func NewGet(way *Way) *Get {
	return &Get{
		schema: newSchema(way),
	}
}

func (s *Get) Comment(comment string) *Get {
	s.schema.comment = comment
	return s
}

func (s *Get) Context(ctx context.Context) *Get {
	s.schema.ctx = ctx
	return s
}

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

func (s *Get) TableSubQuery(prepare string, args ...interface{}) *Get {
	s.subQuery = NewSubQuery(prepare, args...)
	return s
}

func (s *Get) TableSubQueryGet(get *Get) *Get {
	prepare, args := get.SQL()
	s.subQuery = NewSubQuery(prepare, args...)
	return s
}

func (s *Get) TableAlias(alias string) *Get {
	s.alias = &alias
	return s
}

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

func (s *Get) InnerJoin(table ...string) *GetJoin {
	return NewGetJoin(SqlJoinInner, table...)
}

func (s *Get) LeftJoin(table ...string) *GetJoin {
	return NewGetJoin(SqlJoinLeft, table...)
}

func (s *Get) RightJoin(table ...string) *GetJoin {
	return NewGetJoin(SqlJoinRight, table...)
}

func (s *Get) FullJoin(table ...string) *GetJoin {
	return NewGetJoin(SqlJoinFull, table...)
}

func (s *Get) Where(where Filter) *Get {
	s.where = where
	return s
}

func (s *Get) WhereFunc(where func(f Filter)) *Get {
	newFilter := NewFilter()
	where(newFilter)
	return s.Where(newFilter)
}

func (s *Get) WhereIn(column string, values ...interface{}) *Get {
	return s.Where(NewFilter().In(column, values...))
}

func (s *Get) WhereEqual(column string, value interface{}) *Get {
	return s.Where(NewFilter().Equal(column, value))
}

func (s *Get) Group(group ...string) *Get {
	s.group = append(s.group, group...)
	return s
}

func (s *Get) Having(having Filter) *Get {
	s.having = having
	return s
}

func (s *Get) HavingFunc(having func(f Filter)) *Get {
	newFilter := NewFilter()
	having(newFilter)
	return s.Having(newFilter)
}

func (s *Get) Alias(name string, alias string) string {
	return fmt.Sprintf("%s %s %s", name, SqlAs, alias)
}

func (s *Get) Prefix(prefix string, name string) string {
	return fmt.Sprintf("%s.%s", prefix, name)
}

func (s *Get) PrefixGroup(prefix string, items ...string) []string {
	for key, val := range items {
		items[key] = s.Prefix(prefix, val)
	}
	return items
}

func (s *Get) AddCol(column ...string) *Get {
	s.column = append(s.column, column...)
	return s
}

func (s *Get) Column(column ...string) *Get {
	s.column = column
	return s
}

func (s *Get) Order(column string, order string) *Get {
	if column == "" || (order != SqlAsc && order != SqlDesc) {
		return s
	}
	s.order = append(s.order, fmt.Sprintf("%s %s", column, order))
	return s
}

func (s *Get) OrderAsc(column string) *Get {
	return s.Order(column, SqlAsc)
}

func (s *Get) OrderDesc(column string) *Get {
	return s.Order(column, SqlDesc)
}

func (s *Get) Limit(limit int64) *Get {
	s.limit = &limit
	return s
}

func (s *Get) Offset(offset int64) *Get {
	s.offset = &offset
	return s
}

func (s *Get) SQLCount(column ...string) (prepare string, args []interface{}) {
	if s.schema.table == "" && s.subQuery == nil {
		return
	}
	buf := comment(s.schema.comment)
	buf.WriteString("SELECT ")
	if column == nil {
		buf.WriteString(s.Alias("COUNT(*)", "count"))
	} else {
		buf.WriteString(strings.Join(column, ", "))
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
	buf.WriteString(" ")
	if s.alias != nil && *s.alias != "" {
		buf.WriteString("AS ")
		buf.WriteString(*s.alias)
		buf.WriteString(" ")
	}
	if s.join != nil {
		length := len(s.join)
		for i := 0; i < length; i++ {
			if s.join[i] == nil {
				continue
			}
			joinPrepare, joinArgs := s.join[i].SQL()
			if joinPrepare != "" {
				buf.WriteString(joinPrepare)
				buf.WriteString(" ")
				args = append(args, joinArgs...)
			}
		}
	}
	if s.where != nil {
		where, whereArgs := s.where.SQL()
		if where != "" {
			buf.WriteString("WHERE ")
			buf.WriteString(where)
			buf.WriteString(" ")
			args = append(args, whereArgs...)
		}
	}
	prepare = strings.TrimSpace(buf.String())
	return
}

func (s *Get) SQL() (prepare string, args []interface{}) {
	if s.schema.table == "" && s.subQuery == nil {
		return
	}
	buf := comment(s.schema.comment)
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
	buf.WriteString(" ")
	if s.alias != nil && *s.alias != "" {
		buf.WriteString("AS ")
		buf.WriteString(*s.alias)
		buf.WriteString(" ")
	}
	if s.join != nil {
		length := len(s.join)
		for i := 0; i < length; i++ {
			if s.join[i] == nil {
				continue
			}
			joinPrepare, joinArgs := s.join[i].SQL()
			if joinPrepare != "" {
				buf.WriteString(joinPrepare)
				buf.WriteString(" ")
				args = append(args, joinArgs...)
			}
		}
	}
	if s.where != nil {
		where, whereArgs := s.where.SQL()
		if where != "" {
			buf.WriteString("WHERE ")
			buf.WriteString(where)
			buf.WriteString(" ")
			args = append(args, whereArgs...)
		}
	}
	if s.group != nil {
		if len(s.group) > 0 {
			buf.WriteString("GROUP BY ")
			buf.WriteString(strings.Join(s.group, ", "))
			buf.WriteString(" ")
		}
	}
	if s.having != nil {
		having, havingArgs := s.having.SQL()
		if having != "" {
			buf.WriteString("HAVING ")
			buf.WriteString(having)
			buf.WriteString(" ")
			args = append(args, havingArgs...)
		}
	}
	if s.order != nil {
		if len(s.order) > 0 {
			buf.WriteString("ORDER BY ")
			buf.WriteString(strings.Join(s.order, ", "))
			buf.WriteString(" ")
		}
	}
	if s.limit != nil {
		if s.limit != nil {
			buf.WriteString(fmt.Sprintf("LIMIT %d", *s.limit))
			buf.WriteString(" ")
			if s.offset != nil {
				buf.WriteString(fmt.Sprintf("OFFSET %d", *s.offset))
				buf.WriteString(" ")
			}
		}
	}
	prepare = strings.TrimSpace(buf.String())
	return
}

func (s *Get) Query(query func(rows *sql.Rows) (err error)) error {
	prepare, args := s.SQL()
	return s.schema.way.QueryContext(s.schema.ctx, query, prepare, args...)
}

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

func (s *Get) Get(result interface{}) error {
	prepare, args := s.SQL()
	return s.schema.way.ScanAllContext(s.schema.ctx, result, prepare, args...)
}

func (s *Get) CountGet(result interface{}, countColumn ...string) (count int64, err error) {
	count, err = s.Count(countColumn...)
	if err != nil || count == 0 {
		return
	}
	err = s.Get(result)
	return
}
