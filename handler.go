package hey

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

func sqlBuilder(comment string) (builder *strings.Builder) {
	builder = &strings.Builder{}
	if comment == "" {
		return
	}
	builder.WriteString("/* ")
	builder.WriteString(strings.TrimSpace(comment))
	builder.WriteString(" */ ")
	return
}

type Insert struct {
	ctx          context.Context
	way          *Way
	tag          string
	table        string
	field        []string
	value        [][]interface{}
	queryPrepare string
	queryArgs    []interface{}
}

func NewInsert(way *Way) *Insert {
	return &Insert{
		ctx: way.ctx,
		way: way,
	}
}

func (s *Insert) WithContext(ctx context.Context) *Insert {
	s.ctx = ctx
	return s
}

func (s *Insert) Tag(tag string) *Insert {
	s.tag = tag
	return s
}

func (s *Insert) Table(table string) *Insert {
	s.table = table
	return s
}

func (s *Insert) Field(field ...string) *Insert {
	s.field = field
	return s
}

func (s *Insert) Value(value ...[]interface{}) *Insert {
	s.value = value
	return s
}

func (s *Insert) ForMap(fieldValue map[string]interface{}) *Insert {
	length := len(fieldValue)
	field := make([]string, length)
	value := make([]interface{}, length)
	index := 0
	for f := range fieldValue {
		field[index] = f
		index++
	}
	sort.Strings(field)
	for i, f := range field {
		value[i] = fieldValue[f]
	}
	return s.Field(field...).Value(value)
}

func (s *Insert) ForSlice(field []string, value []interface{}) *Insert {
	return s.Field(field...).Value(value)
}

func (s *Insert) ValuesFromQuery(prepare string, args ...interface{}) *Insert {
	s.queryPrepare, s.queryArgs = prepare, args
	return s
}

func (s *Insert) ValuesFromSelect(selector *Select) *Insert {
	if selector == nil {
		return s
	}
	s.queryPrepare, s.queryArgs = selector.Result()
	return s
}

func (s *Insert) Result() (string, []interface{}) {
	if s.table == "" || s.field == nil {
		return "", nil
	}
	return buildSqlInsert(s)
}

func buildSqlInsert(s *Insert) (prepare string, args []interface{}) {
	if s.queryPrepare != "" {
		buf := sqlBuilder(s.tag)
		buf.WriteString("INSERT INTO ")
		buf.WriteString(s.table)
		if len(s.field) > 0 {
			buf.WriteString(" ( ")
			buf.WriteString(strings.Join(s.field, ", "))
			buf.WriteString(" )")
		}
		buf.WriteString(" ")
		buf.WriteString(s.queryPrepare)
		prepare = buf.String()
		args = s.queryArgs
		return
	}
	length := len(s.field)
	if length == 0 {
		return // field list is empty
	}
	amount := len(s.value)
	if amount == 0 {
		return // value list is empty
	}
	for _, v := range s.value {
		if len(v) != length {
			return // field does not match field value length
		}
	}
	args = make([]interface{}, amount*length)
	buf := sqlBuilder(s.tag)
	buf.WriteString("INSERT INTO ")
	buf.WriteString(s.table)
	buf.WriteString(" ( ")
	for i := 0; i < length; i++ {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(s.field[i])
	}
	buf.WriteString(" )")
	buf.WriteString(" VALUES ")
	i := 0
	for key, val := range s.value {
		if key != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("( ")
		for k, v := range val {
			if k != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(Placeholder)
			args[i] = v
			i++
		}
		buf.WriteString(" )")
	}
	prepare = buf.String()
	return
}

type Delete struct {
	ctx   context.Context
	way   *Way
	tag   string
	table string
	where Filter
	force bool
}

func NewDelete(way *Way) *Delete {
	return &Delete{
		ctx: way.ctx,
		way: way,
	}
}

func (s *Delete) WithContext(ctx context.Context) *Delete {
	s.ctx = ctx
	return s
}

func (s *Delete) Tag(tag string) *Delete {
	s.tag = tag
	return s
}

func (s *Delete) Table(table string) *Delete {
	s.table = table
	return s
}

func (s *Delete) Where(where Filter) *Delete {
	s.where = where
	return s
}

func (s *Delete) Force() *Delete {
	s.force = true
	return s
}

func (s *Delete) Result() (string, []interface{}) {
	if s.table == "" {
		return "", nil
	}
	return buildSqlDelete(s)
}

func buildSqlDelete(s *Delete) (prepare string, args []interface{}) {
	buf := sqlBuilder(s.tag)
	buf.WriteString("DELETE FROM ")
	buf.WriteString(s.table)
	if s.where == nil {
		if s.force {
			prepare = buf.String()
		}
		return
	}
	key, val := s.where.Result()
	if key == "" {
		if s.force {
			prepare = buf.String()
		}
		return
	}
	buf.WriteString(" WHERE ")
	buf.WriteString(key)
	prepare, args = buf.String(), val
	return
}

type _modify struct {
	expr string
	args []interface{}
}

type Update struct {
	ctx    context.Context
	way    *Way
	tag    string
	table  string
	update map[string]*_modify
	where  Filter
	force  bool
}

func NewUpdate(way *Way) *Update {
	return &Update{
		ctx:    way.ctx,
		way:    way,
		update: make(map[string]*_modify, 1),
	}
}

func (s *Update) WithContext(ctx context.Context) *Update {
	s.ctx = ctx
	return s
}

func (s *Update) Tag(tag string) *Update {
	s.tag = tag
	return s
}

func (s *Update) Table(table string) *Update {
	s.table = table
	return s
}

func (s *Update) Expr(expr string, args ...interface{}) *Update {
	s.update[expr] = &_modify{
		expr: expr,
		args: args,
	}
	return s
}

func (s *Update) Set(field string, value interface{}) *Update {

	return s.Expr(fmt.Sprintf("%s = %s", field, Placeholder), value)
}

func (s *Update) Incr(field string, value interface{}) *Update {
	return s.Expr(fmt.Sprintf("%s = %s + %s", field, field, Placeholder), value)
}

func (s *Update) Decr(field string, value interface{}) *Update {
	return s.Expr(fmt.Sprintf("%s = %s - %s", field, field, Placeholder), value)
}

func (s *Update) ForMap(fieldValue map[string]interface{}) *Update {
	for field, value := range fieldValue {
		s.Set(field, value)
	}
	return s
}

func (s *Update) ForSlice(field []string, value []interface{}) *Update {
	lf, lv := len(field), len(value)
	if lf != lv {
		return s
	}
	for i, f := range field {
		s.Set(f, value[i])
	}
	return s
}

func (s *Update) Where(where Filter) *Update {
	s.where = where
	return s
}

func (s *Update) Force() *Update {
	s.force = true
	return s
}

func (s *Update) Result() (string, []interface{}) {
	if s.table == "" {
		return "", nil
	}
	return buildSqlUpdate(s)
}

func buildSqlUpdate(s *Update) (prepare string, args []interface{}) {
	length := len(s.update)
	if length == 0 {
		return
	}
	items := make([]string, 0, length)
	for _, v := range s.update {
		items = append(items, v.expr)
	}
	sort.Strings(items)
	expr := make([]string, length)
	args = make([]interface{}, 0, length)
	for k, v := range items {
		expr[k] = s.update[v].expr
		args = append(args, s.update[v].args...)
	}
	buf := sqlBuilder(s.tag)
	buf.WriteString("UPDATE ")
	buf.WriteString(s.table)
	buf.WriteString(" SET ")
	buf.WriteString(strings.Join(expr, ", "))
	if s.where == nil {
		if s.force {
			prepare = buf.String()
		}
		return
	}
	key, val := s.where.Result()
	if key == "" {
		if s.force {
			prepare = buf.String()
		}
		return
	}
	buf.WriteString(" WHERE ")
	buf.WriteString(key)
	prepare, args = buf.String(), append(args, val...)
	return
}

const (
	// DefaultColumnName default column name
	DefaultColumnName = "*"

	// DefaultCountAliasName default total rows alias name
	DefaultCountAliasName = "hey_rows_total"

	// DefaultUnionResultTableAliasName union query result table alias name
	DefaultUnionResultTableAliasName = "hey_union_result_table_alias_name"
)

type TypeUnion string

const (
	UnknownUnion TypeUnion = ""
	Union        TypeUnion = "UNION"
	UnionAll     TypeUnion = "UNION ALL"
)

type _union struct {
	typeUnion TypeUnion
	selector  *Select
}

type Select struct {
	ctx       context.Context
	way       *Way
	tag       string
	table     string
	tableArgs []interface{}
	tableAs   *string
	field     []string
	join      []Joiner
	where     Filter
	group     []string
	having    Filter
	order     *Order
	limit     *int64
	offset    *int64
	union     []*_union
}

func NewSelect(way *Way) *Select {
	return &Select{
		ctx: way.ctx,
		way: way,
	}
}

func (s *Select) WithContext(ctx context.Context) *Select {
	s.ctx = ctx
	return s
}

func (s *Select) Tag(tag string) *Select {
	s.tag = tag
	return s
}

func (s *Select) Table(table string, args ...interface{}) *Select {
	s.table, s.tableArgs = table, args
	return s
}

func (s *Select) FromSelect(selector *Select, alias string) *Select {
	if selector == nil || alias == "" {
		return s
	}
	prepare, args := selector.Result()
	s.Table(SubQuery(prepare), args...).Alias(alias)
	return s
}

func (s *Select) Alias(alias string) *Select {
	s.tableAs = &alias
	return s
}

func (s *Select) Field(field ...string) *Select {
	s.field = field
	return s
}

func (s *Select) Master(master Joiner) *Select {
	s.Table(master.Table())
	alias := master.Alias()
	if alias != "" {
		s.Alias(alias)
	}
	field := master.QueryFields()
	if field != nil {
		s.Field(field...)
	}
	return s
}

func (s *Select) Join(join ...Joiner) *Select {
	s.join = join
	return s
}

func (s *Select) Where(where Filter) *Select {
	s.where = where
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

func (s *Select) Order(order *Order) *Select {
	s.order = order
	return s
}

func (s *Select) Asc(field ...string) *Select {
	if s.order == nil {
		s.order = NewOrder()
	}
	s.order.Asc(field...)
	return s
}

func (s *Select) Desc(field ...string) *Select {
	if s.order == nil {
		s.order = NewOrder()
	}
	s.order.Desc(field...)
	return s
}

func (s *Select) Limit(limit int64) *Select {
	s.limit = &limit
	return s
}

func (s *Select) Offset(offset int64) *Select {
	s.offset = &offset
	return s
}

func (s *Select) typeUnion(typeUnion TypeUnion, c ...*Select) *Select {
	if typeUnion == UnknownUnion {
		return s
	}
	length := len(c)
	if length == 0 {
		s.union = nil
		return s
	}
	result := make([]*_union, 0, length)
	for _, v := range c {
		if v == nil {
			continue
		}
		result = append(result, &_union{
			typeUnion: typeUnion,
			selector:  v,
		})
	}
	s.union = append(s.union, result...)
	return s
}

func (s *Select) Union(c ...*Select) *Select {
	return s.typeUnion(Union, c...)
}

func (s *Select) UnionAll(c ...*Select) *Select {
	return s.typeUnion(UnionAll, c...)
}

func (s *Select) ResultForCount() (string, []interface{}) {
	return buildSqlSelectForCount(s)
}

func (s *Select) Result() (string, []interface{}) {
	if s.table == "" {
		return "", nil
	}
	return buildSqlSelect(s)
}

func (s *Select) Count() (result int64, err error) {
	prepare, args := s.ResultForCount()
	err = s.way.QueryContext(s.ctx, func(rows *sql.Rows) (err error) {
		for rows.Next() {
			if err = rows.Scan(&result); err != nil {
				return
			}
		}
		return
	}, prepare, args...)
	return
}

func (s *Select) Get(forRowsNextScan func(rows *sql.Rows) (err error)) error {
	prepare, args := s.Result()
	return s.way.QueryContext(s.ctx, func(rows *sql.Rows) (err error) { return ForRowsNextScan(rows, forRowsNextScan) }, prepare, args...)
}

func (s *Select) Scan(result interface{}) error {
	prepare, args := s.Result()
	return s.way.QueryContext(s.ctx, func(rows *sql.Rows) error { return s.way.scanner(rows, result) }, prepare, args...)
}

func buildSqlSelectForCount(s *Select) (prepare string, args []interface{}) {
	buf := sqlBuilder(s.tag)
	buf.WriteString("SELECT COUNT(")
	buf.WriteString(DefaultColumnName)
	buf.WriteString(") AS ")
	buf.WriteString(DefaultCountAliasName)
	buf.WriteString(" FROM ")
	buf.WriteString(s.table)
	args = append(args, s.tableArgs...)
	if s.tableAs != nil && *s.tableAs != "" {
		buf.WriteString(" AS ")
		buf.WriteString(*s.tableAs)
	}
	for _, join := range s.join {
		key, val := join.Result()
		if key != "" {
			buf.WriteString(" ")
			buf.WriteString(key)
			args = append(args, val...)
		}
	}
	if s.where != nil {
		key, val := s.where.Result()
		if key != "" {
			buf.WriteString(" WHERE ")
			buf.WriteString(key)
			args = append(args, val...)
		}
	}
	prepare = buf.String()
	if s.union != nil && len(s.union) > 0 {
		ok := false
		union := &strings.Builder{}
		union.WriteString("SELECT SUM(")
		union.WriteString(DefaultUnionResultTableAliasName)
		union.WriteString(".")
		union.WriteString(DefaultCountAliasName)
		union.WriteString(") AS ")
		union.WriteString(DefaultCountAliasName)
		union.WriteString(" FROM (")
		union.WriteString(prepare)
		unionArgsAll := args
		for _, v := range s.union {
			if v.typeUnion == UnknownUnion || v.selector == nil {
				continue
			}
			ok = true
			unionPrepare, unionArgs := v.selector.ResultForCount()
			union.WriteString(" ")
			union.WriteString(string(v.typeUnion))
			union.WriteString(" ")
			union.WriteString(unionPrepare)
			unionArgsAll = append(unionArgsAll, unionArgs...)
		}
		union.WriteString(" ) AS ")
		union.WriteString(DefaultUnionResultTableAliasName)
		if ok {
			prepare = union.String()
			args = unionArgsAll
		}
	}
	return
}

func buildSqlSelect(s *Select) (prepare string, args []interface{}) {
	buf := sqlBuilder(s.tag)
	columns := DefaultColumnName
	if len(s.field) > 0 {
		columns = strings.Join(s.field, ", ")
	}
	buf.WriteString("SELECT ")
	buf.WriteString(columns)
	buf.WriteString(" FROM ")
	buf.WriteString(s.table)
	args = append(args, s.tableArgs...)
	if s.tableAs != nil && *s.tableAs != "" {
		buf.WriteString(" AS ")
		buf.WriteString(*s.tableAs)
	}
	for _, join := range s.join {
		key, val := join.Result()
		if key != "" {
			s.field = append(s.field, join.QueryFields()...)
			buf.WriteString(" ")
			buf.WriteString(key)
			args = append(args, val...)
		}
	}
	if s.where != nil {
		key, val := s.where.Result()
		if key != "" {
			buf.WriteString(" WHERE ")
			buf.WriteString(key)
			args = append(args, val...)
		}
	}
	if s.group != nil {
		group := strings.Join(s.group, ", ")
		if group != "" {
			buf.WriteString(" GROUP BY ")
			buf.WriteString(group)
		}
	}
	if s.having != nil {
		key, val := s.having.Result()
		if key != "" {
			buf.WriteString(" HAVING ")
			buf.WriteString(key)
			args = append(args, val...)
		}
	}
	for _, v := range s.union {
		if v.typeUnion == UnknownUnion || v.selector == nil {
			continue
		}
		unionPrepare, unionArgs := v.selector.Result()
		buf.WriteString(" ")
		buf.WriteString(string(v.typeUnion))
		buf.WriteString(" ")
		buf.WriteString(unionPrepare)
		args = append(args, unionArgs...)
	}
	if s.order != nil {
		order := s.order.Result()
		if order != "" {
			buf.WriteString(" ORDER BY ")
			buf.WriteString(order)
		}
	}
	if s.limit != nil && *s.limit > 0 {
		buf.WriteString(fmt.Sprintf(" LIMIT %d", *s.limit))
		if s.offset == nil {
			s.offset = new(int64)
		}
		buf.WriteString(fmt.Sprintf(" OFFSET %d", *s.offset))
	}
	prepare = buf.String()
	return
}

type Table struct {
	way   *Way
	table string
}

func (s *Table) Insert(fn func(tmp *Insert)) (int64, error) {
	tmp := NewInsert(s.way).Table(s.table)
	if fn != nil {
		fn(tmp)
	}
	prepare, args := tmp.Result()
	return s.way.ExecContext(tmp.ctx, prepare, args...)
}

func (s *Table) Delete(fn func(tmp *Delete)) (int64, error) {
	tmp := NewDelete(s.way).Table(s.table)
	if fn != nil {
		fn(tmp)
	}
	prepare, args := tmp.Result()
	return s.way.ExecContext(tmp.ctx, prepare, args...)
}

func (s *Table) Update(fn func(tmp *Update)) (int64, error) {
	tmp := NewUpdate(s.way).Table(s.table)
	if fn != nil {
		fn(tmp)
	}
	prepare, args := tmp.Result()
	return s.way.ExecContext(tmp.ctx, prepare, args...)
}

func (s *Table) Select() *Select {
	return NewSelect(s.way).Table(s.table)
}
