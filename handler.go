package hey

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

type Inserter interface {
	// Table target table name
	Table(table string) Inserter

	// Field specify a list of field names to insert
	Field(field ...string) Inserter

	// Value set the corresponding value of the field to be inserted, insert single or multiple records
	Value(values ...[]interface{}) Inserter

	// ForMap set the field name and field corresponding value at the same time, insert single record, for map
	ForMap(fieldValue map[string]interface{}) Inserter

	// ForSlice set the field name and field corresponding value at the same time, insert single record, for slice
	ForSlice(field []string, value []interface{}) Inserter

	// ValuesFromQuery insert value from query result, insert single or multiple records
	// list of specified fields: INSERT INTO table1 ( field1, field2, field3 ) SELECT column1, column2, column3 FROM table2 WHERE ( id > 0 )
	// list of unspecified fields: INSERT INTO table1 SELECT column1, column2, column3 FROM table2 WHERE ( id > 0 )
	ValuesFromQuery(prepare string, args ...interface{}) Inserter

	// ValuesFromSelector like ValuesFromQuery
	ValuesFromSelector(selector Selector) Inserter

	// Result build insert sql statement
	Result() (prepare string, args []interface{})
}

type _insert struct {
	table        string
	field        []string
	value        [][]interface{}
	queryPrepare string
	queryArgs    []interface{}
}

func NewInserter() Inserter {
	return &_insert{}
}

func (s *_insert) Table(table string) Inserter {
	s.table = table
	return s
}

func (s *_insert) Field(field ...string) Inserter {
	s.field = field
	return s
}

func (s *_insert) Value(value ...[]interface{}) Inserter {
	s.value = value
	return s
}

func (s *_insert) ForMap(fieldValue map[string]interface{}) Inserter {
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

func (s *_insert) ForSlice(field []string, value []interface{}) Inserter {
	return s.Field(field...).Value(value)
}

func (s *_insert) ValuesFromQuery(prepare string, args ...interface{}) Inserter {
	s.queryPrepare, s.queryArgs = prepare, args
	return s
}

func (s *_insert) ValuesFromSelector(selector Selector) Inserter {
	if selector == nil {
		return s
	}
	s.queryPrepare, s.queryArgs = selector.Result()
	return s
}

func (s *_insert) Result() (string, []interface{}) {
	if s.table == "" || s.field == nil {
		return "", nil
	}
	return buildSqlInsert(s)
}

func buildSqlInsert(s *_insert) (prepare string, args []interface{}) {
	if s.queryPrepare != "" {
		buf := &bytes.Buffer{}
		buf.WriteString("INSERT INTO")
		buf.WriteString(" ")
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
	buf := &bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("INSERT INTO %s ( ", s.table))
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

type Deleter interface {
	// Table target table name
	Table(table string) Deleter

	// Where delete data filter
	Where(where Filter) Deleter

	// Force it is allowed not to specify the where condition
	Force() Deleter

	// Result build delete sql statement
	Result() (prepare string, args []interface{})
}

type _delete struct {
	table string
	where Filter
	force bool
}

func NewDeleter() Deleter {
	return &_delete{}
}

func (s *_delete) Table(table string) Deleter {
	s.table = table
	return s
}

func (s *_delete) Where(where Filter) Deleter {
	s.where = where
	return s
}

func (s *_delete) Force() Deleter {
	s.force = true
	return s
}

func (s *_delete) Result() (string, []interface{}) {
	if s.table == "" {
		return "", nil
	}
	return buildSqlDelete(s)
}

func buildSqlDelete(s *_delete) (prepare string, args []interface{}) {
	buf := &bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("DELETE FROM %s", s.table))
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
	buf.WriteString(fmt.Sprintf(" WHERE %s", key))
	prepare, args = buf.String(), val
	return
}

type Updater interface {
	// Table target table name
	Table(table string) Updater

	// Set key-value, set the key-value that needs to be updated, field = ?
	Set(field string, value interface{}) Updater

	// Incr set increment value, field = field + ?
	Incr(field string, value interface{}) Updater

	// Decr set decrement value, field = field - ?
	Decr(field string, value interface{}) Updater

	// ForMap use map to batch update key-value, field = ?
	ForMap(fieldValue map[string]interface{}) Updater

	// ForSlice use slice to batch update key-value, field = ?
	ForSlice(field []string, value []interface{}) Updater

	// Where update data filter
	Where(where Filter) Updater

	// Force it is allowed not to specify the where condition
	Force() Updater

	// Clear attribute value clear
	Clear() Updater

	// Result build update sql statement
	Result() (prepare string, args []interface{})
}

type _modify struct {
	field  string
	value  interface{}
	result string
}

type _update struct {
	table  string
	update map[string]*_modify
	where  Filter
	force  bool
}

func NewUpdater() Updater {
	return &_update{
		update: make(map[string]*_modify, 1),
	}
}

func (s *_update) Table(table string) Updater {
	s.table = table
	return s
}

func (s *_update) Set(field string, value interface{}) Updater {
	s.update[field] = &_modify{
		field:  field,
		value:  value,
		result: fmt.Sprintf("%s = ?", field),
	}
	return s
}

func (s *_update) Incr(field string, value interface{}) Updater {
	s.update[field] = &_modify{
		field:  field,
		value:  value,
		result: fmt.Sprintf("%s = %s + ?", field, field),
	}
	return s
}

func (s *_update) Decr(field string, value interface{}) Updater {
	s.update[field] = &_modify{
		field:  field,
		value:  value,
		result: fmt.Sprintf("%s = %s - ?", field, field),
	}
	return s
}

func (s *_update) ForMap(fieldValue map[string]interface{}) Updater {
	for field, value := range fieldValue {
		s.Set(field, value)
	}
	return s
}

func (s *_update) ForSlice(field []string, value []interface{}) Updater {
	lf, lv := len(field), len(value)
	if lf != lv {
		return s
	}
	for i, f := range field {
		s.Set(f, value[i])
	}
	return s
}

func (s *_update) Where(where Filter) Updater {
	s.where = where
	return s
}

func (s *_update) Force() Updater {
	s.force = true
	return s
}

func (s *_update) Clear() Updater {
	s.table = ""
	s.update = make(map[string]*_modify, 1)
	s.where = nil
	return s
}

func (s *_update) Result() (string, []interface{}) {
	if s.table == "" || len(s.update) == 0 {
		return "", nil
	}
	return buildSqlUpdate(s)
}

func buildSqlUpdate(s *_update) (prepare string, args []interface{}) {
	length := len(s.update)
	items := make([]string, 0, length)
	for _, v := range s.update {
		items = append(items, v.field)
	}
	sort.Strings(items)
	field := make([]string, length)
	value := make([]interface{}, length)
	for k, v := range items {
		field[k] = s.update[v].result
		value[k] = s.update[v].value
	}
	buf := &bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("UPDATE %s SET", s.table))
	buf.WriteString(" ")
	buf.WriteString(strings.Join(field, ", "))
	args = value
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
	buf.WriteString(fmt.Sprintf(" WHERE %s", key))
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

type Selector interface {
	// Table target table name
	Table(table string, args ...interface{}) Selector

	// FromSelector select from Selector
	FromSelector(selector Selector, alias string) Selector

	// Alias target table alias name
	Alias(alias string) Selector

	// Field select field lists
	Field(field ...string) Selector

	// Where conditional filter data
	Where(where Filter) Selector

	// Master for join master table
	Master(master Joiner) Selector

	// Join set join single or multiple tables
	Join(join ...Joiner) Selector

	// Group query result grouping
	Group(group ...string) Selector

	// Having query group filter data
	Having(having Filter) Selector

	// Order set query order
	Order(order *Order) Selector

	// Asc ORDER BY field1 ASC, field2 ASC, field3 ASC ...
	Asc(field ...string) Selector

	// Desc ORDER BY field1 DESC, field2 DESC, field3 DESC ...
	Desc(field ...string) Selector

	// Limit set limit the number of query data
	Limit(limit int64) Selector

	// Offset set query data offset
	Offset(offset int64) Selector

	// Union query for UNION, do not specify ORDER, LIMIT, OFFSET values on the object being union Selector
	Union(c ...Selector) Selector

	// UnionAll query for UNION ALL, do not specify ORDER, LIMIT, OFFSET values on the object being union Selector
	UnionAll(c ...Selector) Selector

	// Clear for clear query object
	Clear() Selector

	// ResultForCount build sql statement for the number of count, use the field specified by the Field method as the parameter value of the COUNT function, if not specified will use COUNT(*)
	ResultForCount() (prepare string, args []interface{})

	// Result build select sql statement
	Result() (prepare string, args []interface{})
}

type _union struct {
	typeUnion TypeUnion
	selector  Selector
}

type _select struct {
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

func NewSelector() Selector {
	return &_select{}
}

func (s *_select) Table(table string, args ...interface{}) Selector {
	s.table, s.tableArgs = table, args
	return s
}

func (s *_select) FromSelector(selector Selector, alias string) Selector {
	if selector == nil || alias == "" {
		return s
	}
	prepare, args := selector.Result()
	s.Table(SubQuery(prepare), args...).Alias(alias)
	return s
}

func (s *_select) Alias(alias string) Selector {
	s.tableAs = &alias
	return s
}

func (s *_select) Field(field ...string) Selector {
	s.field = field
	return s
}

func (s *_select) Master(master Joiner) Selector {
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

func (s *_select) Join(join ...Joiner) Selector {
	s.join = join
	return s
}

func (s *_select) Where(where Filter) Selector {
	s.where = where
	return s
}

func (s *_select) Group(group ...string) Selector {
	s.group = group
	return s
}

func (s *_select) Having(having Filter) Selector {
	s.having = having
	return s
}

func (s *_select) Order(order *Order) Selector {
	s.order = order
	return s
}

func (s *_select) Asc(field ...string) Selector {
	if s.order == nil {
		s.order = NewOrder()
	}
	s.order.Asc(field...)
	return s
}

func (s *_select) Desc(field ...string) Selector {
	if s.order == nil {
		s.order = NewOrder()
	}
	s.order.Desc(field...)
	return s
}

func (s *_select) Limit(limit int64) Selector {
	s.limit = &limit
	return s
}

func (s *_select) Offset(offset int64) Selector {
	s.offset = &offset
	return s
}

func (s *_select) typeUnion(typeUnion TypeUnion, c ...Selector) Selector {
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

func (s *_select) Union(c ...Selector) Selector {
	return s.typeUnion(Union, c...)
}

func (s *_select) UnionAll(c ...Selector) Selector {
	return s.typeUnion(UnionAll, c...)
}

func (s *_select) Clear() Selector {
	s.table = ""
	s.tableArgs = nil
	s.tableAs = nil
	s.field = nil
	s.join = nil
	s.where = nil
	s.group = nil
	s.having = nil
	s.order = nil
	s.limit = nil
	s.offset = nil
	s.union = nil
	return s
}

func (s *_select) ResultForCount() (string, []interface{}) {
	return buildSqlSelectForCount(s)
}

func (s *_select) Result() (string, []interface{}) {
	if s.table == "" {
		return "", nil
	}
	return buildSqlSelect(s)
}

func buildSqlSelectForCount(s *_select) (prepare string, args []interface{}) {
	buf := &bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("SELECT COUNT(%s) AS %s FROM %s", DefaultColumnName, DefaultCountAliasName, s.table))
	args = append(args, s.tableArgs...)
	if s.tableAs != nil && *s.tableAs != "" {
		buf.WriteString(fmt.Sprintf(" AS %s", *s.tableAs))
	}
	for _, join := range s.join {
		key, val := join.Result()
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
	if s.union != nil && len(s.union) > 0 {
		ok := false
		union := &bytes.Buffer{}
		var unionAllArgs []interface{}
		union.WriteString(fmt.Sprintf("SELECT SUM(%s.%s) AS %s FROM", DefaultUnionResultTableAliasName, DefaultCountAliasName, DefaultCountAliasName))
		union.WriteString(" (")
		union.WriteString(prepare)
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
			unionAllArgs = append(unionAllArgs, unionArgs...)
		}
		union.WriteString(" ) ")
		union.WriteString(DefaultUnionResultTableAliasName)
		if ok {
			prepare = union.String()
			args = append(args, unionAllArgs...)
		}
	}
	return
}

func buildSqlSelect(s *_select) (prepare string, args []interface{}) {
	buf := &bytes.Buffer{}
	columns := DefaultColumnName
	if len(s.field) > 0 {
		columns = strings.Join(s.field, ", ")
	}
	buf.WriteString(fmt.Sprintf("SELECT %s FROM %s", columns, s.table))
	args = append(args, s.tableArgs...)
	if s.tableAs != nil && *s.tableAs != "" {
		buf.WriteString(fmt.Sprintf(" AS %s", *s.tableAs))
	}
	for _, join := range s.join {
		key, val := join.Result()
		if key != "" {
			s.field = append(s.field, join.QueryFields()...)
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
	if s.group != nil {
		group := strings.Join(s.group, ", ")
		if group != "" {
			buf.WriteString(fmt.Sprintf(" GROUP BY %s", group))
		}
	}
	if s.having != nil {
		key, val := s.having.Result()
		if key != "" {
			buf.WriteString(fmt.Sprintf(" HAVING %s", key))
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
			buf.WriteString(fmt.Sprintf(" ORDER BY %s", order))
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
