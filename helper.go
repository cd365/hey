// Quickly build INSERT, DELETE, UPDATE, SELECT statements.
// Also allows you to call them to get the corresponding results.

package hey

import (
	"context"
	"database/sql"
	"regexp"
	"strings"
)

// schema used to store basic information such as context.Context, *Way, SQL comment, table name.
type schema struct {
	ctx context.Context

	way *Way

	comment string

	table SQLTable
}

// newSchema new schema with *Way.
func newSchema(way *Way) *schema {
	return &schema{
		ctx: context.Background(),
		way: way,
	}
}

// comment make SQL statement builder, SqlPlaceholder `?` should not appear in comments.
// defer putStringBuilder(builder) should be called immediately after calling the current method.
func comment(schema *schema) (b *strings.Builder) {
	b = getStringBuilder()
	schema.comment = strings.TrimSpace(schema.comment)
	schema.comment = strings.ReplaceAll(schema.comment, SqlPlaceholder, EmptyString)
	if schema.comment == EmptyString {
		return
	}
	b.WriteString("/*")
	b.WriteString(schema.comment)
	b.WriteString("*/")
	return
}

// Del for DELETE.
type Del struct {
	schema *schema

	where Filter
}

// NewDel for DELETE.
func NewDel(way *Way) *Del {
	return &Del{
		schema: newSchema(way),
	}
}

// Comment set comment.
func (s *Del) Comment(comment string) *Del {
	s.schema.comment = comment
	return s
}

// Context set context.
func (s *Del) Context(ctx context.Context) *Del {
	s.schema.ctx = ctx
	return s
}

// GetContext get context.
func (s *Del) GetContext() context.Context {
	return s.schema.ctx
}

// Table set table name.
func (s *Del) Table(table string, args ...any) *Del {
	s.schema.table = NewSQLTable(s.schema.way.Replace(table), args)
	return s
}

// SQLTable Set table script.
func (s *Del) SQLTable(sqlTable SQLTable) *Del {
	if sqlTable == nil || EmptyMaker(sqlTable) {
		return s
	}
	s.schema.table = sqlTable
	return s
}

// Where set where.
func (s *Del) Where(where func(f Filter)) *Del {
	if where == nil {
		return s
	}
	if s.where == nil {
		s.where = s.schema.way.F()
	}
	where(s.where)
	return s
}

// Filter Using multiple WHERE conditions.
func (s *Del) Filter(filters ...Filter) *Del {
	return s.Where(func(f Filter) {
		f.Use(filters...)
	})
}

// ToSQL build SQL statement.
func (s *Del) ToSQL() *SQL {
	script := NewSQL(EmptyString)
	if s.schema.table == nil || s.schema.table.Empty() {
		return script
	}

	b := comment(s.schema)
	defer putStringBuilder(b)
	b.WriteString(ConcatString(SqlDelete, SqlSpace, SqlFrom, SqlSpace))

	tableSQL := s.schema.table.ToSQL()
	b.WriteString(s.schema.way.Replace(tableSQL.Prepare))
	script.Args = append(script.Args, tableSQL.Args...)

	cfg := s.schema.way.cfg
	if cfg.DeleteMustUseWhere && (s.where == nil || s.where.Empty()) {
		return NewSQL(EmptyString)
	}

	if s.where != nil && !s.where.Empty() {
		where := ParcelFilter(s.where).ToSQL()
		b.WriteString(ConcatString(SqlSpace, SqlWhere, SqlSpace))
		b.WriteString(where.Prepare)
		script.Args = append(script.Args, where.Args...)
	}

	script.Prepare = b.String()
	return script
}

// Del execute the built SQL statement.
func (s *Del) Del() (int64, error) {
	script := s.ToSQL()
	return s.schema.way.ExecContext(s.schema.ctx, script.Prepare, script.Args...)
}

// GetWay get current *Way.
func (s *Del) GetWay() *Way {
	return s.schema.way
}

// SetWay use the specified *Way object.
func (s *Del) SetWay(way *Way) *Del {
	if way != nil {
		s.schema.way = way
	}
	return s
}

// Add for INSERT.
type Add struct {
	schema *schema

	except SQLUpsertColumn

	permit SQLUpsertColumn

	columns SQLUpsertColumn

	values SQLInsertValue

	columnsDefault SQLUpsertColumn

	valuesDefault SQLInsertValue

	fromMaker Maker
}

// NewAdd for INSERT.
func NewAdd(way *Way) *Add {
	add := &Add{
		schema:  newSchema(way),
		columns: NewSQLUpsertColumn(way),
		values:  NewSQLInsertValue(),
	}
	return add
}

// Comment set comment.
func (s *Add) Comment(comment string) *Add {
	s.schema.comment = comment
	return s
}

// Context set context.
func (s *Add) Context(ctx context.Context) *Add {
	s.schema.ctx = ctx
	return s
}

// GetContext get context.
func (s *Add) GetContext() context.Context {
	return s.schema.ctx
}

// Table set table name.
func (s *Add) Table(table string) *Add {
	s.schema.table = NewSQLTable(s.schema.way.Replace(table), nil)
	return s
}

// ExceptPermit Set a list of columns that are not allowed to be inserted and a list of columns that are only allowed to be inserted.
func (s *Add) ExceptPermit(custom func(except SQLUpsertColumn, permit SQLUpsertColumn)) *Add {
	if custom == nil {
		return s
	}
	except, permit := s.except, s.permit
	if except == nil {
		except = NewSQLUpsertColumn(s.schema.way)
	}
	if permit == nil {
		permit = NewSQLUpsertColumn(s.schema.way)
	}
	custom(except, permit)
	s.except, s.permit = except, permit
	if s.except.Empty() {
		s.except = nil
	}
	if s.permit.Empty() {
		s.permit = nil
	}
	return s
}

// ColumnsValues set columns and values.
func (s *Add) ColumnsValues(columns []string, values [][]any) *Add {
	length1, length2 := len(columns), len(values)
	if length1 == 0 || length2 == 0 {
		return s
	}
	for _, v := range values {
		if length1 != len(v) {
			return s
		}
	}
	s.columns.SetColumns(columns)
	s.values.SetValues(values...)
	if s.permit != nil {
		columns1 := s.permit.GetColumnsMap()
		deletes := make([]string, 0, 32)
		for _, column := range s.columns.GetColumns() {
			if _, ok := columns1[column]; !ok {
				deletes = append(deletes, column)
			}
		}
		for _, column := range deletes {
			s.values.Del(s.columns.ColumnIndex(column))
			s.columns.Del(column)
		}
	}
	if s.except != nil {
		columns1 := s.except.GetColumnsMap()
		deletes := make([]string, 0, 32)
		for _, column := range s.columns.GetColumns() {
			if _, ok := columns1[column]; ok {
				deletes = append(deletes, column)
			}
		}
		for _, column := range deletes {
			s.values.Del(s.columns.ColumnIndex(column))
			s.columns.Del(column)
		}
	}
	return s
}

// ColumnValue append column-value for insert one or more rows.
func (s *Add) ColumnValue(column string, value any) *Add {
	if s.permit != nil {
		if !s.permit.ColumnExists(column) {
			return s
		}
	}
	if s.except != nil {
		if s.except.ColumnExists(column) {
			return s
		}
	}
	s.columns.Add(column)
	s.values.Set(s.columns.ColumnIndex(column), value)
	return s
}

// Default Add column = value .
func (s *Add) Default(add func(add *Add)) *Add {
	if add == nil {
		return s
	}
	v := *s
	tmp := &v
	tmp.columns, tmp.values = NewSQLUpsertColumn(s.schema.way), NewSQLInsertValue()
	add(tmp)
	if !tmp.columns.Empty() && !tmp.values.Empty() {
		if s.columnsDefault == nil {
			s.columnsDefault = NewSQLUpsertColumn(s.schema.way)
		}
		if tmp.valuesDefault == nil {
			s.valuesDefault = NewSQLInsertValue()
		}
		columns, values := tmp.columns.GetColumns(), tmp.values.GetValues()
		for index, column := range columns {
			s.columnsDefault.Add(column)
			s.valuesDefault.Set(s.columnsDefault.ColumnIndex(column), values[0][index])
		}
	}
	return s
}

// Create value of creation should be one of struct{}, *struct{}, map[string]any, []struct, []*struct{}, *[]struct{}, *[]*struct{}.
func (s *Add) Create(create any) *Add {
	if columnValue, ok := create.(map[string]any); ok {
		for column, value := range columnValue {
			s.ColumnValue(column, value)
		}
		return s
	}
	var except, permit []string
	if s.except != nil {
		except = s.except.GetColumns()
	}
	if s.permit != nil {
		permit = s.permit.GetColumns()
	}
	return s.ColumnsValues(StructInsert(create, s.schema.way.cfg.ScanTag, except, permit))
}

// MakerValues values is a query SQL statement.
func (s *Add) MakerValues(values Maker, columns []string) *Add {
	if values == nil || EmptyMaker(values) {
		return s
	}
	s.columns = NewSQLUpsertColumn(s.schema.way).SetColumns(columns)
	s.fromMaker = values
	return s
}

// GetColumns list of columns to insert.
func (s *Add) GetColumns() []string {
	return s.columns.GetColumns()
}

// GetValues list of values to insert.
func (s *Add) GetValues() [][]any {
	return s.values.GetValues()
}

// ToSQL build SQL statement.
func (s *Add) ToSQL() *SQL {
	script := NewSQL(EmptyString)
	if s.schema.table == nil || s.schema.table.Empty() {
		return script
	}
	b := comment(s.schema)
	defer putStringBuilder(b)
	if s.fromMaker != nil {
		b.WriteString(ConcatString(SqlInsert, SqlSpace, SqlInto, SqlSpace))
		table := s.schema.table.ToSQL()
		b.WriteString(s.schema.way.Replace(table.Prepare))
		if !EmptyMaker(s.columns) {
			b.WriteString(SqlSpace)
			columns := s.columns.ToSQL()
			b.WriteString(columns.Prepare)
		}
		b.WriteString(SqlSpace)
		from := s.fromMaker.ToSQL()
		b.WriteString(from.Prepare)
		script.Args = append(script.Args, from.Args...)
		script.Prepare = b.String()
		return script
	}
	if EmptyMaker(s.columns) || EmptyMaker(s.values) {
		return script
	}
	b.WriteString(ConcatString(SqlInsert, SqlSpace, SqlInto, SqlSpace))
	table := s.schema.table.ToSQL()
	b.WriteString(s.schema.way.Replace(table.Prepare))
	b.WriteString(SqlSpace)
	// add default column-value
	if s.columnsDefault != nil {
		columns, values := s.columnsDefault.GetColumns(), s.valuesDefault.GetValues()
		if len(values) > 0 && len(columns) == len(values[0]) {
			for index, column := range columns {
				if s.columns.ColumnExists(column) {
					continue
				}
				s.ColumnValue(column, values[0][index])
			}
		}
	}
	columns := s.columns.ToSQL()
	b.WriteString(columns.Prepare)
	b.WriteString(SqlSpace)
	values := s.values.ToSQL()
	b.WriteString(ConcatString(SqlValues, SqlSpace))
	b.WriteString(values.Prepare)
	script.Args = append(script.Args, values.Args...)
	script.Prepare = b.String()
	return script
}

// Add execute the built SQL statement.
func (s *Add) Add() (int64, error) {
	script := s.ToSQL()
	if script.Empty() {
		return 0, nil
	}
	return s.schema.way.ExecContext(s.schema.ctx, script.Prepare, script.Args...)
}

// AddOne execute the built SQL statement, return the sequence value of the data.
func (s *Add) AddOne(custom func(add AddOneReturnSequenceValue)) (int64, error) {
	script := s.ToSQL()
	add := s.schema.way.NewAddOne(script.Prepare, script.Args).Context(s.schema.ctx)
	custom(add)
	return add.AddOne()
}

// GetWay get current *Way.
func (s *Add) GetWay() *Way {
	return s.schema.way
}

// SetWay use the specified *Way object.
func (s *Add) SetWay(way *Way) *Add {
	if way != nil {
		s.schema.way = way
	}
	return s
}

// Mod for UPDATE.
type Mod struct {
	schema *schema

	except SQLUpsertColumn

	permit SQLUpsertColumn

	update SQLUpdateSet

	updateDefault SQLUpdateSet

	where Filter
}

// NewMod for UPDATE.
func NewMod(way *Way) *Mod {
	return &Mod{
		schema: newSchema(way),
		update: NewSQLUpdateSet(way),
	}
}

// Comment set comment.
func (s *Mod) Comment(comment string) *Mod {
	s.schema.comment = comment
	return s
}

// Context set context.
func (s *Mod) Context(ctx context.Context) *Mod {
	s.schema.ctx = ctx
	return s
}

// GetContext get context.
func (s *Mod) GetContext() context.Context {
	return s.schema.ctx
}

// Table set table name.
func (s *Mod) Table(table string, args ...any) *Mod {
	s.schema.table = NewSQLTable(s.schema.way.Replace(table), args)
	return s
}

// ExceptPermit Set a list of columns that are not allowed to be updated and a list of columns that are only allowed to be updated.
func (s *Mod) ExceptPermit(custom func(except SQLUpsertColumn, permit SQLUpsertColumn)) *Mod {
	if custom == nil {
		return s
	}
	except, permit := s.except, s.permit
	if except == nil {
		except = NewSQLUpsertColumn(s.schema.way)
	}
	if permit == nil {
		permit = NewSQLUpsertColumn(s.schema.way)
	}
	custom(except, permit)
	s.except, s.permit = except, permit
	if s.except.Empty() {
		s.except = nil
	}
	if s.permit.Empty() {
		s.permit = nil
	}
	return s
}

// Default SET column = expr .
func (s *Mod) Default(custom func(mod *Mod)) *Mod {
	if custom == nil {
		return s
	}
	tmp := *s
	mod := &tmp
	mod.update = NewSQLUpdateSet(s.schema.way)
	custom(mod)
	if !mod.update.Empty() {
		if s.updateDefault == nil {
			s.updateDefault = NewSQLUpdateSet(s.schema.way)
		}
		prepares, args := mod.update.GetUpdate()
		for index, prepare := range prepares {
			if s.update.UpdateExists(prepare) {
				continue
			}
			s.update.Update(prepare, args[index]...)
		}
	}
	return s
}

// Expr update column using custom expression.
func (s *Mod) Expr(expr string, args ...any) *Mod {
	s.update.Update(expr, args...)
	return s
}

// Set column = value.
func (s *Mod) Set(column string, value any) *Mod {
	if s.permit != nil {
		if !s.permit.ColumnExists(column) {
			return s
		}
	}
	if s.except != nil {
		if s.except.ColumnExists(column) {
			return s
		}
	}
	s.update.Set(column, value)
	return s
}

// Incr SET column = column + value.
func (s *Mod) Incr(column string, value any) *Mod {
	if s.permit != nil {
		if !s.permit.ColumnExists(column) {
			return s
		}
	}
	if s.except != nil {
		if s.except.ColumnExists(column) {
			return s
		}
	}
	s.update.Incr(column, value)
	return s
}

// Decr SET column = column - value.
func (s *Mod) Decr(column string, value any) *Mod {
	if s.permit != nil {
		if !s.permit.ColumnExists(column) {
			return s
		}
	}
	if s.except != nil {
		if s.except.ColumnExists(column) {
			return s
		}
	}
	s.update.Decr(column, value)
	return s
}

// ColumnsValues SET column = value by slice, require len(columns) == len(values).
func (s *Mod) ColumnsValues(columns []string, values []any) *Mod {
	len1, len2 := len(columns), len(values)
	if len1 != len2 {
		return s
	}
	for i := range len1 {
		s.Set(columns[i], values[i])
	}
	return s
}

// Update Value of update should be one of anyStruct, *anyStruct, map[string]any.
func (s *Mod) Update(update any) *Mod {
	if columnValue, ok := update.(map[string]any); ok {
		for column, value := range columnValue {
			s.Set(column, value)
		}
		return s
	}
	return s.ColumnsValues(StructModify(update, s.schema.way.cfg.ScanTag))
}

// Compare For compare old and new to automatically calculate the need to update columns.
func (s *Mod) Compare(old, new any, except ...string) *Mod {
	return s.ColumnsValues(StructUpdate(old, new, s.schema.way.cfg.ScanTag, except...))
}

// Where set where.
func (s *Mod) Where(where func(f Filter)) *Mod {
	if where == nil {
		return s
	}
	if s.where == nil {
		s.where = s.schema.way.F()
	}
	where(s.where)
	return s
}

// Filter Using multiple WHERE conditions.
func (s *Mod) Filter(filters ...Filter) *Mod {
	return s.Where(func(f Filter) {
		f.Use(filters...)
	})
}

// GetUpdateSet prepare args of SET.
func (s *Mod) GetUpdateSet() *SQL {
	if s.update.Empty() {
		return nil
	}
	return s.update.ToSQL()
}

// ToSQL build SQL statement.
func (s *Mod) ToSQL() *SQL {
	script := NewSQL(EmptyString)
	if s.schema.table == nil || s.schema.table.Empty() {
		return script
	}
	update := s.GetUpdateSet()
	if update == nil || update.Empty() {
		return script
	}
	b := comment(s.schema)
	defer putStringBuilder(b)
	b.WriteString(ConcatString(SqlUpdate, SqlSpace))

	table := s.schema.table.ToSQL()
	b.WriteString(s.schema.way.Replace(table.Prepare))
	script.Args = append(script.Args, table.Args...)

	b.WriteString(ConcatString(SqlSpace, SqlSet, SqlSpace))
	b.WriteString(update.Prepare)
	script.Args = append(script.Args, update.Args...)

	cfg := s.schema.way.cfg
	if cfg.DeleteMustUseWhere && (s.where == nil || s.where.Empty()) {
		return NewSQL(EmptyString)
	}

	if s.where != nil && !s.where.Empty() {
		where := ParcelFilter(s.where).ToSQL()
		b.WriteString(ConcatString(SqlSpace, SqlWhere, SqlSpace))
		b.WriteString(where.Prepare)
		script.Args = append(script.Args, where.Args...)
	}

	script.Prepare = b.String()
	return script
}

// Mod execute the built SQL statement.
func (s *Mod) Mod() (int64, error) {
	script := s.ToSQL()
	if script.Empty() {
		return 0, nil
	}
	return s.schema.way.ExecContext(s.schema.ctx, script.Prepare, script.Args...)
}

// GetWay get current *Way.
func (s *Mod) GetWay() *Way {
	return s.schema.way
}

// SetWay use the specified *Way object.
func (s *Mod) SetWay(way *Way) *Mod {
	if way != nil {
		s.schema.way = way
	}
	return s
}

// Limiter limit and offset.
type Limiter interface {
	GetLimit() int64
	GetOffset() int64
}

// Get for SELECT.
type Get struct {
	schema *schema

	with SQLWith

	columns SQLSelect

	join SQLJoin

	where Filter

	group SQLGroupBy

	order SQLOrderBy

	limit SQLLimit
}

// NewGet for SELECT.
func NewGet(way *Way) *Get {
	return &Get{
		schema:  newSchema(way),
		columns: NewSQLSelect(way),
		where:   way.F(),
		group:   NewSQLGroupBy(way),
		order:   NewSQLOrderBy(way),
		limit:   NewSQLLimit(),
	}
}

// Comment set comment.
func (s *Get) Comment(comment string) *Get {
	s.schema.comment = comment
	return s
}

// Context set context.
func (s *Get) Context(ctx context.Context) *Get {
	s.schema.ctx = ctx
	return s
}

// GetContext get context.
func (s *Get) GetContext() context.Context {
	return s.schema.ctx
}

// With for with query, COMMON TABLE EXPRESSION.
func (s *Get) With(fc func(w SQLWith)) *Get {
	if fc == nil {
		return s
	}
	if s.with == nil {
		s.with = NewSQLWith()
	}
	fc(s.with)
	return s
}

// Table set table name.
func (s *Get) Table(table string) *Get {
	s.schema.table = NewSQLTable(s.schema.way.Replace(table), nil)
	return s
}

// Alias for table alias name, remember to call the current method when the table is a SQL statement.
func (s *Get) Alias(alias string) *Get {
	s.schema.table.Alias(s.schema.way.Replace(alias))
	return s
}

// Subquery table is a subquery.
func (s *Get) Subquery(subquery Maker, alias string) *Get {
	if EmptyMaker(subquery) {
		return s
	}
	if alias == EmptyString {
		if tab, ok := subquery.(*Get); ok && tab != nil {
			alias = tab.schema.table.GetAlias()
			if alias != EmptyString {
				tab.Alias(EmptyString)
				defer tab.Alias(alias)
			}
		}
		if alias == EmptyString {
			if tab, ok := subquery.(SQLTable); ok && tab != nil {
				alias = tab.GetAlias()
				if alias != EmptyString {
					tab.Alias(EmptyString)
					defer tab.Alias(alias)
				}
			}
		}
	}
	if alias == EmptyString {
		return s
	}
	script := ParcelMaker(subquery).ToSQL()
	s.schema.table = NewSQLTable(script.Prepare, script.Args).Alias(alias)
	return s
}

// Join for `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN` ...
func (s *Get) Join(custom func(join SQLJoin)) *Get {
	if custom == nil {
		return s
	}
	if s.join != nil {
		custom(s.join)
		return s
	}
	master := s.schema.table
	if EmptyMaker(master) {
		return s
	}
	alias := master.GetAlias()
	script := master.Alias(EmptyString).ToSQL()
	if alias == EmptyString {
		alias = AliasA // set primary default alias name.
	} else {
		master.Alias(alias) // restore master default alias name.
	}
	way := s.schema.way
	s.join = NewSQLJoin(way).SetMaster(NewSQLTable(script.Prepare, script.Args).Alias(alias))
	custom(s.join)
	return s
}

// Where set where.
func (s *Get) Where(where func(f Filter)) *Get {
	if where == nil {
		return s
	}
	if s.where == nil {
		s.where = s.schema.way.F()
	}
	where(s.where)
	return s
}

// Filter Using multiple WHERE conditions.
func (s *Get) Filter(filters ...Filter) *Get {
	return s.Where(func(f Filter) {
		f.Use(filters...)
	})
}

// GroupBy set group expression.
func (s *Get) GroupBy(fc func(g SQLGroupBy)) *Get {
	if fc != nil {
		fc(s.group)
	}
	return s
}

// Group set group columns.
func (s *Get) Group(columns ...string) *Get {
	return s.GroupBy(func(g SQLGroupBy) { g.Column(columns...) })
}

// Having set filter of group results.
func (s *Get) Having(having func(f Filter)) *Get {
	s.group.Having(having)
	return s
}

// GetSelect Get select object.
func (s *Get) GetSelect() SQLSelect {
	if s.join != nil {
		return s.join.Queries()
	}
	return s.columns
}

// SetSelect Set select object.
func (s *Get) SetSelect(sqlSelect SQLSelect) *Get {
	if sqlSelect == nil {
		return s
	}
	if s.join != nil {
		s.join.Queries().Set(sqlSelect.Get())
		return s
	}
	s.columns.Set(sqlSelect.Get())
	return s
}

// Select Set the column list for select.
func (s *Get) Select(columns ...string) *Get {
	if length := len(columns); length == 0 {
		return s
	}
	if tmp := NewSQLSelect(s.schema.way).AddAll(columns...); tmp.Len() > 0 {
		s.SetSelect(tmp)
	}
	return s
}

// OrderBy SQL ORDER BY.
func (s *Get) OrderBy(fc func(o SQLOrderBy)) *Get {
	if fc != nil {
		fc(s.order)
	}
	return s
}

// Asc set order by column ASC.
func (s *Get) Asc(column string) *Get {
	s.order.Asc(column)
	return s
}

// Desc set order by column Desc.
func (s *Get) Desc(column string) *Get {
	s.order.Desc(column)
	return s
}

var (
	// orderRegexp `column_name_first:a,column_name_second:d` => `column_name_first ASC, column_name_second DESC`.
	orderRegexp = regexp.MustCompile(`^([a-zA-Z][a-zA-Z0-9_]*([.][a-zA-Z][a-zA-Z0-9_]*)*):([ad])$`)
)

// Order set the column sorting list in batches through regular expressions according to the request parameter value.
func (s *Get) Order(order string, replaces ...map[string]string) *Get {
	columns := make(map[string]string, 8)
	mapping := make([]string, 0, 8)
	for _, tmp := range replaces {
		for column, value := range tmp {
			if column != EmptyString {
				columns[column] = value
			}
			if value != EmptyString {
				mapping = append(mapping, value)
			}
		}
	}
	if len(mapping) > 0 {
		s.order.Use(mapping...)
	}
	orders := strings.Split(order, ",")
	for _, v := range orders {
		if len(v) > 32 {
			continue
		}
		match := orderRegexp.FindAllStringSubmatch(strings.TrimSpace(v), -1)
		length := len(match)
		if length != 1 {
			continue
		}
		matched := match[0]
		length = len(matched) // the length should be 4.
		if length < 4 || matched[3] == EmptyString {
			continue
		}
		column := matched[1]
		if value, ok := columns[column]; ok {
			column = value
		}
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

// Limit set limit.
func (s *Get) Limit(limit int64) *Get {
	s.limit.Limit(limit)
	return s
}

// Offset set offset.
func (s *Get) Offset(offset int64) *Get {
	s.limit.Offset(offset)
	return s
}

// Limiter set limit and offset at the same time.
func (s *Get) Limiter(limiter Limiter) *Get {
	if limiter == nil {
		return s
	}
	return s.Limit(limiter.GetLimit()).Offset(limiter.GetOffset())
}

// MakerGetTable Build query table (without ORDER BY, LIMIT, OFFSET).
// [WITH xxx] SELECT xxx FROM xxx [INNER JOIN xxx ON xxx] [WHERE xxx] [GROUP BY xxx [HAVING xxx]]
func MakerGetTable(s *Get) *SQL {
	script := NewSQL(EmptyString)
	if s.schema.table == nil || s.schema.table.Empty() {
		return script
	}
	b := comment(s.schema)
	defer putStringBuilder(b)
	if s.with != nil {
		with := s.with.ToSQL()
		if !with.Empty() {
			b.WriteString(with.Prepare)
			b.WriteString(SqlSpace)
			script.Args = append(script.Args, with.Args...)
		}
	}
	if s.join != nil {
		join := s.join.ToSQL()
		if !join.Empty() {
			b.WriteString(join.Prepare)
			script.Args = append(script.Args, join.Args...)
		}
	} else {
		b.WriteString(ConcatString(SqlSelect, SqlSpace))
		columns := s.columns.ToSQL()
		b.WriteString(columns.Prepare)
		script.Args = append(script.Args, columns.Args...)
		b.WriteString(ConcatString(SqlSpace, SqlFrom, SqlSpace))
		table := s.schema.table.ToSQL()
		b.WriteString(s.schema.way.Replace(table.Prepare))
		script.Args = append(script.Args, table.Args...)
	}
	if s.where != nil && !s.where.Empty() {
		where := ParcelFilter(s.where).ToSQL()
		b.WriteString(ConcatString(SqlSpace, SqlWhere, SqlSpace))
		b.WriteString(where.Prepare)
		script.Args = append(script.Args, where.Args...)
	}
	if s.group != nil && !s.group.Empty() {
		b.WriteString(SqlSpace)
		group := s.group.ToSQL()
		b.WriteString(group.Prepare)
		script.Args = append(script.Args, group.Args...)
	}
	script.Prepare = strings.TrimSpace(b.String())
	return script
}

// MakerGetOrderLimitOffset Build query table of ORDER BY, LIMIT, OFFSET.
// [ORDER BY xxx] [LIMIT xxx [OFFSET xxx]]
func MakerGetOrderLimitOffset(s *Get) *SQL {
	b := getStringBuilder()
	defer putStringBuilder(b)
	script := NewSQL(EmptyString)
	if !s.order.Empty() {
		order := s.order.ToSQL()
		if !order.Empty() {
			b.WriteString(SqlSpace)
			b.WriteString(order.Prepare)
			script.Args = append(script.Args, order.Args...)
		}
	}
	if !s.limit.Empty() {
		limit := s.limit.ToSQL()
		if !limit.Empty() {
			b.WriteString(SqlSpace)
			b.WriteString(limit.Prepare)
			script.Args = append(script.Args, limit.Args...)
		}
	}
	script.Prepare = b.String()
	return script
}

// MakerGetSQL Build a complete query.
// [WITH xxx] SELECT xxx FROM xxx [INNER JOIN xxx ON xxx] [WHERE xxx] [GROUP BY xxx [HAVING xxx]] [ORDER BY xxx] [LIMIT xxx [OFFSET xxx]]
func MakerGetSQL(s *Get) *SQL {
	script := MakerGetTable(s)
	if script.Empty() {
		return script
	}
	if s.group != nil && !s.group.Empty() {
		return script
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(script.Prepare)
	limit := MakerGetOrderLimitOffset(s)
	if !limit.Empty() {
		b.WriteString(limit.Prepare)
		script.Args = append(script.Args, limit.Args...)
	}
	script.Prepare = strings.TrimSpace(b.String())
	return script
}

// MakerGetCount Build count query.
// SELECT COUNT(*) AS count FROM ( [WITH xxx] SELECT xxx FROM xxx [INNER JOIN xxx ON xxx] [WHERE xxx] [GROUP BY xxx [HAVING xxx]] ) AS a
// SELECT COUNT(*) AS count FROM ( query1 UNION [ALL] query2 [UNION [ALL] ...] ) AS a
func MakerGetCount(s *Get, countColumns ...string) *SQL {
	script := NewSQL(EmptyString)
	if EmptyMaker(s) {
		return script
	}

	if countColumns == nil {
		countColumns = []string{
			SqlAlias("COUNT(*)", s.schema.way.Replace(DefaultAliasNameCount)),
		}
	}

	selects := s.GetSelect()

	script = s.Select(countColumns...).ToSQL()

	s.SetSelect(selects)
	return script
}

// ToSQL build SQL statement.
func (s *Get) ToSQL() *SQL {
	return MakerGetSQL(s)
}

// CountSQL build SQL statement for count.
func (s *Get) CountSQL(columns ...string) *SQL {
	return MakerGetCount(s, columns...)
}

// GetCount execute the built SQL statement and scan query results for count.
func GetCount(get *Get, countColumns ...string) (count int64, err error) {
	script := MakerGetCount(get, countColumns...)
	err = get.schema.way.QueryContext(get.schema.ctx, func(rows *sql.Rows) (err error) {
		if rows.Next() {
			err = rows.Scan(&count)
		}
		return
	}, script.Prepare, script.Args...)
	return
}

// GetQuery execute the built SQL statement and scan query results.
func GetQuery(get *Get, query func(rows *sql.Rows) (err error)) error {
	script := MakerGetSQL(get)
	return get.schema.way.QueryContext(get.schema.ctx, query, script.Prepare, script.Args...)
}

// GetGet execute the built SQL statement and scan query result.
func GetGet(get *Get, result any) error {
	script := MakerGetSQL(get)
	return get.schema.way.TakeAllContext(get.schema.ctx, result, script.Prepare, script.Args...)
}

// GetExists Determine whether the query result exists.
func GetExists(get *Get) (exists bool, err error) {
	get.Limit(1)
	err = GetQuery(get, func(rows *sql.Rows) error {
		exists = rows.Next()
		return nil
	})
	return
}

// GetScanAll execute the built SQL statement and scan all from the query results.
func GetScanAll(get *Get, fc func(rows *sql.Rows) error) error {
	return GetQuery(get, func(rows *sql.Rows) error {
		return get.schema.way.ScanAll(rows, fc)
	})
}

// GetScanOne execute the built SQL statement and scan at most once from the query results.
func GetScanOne(get *Get, dest ...any) error {
	return GetQuery(get, func(rows *sql.Rows) error {
		return get.schema.way.ScanOne(rows, dest...)
	})
}

// GetScanMap execute the built SQL statement and scan all from the query results.
func GetScanMap(get *Get) (result []map[string]any, err error) {
	err = GetQuery(get, func(rows *sql.Rows) (err error) {
		result, err = ScanMap(rows)
		return
	})
	return
}

// GetCountQuery execute the built SQL statement and scan query result, count + query.
func GetCountQuery(get *Get, query func(rows *sql.Rows) (err error), countColumn ...string) (int64, error) {
	count, err := GetCount(get, countColumn...)
	if err != nil {
		return 0, err
	}
	if count <= 0 {
		return 0, nil
	}
	return count, GetQuery(get, query)
}

// GetCountGet execute the built SQL statement and scan query results, count + get.
func GetCountGet(get *Get, result any, countColumn ...string) (int64, error) {
	count, err := GetCount(get, countColumn...)
	if err != nil {
		return 0, err
	}
	if count <= 0 {
		return 0, nil
	}
	return count, GetGet(get, result)
}

// Count execute the built SQL statement and scan query results for count.
func (s *Get) Count(column ...string) (int64, error) {
	return GetCount(s, column...)
}

// Query executes the built SQL statement and scan query results.
func (s *Get) Query(query func(rows *sql.Rows) (err error)) error {
	return GetQuery(s, query)
}

// Get execute the built SQL statement and scan query results.
func (s *Get) Get(result any) error {
	return GetGet(s, result)
}

// Exists Determine whether the query result exists.
func (s *Get) Exists() (bool, error) {
	return GetExists(s)
}

// ScanAll execute the built SQL statement and scan all from the query results.
func (s *Get) ScanAll(fc func(rows *sql.Rows) error) error {
	return GetScanAll(s, fc)
}

// ScanOne execute the built SQL statement and scan at most once from the query results.
func (s *Get) ScanOne(dest ...any) error {
	return GetScanOne(s, dest...)
}

// ScanMap execute the built SQL statement and scan all from the query results.
func (s *Get) ScanMap() ([]map[string]any, error) {
	return GetScanMap(s)
}

// CountQuery execute the built SQL statement and scan query results, count + query.
func (s *Get) CountQuery(query func(rows *sql.Rows) (err error), countColumn ...string) (int64, error) {
	return GetCountQuery(s, query, countColumn...)
}

// CountGet execute the built SQL statement and scan query result, count and get.
func (s *Get) CountGet(result any, countColumn ...string) (int64, error) {
	return GetCountGet(s, result, countColumn...)
}

// GetWay get current *Way.
func (s *Get) GetWay() *Way {
	return s.schema.way
}

// SetWay use the specified *Way object.
func (s *Get) SetWay(way *Way) *Get {
	if way != nil {
		s.schema.way = way
	}
	return s
}
