// a. Custom condition filter hook method, for delete operation, update operation, build subquery operation.
// b. Customize hook methods for non-query operations and pass parameter values through context.
// c. Quickly build subquery statements required by the business through the current table.
// d. Quickly operate data in a single table.

package hey

import (
	"context"
)

// MyContext Custom context key type.
type MyContext string

const (
	// MyWay Store the *hey.Way, *hey.Way.
	MyWay MyContext = "HEY_MY_WAY"

	// MyTable Store the *hey.Table, *hey.Table.
	MyTable MyContext = "HEY_MY_TABLE"

	// MyTableName Store the original table name, string.
	MyTableName MyContext = "HEY_MY_TABLE_NAME"

	// MyAffectedRows Store the number of rows affected, int64.
	MyAffectedRows MyContext = "HEY_MY_AFFECTED_ROWS"

	// MyInsertOne Store the status for inserting a piece of data, bool.
	MyInsertOne MyContext = "HEY_MY_INSERT_ONE"

	// MyInsertAll Store the status for inserting multiple records status, bool.
	MyInsertAll MyContext = "HEY_MY_INSERT_ALL"

	// MyInsertData Store the inserted data, any.
	MyInsertData MyContext = "HEY_MY_INSERT_DATA"

	// MyInsertId Store the inserted id value, int64.
	MyInsertId MyContext = "HEY_MY_INSERT_ID"

	// MyInsertQuery Insert the query result data into the table, for example: data statistics table, *SQL.
	MyInsertQuery MyContext = "HEY_MY_INSERT_QUERY"
)

// ContextWay Try to extract *Way from the context.
func ContextWay(ctx context.Context, defaultWay *Way) *Way {
	if ctx == nil {
		return defaultWay
	}
	if value := ctx.Value(MyWay); value == nil {
		return defaultWay
	} else {
		if way, ok := value.(*Way); ok && way != nil {
			return way
		}
		return defaultWay
	}
}

// WayContext Store *Way in the context.
func WayContext(ctx context.Context, way *Way) context.Context {
	return context.WithValue(ctx, MyWay, way)
}

type myHook struct {
	after  func(ctx context.Context) (context.Context, error)
	before func(ctx context.Context) (context.Context, error)
}

// MyInsert For INSERT.
type MyInsert interface {
	// AfterInsert Set post-insert data hook.
	AfterInsert(fc func(ctx context.Context) (context.Context, error))

	// BeforeInsert Set pre-insert data hook.
	BeforeInsert(fc func(ctx context.Context) (context.Context, error))

	// InsertOne Insert a record and return the primary key value (usually an integer value).
	InsertOne(ctx context.Context, insert any) (id int64, err error)

	// InsertAll Insert multiple records and return the number of rows affected.
	InsertAll(ctx context.Context, insert any) (affectedRows int64, err error)

	// InsertFromQuery Insert the query result data into the table, for example: data statistics table.
	InsertFromQuery(ctx context.Context, columns []string, query Maker) (affectedRows int64, err error)
}

type myInsert struct {
	*myHook
	way   *Way
	table string
}

func newMyInsert(way *Way, table string) *myInsert {
	return &myInsert{
		myHook: &myHook{},
		way:    way,
		table:  table,
	}
}

func (s *Way) MyInsert(table string) MyInsert {
	return newMyInsert(s, table)
}

func (s *myInsert) AfterInsert(fc func(ctx context.Context) (context.Context, error)) {
	s.after = fc
}

func (s *myInsert) BeforeInsert(fc func(ctx context.Context) (context.Context, error)) {
	s.before = fc
}

func (s *myInsert) InsertOne(ctx context.Context, insert any) (id int64, err error) {
	way := ContextWay(ctx, s.way)
	table := way.Table(s.table)
	ctx = context.WithValue(ctx, MyInsertOne, true)
	ctx = context.WithValue(ctx, MyTableName, s.table)
	ctx = context.WithValue(ctx, MyTable, table)
	if s.before != nil {
		if ctx, err = s.before(ctx); err != nil {
			return id, err
		}
	}
	if id, err = table.Create(ctx, insert); err != nil {
		return id, err
	}
	ctx = context.WithValue(ctx, MyInsertData, insert)
	ctx = context.WithValue(ctx, MyInsertId, id)
	if s.after != nil {
		if ctx, err = s.after(ctx); err != nil {
			return id, err
		}
	}
	return id, err
}

func (s *myInsert) InsertAll(ctx context.Context, insert any) (affectedRows int64, err error) {
	way := ContextWay(ctx, s.way)
	table := way.Table(s.table)
	ctx = context.WithValue(ctx, MyInsertAll, true)
	ctx = context.WithValue(ctx, MyTableName, s.table)
	ctx = context.WithValue(ctx, MyTable, table)
	if s.before != nil {
		if ctx, err = s.before(ctx); err != nil {
			return affectedRows, err
		}
	}
	if affectedRows, err = table.Create(ctx, insert); err != nil {
		return affectedRows, err
	}
	ctx = context.WithValue(ctx, MyInsertData, insert)
	ctx = context.WithValue(ctx, MyAffectedRows, affectedRows)
	if s.after != nil {
		if ctx, err = s.after(ctx); err != nil {
			return affectedRows, err
		}
	}
	return affectedRows, err
}

func (s *myInsert) InsertFromQuery(ctx context.Context, columns []string, query Maker) (affectedRows int64, err error) {
	way := ContextWay(ctx, s.way)
	script := query.ToSQL()
	table := way.Table(s.table)
	ctx = context.WithValue(ctx, MyInsertQuery, script)
	ctx = context.WithValue(ctx, MyTableName, s.table)
	ctx = context.WithValue(ctx, MyTable, table)
	if s.before != nil {
		if ctx, err = s.before(ctx); err != nil {
			return affectedRows, err
		}
	}
	table.INSERT(func(i SQLInsert) {
		i.Column(columns...).Subquery(script)
	})
	if affectedRows, err = table.Insert(ctx); err != nil {
		return affectedRows, err
	}
	ctx = context.WithValue(ctx, MyAffectedRows, affectedRows)
	if s.after != nil {
		if ctx, err = s.after(ctx); err != nil {
			return affectedRows, err
		}
	}
	return affectedRows, err
}

// myFilter Custom logic is typically called from:
// 1. After the pre-hook method executes, but before a delete operation or modify operation is performed.
// 2. Before querying data.
// 3. Before generating a subquery statement.
type myFilter struct {
	filter func(f Filter)
}

// MyDelete For DELETE.
type MyDelete interface {
	// AfterDelete Set post-delete data hook.
	AfterDelete(fc func(ctx context.Context) (context.Context, error))

	// BeforeDelete Set pre-delete data hook.
	BeforeDelete(fc func(ctx context.Context) (context.Context, error))

	// DeleteFilter Custom delete logic Filter.
	DeleteFilter(fc func(f Filter))

	// Delete Physically delete data.
	Delete(ctx context.Context, where Filter) (affectedRows int64, err error)

	// DeleteById Physically delete data.
	DeleteById(ctx context.Context, ids any) (affectedRows int64, err error)
}

type myDelete struct {
	*myHook
	*myFilter
	way   *Way
	table string
}

func newMyDelete(way *Way, table string) *myDelete {
	return &myDelete{
		myHook:   &myHook{},
		myFilter: &myFilter{},
		way:      way,
		table:    table,
	}
}

func (s *Way) MyDelete(table string) MyDelete {
	return newMyDelete(s, table)
}

func (s *myDelete) AfterDelete(fc func(ctx context.Context) (context.Context, error)) {
	s.after = fc
}

func (s *myDelete) BeforeDelete(fc func(ctx context.Context) (context.Context, error)) {
	s.before = fc
}

func (s *myDelete) DeleteFilter(fc func(f Filter)) {
	s.filter = fc
}

func (s *myDelete) Delete(ctx context.Context, where Filter) (affectedRows int64, err error) {
	way := ContextWay(ctx, s.way)
	table := way.Table(s.table)
	ctx = context.WithValue(ctx, MyTableName, s.table)
	ctx = context.WithValue(ctx, MyTable, table)
	if s.before != nil {
		if ctx, err = s.before(ctx); err != nil {
			return affectedRows, err
		}
	}
	if s.filter != nil {
		where = where.New(where)
		s.filter(where)
	}
	table.Where(where)
	if affectedRows, err = table.Delete(ctx); err != nil {
		return affectedRows, err
	}
	ctx = context.WithValue(ctx, MyAffectedRows, affectedRows)
	if s.after != nil {
		if ctx, err = s.after(ctx); err != nil {
			return affectedRows, err
		}
	}
	return affectedRows, err
}

func (s *myDelete) DeleteById(ctx context.Context, ids any) (affectedRows int64, err error) {
	return s.Delete(ctx, s.way.F().In(StrId, ids))
}

// MyUpdate For UPDATE.
type MyUpdate interface {
	// AfterUpdate Set post-update data hook.
	AfterUpdate(fc func(ctx context.Context) (context.Context, error))

	// BeforeUpdate Set pre-update data hook.
	BeforeUpdate(fc func(ctx context.Context) (context.Context, error))

	// UpdateFilter Custom update logic Filter.
	UpdateFilter(fc func(f Filter))

	// Update Implementing updated data.
	Update(ctx context.Context, where Filter, update func(u SQLUpdateSet)) (affectedRows int64, err error)

	// UpdateById Implementing updated data.
	UpdateById(ctx context.Context, ids any, update func(u SQLUpdateSet)) (affectedRows int64, err error)

	// Modify Implementing updated data.
	Modify(ctx context.Context, where Filter, modify any) (affectedRows int64, err error)

	// ModifyById Implementing updated data.
	ModifyById(ctx context.Context, id any, modify any) (affectedRows int64, err error)
}

type myUpdate struct {
	*myHook
	*myFilter
	way   *Way
	table string
}

func newMyUpdate(way *Way, table string) *myUpdate {
	return &myUpdate{
		myHook:   &myHook{},
		myFilter: &myFilter{},
		way:      way,
		table:    table,
	}
}

func (s *Way) MyUpdate(table string) MyUpdate {
	return newMyUpdate(s, table)
}

func (s *myUpdate) AfterUpdate(fc func(ctx context.Context) (context.Context, error)) {
	s.after = fc
}

func (s *myUpdate) BeforeUpdate(fc func(ctx context.Context) (context.Context, error)) {
	s.before = fc
}

func (s *myUpdate) UpdateFilter(fc func(f Filter)) {
	s.filter = fc
}

func (s *myUpdate) Update(ctx context.Context, where Filter, update func(u SQLUpdateSet)) (affectedRows int64, err error) {
	way := ContextWay(ctx, s.way)
	table := way.Table(s.table)
	ctx = context.WithValue(ctx, MyTableName, s.table)
	ctx = context.WithValue(ctx, MyTable, table)
	if s.before != nil {
		if ctx, err = s.before(ctx); err != nil {
			return affectedRows, err
		}
	}
	table.UPDATE(func(f Filter, u SQLUpdateSet) { update(u) })
	if s.filter != nil {
		where = where.New(where)
		s.filter(where)
	}
	table.Where(where)
	if affectedRows, err = table.Update(ctx); err != nil {
		return affectedRows, err
	}
	ctx = context.WithValue(ctx, MyAffectedRows, affectedRows)
	if s.after != nil {
		if ctx, err = s.after(ctx); err != nil {
			return affectedRows, err
		}
	}
	return affectedRows, err
}

func (s *myUpdate) UpdateById(ctx context.Context, id any, update func(u SQLUpdateSet)) (affectedRows int64, err error) {
	return s.Update(ctx, s.way.F().Equal(StrId, id), update)
}

func (s *myUpdate) Modify(ctx context.Context, where Filter, modify any) (affectedRows int64, err error) {
	return s.Update(ctx, where, func(u SQLUpdateSet) {
		u.Update(modify)
	})
}

func (s *myUpdate) ModifyById(ctx context.Context, id any, modify any) (affectedRows int64, err error) {
	return s.UpdateById(ctx, id, func(u SQLUpdateSet) {
		u.Update(modify)
	})
}

// MySelect For SELECT.
type MySelect interface {
	// SelectFilter Custom select logic Filter.
	SelectFilter(fc func(f Filter))

	// SelectAll Query multiple data.
	SelectAll(ctx context.Context, receiver any, options ...func(o *Table)) error

	// SelectOne Query a piece of data.
	SelectOne(ctx context.Context, receiver any, options ...func(o *Table)) error

	// SelectAllById Query multiple data.
	SelectAllById(ctx context.Context, ids any, receiver any, options ...func(o *Table)) error

	// SelectOneById Query a piece of data.
	SelectOneById(ctx context.Context, id any, receiver any, options ...func(o *Table)) error

	// SelectSQL Construct a subquery as a query table or CTE.
	SelectSQL(options ...func(o *Table)) *SQL

	// SelectExists Check whether the data exists.
	SelectExists(ctx context.Context, options ...func(o *Table)) (bool, error)

	// SelectCount Total number of statistical records.
	SelectCount(ctx context.Context, options ...func(o *Table)) (count int64, err error)

	// SelectCountFetch First count the total number of entries, then scan the list data.
	SelectCountFetch(ctx context.Context, receiver any, options ...func(o *Table)) (count int64, err error)

	// F Quickly create a new Filter instance.
	F() Filter

	// Equal Filter.Equal
	Equal(column any, value any) Filter

	// In Filter.In
	In(column any, values ...any) Filter

	// InGroup Filter.InGroup
	InGroup(columns any, values any) Filter

	// IdEqual Filter.Equal using id.
	IdEqual(value any) Filter

	// IdIn Filter.In using id.
	IdIn(values ...any) Filter

	// LikeSearch Keyword search matches multiple columns.
	LikeSearch(keyword string, columns ...string) Filter
}

type mySelect struct {
	*myFilter
	way     *Way
	table   string
	columns []string
}

func newMySelect(way *Way, table string, columns []string) *mySelect {
	return &mySelect{
		myFilter: &myFilter{},
		way:      way,
		table:    table,
		columns:  columns,
	}
}

func (s *Way) MySelect(table string, columns []string) MySelect {
	return newMySelect(s, table, columns)
}

func (s *mySelect) SelectFilter(fc func(f Filter)) {
	s.filter = fc
}

func (s *mySelect) selectAll(ctx context.Context, selectAll func(ctx context.Context, table *Table) error, options ...func(o *Table)) error {
	way := ContextWay(ctx, s.way)
	table := way.Table(s.table)
	for _, fc := range options {
		if fc != nil {
			fc(table)
		}
	}
	if s.filter != nil {
		table.WHERE(func(f Filter) {
			where := f.New(f)
			s.filter(where)
			f.ToEmpty().Use(where)
		})
	}
	table.SELECT(func(q SQLSelect) {
		if q.IsEmpty() {
			q.Select(s.columns)
		}
	})
	return selectAll(ctx, table)
}

func (s *mySelect) SelectAll(ctx context.Context, receiver any, options ...func(o *Table)) error {
	return s.selectAll(ctx, func(ctx context.Context, table *Table) error {
		return table.Fetch(ctx, receiver)
	}, options...)
}

func (s *mySelect) SelectOne(ctx context.Context, receiver any, options ...func(o *Table)) error {
	options = append(options, func(o *Table) {
		o.Limit(1)
	})
	return s.SelectAll(ctx, receiver, options...)
}

func (s *mySelect) SelectAllById(ctx context.Context, ids any, receiver any, options ...func(o *Table)) error {
	options = append(options, func(o *Table) {
		o.WHERE(func(f Filter) { f.In(StrId, ids) })
	})
	return s.SelectAll(ctx, receiver, options...)
}

func (s *mySelect) SelectOneById(ctx context.Context, id any, receiver any, options ...func(o *Table)) error {
	options = append(options, func(o *Table) {
		o.WHERE(func(f Filter) { f.Equal(strId, id) })
	})
	return s.SelectOne(ctx, receiver, options...)
}

func (s *mySelect) SelectSQL(options ...func(o *Table)) *SQL {
	table := s.way.Table(s.table)
	for _, fc := range options {
		if fc != nil {
			fc(table)
		}
	}
	if s.filter != nil {
		where := s.way.F()
		s.filter(where)
		table.WHERE(func(f Filter) { f.Use(where) })
	}
	table.SELECT(func(q SQLSelect) {
		if q.IsEmpty() {
			q.Select(s.columns)
		}
	})
	return table.ToSelect()
}

func (s *mySelect) SelectExists(ctx context.Context, options ...func(o *Table)) (bool, error) {
	script := s.SelectSQL(func(o *Table) {
		for _, fc := range options {
			if fc != nil {
				fc(o)
			}
		}
		if len(s.columns) > 0 {
			o.SELECT(func(q SQLSelect) {
				q.ToEmpty()
				q.Select(s.columns[0])
			})
		}
		o.Limit(1)
	})
	alias := "a"
	count, err := s.way.Table(alias).With(alias, script).Count(ctx)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *mySelect) SelectCount(ctx context.Context, options ...func(o *Table)) (count int64, err error) {
	err = s.selectAll(ctx, func(ctx context.Context, table *Table) error {
		count, err = table.Count(ctx)
		return err
	}, options...)
	return count, err
}

func (s *mySelect) SelectCountFetch(ctx context.Context, receiver any, options ...func(o *Table)) (count int64, err error) {
	err = s.selectAll(ctx, func(ctx context.Context, table *Table) error {
		count, err = table.CountFetch(ctx, receiver)
		return err
	}, options...)
	return count, err
}

func (s *mySelect) F() Filter {
	return s.way.F()
}

func (s *mySelect) Equal(column any, value any) Filter {
	return s.F().Equal(column, value)
}

func (s *mySelect) In(column any, values ...any) Filter {
	return s.F().In(column, values...)
}

func (s *mySelect) InGroup(columns any, values any) Filter {
	return s.F().InGroup(columns, values)
}

func (s *mySelect) IdEqual(value any) Filter {
	return s.Equal(StrId, value)
}

func (s *mySelect) IdIn(values ...any) Filter {
	return s.In(StrId, values...)
}

func (s *mySelect) LikeSearch(keyword string, columns ...string) Filter {
	search := s.F()
	LikeSearch(search, keyword, columns...)
	return search
}

// MyHidden Logical deletion of data.
type MyHidden interface {
	// AfterHidden Set post-hidden data hook.
	AfterHidden(fc func(ctx context.Context) (context.Context, error))

	// BeforeHidden Set pre-hidden data hook.
	BeforeHidden(fc func(ctx context.Context) (context.Context, error))

	// HiddenFilter Custom hidden logic Filter.
	HiddenFilter(fc func(f Filter))

	// Hidden Logical deletion of data.
	Hidden(ctx context.Context, where Filter) (affectedRows int64, err error)

	// HiddenById Logical deletion of data.
	HiddenById(ctx context.Context, ids any) (affectedRows int64, err error)
}

type myHidden struct {
	*myHook
	*myFilter
	way    *Way
	update func(u SQLUpdateSet)
	table  string
}

func newMyHidden(way *Way, table string, update func(u SQLUpdateSet)) *myHidden {
	return &myHidden{
		myHook:   &myHook{},
		myFilter: &myFilter{},
		way:      way,
		update:   update,
		table:    table,
	}
}

func (s *Way) MyHidden(table string, update func(u SQLUpdateSet)) MyHidden {
	return newMyHidden(s, table, update)
}

func (s *myHidden) BeforeHidden(fc func(ctx context.Context) (context.Context, error)) {
	s.before = fc
}

func (s *myHidden) AfterHidden(fc func(ctx context.Context) (context.Context, error)) {
	s.after = fc
}

func (s *myHidden) HiddenFilter(fc func(f Filter)) {
	s.filter = fc
}

func (s *myHidden) Hidden(ctx context.Context, where Filter) (affectedRows int64, err error) {
	way := ContextWay(ctx, s.way)
	table := way.Table(s.table)
	ctx = context.WithValue(ctx, MyTableName, s.table)
	ctx = context.WithValue(ctx, MyTable, table)
	if s.before != nil {
		if ctx, err = s.before(ctx); err != nil {
			return affectedRows, err
		}
	}
	if s.update != nil {
		table.UPDATE(func(f Filter, u SQLUpdateSet) { s.update(u) })
	}
	if s.filter != nil {
		where = where.New(where)
		s.filter(where)
	}
	table.Where(where)
	if affectedRows, err = table.Update(ctx); err != nil {
		return affectedRows, err
	}
	ctx = context.WithValue(ctx, MyAffectedRows, affectedRows)
	if s.after != nil {
		if ctx, err = s.after(ctx); err != nil {
			return affectedRows, err
		}
	}
	return affectedRows, err
}

func (s *myHidden) HiddenById(ctx context.Context, ids any) (affectedRows int64, err error) {
	return s.Hidden(ctx, s.way.F().In(StrId, ids))
}

// MySchema Combo MyInsert, MyDelete, MyUpdate, MySelect, MyHidden interfaces.
type MySchema interface {
	MyInsert
	MyDelete
	MyUpdate
	MySelect
	MyHidden
}

type mySchema struct {
	*myInsert
	*myDelete
	*myUpdate
	*mySelect
	*myHidden
}

func newMySchema(way *Way, table string, columns []string) *mySchema {
	return &mySchema{
		myInsert: newMyInsert(way, table),
		myDelete: newMyDelete(way, table),
		myUpdate: newMyUpdate(way, table),
		mySelect: newMySelect(way, table, columns),
		myHidden: newMyHidden(way, table, nil),
	}
}

func (s *Way) MySchema(table string, columns []string) MySchema {
	return newMySchema(s, table, columns)
}
