// Quickly build SELECT, INSERT, UPDATE, DELETE statements and support immediate execution of them.

package hey

import (
	"context"
	"database/sql"

	"github.com/cd365/hey/v7/cst"
)

// TableNamer Generic interface for getting table name.
type TableNamer interface {
	// Table Get the table name.
	Table() string
}

// MakeSQL Data structures for building SQL scripts.
type MakeSQL struct {
	// Label Used for setting SQL statement labels.
	Label SQLLabel

	// With -> WITH
	With SQLWith

	// Query -> SELECT
	Query SQLSelect

	// Query -> FROM
	Table SQLAlias

	// Join -> JOIN
	Join SQLJoin

	// Where -> WHERE ( xxx )
	Where Filter

	// GroupBy -> GROUP BY xxx [HAVING xxx]
	GroupBy SQLGroupBy

	// Window -> WINDOW
	Window SQLWindow

	// OrderBy -> ORDER BY
	OrderBy SQLOrderBy

	// Limit -> LIMIT m [OFFSET n]
	Limit SQLLimit

	// Insert -> INSERT INTO xxx
	Insert SQLInsert

	// UpdateSet -> UPDATE xxx SET xxx [WHERE ( xxx )]
	UpdateSet SQLUpdateSet

	Way *Way

	// Other additional parameters.

	// ToExistsSubquery -> SELECT EXISTS ( SELECT 1 FROM xxx ) AS a
	ToExistsSubquery []func(script *SQL)

	// ToCountColumns -> SELECT COUNT(*) FROM xxx ...
	ToCountColumns []string
}

// toSQLSelect SQL: SELECT xxx ...
func toSQLSelect(s MakeSQL) *SQL {
	way := s.Way
	if s.Query == nil {
		s.Query = way.cfg.NewSQLSelect(way)
	}
	if s.Table == nil {
		return NewEmptySQL()
	}
	if s.Table.IsEmpty() {
		if s.Query.IsEmpty() {
			return NewEmptySQL()
		}
		lists := make([]any, 0, 3)
		lists = append(lists, s.Label, cst.SELECT, s.Query)
		return JoinSQLSpace(lists...).ToSQL()
	}

	lists := make([]any, 0, 16)
	lists = append(
		lists,
		s.Label, s.With, cst.SELECT,
		s.Query, cst.FROM, s.Table,
		s.Join,
	)
	if s.Where != nil && !s.Where.IsEmpty() {
		lists = append(lists, cst.WHERE, parcelSingleFilter(s.Where))
	}
	lists = append(lists, s.GroupBy)
	if s.Window != nil {
		window := s.Window.ToSQL()
		if window != nil && !window.IsEmpty() {
			lists = append(lists, window)
		}
	}
	lists = append(lists, s.OrderBy, s.Limit)
	return JoinSQLSpace(lists...).ToSQL()
}

// toSQLInsert SQL: INSERT INTO xxx ...
func toSQLInsert(s MakeSQL) *SQL {
	insert := s.Insert
	if insert == nil {
		return NewEmptySQL()
	}
	if !insert.TableIsValid() {
		insert.Table(s.Table)
	}
	script := insert.ToSQL()
	if script.IsEmpty() {
		return NewEmptySQL()
	}
	return JoinSQLSpace(s.Label, script).ToSQL()
}

func toSQLUpdateDelete(s MakeSQL, category string) *SQL {
	lists := make([]any, 0, 8)
	way := s.Way
	whereIsEmpty := s.Where == nil || s.Where.IsEmpty()
	switch category {
	case cst.UPDATE:
		if way.cfg.UpdateRequireWhere && whereIsEmpty {
			return NewEmptySQL()
		}
		lists = append(
			lists,
			s.Label, s.With, cst.UPDATE,
			s.Table, cst.SET, s.UpdateSet,
		)
	case cst.DELETE:
		if way.cfg.DeleteRequireWhere && whereIsEmpty {
			return NewEmptySQL()
		}
		lists = append(
			lists,
			s.Label, s.With, cst.DELETE,
			cst.FROM, s.Table, s.Join,
		)
	default:
		return NewEmptySQL()
	}
	lists = append(lists, cst.WHERE, parcelSingleFilter(s.Where))
	return JoinSQLSpace(lists...).ToSQL()
}

// toSQLUpdate SQL: UPDATE xxx SET ...
func toSQLUpdate(s MakeSQL) *SQL {
	if s.UpdateSet == nil || s.Table == nil || s.Table.IsEmpty() || s.UpdateSet.IsEmpty() {
		return NewEmptySQL()
	}
	return toSQLUpdateDelete(s, cst.UPDATE)
}

// toSQLDelete SQL: DELETE FROM xxx ...
func toSQLDelete(s MakeSQL) *SQL {
	if s.Table == nil || s.Table.IsEmpty() {
		return NewEmptySQL()
	}
	return toSQLUpdateDelete(s, cst.DELETE)
}

// toSQLSelectExists SQL: SELECT EXISTS ( SELECT 1 FROM xxx ... ) AS a
func toSQLSelectExists(s MakeSQL) *SQL {
	// SELECT EXISTS ( SELECT 1 FROM example_table ) AS a
	// SELECT EXISTS ( SELECT 1 FROM example_table WHERE ( id > 0 ) ) AS a
	// SELECT EXISTS ( ( SELECT 1 FROM example_table WHERE ( column1 = 'value1' ) ) UNION ALL ( SELECT 1 FROM example_table WHERE ( column2 = 'value2' ) ) ) AS a

	columns, columnsArgs := ([]string)(nil), (map[int][]any)(nil)
	way := s.Way
	if s.Query == nil {
		s.Query = way.cfg.NewSQLSelect(way)
	}
	if s.Query.Len() > 0 {
		columns, columnsArgs = s.Query.Get()
		s.Query.ToEmpty()
	}
	s.Query.Select("1")
	defer func() {
		if len(columns) == 0 {
			s.Query.ToEmpty()
		} else {
			s.Query.Set(columns, columnsArgs)
		}
	}()
	subquery := toSQLSelect(s)
	exists := s.ToExistsSubquery
	for i := len(exists) - 1; i >= 0; i-- {
		if exists[i] != nil {
			exists[i](subquery)
			break
		}
	}
	if subquery.IsEmpty() {
		return NewEmptySQL()
	}
	lists := make([]any, 0, 8)
	lists = append(
		lists,
		cst.SELECT, cst.EXISTS,
		cst.LeftParenthesis, subquery, cst.RightParenthesis,
		cst.AS, way.Replace(cst.A),
	)
	return JoinSQLSpace(lists...).ToSQL()
}

// toSQLSelectCount SQL: SELECT COUNT(*) xxx FROM ...
func toSQLSelectCount(s MakeSQL) *SQL {
	if s.Table == nil || s.Table.IsEmpty() {
		return NewEmptySQL()
	}
	counts := s.ToCountColumns[:]
	way := s.Way
	query := JoinSQLSpace("COUNT(*)", cst.AS, way.Replace("counts"))
	if len(counts) > 0 {
		query = JoinSQLSpace(AnyAny(counts)...)
	}
	lists := make([]any, 0, 9)
	lists = append(
		lists,
		s.Label, s.With, cst.SELECT,
		query, cst.FROM, s.Table, s.Join,
	)
	if s.Where != nil && !s.Where.IsEmpty() {
		lists = append(lists, cst.WHERE, parcelSingleFilter(s.Where))
	}
	return JoinSQLSpace(lists...).ToSQL()
}

// Table Quickly build SELECT, INSERT, UPDATE, DELETE statements and support immediate execution of them.
type Table struct {
	way *Way

	// label Used for setting SQL statement labels.
	label SQLLabel

	// with -> WITH
	with SQLWith

	// query -> SELECT
	query SQLSelect

	// table -> FROM
	table SQLAlias

	// joins -> JOIN
	joins SQLJoin

	// where -> WHERE ( xxx )
	where Filter

	// groupBy -> GROUP BY xxx [HAVING xxx]
	groupBy SQLGroupBy

	// window -> WINDOW
	window SQLWindow

	// orderBy -> ORDER BY
	orderBy SQLOrderBy

	// limit -> LIMIT m [OFFSET n]
	limit SQLLimit

	// insert -> INSERT INTO xxx
	insert SQLInsert

	// updateSet -> UPDATE xxx SET xxx [WHERE ( xxx )]
	updateSet SQLUpdateSet
}

// Table Create a *Table object to execute SELECT, INSERT, UPDATE, and DELETE statements.
func (s *Way) Table(table any) *Table {
	return &Table{
		way:   s,
		table: s.cfg.NewSQLTable(s, table),
	}
}

// ToEmpty Do not reset table.
func (s *Table) ToEmpty() *Table {
	for _, v := range []ToEmpty{
		s.label, s.with, s.query,
		s.joins, s.where, s.groupBy,
		s.window, s.orderBy, s.limit,
		s.insert, s.updateSet,
	} {
		if v != nil {
			v.ToEmpty()
		}
	}
	return s
}

// F Quickly create a Filter.
func (s *Table) F(makers ...Maker) Filter {
	return s.way.F(makers...)
}

// V Get the currently used *Way object.
func (s *Table) V() *Way {
	return s.way
}

// W Use *Way given a non-nil value.
func (s *Table) W(way *Way) {
	if way != nil {
		s.way = way
	}
}

// LabelFunc Set label through func.
func (s *Table) LabelFunc(fx func(c SQLLabel)) *Table {
	if s.label == nil {
		s.label = s.way.cfg.NewSQLLabel(s.way)
	}
	fx(s.label)
	return s
}

// Labels SQL statement labels.
func (s *Table) Labels(labels ...string) *Table {
	return s.LabelFunc(func(c SQLLabel) {
		c.Labels(labels...)
	})
}

// WithFunc Custom common table expression (CTE).
func (s *Table) WithFunc(fx func(w SQLWith)) *Table {
	if s.with == nil {
		s.with = s.way.cfg.NewSQLWith(s.way)
	}
	fx(s.with)
	return s
}

// With Add a common table expression.
func (s *Table) With(alias string, maker Maker, columns ...string) *Table {
	return s.WithFunc(func(w SQLWith) {
		w.Set(alias, maker, columns...)
	})
}

// SelectFunc Set SELECT through func.
func (s *Table) SelectFunc(fx func(q SQLSelect)) *Table {
	if s.query == nil {
		s.query = s.way.cfg.NewSQLSelect(s.way)
	}
	fx(s.query)
	return s
}

// Distinct SQL DISTINCT columns.
func (s *Table) Distinct() *Table {
	return s.SelectFunc(func(q SQLSelect) {
		q.Distinct()
	})
}

// Select Add one or more query lists. If no parameter is provided, all existing query lists will be deleted.
func (s *Table) Select(columns ...any) *Table {
	return s.SelectFunc(func(q SQLSelect) {
		q.Select(columns...)
	})
}

// TableFunc Set query table through func.
func (s *Table) TableFunc(fx func(t SQLAlias)) *Table {
	fx(s.table)
	return s
}

// Table Set the table name, or possibly a subquery with an alias.
func (s *Table) Table(table any) *Table {
	return s.TableFunc(func(t SQLAlias) {
		t.SetSQL(s.way.cfg.NewSQLTable(s.way, table).GetSQL())
	})
}

// Alias Set the table alias name.
func (s *Table) Alias(alias string) *Table {
	return s.TableFunc(func(t SQLAlias) {
		t.SetAlias(alias)
	})
}

// JoinFunc Custom join query.
func (s *Table) JoinFunc(fx func(join SQLJoin)) *Table {
	if s.query == nil {
		s.query = s.way.cfg.NewSQLSelect(s.way)
	}
	if s.joins == nil {
		s.joins = s.way.cfg.NewSQLJoin(s.way, s.query)
		s.joins.SetMain(s.table)
	}
	fx(s.joins)
	return s
}

// InnerJoin INNER JOIN.
func (s *Table) InnerJoin(fx func(join SQLJoin) (SQLAlias, SQLJoinOn)) *Table {
	return s.JoinFunc(func(join SQLJoin) {
		join.InnerJoin(fx(join))
	})
}

// LeftJoin LEFT JOIN.
func (s *Table) LeftJoin(fx func(join SQLJoin) (SQLAlias, SQLJoinOn)) *Table {
	return s.JoinFunc(func(join SQLJoin) {
		join.LeftJoin(fx(join))
	})
}

// RightJoin RIGHT JOIN.
func (s *Table) RightJoin(fx func(join SQLJoin) (SQLAlias, SQLJoinOn)) *Table {
	return s.JoinFunc(func(join SQLJoin) {
		join.RightJoin(fx(join))
	})
}

// WhereFunc Set WHERE through func.
func (s *Table) WhereFunc(fx func(f Filter)) *Table {
	if s.where == nil {
		s.where = s.way.F()
	}
	fx(s.where)
	return s
}

// Where Set the WHERE condition.
func (s *Table) Where(values ...Maker) *Table {
	return s.WhereFunc(func(f Filter) {
		f.ToEmpty()
		f.Use(values...)
	})
}

// GroupFunc Set GROUP BY through func.
func (s *Table) GroupFunc(fx func(g SQLGroupBy)) *Table {
	if s.groupBy == nil {
		s.groupBy = s.way.cfg.NewSQLGroupBy(s.way)
	}
	fx(s.groupBy)
	return s
}

// Group Set GROUP BY condition.
func (s *Table) Group(groups ...any) *Table {
	return s.GroupFunc(func(g SQLGroupBy) {
		g.Group(groups...)
	})
}

// HavingFunc Set HAVING through func.
func (s *Table) HavingFunc(fx func(h Filter)) *Table {
	return s.GroupFunc(func(g SQLGroupBy) {
		g.Having(fx)
	})
}

// Having Set the HAVING condition.
func (s *Table) Having(values ...Maker) *Table {
	return s.HavingFunc(func(f Filter) {
		f.ToEmpty()
		f.Use(values...)
	})
}

// WINDOW Statements:
// WINDOW alias1 AS ( PARTITION BY column1, column2 ORDER BY column3 DESC, column4 DESC ), alias2 AS ( PARTITION BY column5 ) ...

// WindowFunc Custom window statements.
func (s *Table) WindowFunc(fx func(w SQLWindow)) *Table {
	if fx == nil {
		return s
	}
	if s.window == nil {
		s.window = s.way.cfg.NewSQLWindow(s.way)
	}
	fx(s.window)
	return s
}

// Window Add a window expression.
func (s *Table) Window(alias string, maker func(o SQLWindowFuncOver)) *Table {
	if alias == cst.Empty || maker == nil {
		return s
	}
	return s.WindowFunc(func(w SQLWindow) {
		w.Set(alias, maker)
	})
}

// OrderFunc Set ORDER BY through func.
func (s *Table) OrderFunc(fx func(o SQLOrderBy)) *Table {
	if s.orderBy == nil {
		s.orderBy = s.way.cfg.NewSQLOrderBy(s.way)
	}
	fx(s.orderBy)
	return s
}

// OrderString Set ORDER BY columns through *string.
func (s *Table) OrderString(order *string) *Table {
	return s.OrderFunc(func(o SQLOrderBy) {
		o.OrderString(order)
	})
}

// Asc Sort ascending.
func (s *Table) Asc(columns ...string) *Table {
	return s.OrderFunc(func(o SQLOrderBy) {
		o.Asc(columns...)
	})
}

// Desc Sort descending.
func (s *Table) Desc(columns ...string) *Table {
	return s.OrderFunc(func(o SQLOrderBy) {
		o.Desc(columns...)
	})
}

// LimitFunc Set LIMIT x [OFFSET x] through func.
func (s *Table) LimitFunc(fx func(o SQLLimit)) *Table {
	if s.limit == nil {
		s.limit = s.way.cfg.NewSQLLimit(s.way)
	}
	fx(s.limit)
	return s
}

// Limit Set the maximum number of query result sets.
func (s *Table) Limit(limit int64) *Table {
	return s.LimitFunc(func(o SQLLimit) {
		o.Limit(limit)
	})
}

// Offset Set the offset of the query target data.
func (s *Table) Offset(offset int64) *Table {
	return s.LimitFunc(func(o SQLLimit) {
		o.Offset(offset)
	})
}

// Limiter Set limit and offset at the same time.
func (s *Table) Limiter(limiter Limiter) *Table {
	if limiter == nil {
		return s
	}
	return s.LimitFunc(func(o SQLLimit) {
		o.Limit(limiter.GetLimit()).Offset(limiter.GetOffset())
	})
}

// Page Pagination query, page and pageSize.
func (s *Table) Page(page int64, pageSize ...int64) *Table {
	return s.LimitFunc(func(o SQLLimit) {
		o.Page(page, pageSize...)
	})
}

// InsertFunc Set inserting data through func.
func (s *Table) InsertFunc(fx func(i SQLInsert)) *Table {
	if s.insert == nil {
		s.insert = s.way.cfg.NewSQLInsert(s.way)
	}
	fx(s.insert)
	return s
}

// UpdateFunc Set updating data through func.
func (s *Table) UpdateFunc(fx func(f Filter, u SQLUpdateSet)) *Table {
	if s.where == nil {
		s.where = s.way.F()
	}
	if s.updateSet == nil {
		s.updateSet = s.way.cfg.NewSQLUpdateSet(s.way)
	}
	fx(s.where, s.updateSet)
	return s
}

func (s *Table) newMakeSQL() MakeSQL {
	return MakeSQL{
		Label:     s.label,
		With:      s.with,
		Query:     s.query,
		Table:     s.table,
		Join:      s.joins,
		Where:     s.where,
		GroupBy:   s.groupBy,
		Window:    s.window,
		OrderBy:   s.orderBy,
		Limit:     s.limit,
		Insert:    s.insert,
		UpdateSet: s.updateSet,
		Way:       s.way,
	}
}

func (s *Table) toSelect() *SQL {
	script := s.newMakeSQL()
	if script.Query == nil {
		script.Query = s.way.cfg.NewSQLSelect(s.way)
	}
	return s.way.cfg.ToSQLSelect(script)
}

// ToSelect Build SELECT statement.
func (s *Table) ToSelect() *SQL {
	return s.toSelect()
}

// ToInsert Build INSERT statement.
func (s *Table) ToInsert() *SQL {
	return s.way.cfg.ToSQLInsert(s.newMakeSQL())
}

// ToUpdate Build UPDATE statement.
func (s *Table) ToUpdate() *SQL {
	return s.way.cfg.ToSQLUpdate(s.newMakeSQL())
}

// ToDelete Build DELETE statement.
func (s *Table) ToDelete() *SQL {
	return s.way.cfg.ToSQLDelete(s.newMakeSQL())
}

// ToCount Build COUNT-SELECT statement.
func (s *Table) ToCount(counts ...string) *SQL {
	script := s.newMakeSQL()
	script.ToCountColumns = counts[:]
	return s.way.cfg.ToSQLSelectCount(script)
}

// ToExists Build SELECT EXISTS statement, allow replacing or updating the subquery script of EXISTS.
func (s *Table) ToExists(exists ...func(script *SQL)) *SQL {
	// SELECT EXISTS ( SELECT 1 FROM example_table ) AS a
	// SELECT EXISTS ( SELECT 1 FROM example_table WHERE ( id > 0 ) ) AS a
	// SELECT EXISTS ( ( SELECT 1 FROM example_table WHERE ( column1 = 'value1' ) ) UNION ALL ( SELECT 1 FROM example_table WHERE ( column2 = 'value2' ) ) ) AS a
	script := s.newMakeSQL()
	script.ToExistsSubquery = exists[:]
	return s.way.cfg.ToSQLSelectExists(script)
}

// Query Execute a SELECT statement.
func (s *Table) Query(ctx context.Context, query func(rows *sql.Rows) error) error {
	return s.way.Query(ctx, s.ToSelect(), query)
}

// QueryExists Check if the data exists, allow replacing or updating the subquery script of EXISTS.
func (s *Table) QueryExists(ctx context.Context, exists ...func(script *SQL)) (bool, error) {
	script := s.ToExists(exists...)
	return s.way.QueryExists(ctx, script)
}

// Count Total number of statistics.
func (s *Table) Count(ctx context.Context, counts ...string) (int64, error) {
	count := int64(0)
	script := s.ToCount(counts...)
	err := s.way.Query(ctx, script, func(rows *sql.Rows) error {
		for rows.Next() {
			if err := rows.Scan(&count); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// Scan Scanning data into result by reflect.
func (s *Table) Scan(ctx context.Context, result any) error {
	return s.way.Scan(ctx, s.ToSelect(), result)
}

// CountScan Merge statistics and scan data.
func (s *Table) CountScan(ctx context.Context, result any, counts ...string) (count int64, err error) {
	count, err = s.Count(ctx, counts...)
	if err != nil {
		return count, err
	}
	if count == 0 {
		return count, err
	}
	err = s.Scan(ctx, result)
	if err != nil {
		return count, err
	}
	return count, err
}

// MapScan Scanning the query results into []map[string]any.
func (s *Table) MapScan(ctx context.Context, adjusts ...AdjustColumnAnyValue) ([]map[string]any, error) {
	return s.way.MapScan(ctx, s.ToSelect(), adjusts...)
}

// Insert Execute an INSERT INTO statement.
func (s *Table) Insert(ctx context.Context) (int64, error) {
	script := s.ToInsert()
	if script.IsEmpty() {
		return 0, ErrEmptySqlStatement
	}
	if insert := s.insert; insert != nil {
		if returning := insert.GetReturning(); returning != nil {
			if execute := returning.GetExecute(); execute != nil {
				stmt, err := s.way.Prepare(ctx, script.Prepare)
				if err != nil {
					return 0, err
				}
				return execute(ctx, stmt, script.Args...)
			}
		}
	}
	return s.way.Execute(ctx, script)
}

// Update Execute an UPDATE statement.
func (s *Table) Update(ctx context.Context) (int64, error) {
	if s.way.cfg.UpdateRequireWhere && (s.where == nil || s.where.IsEmpty()) {
		return 0, ErrNoWhereCondition
	}
	return s.way.Execute(ctx, s.ToUpdate())
}

// Delete Execute a DELETE statement.
func (s *Table) Delete(ctx context.Context) (int64, error) {
	if s.way.cfg.DeleteRequireWhere && (s.where == nil || s.where.IsEmpty()) {
		return 0, ErrNoWhereCondition
	}
	return s.way.Execute(ctx, s.ToDelete())
}

// Create Quickly insert data into the table.
func (s *Table) Create(ctx context.Context, create any) (int64, error) {
	return s.InsertFunc(func(i SQLInsert) {
		i.Create(create)
	}).Insert(ctx)
}

// Modify Quickly update data in the table.
func (s *Table) Modify(ctx context.Context, modify any) (int64, error) {
	return s.UpdateFunc(func(f Filter, u SQLUpdateSet) {
		u.Update(modify)
	}).Update(ctx)
}

// Complex Execute a set of SQL statements within a transaction.
type Complex interface {
	// Upsert Update or insert data.
	Upsert(ctx context.Context) (updateAffectedRows int64, insertResult int64, err error)

	// DeleteCreate Delete data first, then insert data.
	DeleteCreate(ctx context.Context) (deleteAffectedRows int64, insertResult int64, err error)
}

// myComplex Implement Complex interface.
type myComplex struct {
	table *Table
}

func (s myComplex) atomic(ctx context.Context, group func(tx *Way) error) error {
	way := s.table.way
	if way.IsInTransaction() {
		return group(way)
	}
	defer func() { s.table.W(way) }()
	return way.TransactionNew(ctx, func(tx *Way) error {
		s.table.W(tx)
		return group(tx)
	})
}

// Upsert If the data exists, update the data; otherwise, insert the data.
func (s myComplex) Upsert(ctx context.Context) (updateAffectedRows int64, insertResult int64, err error) {
	err = s.atomic(ctx, func(tx *Way) error {
		exist, table := false, s.table
		exist, err = table.QueryExists(ctx)
		if err != nil {
			return err
		}
		if exist {
			updateAffectedRows, err = table.Update(ctx)
			if err != nil {
				return err
			}
		} else {
			insertResult, err = table.Insert(ctx)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return
}

// DeleteCreate Delete data first, then insert data.
func (s myComplex) DeleteCreate(ctx context.Context) (deleteAffectedRows int64, insertResult int64, err error) {
	err = s.atomic(ctx, func(tx *Way) error {
		table := s.table
		if where := table.where; where != nil && !where.IsEmpty() {
			deleteAffectedRows, err = table.Delete(ctx)
			if err != nil {
				return err
			}
		}
		insertResult, err = table.Insert(ctx)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

// NewComplex Create a Complex object.
func NewComplex(table *Table) Complex {
	return myComplex{
		table: table,
	}
}

// UpsertFunc Set UPDATE and INSERT through func, ultimately execute UPDATE or INSERT.
func (s *Table) UpsertFunc(upsert any, where func(f Filter)) *Table {
	s.UpdateFunc(func(f Filter, u SQLUpdateSet) {
		if where != nil {
			where(f)
		}
		u.Update(upsert)
	})
	s.InsertFunc(func(i SQLInsert) {
		i.Create(upsert)
	})
	return s
}

// Upsert Data for the same table.
// Filter data based on the WHERE condition. If a record exists, update all data selected by the filter condition;
// otherwise, perform an insert operation.
func (s *Table) Upsert(ctx context.Context, upsert any, where func(f Filter)) (updateAffectedRows int64, insertResult int64, err error) {
	return NewComplex(s.UpsertFunc(upsert, where)).Upsert(ctx)
}

// DeleteCreate Data for the same table.
// First delete all data that matches the specified WHERE condition, then insert the new data;
// if no WHERE condition is set, no data will be deleted.
func (s *Table) DeleteCreate(ctx context.Context, create any, where func(f Filter)) (deleteAffectedRows int64, insertResult int64, err error) {
	s.WhereFunc(func(f Filter) {
		if where != nil {
			where(f)
		}
	})
	s.InsertFunc(func(i SQLInsert) {
		i.Create(create)
	})
	return NewComplex(s).DeleteCreate(ctx)
}
