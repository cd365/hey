// Quickly build SELECT, INSERT, UPDATE, DELETE statements and support immediate execution of them.

package hey

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/cd365/hey/v6/cst"
)

// TableNamer Generic interface for getting table name.
type TableNamer interface {
	// Table Get the table name.
	Table() string
}

// Table Quickly build SELECT, INSERT, UPDATE, DELETE statements and support immediate execution of them.
type Table struct {
	way *Way

	comment *sqlComment

	with *sqlWith

	query *sqlSelect

	table *sqlAlias

	joins *sqlJoin

	where Filter

	groupBy *sqlGroupBy

	orderBy *sqlOrderBy

	limit *sqlLimit

	insert *sqlInsert

	updateSet *sqlUpdateSet
}

// optimizeTableSQL Optimize table SQL.
func optimizeTableSQL(way *Way, table *SQL) *SQL {
	result := NewEmptySQL()
	if way == nil || table == nil || table.IsEmpty() {
		return result
	}
	latest := table.Clone()
	latest.Prepare = strings.TrimSpace(latest.Prepare)
	if latest.IsEmpty() {
		return result
	}
	single := cst.Space
	double := JoinString(single, single)
	for strings.Contains(latest.Prepare, double) {
		latest.Prepare = strings.ReplaceAll(latest.Prepare, double, single)
	}
	count := strings.Count(latest.Prepare, cst.Space)
	if count == 0 {
		latest.Prepare = way.Replace(latest.Prepare)
	} else {
		if count > 2 {
			// Using a subquery as a table.
			// Consider subqueries with alias name.
			latest = ParcelSQL(latest)
		}
	}
	return latest
}

// getTable Extract table names from any type.
func (s *Way) getTable(table any) *sqlAlias {
	result := newSqlAlias(cst.Empty).v(s)
	switch example := table.(type) {
	case string:
		result.SetSQL(optimizeTableSQL(s, NewSQL(example)))
	case *SQL:
		result.SetSQL(optimizeTableSQL(s, example))
	case Maker:
		if example != nil {
			result.SetSQL(optimizeTableSQL(s, example.ToSQL()))
		}
	case TableNamer:
		result.SetSQL(optimizeTableSQL(s, NewSQL(example.Table())))
	default:
		if value := reflect.ValueOf(table); !value.IsNil() {
			if method := value.MethodByName(s.cfg.tableMethodName); method.IsValid() {
				if values := method.Call(nil); len(values) == 1 {
					return s.getTable(values[0].Interface())
				}
			}
		}
		// Consider multi-level pointers.
		result.SetSQL(AnyToSQL(table))
	}
	return result
}

// Table Create a *Table object to execute SELECT, INSERT, UPDATE, and DELETE statements.
func (s *Way) Table(table any) *Table {
	query := newSqlSelect(s)
	result := &Table{
		way:       s,
		comment:   newSqlComment(),
		with:      newSqlWith(),
		query:     query,
		table:     s.getTable(table),
		joins:     newSqlJoin(s),
		where:     s.F(),
		groupBy:   newSqlGroupBy(s),
		orderBy:   newSqlOrderBy(s),
		limit:     newSqlLimit(s),
		insert:    newSqlInsert(s),
		updateSet: newSqlUpdateSet(s),
	}
	result.joins.query = query
	return result
}

// ToEmpty Do not reset table.
func (s *Table) ToEmpty() *Table {
	s.comment.ToEmpty()
	s.with.ToEmpty()
	s.query.ToEmpty()
	s.joins.ToEmpty()
	s.where.ToEmpty()
	s.groupBy.ToEmpty()
	s.orderBy.ToEmpty()
	s.limit.ToEmpty()
	s.insert.ToEmpty()
	s.updateSet.ToEmpty()
	return s
}

// F Quickly create a Filter.
func (s *Table) F(filters ...Filter) Filter {
	return s.way.F(filters...)
}

// V Use *Way given a non-nil value.
func (s *Table) V(values ...*Way) *Table {
	s.way = s.way.V(values...)
	return s
}

// W Get the currently used *Way object.
func (s *Table) W() *Way {
	return s.way
}

// Comment SQL statement notes.
func (s *Table) Comment(comment string) *Table {
	s.comment.Comment(comment)
	return s
}

// WithFunc Custom common table expression (CTE).
func (s *Table) WithFunc(fc func(w SQLWith)) *Table {
	fc(s.with)
	return s
}

// With Add a common table expression.
func (s *Table) With(alias string, maker Maker, columns ...string) *Table {
	return s.WithFunc(func(w SQLWith) {
		w.Set(alias, maker, columns...)
	})
}

// SelectFunc Set SELECT through func.
func (s *Table) SelectFunc(fc func(q SQLSelect)) *Table {
	fc(s.query)
	return s
}

// Distinct SQL DISTINCT columns.
func (s *Table) Distinct() *Table {
	return s.SelectFunc(func(q SQLSelect) {
		q.Distinct()
	})
}

// Select Add one or more query lists. If no parameter is provided, all existing query lists will be deleted.
func (s *Table) Select(selects ...any) *Table {
	return s.SelectFunc(func(q SQLSelect) {
		q.Select(selects...)
	})
}

// TableFunc Set query table through func.
func (s *Table) TableFunc(fc func(t SQLAlias)) *Table {
	fc(s.table)
	return s
}

// Table Set the table name, or possibly a subquery with an alias.
func (s *Table) Table(table any) *Table {
	return s.TableFunc(func(t SQLAlias) {
		t.SetSQL(s.way.getTable(table).GetSQL())
	})
}

// Alias Set the table alias name.
func (s *Table) Alias(alias string) *Table {
	return s.TableFunc(func(t SQLAlias) {
		t.SetAlias(alias)
	})
}

// JoinFunc Custom join query.
func (s *Table) JoinFunc(fc func(j SQLJoin)) *Table {
	if s.joins.table == nil {
		s.joins.table = s.table
	}
	fc(s.joins)
	return s
}

// InnerJoin INNER JOIN.
func (s *Table) InnerJoin(fc func(j SQLJoin) (left SQLAlias, right SQLAlias, assoc SQLJoinAssoc)) *Table {
	return s.JoinFunc(func(j SQLJoin) {
		j.InnerJoin(fc(j))
	})
}

// LeftJoin LEFT JOIN.
func (s *Table) LeftJoin(fc func(j SQLJoin) (left SQLAlias, right SQLAlias, assoc SQLJoinAssoc)) *Table {
	return s.JoinFunc(func(j SQLJoin) {
		j.LeftJoin(fc(j))
	})
}

// RightJoin RIGHT JOIN.
func (s *Table) RightJoin(fc func(j SQLJoin) (left SQLAlias, right SQLAlias, assoc SQLJoinAssoc)) *Table {
	return s.JoinFunc(func(j SQLJoin) {
		j.RightJoin(fc(j))
	})
}

// WhereFunc Set WHERE through func.
func (s *Table) WhereFunc(fc func(f Filter)) *Table {
	fc(s.where)
	return s
}

// Where Set the WHERE condition.
func (s *Table) Where(filters ...Filter) *Table {
	return s.WhereFunc(func(f Filter) {
		f.ToEmpty().Use(filters...)
	})
}

// GroupFunc Set GROUP BY through func.
func (s *Table) GroupFunc(fc func(g SQLGroupBy)) *Table {
	fc(s.groupBy)
	return s
}

// Group Set GROUP BY condition.
func (s *Table) Group(groups ...any) *Table {
	return s.GroupFunc(func(g SQLGroupBy) {
		g.Group(groups...)
	})
}

// HavingFunc Set HAVING through func.
func (s *Table) HavingFunc(fc func(h Filter)) *Table {
	return s.GroupFunc(func(g SQLGroupBy) {
		g.Having(fc)
	})
}

// Having Set the HAVING condition.
func (s *Table) Having(filters ...Filter) *Table {
	return s.HavingFunc(func(f Filter) {
		f.ToEmpty().Use(filters...)
	})
}

// OrderFunc Set ORDER BY through func.
func (s *Table) OrderFunc(fc func(o SQLOrderBy)) *Table {
	fc(s.orderBy)
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
func (s *Table) LimitFunc(fc func(o SQLLimit)) *Table {
	fc(s.limit)
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
func (s *Table) InsertFunc(fc func(i SQLInsert)) *Table {
	fc(s.insert)
	return s
}

// UpdateFunc Set updating data through func.
func (s *Table) UpdateFunc(fc func(f Filter, u SQLUpdateSet)) *Table {
	fc(s.where, s.updateSet)
	return s
}

// ToSelect Build SELECT statement.
func (s *Table) ToSelect() *SQL {
	if s.table.IsEmpty() {
		return NewEmptySQL()
	}
	lists := make([]any, 0, 12)
	lists = append(lists, s.comment, s.with, cst.SELECT)
	lists = append(lists, s.query, cst.FROM, s.table)
	if len(s.joins.joins) > 0 {
		lists = append(lists, s.joins)
	}
	if !s.where.IsEmpty() {
		lists = append(lists, cst.WHERE, parcelSingleFilter(s.where))
	}
	lists = append(lists, s.groupBy, s.orderBy, s.limit)
	return JoinSQLSpace(lists...).ToSQL()
}

// ToInsert Build INSERT statement.
func (s *Table) ToInsert() *SQL {
	insert := s.insert
	if insert.table == nil || insert.table.IsEmpty() {
		insert.table = s.table.ToSQL()
	}
	script := insert.ToSQL()
	if script.IsEmpty() {
		return NewEmptySQL()
	}
	return JoinSQLSpace(s.comment, script).ToSQL()
}

// ToUpdate Build UPDATE statement.
func (s *Table) ToUpdate() *SQL {
	if s.table.IsEmpty() || s.updateSet.IsEmpty() {
		return NewEmptySQL()
	}
	lists := make([]any, 0, 8)
	lists = append(lists, s.comment, s.with, cst.UPDATE, s.table)
	lists = append(lists, cst.SET, s.updateSet)
	if s.where.IsEmpty() {
		if s.way.cfg.updateRequireWhere {
			return NewEmptySQL()
		}
	} else {
		lists = append(lists, cst.WHERE, parcelSingleFilter(s.where))
	}
	return JoinSQLSpace(lists...).ToSQL()
}

// ToDelete Build DELETE statement.
func (s *Table) ToDelete() *SQL {
	if s.table.IsEmpty() {
		return NewEmptySQL()
	}
	lists := make([]any, 0, 8)
	lists = append(lists, s.comment, s.with, cst.DELETE, cst.FROM, s.table, s.joins)
	if s.where.IsEmpty() {
		if s.way.cfg.deleteRequireWhere {
			return NewEmptySQL()
		}
	} else {
		lists = append(lists, cst.WHERE, parcelSingleFilter(s.where))
	}
	return JoinSQLSpace(lists...).ToSQL()
}

// ToSQL Implementing the Maker interface using query statement.
func (s *Table) ToSQL() *SQL {
	return s.ToSelect()
}

// ToCount Build COUNT-SELECT statement.
func (s *Table) ToCount(counts ...string) *SQL {
	if s.table.IsEmpty() {
		return NewEmptySQL()
	}
	if len(counts) == 0 {
		counts = []string{
			JoinString("COUNT(*)", cst.Space, cst.AS, cst.Space, "counts"),
		}
	}
	lists := make([]any, 0, 1<<3)
	lists = append(lists, s.comment, s.with, cst.SELECT, newSqlSelect(s.way).AddAll(counts...), cst.FROM, s.table)
	if len(s.joins.joins) > 0 {
		lists = append(lists, s.joins)
	}
	if !s.where.IsEmpty() {
		lists = append(lists, cst.WHERE, parcelSingleFilter(s.where))
	}
	return JoinSQLSpace(lists...).ToSQL()
}

// Query Execute a SELECT statement.
func (s *Table) Query(ctx context.Context, query func(rows *sql.Rows) error) error {
	return s.way.Query(ctx, s.ToSelect(), query)
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

// Exists Determine if the data exists by querying, allow replacing or updating the subquery script of EXISTS.
func (s *Table) Exists(ctx context.Context, exists ...func(script *SQL)) (bool, error) {
	// SELECT EXISTS ( SELECT 1 FROM example_table ) AS a
	// SELECT EXISTS ( SELECT 1 FROM example_table WHERE ( id > 0 ) ) AS a
	// SELECT EXISTS ( ( SELECT 1 FROM example_table WHERE ( column1 = 'value1' ) ) UNION ALL ( SELECT 1 FROM example_table WHERE ( column2 = 'value2' ) ) ) AS a
	columns, columnsArgs := ([]string)(nil), (map[int][]any)(nil)
	s.SelectFunc(func(q SQLSelect) {
		if q.Len() > 0 {
			columns, columnsArgs = q.Get()
			q.ToEmpty()
		}
		q.Select("1")
	})
	defer func() {
		s.SelectFunc(func(q SQLSelect) {
			if len(columns) == 0 {
				q.ToEmpty()
			} else {
				q.Set(columns, columnsArgs)
			}
		})
	}()
	query := s.ToSelect()
	for i := len(exists) - 1; i >= 0; i-- {
		if exists[i] != nil {
			exists[i](query)
			break
		}
	}
	if query.IsEmpty() {
		return false, ErrEmptyScript
	}
	script := JoinSQLSpace(cst.SELECT, cst.EXISTS, cst.LeftParenthesis, query, cst.RightParenthesis, cst.AS, s.way.Replace(cst.A))
	var result any
	err := s.way.Query(ctx, script, func(rows *sql.Rows) error {
		for rows.Next() {
			if err := rows.Scan(&result); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	switch value := result.(type) {
	case bool:
		return value, nil
	case int:
		return value != 0, nil
	case int8:
		return value != 0, nil
	case int16:
		return value != 0, nil
	case int32:
		return value != 0, nil
	case int64:
		return value != 0, nil
	case uint:
		return value != 0, nil
	case uint8:
		return value != 0, nil
	case uint16:
		return value != 0, nil
	case uint32:
		return value != 0, nil
	case uint64:
		return value != 0, nil
	default:
		return false, fmt.Errorf("unexpected type %T %v", result, result)
	}
}

// Insert Execute an INSERT INTO statement.
func (s *Table) Insert(ctx context.Context) (int64, error) {
	script := s.ToInsert()
	if script.IsEmpty() {
		return 0, ErrEmptyScript
	}
	if insert := s.insert; insert != nil {
		if returning := insert.returning; returning != nil && returning.execute != nil {
			stmt, err := s.way.Prepare(ctx, script.Prepare)
			if err != nil {
				return 0, err
			}
			return returning.execute(ctx, stmt, script.Args...)
		}
	}
	return s.way.Execute(ctx, script)
}

// Update Execute an UPDATE statement.
func (s *Table) Update(ctx context.Context) (int64, error) {
	if s.way.cfg.updateRequireWhere && (s.where == nil || s.where.IsEmpty()) {
		return 0, ErrNoRowsAffected
	}
	return s.way.Execute(ctx, s.ToUpdate())
}

// Delete Execute a DELETE statement.
func (s *Table) Delete(ctx context.Context) (int64, error) {
	if s.way.cfg.deleteRequireWhere && (s.where == nil || s.where.IsEmpty()) {
		return 0, ErrNoRowsAffected
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

// BatchCreate Split a large slice of data into multiple smaller slices and insert them in batches.
func (s *Table) BatchCreate(ctx context.Context, batchSize int, creates []any, prefix func(i SQLInsert), suffix func(i SQLInsert)) (affectedRows int64, err error) {
	if batchSize <= 0 {
		batchSize = 1000
	}

	size := batchSize
	pending := creates[:]

	var (
		length int
		create []any
		script *SQL
		stmt   *Stmt
		rows   int64
	)

	defer func() {
		if stmt != nil {
			_ = stmt.Close()
		}
	}()

	makeScript := func() {
		script = s.InsertFunc(func(i SQLInsert) {
			i.ToEmpty()
			if prefix != nil {
				prefix(i)
			}
			i.Create(create)
			if suffix != nil {
				suffix(i)
			}
		}).ToInsert()
	}

	makeStmt := func() {
		if stmt != nil {
			if err = stmt.Close(); err != nil {
				return
			}
		}
		stmt, err = s.way.Prepare(ctx, script.Prepare)
	}

	for {
		length = len(pending)
		if length == 0 {
			break
		}
		if length < size {
			size = length
		}
		create = pending[:size]
		pending = pending[size:]
		makeScript()
		if stmt == nil || length < batchSize {
			makeStmt()
		}
		if err != nil {
			return
		}
		rows, err = stmt.Execute(ctx, script.Args...)
		if err != nil {
			return
		}
		affectedRows += rows
	}

	return
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

func (s *myComplex) atomic(ctx context.Context, group func(tx *Way) error) error {
	way := s.table.way
	if way.IsInTransaction() {
		return group(way)
	}
	defer func() { s.table.V(way) }()
	return way.TransactionNew(ctx, func(tx *Way) error {
		s.table.V(tx)
		return group(tx)
	})
}

// Upsert If the data exists, update the data; otherwise, insert the data.
func (s *myComplex) Upsert(ctx context.Context) (updateAffectedRows int64, insertResult int64, err error) {
	err = s.atomic(ctx, func(tx *Way) error {
		exist, table := false, s.table
		exist, err = table.Exists(ctx)
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
func (s *myComplex) DeleteCreate(ctx context.Context) (deleteAffectedRows int64, insertResult int64, err error) {
	err = s.atomic(ctx, func(tx *Way) error {
		table := s.table
		deleteAffectedRows, err = table.Delete(ctx)
		if err != nil {
			return err
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
	return &myComplex{
		table: table,
	}
}
