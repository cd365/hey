package hey

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
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

	selects *sqlSelect

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
	latest := table.Copy()
	latest.Prepare = strings.TrimSpace(latest.Prepare)
	if latest.IsEmpty() {
		return result
	}
	single := StrSpace
	double := Strings(single, single)
	for strings.Contains(latest.Prepare, double) {
		latest.Prepare = strings.ReplaceAll(latest.Prepare, double, single)
	}
	count := strings.Count(latest.Prepare, StrSpace)
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
	result := newSqlAlias(StrEmpty).v(s)
	switch example := table.(type) {
	case string:
		result.SetSQL(s.Replace(example))
	case *SQL:
		result.SetSQL(optimizeTableSQL(s, example))
	case Maker:
		if example != nil {
			result.SetSQL(optimizeTableSQL(s, example.ToSQL()))
		}
	case TableNamer:
		result.SetSQL(s.Replace(example.Table()))
	default:
		if value := reflect.ValueOf(table); !value.IsNil() {
			if method := value.MethodByName(s.cfg.TableMethodName); method.IsValid() {
				if values := method.Call(nil); len(values) == 1 {
					return s.getTable(values[0].Interface())
				}
			}
		}
		// Consider multi-level pointers.
		result.SetSQL(any2sql(table))
	}
	return result
}

// Table Create a *Table object to execute SELECT, INSERT, UPDATE, and DELETE statements.
func (s *Way) Table(table any) *Table {
	selects := newSqlSelect(s)
	result := &Table{
		way:       s,
		comment:   newSqlComment(),
		with:      newSqlWith(),
		selects:   selects,
		table:     s.getTable(table),
		joins:     newSqlJoin(s),
		where:     s.F(),
		groupBy:   newSqlGroupBy(s),
		orderBy:   newSqlOrderBy(s),
		limit:     newSqlLimit(),
		insert:    newSqlInsert(s),
		updateSet: newSqlUpdateSet(s),
	}
	result.joins.selects = selects
	return result
}

// ToEmpty Do not reset table.
func (s *Table) ToEmpty() *Table {
	s.comment.ToEmpty()
	s.with.ToEmpty()
	s.selects.ToEmpty()
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

// WITH Custom common table expression (CTE).
func (s *Table) WITH(fc func(w SQLWith)) *Table {
	fc(s.with)
	return s
}

// With Add a common table expression.
func (s *Table) With(alias string, maker Maker, columns ...string) *Table {
	return s.WITH(func(w SQLWith) {
		w.Set(alias, maker, columns...)
	})
}

// SELECT Set SELECT through func.
func (s *Table) SELECT(fc func(q SQLSelect)) *Table {
	fc(s.selects)
	return s
}

// Distinct SQL DISTINCT columns.
func (s *Table) Distinct() *Table {
	return s.SELECT(func(q SQLSelect) {
		q.Distinct()
	})
}

// Select Add one or more query lists. If no parameter is provided, all existing query lists will be deleted.
func (s *Table) Select(selects ...any) *Table {
	return s.SELECT(func(q SQLSelect) {
		q.Select(selects...)
	})
}

// TABLE Set query table through func.
func (s *Table) TABLE(fc func(t SQLAlias)) *Table {
	fc(s.table)
	return s
}

// Table Set the table name, or possibly a subquery with an alias.
func (s *Table) Table(table any) *Table {
	return s.TABLE(func(t SQLAlias) {
		t.SetSQL(s.way.getTable(table).GetSQL())
	})
}

// Alias Set the table alias name.
func (s *Table) Alias(alias string) *Table {
	return s.TABLE(func(t SQLAlias) {
		t.SetAlias(alias)
	})
}

// JOIN Custom join query.
func (s *Table) JOIN(fc func(j SQLJoin)) *Table {
	if s.joins.table == nil {
		s.joins.table = s.table
	}
	fc(s.joins)
	return s
}

// InnerJoin INNER JOIN.
func (s *Table) InnerJoin(fc func(j SQLJoin) (left SQLAlias, right SQLAlias, assoc SQLJoinAssoc)) *Table {
	return s.JOIN(func(j SQLJoin) {
		left, right, on := fc(j)
		if on == nil || right == nil || right.IsEmpty() {
			return
		}
		if left == nil || left.IsEmpty() {
			left = j.GetTable()
		}
		j.InnerJoin(left, right, on)
	})
}

// LeftJoin LEFT JOIN.
func (s *Table) LeftJoin(fc func(j SQLJoin) (left SQLAlias, right SQLAlias, assoc SQLJoinAssoc)) *Table {
	return s.JOIN(func(j SQLJoin) {
		left, right, on := fc(j)
		if on == nil || right == nil || right.IsEmpty() {
			return
		}
		if left == nil || left.IsEmpty() {
			left = j.GetTable()
		}
		j.LeftJoin(left, right, on)
	})
}

// RightJoin RIGHT JOIN.
func (s *Table) RightJoin(fc func(j SQLJoin) (left SQLAlias, right SQLAlias, assoc SQLJoinAssoc)) *Table {
	return s.JOIN(func(j SQLJoin) {
		left, right, on := fc(j)
		if on == nil || right == nil || right.IsEmpty() {
			return
		}
		if left == nil || left.IsEmpty() {
			left = j.GetTable()
		}
		j.RightJoin(left, right, on)
	})
}

// WHERE Set WHERE through func.
func (s *Table) WHERE(fc func(f Filter)) *Table {
	fc(s.where)
	return s
}

// Where Set the WHERE condition.
func (s *Table) Where(filters ...Filter) *Table {
	return s.WHERE(func(f Filter) {
		f.ToEmpty().Use(filters...)
	})
}

// GROUP Set GROUP BY through func.
func (s *Table) GROUP(fc func(g SQLGroupBy)) *Table {
	fc(s.groupBy)
	return s
}

// Group Set GROUP BY condition.
func (s *Table) Group(groups ...any) *Table {
	return s.GROUP(func(g SQLGroupBy) {
		g.Group(groups...)
	})
}

// HAVING Set HAVING through func.
func (s *Table) HAVING(fc func(h Filter)) *Table {
	return s.GROUP(func(g SQLGroupBy) {
		g.Having(fc)
	})
}

// Having Set the HAVING condition.
func (s *Table) Having(filters ...Filter) *Table {
	return s.HAVING(func(f Filter) {
		f.ToEmpty().Use(filters...)
	})
}

// ORDER Set ORDER BY through func.
func (s *Table) ORDER(fc func(o SQLOrderBy)) *Table {
	fc(s.orderBy)
	return s
}

// Asc Sort ascending.
func (s *Table) Asc(column string) *Table {
	return s.ORDER(func(o SQLOrderBy) {
		o.Asc(column)
	})
}

// Desc Sort descending.
func (s *Table) Desc(column string) *Table {
	return s.ORDER(func(o SQLOrderBy) {
		o.Desc(column)
	})
}

// LIMIT Set LIMIT x [OFFSET x] through func.
func (s *Table) LIMIT(fc func(o SQLLimit)) *Table {
	fc(s.limit)
	return s
}

// Limit Set the maximum number of query result sets.
func (s *Table) Limit(limit int64) *Table {
	return s.LIMIT(func(o SQLLimit) {
		o.Limit(limit)
	})
}

// Offset Set the offset of the query target data.
func (s *Table) Offset(offset int64) *Table {
	return s.LIMIT(func(o SQLLimit) {
		o.Offset(offset)
	})
}

// Limiter Set limit and offset at the same time.
func (s *Table) Limiter(limiter Limiter) *Table {
	return s.LIMIT(func(o SQLLimit) {
		o.Limit(limiter.GetLimit()).Offset(limiter.GetOffset())
	})
}

// Page Pagination query, page number + page limit.
func (s *Table) Page(page int64, limit ...int64) *Table {
	return s.LIMIT(func(o SQLLimit) {
		o.Page(page, limit...)
	})
}

// INSERT Set inserting data through func.
func (s *Table) INSERT(fc func(i SQLInsert)) *Table {
	fc(s.insert)
	return s
}

// UPDATE Set updating data through func.
func (s *Table) UPDATE(fc func(f Filter, u SQLUpdateSet)) *Table {
	fc(s.where, s.updateSet)
	return s
}

// ToSelect Build SELECT statement.
func (s *Table) ToSelect() *SQL {
	lists := make([]any, 0, 12)
	lists = append(lists, s.comment, s.with, StrSelect)
	lists = append(lists, s.selects, StrFrom, s.table)
	if len(s.joins.joins) > 0 {
		lists = append(lists, s.joins)
	}
	if !s.where.IsEmpty() {
		lists = append(lists, StrWhere, ParcelFilter(s.where))
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
	return JoinSQLSpace(s.comment, insert).ToSQL()
}

// ToUpdate Build UPDATE statement.
func (s *Table) ToUpdate() *SQL {
	lists := make([]any, 0, 8)
	lists = append(lists, s.comment, s.with, StrUpdate, s.table)
	lists = append(lists, StrSet, s.updateSet)
	if !s.where.IsEmpty() {
		lists = append(lists, StrWhere, ParcelFilter(s.where))
	}
	return JoinSQLSpace(lists...).ToSQL()
}

// ToDelete Build DELETE statement.
func (s *Table) ToDelete() *SQL {
	lists := make([]any, 0, 8)
	lists = append(lists, s.comment, s.with, StrDelete, StrFrom, s.table, s.joins)
	if !s.where.IsEmpty() {
		lists = append(lists, StrWhere, ParcelFilter(s.where))
	}
	return JoinSQLSpace(lists...).ToSQL()
}

// ToSQL Implementing the Maker interface using query statement.
func (s *Table) ToSQL() *SQL {
	return s.ToSelect()
}

// countToSQL Build SELECT-COUNT statement.
func (s *Table) countToSQL(counts ...string) *SQL {
	if len(counts) == 0 {
		counts = []string{
			Strings("COUNT(*)", StrSpace, StrAs, StrSpace, s.way.Replace(DefaultAliasNameCount)),
		}
	}
	lists := make([]any, 0, 1<<3)
	lists = append(lists, s.comment, s.with, StrSelect, newSqlSelect(s.way).AddAll(counts...), StrFrom, s.table)
	if len(s.joins.joins) > 0 {
		lists = append(lists, s.joins)
	}
	if !s.where.IsEmpty() {
		lists = append(lists, StrWhere, ParcelFilter(s.where))
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
	script := s.countToSQL(counts...)
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

// Fetch Scan data into result by reflect.
func (s *Table) Fetch(ctx context.Context, result any) error {
	return s.way.Fetch(ctx, s.ToSelect(), result)
}

// Insert Execute an INSERT INTO statement.
func (s *Table) Insert(ctx context.Context) (int64, error) {
	script := s.ToInsert()
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
	if s.way.cfg.UpdateMustUseWhere && (s.where == nil || s.where.IsEmpty()) {
		return 0, nil
	}
	return s.way.Execute(ctx, s.ToUpdate())
}

// Delete Execute a DELETE statement.
func (s *Table) Delete(ctx context.Context) (int64, error) {
	if s.way.cfg.DeleteMustUseWhere && (s.where == nil || s.where.IsEmpty()) {
		return 0, nil
	}
	return s.way.Execute(ctx, s.ToDelete())
}

// Create Quickly insert data into the table.
func (s *Table) Create(ctx context.Context, create any) (int64, error) {
	return s.INSERT(func(i SQLInsert) {
		i.Create(create)
	}).Insert(ctx)
}

// Modify Quickly update data in the table.
func (s *Table) Modify(ctx context.Context, modify any) (int64, error) {
	return s.UPDATE(func(f Filter, u SQLUpdateSet) {
		u.Update(modify)
	}).Update(ctx)
}

// Create Quick insert, the first method in options will serve as the pre-method, and all the subsequent methods will serve as post-methods.
func (s *Way) Create(ctx context.Context, table any, values any, options ...func(t *Table)) (int64, error) {
	tmp := s.Table(table)
	length := len(options)
	if length == 0 {
		return tmp.INSERT(func(i SQLInsert) { i.Create(values) }).Insert(ctx)
	}
	if first := options[0]; first != nil {
		first(tmp)
	}
	tmp.INSERT(func(i SQLInsert) { i.Create(values) })
	for _, fc := range options[1:] {
		if fc != nil {
			fc(tmp)
		}
	}
	return tmp.Insert(ctx)
}

// Delete Quick delete.
func (s *Way) Delete(ctx context.Context, table any, column string, values any, options ...func(f Filter)) (int64, error) {
	where := s.F().In(column, values)
	for _, fc := range options {
		if fc != nil {
			fc(where)
		}
	}
	return s.Table(table).Where(where).Delete(ctx)
}

// Update Quick update, the first method in options will serve as the pre-method, and all the subsequent methods will serve as post-methods.
func (s *Way) Update(ctx context.Context, table any, column string, value any, update any, options ...func(f Filter, u SQLUpdateSet)) (int64, error) {
	return s.Table(table).UPDATE(func(f Filter, u SQLUpdateSet) {
		length := len(options)
		if length == 0 {
			f.Equal(column, value)
			u.Update(update)
			return
		}
		if first := options[0]; first != nil {
			first(f, u)
		}
		f.Equal(column, value)
		u.Update(update)
		for _, fc := range options[1:] {
			if fc != nil {
				fc(f, u)
			}
		}
	}).Update(ctx)
}

const StrId = "id"

// DeleteById Delete data according to one or more id values.
func (s *Way) DeleteById(ctx context.Context, table any, values any, options ...func(f Filter)) (int64, error) {
	return s.Delete(ctx, table, StrId, values, options...)
}

// UpdateById Update by id value, the first method in options will serve as the pre-method, and all the subsequent methods will serve as post-methods.
func (s *Way) UpdateById(ctx context.Context, table any, value any, update any, options ...func(f Filter, u SQLUpdateSet)) (int64, error) {
	return s.Update(ctx, table, StrId, value, update, options...)
}
