package hey

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

const (
	Placeholder = "?"
)

type Result struct {
	TimeStart time.Time
	TimeEnd   time.Time
	Prepare   string
	Args      []interface{}
	Error     error
}

type Way struct {
	db      *sql.DB
	tx      *sql.Tx
	prepare func(prepare string) (result string)           // preprocess sql script before execute
	scanner func(rows *sql.Rows, result interface{}) error // scan query result
	script  func(result *Result)                           // sql execute result
}

func NewWay(db *sql.DB) *Way {
	return &Way{
		db: db,
	}
}

func (s *Way) Prepare(fn func(prepare string) (result string)) *Way {
	s.prepare = fn
	return s
}

func (s *Way) Scanner(fn func(rows *sql.Rows, result interface{}) error) *Way {
	s.scanner = fn
	return s
}

func (s *Way) Script(fn func(result *Result)) *Way {
	s.script = fn
	return s
}

func (s *Way) DB() *sql.DB {
	return s.db
}

func (s *Way) clone() *Way {
	return NewWay(s.db).Prepare(s.prepare).Scanner(s.scanner).Script(s.script)
}

func (s *Way) begin(ctx context.Context, opts *sql.TxOptions) (err error) {
	if s.tx != nil {
		return
	}
	s.tx, err = s.db.BeginTx(ctx, opts)
	return
}

func (s *Way) commit() (err error) {
	if s.tx == nil {
		return
	}
	err = s.tx.Commit()
	s.tx = nil
	return
}

func (s *Way) rollback() (err error) {
	if s.tx == nil {
		return
	}
	err = s.tx.Rollback()
	s.tx = nil
	return
}

func (s *Way) TransactionContext(ctx context.Context, opts *sql.TxOptions, fn func(tx *Way) (err error)) error {
	if s.tx != nil {
		return fn(s)
	}
	way := s.clone()
	err := way.begin(ctx, opts)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		if err == nil && ok {
			_ = way.commit()
			return
		}
		_ = way.rollback()
	}()
	if err = fn(way); err != nil {
		return err
	}
	ok = true
	return nil
}

func (s *Way) Transaction(fn func(tx *Way) (err error)) error {
	return s.TransactionContext(context.Background(), nil, fn)
}

func (s *Way) QueryContext(ctx context.Context, handlingRows func(rows *sql.Rows) (err error), prepare string, args ...interface{}) (err error) {
	if handlingRows == nil || prepare == "" {
		return
	}
	if s.prepare != nil {
		prepare = s.prepare(prepare)
	}
	var result *Result
	if s.script != nil {
		result = &Result{
			TimeStart: time.Now(),
			Prepare:   prepare,
			Args:      args,
		}
		defer func() {
			if err != nil {
				result.Error = err
			}
			s.script(result)
		}()
	}
	var stmt *sql.Stmt
	if s.tx != nil {
		stmt, err = s.tx.PrepareContext(ctx, prepare)
	} else {
		stmt, err = s.db.PrepareContext(ctx, prepare)
	}
	if err != nil {
		if result != nil {
			result.TimeEnd = time.Now()
		}
		return
	}
	defer stmt.Close()
	var rows *sql.Rows
	rows, err = stmt.QueryContext(ctx, args...)
	if result != nil {
		result.TimeEnd = time.Now()
	}
	if err != nil {
		return
	}
	defer rows.Close()
	err = handlingRows(rows)
	return
}

func (s *Way) ExecContext(ctx context.Context, prepare string, args ...interface{}) (rowsAffected int64, err error) {
	if prepare == "" {
		return
	}
	if s.prepare != nil {
		prepare = s.prepare(prepare)
	}
	var result *Result
	if s.script != nil {
		result = &Result{
			TimeStart: time.Now(),
			Prepare:   prepare,
			Args:      args,
		}
		defer func() {
			if err != nil {
				result.Error = err
			}
			s.script(result)
		}()
	}
	var stmt *sql.Stmt
	if s.tx != nil {
		stmt, err = s.tx.PrepareContext(ctx, prepare)
	} else {
		stmt, err = s.db.PrepareContext(ctx, prepare)
	}
	if err != nil {
		if result != nil {
			result.TimeEnd = time.Now()
		}
		return
	}
	defer stmt.Close()
	var sqlResult sql.Result
	sqlResult, err = stmt.ExecContext(ctx, args...)
	if result != nil {
		result.TimeEnd = time.Now()
	}
	if err != nil {
		return
	}
	rowsAffected, err = sqlResult.RowsAffected()
	return
}

func (s *Way) Query(handlingRows func(rows *sql.Rows) (err error), prepare string, args ...interface{}) error {
	return s.QueryContext(context.Background(), handlingRows, prepare, args...)
}

func (s *Way) Exec(prepare string, args ...interface{}) (int64, error) {
	return s.ExecContext(context.Background(), prepare, args...)
}

func (s *Way) Insert(add Inserter) (int64, error) {
	prepare, args := add.Result()
	return s.Exec(prepare, args...)
}

func (s *Way) Delete(del Deleter) (int64, error) {
	prepare, args := del.Result()
	return s.Exec(prepare, args...)
}

func (s *Way) Update(mod Updater) (int64, error) {
	prepare, args := mod.Result()
	return s.Exec(prepare, args...)
}

func (s *Way) Select(get Selector, result interface{}) error {
	prepare, args := get.Result()
	return s.Query(func(rows *sql.Rows) error {
		if s.scanner == nil {
			s.scanner = func(rows *sql.Rows, result interface{}) error { return ScanSliceStruct(rows, result, "db") }
		}
		return s.scanner(rows, result)
	}, prepare, args...)
}

func (s *Way) Count(get Selector) (result int64, err error) {
	prepare, args := get.ResultForCount()
	err = s.Query(func(rows *sql.Rows) (err error) {
		for rows.Next() {
			if err = rows.Scan(&result); err != nil {
				return
			}
		}
		return
	}, prepare, args...)
	return
}

func (s *Way) Get(get Selector, handlingRows func(rows *sql.Rows) (err error)) error {
	prepare, args := get.Result()
	return s.Query(handlingRows, prepare, args...)
}

func (s *Way) GetNext(get Selector, forRowsNextScan func(rows *sql.Rows) (err error)) error {
	return s.Get(get, func(rows *sql.Rows) error { return ForRowsNextScan(rows, forRowsNextScan) })
}

func (s *Way) NewInsert(fn func(add Inserter)) (int64, error) {
	add := NewInserter()
	fn(add)
	return s.Insert(add)
}

func (s *Way) NewDelete(fn func(del Deleter)) (int64, error) {
	del := NewDeleter()
	fn(del)
	return s.Delete(del)
}

func (s *Way) NewUpdate(fn func(mod Updater)) (int64, error) {
	mod := NewUpdater()
	fn(mod)
	return s.Update(mod)
}

func (s *Way) NewSelect(fn func(get Selector) (err error)) error {
	return fn(NewSelector())
}

func ForRowsNextScan(rows *sql.Rows, rowsScan func(rows *sql.Rows) (err error)) (err error) {
	for rows.Next() {
		if err = rowsScan(rows); err != nil {
			return
		}
	}
	return
}

func PreparePostgresql(prepare string) string {
	index := 0
	for strings.Contains(prepare, Placeholder) {
		index++
		prepare = strings.Replace(prepare, Placeholder, fmt.Sprintf("$%d", index), 1)
	}
	return prepare
}
