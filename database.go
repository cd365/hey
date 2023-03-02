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

var (
	ErrInvalidTransaction       = fmt.Errorf("sql: invalid transaction")
	ErrTryBeginTransactionTwice = fmt.Errorf("sql: attempt to start transaction twice")
	ErrPrepareEmpty             = fmt.Errorf("sql: prepare value is empty")
	ErrHandleRowsFuncIsNil      = fmt.Errorf("sql: handle rows func value is nil")
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
	prepare func(prepare string) (result string) // preprocess sql script before execute
	script  func(result *Result)                 // sql execute result
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

func (s *Way) Script(fn func(result *Result)) *Way {
	s.script = fn
	return s
}

func (s *Way) DB() *sql.DB {
	return s.db
}

func (s *Way) BeginTx(ctx context.Context, opts *sql.TxOptions) (err error) {
	if s.tx != nil {
		err = ErrTryBeginTransactionTwice
		return
	}
	s.tx, err = s.db.BeginTx(ctx, opts)
	if err != nil {
		return
	}
	return
}

func (s *Way) Begin() error {
	return s.BeginTx(context.Background(), nil)
}

func (s *Way) Commit() (err error) {
	if s.tx == nil {
		err = ErrInvalidTransaction
		return
	}
	err = s.tx.Commit()
	s.tx = nil
	return
}

func (s *Way) Rollback() (err error) {
	if s.tx == nil {
		err = ErrInvalidTransaction
		return
	}
	err = s.tx.Rollback()
	s.tx = nil
	return
}

func (s *Way) Idle() bool {
	return s.tx == nil
}

func (s *Way) Clone() *Way {
	return NewWay(s.db).Prepare(s.prepare).Script(s.script)
}

func (s *Way) TransactionContext(ctx context.Context, opts *sql.TxOptions, fn func(way *Way) (err error)) (err error) {
	if fn == nil {
		return
	}
	way := s.Clone()
	err = way.BeginTx(ctx, opts)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			_ = way.Rollback()
			return
		}
		_ = way.Commit()
	}()
	if err = fn(way); err != nil {
		return
	}
	return
}

func (s *Way) Transaction(fn func(way *Way) (err error)) (err error) {
	if fn == nil {
		return
	}
	way := s.Clone()
	err = way.Begin()
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			_ = way.Rollback()
			return
		}
		_ = way.Commit()
	}()
	if err = fn(way); err != nil {
		return
	}
	return
}

func (s *Way) QueryContext(ctx context.Context, handle func(rows *sql.Rows) (err error), prepare string, args ...interface{}) (err error) {
	if prepare == "" {
		err = ErrPrepareEmpty
		return
	}
	if handle == nil {
		err = ErrHandleRowsFuncIsNil
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
	err = handle(rows)
	return
}

func (s *Way) ExecContext(ctx context.Context, prepare string, args ...interface{}) (rowsAffected int64, err error) {
	if prepare == "" {
		err = ErrPrepareEmpty
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

func (s *Way) Query(handle func(rows *sql.Rows) (err error), prepare string, args ...interface{}) error {
	return s.QueryContext(context.Background(), handle, prepare, args...)
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

func (s *Way) Get(get Selector, handle func(rows *sql.Rows) (err error)) error {
	prepare, args := get.Result()
	return s.Query(handle, prepare, args...)
}

func (s *Way) GetScan(get Selector, scan func(rows *sql.Rows) (err error)) error {
	return s.Get(get, func(rows *sql.Rows) error { return ForRowsNextScan(rows, scan) })
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

func ForRowsNextScan(rows *sql.Rows, scan func(rows *sql.Rows) (err error)) (err error) {
	for rows.Next() {
		if err = scan(rows); err != nil {
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
