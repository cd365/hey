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
	InvalidTransaction       = fmt.Errorf("sql: invalid transaction")
	TryBeginTransactionTwice = fmt.Errorf("sql: attempt to start transaction twice")
	PrepareEmpty             = fmt.Errorf("sql: prepare value is empty")
	HandleRowsFuncNil        = fmt.Errorf("sql: handle rows func value is nil")
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
		err = TryBeginTransactionTwice
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
		err = InvalidTransaction
		return
	}
	err = s.tx.Commit()
	s.tx = nil
	return
}

func (s *Way) Rollback() (err error) {
	if s.tx == nil {
		err = InvalidTransaction
		return
	}
	err = s.tx.Rollback()
	s.tx = nil
	return
}

func (s *Way) Idle() bool {
	return s.tx == nil
}

func (s *Way) Transaction(transaction func() (msg error, err error)) (msg error, err error) {
	if transaction == nil {
		return
	}
	if s.Idle() {
		if err = s.Begin(); err != nil {
			return
		}
		defer func() {
			if err == nil && msg == nil {
				_ = s.Commit()
			} else {
				_ = s.Rollback()
			}
		}()
	}
	msg, err = transaction()
	return
}

func (s *Way) QueryContext(ctx context.Context, handle func(rows *sql.Rows) (err error), prepare string, args ...interface{}) (err error) {
	if prepare == "" {
		err = PrepareEmpty
		return
	}
	if handle == nil {
		err = HandleRowsFuncNil
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
		err = PrepareEmpty
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

func (s *Way) Insert(c Inserter) (int64, error) {
	prepare, args := c.Result()
	return s.Exec(prepare, args...)
}

func (s *Way) Delete(c Deleter) (int64, error) {
	prepare, args := c.Result()
	return s.Exec(prepare, args...)
}

func (s *Way) Update(c Updater) (int64, error) {
	prepare, args := c.Result()
	return s.Exec(prepare, args...)
}

func (s *Way) Count(c Selector) (result int64, err error) {
	prepare, args := c.ResultForCount()
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

func (s *Way) Get(c Selector, scan func(rows *sql.Rows) error) error {
	prepare, args := c.Result()
	return s.Query(func(rows *sql.Rows) error { return ForRowsNextScan(rows, scan) }, prepare, args...)
}

func (s *Way) NewInsert(fn func(c Inserter)) (int64, error) {
	add := NewInserter()
	fn(add)
	return s.Insert(add)
}

func (s *Way) NewDelete(fn func(c Deleter)) (int64, error) {
	del := NewDeleter()
	fn(del)
	return s.Delete(del)
}

func (s *Way) NewUpdate(fn func(c Updater)) (int64, error) {
	mod := NewUpdater()
	fn(mod)
	return s.Update(mod)
}

func (s *Way) NewSelect(fn func(c Selector) (err error)) error {
	return fn(NewSelector())
}

func ForRowsNextScan(rows *sql.Rows, scan func(rows *sql.Rows) error) (err error) {
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
