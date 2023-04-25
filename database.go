package hey

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

const (
	Placeholder = "?"
)

const (
	scannerDefaultStructTag = "db"
)

type Logger interface {
	Info(v ...interface{})
	Infof(format string, v ...interface{})
	Warn(v ...interface{})
	Warnf(format string, v ...interface{})
	Error(v ...interface{})
	Errorf(format string, v ...interface{})
}

type Way struct {
	db              *sql.DB
	tx              *sql.Tx
	txId            string
	logger          Logger
	prepare         func(prepare string) (result string)           // preprocess sql script before execute
	scanner         func(rows *sql.Rows, result interface{}) error // scan query result
	sqlCallTakeWarn int64                                          // execute sql time threshold, milliseconds
}

func NewWay(db *sql.DB) *Way {
	way := &Way{
		db: db,
	}
	way.scanner = func(rows *sql.Rows, result interface{}) error {
		return ScanSliceStruct(rows, result, scannerDefaultStructTag)
	}
	return way
}

func (s *Way) Logger(logger Logger) *Way {
	s.logger = logger
	return s
}

func (s *Way) Prepare(fn func(prepare string) (result string)) *Way {
	s.prepare = fn
	return s
}

func (s *Way) Scanner(fn func(rows *sql.Rows, result interface{}) error) *Way {
	s.scanner = fn
	return s
}

func (s *Way) CallTakeWarn(milliseconds int64) *Way {
	s.sqlCallTakeWarn = milliseconds
	return s
}

func (s *Way) DB() *sql.DB {
	return s.db
}

func (s *Way) clone() *Way {
	return NewWay(s.db).Logger(s.logger).Prepare(s.prepare).Scanner(s.scanner).CallTakeWarn(s.sqlCallTakeWarn)
}

func (s *Way) begin(ctx context.Context, opts *sql.TxOptions) (err error) {
	s.tx, err = s.db.BeginTx(ctx, opts)
	if err != nil {
		return
	}
	s.txId = fmt.Sprintf("txid.%d.%p", time.Now().UnixNano(), s.tx)
	return
}

func (s *Way) commit() (err error) {
	err = s.tx.Commit()
	s.tx, s.txId = nil, ""
	return
}

func (s *Way) rollback() (err error) {
	err = s.tx.Rollback()
	s.tx, s.txId = nil, ""
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

func (s *Way) output(start time.Time, end time.Time, prepare string, args []interface{}, err error) {
	if s.logger == nil {
		return
	}
	buf := bytes.NewBuffer(nil)
	tag := ">>>"
	if s.tx != nil {
		buf.WriteString(fmt.Sprintf(" %sTXID:%s", tag, s.txId))
	}
	buf.WriteString(fmt.Sprintf(" %sCOST:%s %sSTART:%s %sEND:%s", tag, end.Sub(start).String(), tag, start.Format(time.RFC3339Nano), tag, end.Format(time.RFC3339Nano)))
	buf.WriteString(fmt.Sprintf(" %sPREPARE:%s %sARGS:%v", tag, prepare, tag, args))
	format := "[SQL]%s"
	if err != nil {
		buf.WriteString(fmt.Sprintf(" %sERROR:%s", tag, err.Error()))
		s.logger.Errorf(format, buf.String())
		return
	}
	if s.sqlCallTakeWarn > 0 && end.Sub(start).Milliseconds() >= s.sqlCallTakeWarn {
		s.logger.Warnf(format, buf.String())
		return
	}
	s.logger.Infof(format, buf.String())
}

func (s *Way) QueryContext(ctx context.Context, handlingRows func(rows *sql.Rows) (err error), prepare string, args ...interface{}) (err error) {
	if handlingRows == nil || prepare == "" {
		return
	}
	if s.prepare != nil {
		prepare = s.prepare(prepare)
	}
	var end time.Time
	start := time.Now()
	defer func() {
		s.output(start, end, prepare, args, err)
	}()
	var stmt *sql.Stmt
	if s.tx != nil {
		stmt, err = s.tx.PrepareContext(ctx, prepare)
	} else {
		stmt, err = s.db.PrepareContext(ctx, prepare)
	}
	if err != nil {
		end = time.Now()
		return
	}
	defer stmt.Close()
	var rows *sql.Rows
	rows, err = stmt.QueryContext(ctx, args...)
	end = time.Now()
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
	var end time.Time
	start := time.Now()
	defer func() {
		s.output(start, end, prepare, args, err)
	}()
	var stmt *sql.Stmt
	if s.tx != nil {
		stmt, err = s.tx.PrepareContext(ctx, prepare)
	} else {
		stmt, err = s.db.PrepareContext(ctx, prepare)
	}
	if err != nil {
		end = time.Now()
		return
	}
	defer stmt.Close()
	var sqlResult sql.Result
	sqlResult, err = stmt.ExecContext(ctx, args...)
	end = time.Now()
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

func (s *Way) ScanContext(ctx context.Context, result interface{}, prepare string, args ...interface{}) error {
	return s.QueryContext(ctx, func(rows *sql.Rows) (err error) { return s.scanner(rows, result) }, prepare, args...)
}

func (s *Way) Scan(result interface{}, prepare string, args ...interface{}) error {
	return s.Query(func(rows *sql.Rows) (err error) { return s.scanner(rows, result) }, prepare, args...)
}

func (s *Way) InsertContext(ctx context.Context, add Inserter) (int64, error) {
	prepare, args := add.Result()
	return s.ExecContext(ctx, prepare, args...)
}

func (s *Way) DeleteContext(ctx context.Context, del Deleter) (int64, error) {
	prepare, args := del.Result()
	return s.ExecContext(ctx, prepare, args...)
}

func (s *Way) UpdateContext(ctx context.Context, mod Updater) (int64, error) {
	prepare, args := mod.Result()
	return s.ExecContext(ctx, prepare, args...)
}

func (s *Way) SelectContext(ctx context.Context, get Selector, result interface{}) error {
	prepare, args := get.Result()
	return s.QueryContext(ctx, func(rows *sql.Rows) error { return s.scanner(rows, result) }, prepare, args...)
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
	return s.Query(func(rows *sql.Rows) error { return s.scanner(rows, result) }, prepare, args...)
}

func (s *Way) CountContext(ctx context.Context, get Selector) (result int64, err error) {
	prepare, args := get.ResultForCount()
	err = s.QueryContext(ctx, func(rows *sql.Rows) (err error) {
		for rows.Next() {
			if err = rows.Scan(&result); err != nil {
				return
			}
		}
		return
	}, prepare, args...)
	return
}

func (s *Way) GetContext(ctx context.Context, get Selector, handlingRows func(rows *sql.Rows) (err error)) error {
	prepare, args := get.Result()
	return s.QueryContext(ctx, handlingRows, prepare, args...)
}

func (s *Way) GetNextContext(ctx context.Context, get Selector, forRowsNextScan func(rows *sql.Rows) (err error)) error {
	return s.GetContext(ctx, get, func(rows *sql.Rows) error { return ForRowsNextScan(rows, forRowsNextScan) })
}

func (s *Way) Count(get Selector) (int64, error) {
	return s.CountContext(context.Background(), get)
}

func (s *Way) Get(get Selector, handlingRows func(rows *sql.Rows) (err error)) error {
	prepare, args := get.Result()
	return s.Query(handlingRows, prepare, args...)
}

func (s *Way) GetNext(get Selector, forRowsNextScan func(rows *sql.Rows) (err error)) error {
	return s.Get(get, func(rows *sql.Rows) error { return ForRowsNextScan(rows, forRowsNextScan) })
}

func (s *Way) NewInsertContext(ctx context.Context, fn func(add Inserter)) (int64, error) {
	add := NewInserter()
	fn(add)
	return s.InsertContext(ctx, add)
}

func (s *Way) NewDeleteContext(ctx context.Context, fn func(del Deleter)) (int64, error) {
	del := NewDeleter()
	fn(del)
	return s.DeleteContext(ctx, del)
}

func (s *Way) NewUpdateContext(ctx context.Context, fn func(mod Updater)) (int64, error) {
	mod := NewUpdater()
	fn(mod)
	return s.UpdateContext(ctx, mod)
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
