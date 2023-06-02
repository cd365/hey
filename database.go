package hey

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
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
	ctx             context.Context                                // context.Context
	db              *sql.DB                                        // the instance of the database connection that was opened
	tx              *sql.Tx                                        // the transaction instance that was opened
	txId            string                                         // the transaction id of the current process
	logger          Logger                                         // record all executed sql
	prepare         func(prepare string) (result string)           // preprocess sql script before execute
	scanner         func(rows *sql.Rows, result interface{}) error // scan query result
	sqlCallTakeWarn int64                                          // execute sql time threshold, the logger attribute is not nil, unit: milliseconds
}

func NewWay(db *sql.DB) *Way {
	way := &Way{
		ctx: context.Background(),
		db:  db,
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

func (s *Way) TxNil() bool {
	return s.tx == nil
}

func (s *Way) TransactionContext(ctx context.Context, opts *sql.TxOptions, fn func(tx *Way) (err error)) (err error) {
	if s.tx != nil {
		return fn(s)
	}
	way := s.clone()
	if err = way.begin(ctx, opts); err != nil {
		return
	}
	ok := false
	defer func() {
		txId := s.txId
		if err == nil && ok {
			if err = way.commit(); err != nil && s.logger != nil {
				s.logger.Errorf("[SQL] txid: %s commit: %s", txId, err.Error())
			}
			return
		}
		if err = way.rollback(); err != nil && s.logger != nil {
			s.logger.Errorf("[SQL] txid: %s rollback: %s", txId, err.Error())
		}
	}()
	if err = fn(way); err != nil {
		return
	}
	ok = true
	return
}

func (s *Way) Transaction(fn func(tx *Way) (err error)) error {
	return s.TransactionContext(s.ctx, nil, fn)
}

func (s *Way) output(start time.Time, end time.Time, prepare string, args []interface{}, err error) {
	if s.logger == nil {
		return
	}
	buf := &strings.Builder{}
	tag := ">>>"
	if s.tx != nil {
		buf.WriteString(fmt.Sprintf(" %stxid: %s", tag, s.txId))
	}
	buf.WriteString(fmt.Sprintf(" %scost: %s %sprepare: %s %sargs: %v", tag, end.Sub(start).String(), tag, prepare, tag, args))
	buf.WriteString(fmt.Sprintf(" %sstart: %s %send: %s", tag, start.Format(time.RFC3339Nano), tag, end.Format(time.RFC3339Nano)))
	format := "[SQL]%s"
	if err != nil {
		buf.WriteString(fmt.Sprintf(" %serror: %s", tag, err.Error()))
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
	var start, end time.Time
	defer func() {
		s.output(start, end, prepare, args, err)
	}()
	var stmt *sql.Stmt
	start = time.Now()
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

func (s *Way) Query(handlingRows func(rows *sql.Rows) (err error), prepare string, args ...interface{}) error {
	return s.QueryContext(s.ctx, handlingRows, prepare, args...)
}

func (s *Way) ExecContext(ctx context.Context, prepare string, args ...interface{}) (rowsAffected int64, err error) {
	if prepare == "" {
		return
	}
	if s.prepare != nil {
		prepare = s.prepare(prepare)
	}
	var start, end time.Time
	defer func() {
		s.output(start, end, prepare, args, err)
	}()
	var stmt *sql.Stmt
	start = time.Now()
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

func (s *Way) Exec(prepare string, args ...interface{}) (int64, error) {
	return s.ExecContext(s.ctx, prepare, args...)
}

func (s *Way) ScanContext(ctx context.Context, result interface{}, prepare string, args ...interface{}) error {
	return s.QueryContext(ctx, func(rows *sql.Rows) (err error) { return s.scanner(rows, result) }, prepare, args...)
}

func (s *Way) Scan(result interface{}, prepare string, args ...interface{}) error {
	return s.ScanContext(s.ctx, result, prepare, args...)
}

func (s *Way) Table(table string) *Table {
	return &Table{
		way:   s,
		table: table,
	}
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
