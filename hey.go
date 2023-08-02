// Package hey is a helper that quickly responds to the results of insert, delete, update, select SQL statements.
// You can also use hey to quickly build SQL statements.
package hey

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	// DefaultTag mapping of default database column name and struct tag
	DefaultTag = "db"
)

// FixPgsql fix postgresql SQL statement
// hey uses `?` as the placeholder symbol of the SQL statement by default
// and may need to use different placeholder symbols for different databases
// use the current method to convert ?, ?, ? ... into $1, $2, $3 ... as placeholders for SQL statements
func FixPgsql(str string) string {
	index := 0
	for strings.Contains(str, Placeholder) {
		index++
		str = strings.Replace(str, Placeholder, fmt.Sprintf("$%d", index), 1)
	}
	return str
}

// Choose returns the first instance of *Way that is not empty in items
// returns way by default
func Choose(way *Way, items ...*Way) *Way {
	for i := len(items) - 1; i >= 0; i-- {
		if items[i] != nil {
			return items[i]
		}
	}
	return way
}

// LogSql record executed SQL statement
type LogSql struct {
	TxId    string
	TxMsg   string
	Prepare string
	Args    []interface{}
	StartAt time.Time
	EndAt   time.Time
	Error   error
}

// Way quick insert, delete, update, select helper
type Way struct {
	db    *sql.DB             // the instance of the database connect pool
	tx    *sql.Tx             // the transaction instance
	txId  string              // the transaction unique id
	txMsg string              // the transaction message
	fix   func(string) string // fix prepare sql script before call prepare method
	tag   string              // bind struct tag and table column
	log   func(ls *LogSql)    // logger method
}

// NewWay instantiate a helper
func NewWay(db *sql.DB) *Way {
	return &Way{
		db:  db,
		tag: DefaultTag,
	}
}

// Fix set the method to repair the SQL statement
func (s *Way) Fix(fix func(string) string) *Way {
	s.fix = fix
	return s
}

// Tag set the struct tag corresponding to the database table.column
func (s *Way) Tag(tag string) *Way {
	s.tag = tag
	return s
}

// Log set the processing method of the log object
func (s *Way) Log(log func(ls *LogSql)) *Way {
	s.log = log
	return s
}

// DB get the database connection pool object in the current instance
func (s *Way) DB() *sql.DB {
	return s.db
}

// Clone make a copy the current object
func (s *Way) Clone() *Way {
	return NewWay(s.db).Fix(s.fix).Tag(s.tag).Log(s.log)
}

// begin for open transaction
func (s *Way) begin(ctx context.Context, opts *sql.TxOptions) (err error) {
	s.tx, err = s.db.BeginTx(ctx, opts)
	if err != nil {
		return
	}
	s.txId = fmt.Sprintf("tid.%d.%d.%p", time.Now().UnixNano(), os.Getpid(), s.tx)
	return
}

// commit for commit transaction
func (s *Way) commit() (err error) {
	err = s.tx.Commit()
	s.tx, s.txId, s.txMsg = nil, "", ""
	return
}

// rollback for rollback transaction
func (s *Way) rollback() (err error) {
	err = s.tx.Rollback()
	s.tx, s.txId, s.txMsg = nil, "", ""
	return
}

// TxNil whether the current instance has not opened a transaction
func (s *Way) TxNil() bool {
	return s.tx == nil
}

// TxMsg set the prompt for the current transaction
func (s *Way) TxMsg(msg string) *Way {
	if s.tx != nil && s.txMsg == "" {
		s.txMsg = msg
	}
	return s
}

// Transaction execute SQL statements in batches in a transaction
// When a transaction is nested, the inner transaction automatically uses the outer transaction object
func (s *Way) Transaction(ctx context.Context, opts *sql.TxOptions, fn func(tx *Way) (err error)) (err error) {
	if s.tx != nil {
		return fn(s)
	}
	way := s.Clone()
	if err = way.begin(ctx, opts); err != nil {
		return
	}
	ok := false
	defer func() {
		if err == nil && ok {
			err = way.commit()
		} else {
			_ = way.rollback()
		}
	}()
	if err = fn(way); err != nil {
		return
	}
	ok = true
	return
}

// Trans concisely call Transaction
func (s *Way) Trans(fn func(tx *Way) (err error)) error {
	return s.Transaction(context.Background(), nil, fn)
}

// QueryContext execute the SQL statement of the query
func (s *Way) QueryContext(ctx context.Context, query func(rows *sql.Rows) (err error), prepare string, args ...interface{}) (err error) {
	if query == nil || prepare == "" {
		return
	}
	if s.fix != nil {
		prepare = s.fix(prepare)
	}
	startAt := time.Now()
	endAt := time.Time{}
	if s.log != nil {
		defer func() {
			if endAt.IsZero() {
				endAt = time.Now()
			}
			ls := &LogSql{
				TxId:    s.txId,
				TxMsg:   s.txMsg,
				Prepare: prepare,
				Args:    args,
				StartAt: startAt,
				EndAt:   endAt,
				Error:   err,
			}
			s.log(ls)
		}()
	}
	var stmt *sql.Stmt
	if s.tx != nil {
		stmt, err = s.tx.PrepareContext(ctx, prepare)
	} else {
		stmt, err = s.db.PrepareContext(ctx, prepare)
	}
	if err != nil {
		return
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx, args...)
	endAt = time.Now()
	if err != nil {
		return
	}
	defer rows.Close()
	err = query(rows)
	return
}

// Query execute the SQL statement of the query
func (s *Way) Query(query func(rows *sql.Rows) (err error), prepare string, args ...interface{}) error {
	return s.QueryContext(context.Background(), query, prepare, args...)
}

// ExecContext execute the SQL statement of the not-query
func (s *Way) ExecContext(ctx context.Context, prepare string, args ...interface{}) (rowsAffected int64, err error) {
	if prepare == "" {
		return
	}
	if s.fix != nil {
		prepare = s.fix(prepare)
	}
	startAt := time.Now()
	endAt := time.Time{}
	if s.log != nil {
		defer func() {
			if endAt.IsZero() {
				endAt = time.Now()
			}
			ls := &LogSql{
				TxId:    s.txId,
				TxMsg:   s.txMsg,
				Prepare: prepare,
				Args:    args,
				StartAt: startAt,
				EndAt:   endAt,
				Error:   err,
			}
			s.log(ls)
		}()
	}
	var stmt *sql.Stmt
	if s.tx != nil {
		stmt, err = s.tx.PrepareContext(ctx, prepare)
	} else {
		stmt, err = s.db.PrepareContext(ctx, prepare)
	}
	if err != nil {
		return
	}
	defer stmt.Close()
	sqlResult, err := stmt.ExecContext(ctx, args...)
	endAt = time.Now()
	if err != nil {
		return
	}
	rowsAffected, err = sqlResult.RowsAffected()
	return
}

// Exec execute the SQL statement of the not-query
func (s *Way) Exec(prepare string, args ...interface{}) (int64, error) {
	return s.ExecContext(context.Background(), prepare, args...)
}

// ExecAllContext execute SQL scripts in batches
// the SQL scripts executed by this method will not call the log method
func (s *Way) ExecAllContext(ctx context.Context, script string, args ...interface{}) (int64, error) {
	result, err := s.db.ExecContext(ctx, script, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// ExecAll concisely call ExecAllContext
func (s *Way) ExecAll(script string, args ...interface{}) (int64, error) {
	return s.ExecAllContext(context.Background(), script, args...)
}

// ScanAllContext scan the query results into the receiver
// through the mapping of column names and struct tags
func (s *Way) ScanAllContext(ctx context.Context, result interface{}, prepare string, args ...interface{}) error {
	return s.QueryContext(ctx, func(rows *sql.Rows) error { return ScanSliceStruct(rows, result, s.tag) }, prepare, args...)
}

// ScanAll concisely call ScanAllContext
func (s *Way) ScanAll(result interface{}, prepare string, args ...interface{}) error {
	return s.ScanAllContext(context.Background(), result, prepare, args...)
}

// Filter quickly initialize a filter
func (s *Way) Filter(filter ...Filter) Filter {
	return NewFilter().Filter(filter...)
}

// Add create an instance that executes the INSERT SQL statement
func (s *Way) Add(table string) *Add {
	return NewAdd(s).Table(table)
}

// Del create an instance that executes the DELETE SQL statement
func (s *Way) Del(table string) *Del {
	return NewDel(s).Table(table)
}

// Mod create an instance that executes the UPDATE SQL statement
func (s *Way) Mod(table string) *Mod {
	return NewMod(s).Table(table)
}

// Get create an instance that executes the SELECT SQL statement
func (s *Way) Get(table ...string) *Get {
	get := NewGet(s)
	for i := len(table) - 1; i >= 0; i-- {
		if table[i] != "" {
			get.Table(table[i])
			break
		}
	}
	return get
}
