// Package hey is a helper that quickly responds to the results of insert, delete, update, select SQL statements.
// You can also use hey to quickly build SQL statements.
package hey

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	// DefaultTag mapping of default database column name and struct tag
	DefaultTag = "db"
)

var (
	// poolWay *Way pool
	poolWay *sync.Pool
)

// init initialize
func init() {
	// initialize *Way
	poolWay = &sync.Pool{}
	poolWay.New = func() interface{} {
		return &Way{}
	}
}

// getWay get *Way from pool
func getWay(origin *Way) *Way {
	latest := poolWay.Get().(*Way)
	latest.db = origin.db
	latest.Fix = origin.Fix
	latest.Tag = origin.Tag
	latest.Log = origin.Log
	latest.TxLog = origin.TxLog
	latest.Config = origin.Config
	return latest
}

// putWay put *Way in the pool
func putWay(s *Way) {
	s.db = nil
	s.Fix = nil
	s.Tag = ""
	s.Log = nil
	s.TxLog = nil
	poolWay.Put(s)
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

// Config configure of Way
type Config struct {
	DeleteMustUseWhere bool
	UpdateMustUseWhere bool

	// SqlNullReplace replace field null value
	// pgsql: COALESCE($field, $replace)
	// call example: NullReplace("email", "''"), NullReplace("account.balance", "0")
	SqlNullReplace func(fieldName string, replaceValue string) string

	// SqlInsertUpdater insert on conflict do update
	SqlInsertUpdater func() InsertUpdater

	// SqlBatchUpdater batch update
	SqlBatchUpdater func() BatchUpdater
}

var (
	DefaultConfig = Config{
		DeleteMustUseWhere: true,
		UpdateMustUseWhere: true,
	}
)

// OfPrepare record executed prepare
type OfPrepare struct {
	txId    string
	txMsg   string
	prepare string
}

// TxId get transaction id
func (s *OfPrepare) TxId() string {
	return s.txId
}

// TxMsg get transaction message
func (s *OfPrepare) TxMsg() string {
	return s.txMsg
}

// Prepare get execute prepare
func (s *OfPrepare) Prepare() string {
	return s.prepare
}

// OfArgs record executed args of prepare
type OfArgs struct {
	Args    []interface{}
	StartAt time.Time
	EndAt   time.Time
	Error   error
}

// OfTransaction record executed transaction
type OfTransaction struct {
	TxId    string    // transaction id
	TxMsg   string    // transaction message
	StartAt time.Time // time start at
	EndAt   time.Time // time end at
	State   string    // COMMIT || ROLLBACK
	Error   error     // error
}

// Way quick insert, delete, update, select helper
type Way struct {
	db     *sql.DB                           // the instance of the database connect pool
	Fix    func(string) string               // fix prepare sql script before call prepare method
	Tag    string                            // bind struct tag and table column
	Log    func(lop *OfPrepare, loa *OfArgs) // logger executed SQL statement
	tx     *sql.Tx                           // the transaction instance
	txId   string                            // the transaction unique id
	txMsg  string                            // the transaction message
	TxLog  func(lt *OfTransaction)           // logger executed transaction
	Config Config                            // configure of Way
}

// NewWay instantiate a helper
func NewWay(db *sql.DB) *Way {
	return &Way{
		db:     db,
		Tag:    DefaultTag,
		Config: DefaultConfig,
	}
}

// DB get the database connection pool object in the current instance
func (s *Way) DB() *sql.DB {
	return s.db
}

// Clone make a copy the current object
func (s *Way) Clone(db ...*sql.DB) *Way {
	way := NewWay(s.db)
	length := len(db)
	for i := length - 1; i >= 0; i-- {
		if db[i] != nil {
			way.db = db[i]
			break
		}
	}
	way.Fix = s.Fix
	way.Tag = s.Tag
	way.Log = s.Log
	way.TxLog = s.TxLog
	way.Config = s.Config
	return way
}

// begin for open transaction
func (s *Way) begin(ctx context.Context, opts *sql.TxOptions) (err error) {
	s.tx, err = s.db.BeginTx(ctx, opts)
	if err != nil {
		return
	}
	s.txId = fmt.Sprintf("%d.%d.%p", time.Now().UnixNano(), os.Getpid(), s.tx)
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

// TxMsg set the prompt for the current transaction, can only be set once
func (s *Way) TxMsg(msg string) *Way {
	if s.tx == nil {
		return s
	}
	if s.txMsg == "" {
		s.txMsg = msg
	}
	return s
}

// transaction execute SQL statements in batches in a transaction
// When a transaction is nested, the inner transaction automatically uses the outer transaction object
func (s *Way) transaction(ctx context.Context, opts *sql.TxOptions, fn func(tx *Way) error) (err error) {
	if s.tx != nil {
		return fn(s)
	}
	way := getWay(s)
	defer putWay(way)
	err = way.begin(ctx, opts)
	if err != nil {
		return
	}
	lt := &OfTransaction{
		TxId:    way.txId,
		StartAt: time.Now(),
	}
	ok := false
	defer func() {
		lt.Error = err
		lt.TxMsg = way.txMsg
		if err == nil && ok {
			err = way.commit()
			lt.State = "COMMIT"
		} else {
			_ = way.rollback()
			lt.State = "ROLLBACK"
		}
		if s.TxLog != nil {
			lt.EndAt = time.Now()
			s.TxLog(lt)
		}
	}()
	if err = fn(way); err != nil {
		return
	}
	ok = true
	return
}

// TxTryCtx call transaction multiple times
func (s *Way) TxTryCtx(ctx context.Context, opts *sql.TxOptions, fn func(tx *Way) error, times int) (err error) {
	if times < 1 {
		times = 1
	}
	for i := 0; i < times; i++ {
		err = s.transaction(ctx, opts, fn)
		if err == nil {
			return
		}
	}
	return
}

// TxTry call transaction multiple times
func (s *Way) TxTry(fn func(tx *Way) error, times int) error {
	return s.TxTryCtx(context.Background(), nil, fn, times)
}

// Stmt is a prepared statement
type Stmt struct {
	logSql *OfPrepare
	stmt   *sql.Stmt
	way    *Way
}

// newStmt new stmt
func newStmt(way *Way) *Stmt {
	stmt := &Stmt{
		logSql: &OfPrepare{
			txId:  way.txId,
			txMsg: way.txMsg,
		},
		way: way,
	}
	return stmt
}

// QueryContext repeated calls with the current object
func (s *Stmt) QueryContext(ctx context.Context, query func(rows *sql.Rows) error, args ...interface{}) error {
	return s.way.queryStmtContext(ctx, query, s, args...)
}

// Query repeated calls with the current object
func (s *Stmt) Query(query func(rows *sql.Rows) error, args ...interface{}) error {
	return s.QueryContext(context.Background(), query, args...)
}

// ExecContext repeated calls with the current object
func (s *Stmt) ExecContext(ctx context.Context, args ...interface{}) (int64, error) {
	return s.way.execStmtContext(ctx, s, args...)
}

// Exec repeated calls with the current object
func (s *Stmt) Exec(args ...interface{}) (int64, error) {
	return s.ExecContext(context.Background(), args...)
}

// ScanAllContext repeated calls with the current object
func (s *Stmt) ScanAllContext(ctx context.Context, result interface{}, args ...interface{}) error {
	return s.QueryContext(ctx, func(rows *sql.Rows) error {
		return ScanSliceStruct(rows, result, s.way.Tag)
	}, args...)
}

// ScanAll repeated calls with the current object
func (s *Stmt) ScanAll(result interface{}, args ...interface{}) error {
	return s.ScanAllContext(context.Background(), result, args...)
}

// Close closes the statement.
func (s *Stmt) Close() error {
	if s.stmt != nil {
		return s.stmt.Close()
	}
	return nil
}

// PrepareContext SQL statement prepare.
func (s *Way) PrepareContext(ctx context.Context, prepare string) (stmt *Stmt, err error) {
	if prepare == "" {
		return
	}
	if s.Fix != nil {
		prepare = s.Fix(prepare)
	}
	stmt = newStmt(s)
	stmt.logSql.prepare = prepare
	if s.tx != nil {
		stmt.stmt, err = s.tx.PrepareContext(ctx, prepare)
		return
	}
	stmt.stmt, err = s.db.PrepareContext(ctx, prepare)
	return
}

// Prepare SQL statement prepare.
func (s *Way) Prepare(prepare string) (*Stmt, error) {
	return s.PrepareContext(context.Background(), prepare)
}

// StmtContext returns a transaction-specific prepared statement from an existing statement.
func (s *Way) StmtContext(ctx context.Context, stmt *Stmt) *Stmt {
	if stmt == nil {
		return stmt
	}
	if s.tx == nil {
		return stmt
	}
	tmpStmt := newStmt(s)
	tmpStmt.logSql.prepare = stmt.logSql.prepare
	tmpStmt.stmt = s.tx.StmtContext(ctx, stmt.stmt)
	return tmpStmt
}

// Stmt returns a transaction-specific prepared statement from an existing statement.
func (s *Way) Stmt(stmt *Stmt) *Stmt {
	return s.StmtContext(context.Background(), stmt)
}

// queryStmtContext query with stmt
func (s *Way) queryStmtContext(ctx context.Context, query func(rows *sql.Rows) error, stmt *Stmt, args ...interface{}) (err error) {
	if query == nil || stmt == nil {
		return
	}
	loa := &OfArgs{
		Args:    args,
		StartAt: time.Now(),
	}
	if s.Log != nil {
		defer func() {
			loa.Error = err
			s.Log(stmt.logSql, loa)
		}()
	}
	var rows *sql.Rows
	rows, err = stmt.stmt.QueryContext(ctx, args...)
	loa.EndAt = time.Now()
	if err != nil {
		return
	}
	defer rows.Close()
	err = query(rows)
	return
}

// execStmtContext exec with stmt
func (s *Way) execStmtContext(ctx context.Context, stmt *Stmt, args ...interface{}) (rowsAffected int64, err error) {
	if stmt == nil {
		return
	}
	loa := &OfArgs{
		Args:    args,
		StartAt: time.Now(),
	}
	if s.Log != nil {
		defer func() {
			loa.Error = err
			s.Log(stmt.logSql, loa)
		}()
	}
	var result sql.Result
	result, err = stmt.stmt.ExecContext(ctx, args...)
	loa.EndAt = time.Now()
	if err != nil {
		return
	}
	rowsAffected, err = result.RowsAffected()
	return
}

// RowsNext traversing and processing query results
func (s *Way) RowsNext(rows *sql.Rows, fc func() error) error {
	return RowsNext(rows, fc)
}

// RowsNextRow scan one line of query results
func (s *Way) RowsNextRow(rows *sql.Rows, dest ...interface{}) error {
	return RowsNextRow(rows, dest...)
}

// QueryContext execute the SQL statement of the query
func (s *Way) QueryContext(ctx context.Context, query func(rows *sql.Rows) error, prepare string, args ...interface{}) error {
	if query == nil || prepare == "" {
		return nil
	}
	stmt, err := s.PrepareContext(ctx, prepare)
	if err != nil {
		return err
	}
	defer stmt.Close()
	return s.queryStmtContext(ctx, query, stmt, args...)
}

// Query execute the SQL statement of the query
func (s *Way) Query(query func(rows *sql.Rows) error, prepare string, args ...interface{}) error {
	return s.QueryContext(context.Background(), query, prepare, args...)
}

// ExecContext execute the SQL statement of the not-query
func (s *Way) ExecContext(ctx context.Context, prepare string, args ...interface{}) (int64, error) {
	if prepare == "" {
		return 0, nil
	}
	stmt, err := s.PrepareContext(ctx, prepare)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	return s.execStmtContext(ctx, stmt, args...)
}

// Exec execute the SQL statement of the not-query
func (s *Way) Exec(prepare string, args ...interface{}) (int64, error) {
	return s.ExecContext(context.Background(), prepare, args...)
}

// ScanAllContext scan the query results into the receiver
// through the mapping of column names and struct tags
func (s *Way) ScanAllContext(ctx context.Context, result interface{}, prepare string, args ...interface{}) error {
	return s.QueryContext(ctx, func(rows *sql.Rows) error {
		return ScanSliceStruct(rows, result, s.Tag)
	}, prepare, args...)
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

// TableField new table helper
func (s *Way) TableField(table ...string) *TableField {
	return newTableField(table...)
}

// WayWriterReader read and write separation
type WayWriterReader interface {
	// W get an object for write
	W() *Way

	// R get an object for read
	R() *Way
}

type wayWriterReader struct {
	choose    func(n int) int
	writer    []*Way
	writerLen int
	reader    []*Way
	readerLen int
}

func (s *wayWriterReader) W() *Way {
	return s.writer[s.choose(s.writerLen)]
}

func (s *wayWriterReader) R() *Way {
	return s.reader[s.choose(s.readerLen)]
}

func NewWayWriterReader(
	choose func(n int) int,
	writer []*Way,
	reader []*Way,
) (WayWriterReader, error) {
	if choose == nil {
		return nil, fmt.Errorf("param choose is nil")
	}

	writerLen, readerLen := len(writer), len(reader)
	if writerLen == 0 || readerLen == 0 {
		return nil, fmt.Errorf("both writer and reader should hold at least one element")
	}

	return &wayWriterReader{
		choose:    choose,
		writer:    writer,
		writerLen: writerLen,
		reader:    reader,
		readerLen: readerLen,
	}, nil
}
