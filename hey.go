// Package hey is a helper that quickly responds to the results of insert, delete, update, select sql statements.
// You can also use hey to quickly build sql statements.
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

	// poolStmt *Stmt pool
	poolStmt *sync.Pool
)

// init initialize
func init() {
	// initialize *Way
	poolWay = &sync.Pool{}
	poolWay.New = func() interface{} {
		return &Way{}
	}

	// initialize *Stmt
	poolStmt = &sync.Pool{}
	poolStmt.New = func() interface{} {
		return &Stmt{}
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
	s.Config = nil
	poolWay.Put(s)
}

// getStmt get *Stmt from pool
func getStmt() *Stmt {
	return poolStmt.Get().(*Stmt)
}

// putStmt put *Stmt in the pool
func putStmt(s *Stmt) {
	s.way = nil
	s.caller = nil
	s.prepare = ""
	s.stmt = nil
	poolStmt.Put(s)
}

// Config configure of Way
type Config struct {
	DeleteMustUseWhere bool
	UpdateMustUseWhere bool

	// SqlNullReplace replace field null value
	// pgsql: COALESCE($field, $replace)
	// call example: NullReplace("email", "''"), NullReplace("account.balance", "0")
	SqlNullReplace func(fieldName string, replaceValue string) string
}

var (
	DefaultConfig = &Config{
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
	Log    func(lop *OfPrepare, loa *OfArgs) // logger executed sql statement
	tx     *sql.Tx                           // the transaction instance
	txAt   *time.Time                        // the transaction start at
	txId   string                            // the transaction unique id
	txMsg  string                            // the transaction message
	TxLog  func(lt *OfTransaction)           // logger executed transaction
	Config *Config                           // configure of Way
}

// NewWay instantiate a helper
func NewWay(db *sql.DB) *Way {
	return &Way{
		db:     db,
		Tag:    DefaultTag,
		Config: DefaultConfig,
	}
}

// DB -> get the database connection pool object in the current instance
func (s *Way) DB() *sql.DB {
	return s.db
}

// begin -> open transaction
func (s *Way) begin(ctx context.Context, opts *sql.TxOptions, conn *sql.Conn) (err error) {
	if conn != nil {
		s.tx, err = conn.BeginTx(ctx, opts)
	} else {
		s.tx, err = s.db.BeginTx(ctx, opts)
	}
	if err != nil {
		return
	}
	now := time.Now()
	s.txAt = &now
	s.txId = fmt.Sprintf("%d.%d.%p", now.UnixNano(), os.Getpid(), s.tx)
	return
}

// commit -> commit transaction
func (s *Way) commit() (err error) {
	err = s.tx.Commit()
	s.tx, s.txAt, s.txId, s.txMsg = nil, nil, "", ""
	return
}

// rollback -> rollback transaction
func (s *Way) rollback() (err error) {
	err = s.tx.Rollback()
	s.tx, s.txAt, s.txId, s.txMsg = nil, nil, "", ""
	return
}

// TxNil -> whether the current instance has not opened a transaction
func (s *Way) TxNil() bool {
	return s.tx == nil
}

// TxMsg -> set the prompt for the current transaction, can only be set once
func (s *Way) TxMsg(msg string) *Way {
	if s.tx == nil {
		return s
	}
	if s.txMsg == "" {
		s.txMsg = msg
	}
	return s
}

// transaction -> execute sql statements in batches in a transaction When a transaction is nested, the inner transaction automatically uses the outer transaction object
func (s *Way) transaction(ctx context.Context, opts *sql.TxOptions, fn func(tx *Way) error, conn *sql.Conn) (err error) {
	if s.tx != nil {
		return fn(s)
	}

	way := getWay(s)
	defer putWay(way)

	if err = way.begin(ctx, opts, conn); err != nil {
		return
	}

	lt := &OfTransaction{
		TxId:    way.txId,
		StartAt: *(way.txAt),
	}

	ok := false
	defer func() {
		lt.Error = err
		lt.TxMsg = way.txMsg
		if err == nil && ok {
			err = way.commit()
			lt.Error = err
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

// TxTryCtx -> call transaction multiple times
func (s *Way) TxTryCtx(ctx context.Context, opts *sql.TxOptions, fn func(tx *Way) error, times int, conn ...*sql.Conn) (err error) {
	if times < 1 {
		times = 1
	}
	var c *sql.Conn
	length := len(conn)
	for i := length - 1; i >= 0; i-- {
		if conn[i] != nil {
			c = conn[i]
		}
	}
	for i := 0; i < times; i++ {
		err = s.transaction(ctx, opts, fn, c)
		if err == nil {
			return
		}
	}
	return
}

// TxTry -> call transaction multiple times
func (s *Way) TxTry(fn func(tx *Way) error, times int, conn ...*sql.Conn) error {
	return s.TxTryCtx(context.Background(), nil, fn, times, conn...)
}

// Now -> get current time, the transaction open status will get the same time
func (s *Way) Now() time.Time {
	if s.TxNil() {
		return time.Now()
	}
	return *(s.txAt)
}

// RowsNext -> traversing and processing query results
func (s *Way) RowsNext(rows *sql.Rows, fc func() error) error {
	return RowsNext(rows, fc)
}

// RowsNextRow -> scan one line of query results
func (s *Way) RowsNextRow(rows *sql.Rows, dest ...interface{}) error {
	return RowsNextRow(rows, dest...)
}

// Caller the implementation object is usually one of *sql.Conn, *sql.DB, *sql.Tx
type Caller interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// caller -> *sql.Conn(or other) first, *sql.Tx(s.tx) second, *sql.DB(s.db) last
func (s *Way) caller(caller ...Caller) Caller {
	length := len(caller)
	for i := length - 1; i >= 0; i-- {
		if caller[i] != nil {
			return caller[i]
		}
	}
	if s.tx != nil {
		return s.tx
	}
	return s.db
}

// withLogger -> call logger
func (s *Way) withLogger(ofPrepare *OfPrepare, ofArgs *OfArgs) {
	if s.Log == nil {
		return
	}
	s.Log(ofPrepare, ofArgs)
}

// Stmt prepare handle
type Stmt struct {
	way     *Way
	caller  Caller
	prepare string
	stmt    *sql.Stmt
}

// Close -> close prepare handle
func (s *Stmt) Close() (err error) {
	if s.stmt != nil {
		err = s.stmt.Close()
		putStmt(s) // successfully called
	}
	return
}

// QueryContext -> query prepared that can be called repeatedly
func (s *Stmt) QueryContext(ctx context.Context, query func(rows *sql.Rows) error, args ...interface{}) (err error) {
	op := &OfPrepare{
		txId:    s.way.txId,
		txMsg:   s.way.txMsg,
		prepare: s.prepare,
	}
	oa := &OfArgs{
		Args:    args,
		StartAt: time.Now(),
	}
	var endAt time.Time
	defer func() {
		if endAt.IsZero() {
			oa.EndAt = time.Now()
		} else {
			oa.EndAt = endAt
		}
		oa.Error = err
		s.way.withLogger(op, oa)
	}()
	var rows *sql.Rows
	rows, err = s.stmt.QueryContext(ctx, args...)
	if err != nil {
		return
	}
	endAt = time.Now()
	defer rows.Close()
	err = query(rows)
	return
}

// Query -> query prepared that can be called repeatedly
func (s *Stmt) Query(query func(rows *sql.Rows) error, args ...interface{}) error {
	return s.QueryContext(context.Background(), query, args...)
}

// ExecContext -> execute prepared that can be called repeatedly
func (s *Stmt) ExecContext(ctx context.Context, args ...interface{}) (rowsAffected int64, err error) {
	op := &OfPrepare{
		txId:    s.way.txId,
		txMsg:   s.way.txMsg,
		prepare: s.prepare,
	}
	oa := &OfArgs{
		Args:    args,
		StartAt: time.Now(),
	}
	var endAt time.Time
	defer func() {
		if endAt.IsZero() {
			oa.EndAt = time.Now()
		} else {
			oa.EndAt = endAt
		}
		oa.Error = err
		s.way.withLogger(op, oa)
	}()
	var result sql.Result
	result, err = s.stmt.ExecContext(ctx, args...)
	if err != nil {
		return
	}
	endAt = time.Now()
	rowsAffected, err = result.RowsAffected()
	return
}

// Exec -> execute prepared that can be called repeatedly
func (s *Stmt) Exec(args ...interface{}) (int64, error) {
	return s.ExecContext(context.Background(), args...)
}

// ScanAllContext -> query prepared and scan results that can be called repeatedly
func (s *Stmt) ScanAllContext(ctx context.Context, result interface{}, args ...interface{}) error {
	return s.QueryContext(ctx, func(rows *sql.Rows) error {
		return ScanSliceStruct(rows, result, s.way.Tag)
	}, args...)
}

// ScanAll -> query prepared and scan results that can be called repeatedly
func (s *Stmt) ScanAll(result interface{}, args ...interface{}) error {
	return s.ScanAllContext(context.Background(), result, args...)
}

// PrepareContext -> prepare sql statement, don't forget to call *Stmt.Close()
func (s *Way) PrepareContext(ctx context.Context, prepare string, caller ...Caller) (*Stmt, error) {
	stmt := getStmt()
	defer func() {
		if stmt.stmt == nil {
			putStmt(stmt) // called when stmt.caller.PrepareContext() failed
		}
	}()
	stmt.way = s
	stmt.caller = s.caller(caller...)
	stmt.prepare = prepare
	if s.Fix != nil {
		stmt.prepare = s.Fix(prepare)
	}
	tmp, err := stmt.caller.PrepareContext(ctx, stmt.prepare)
	if err != nil {
		return nil, err
	}
	stmt.stmt = tmp
	return stmt, nil
}

// Prepare -> prepare sql statement, don't forget to call *Stmt.Close()
func (s *Way) Prepare(prepare string, caller ...Caller) (*Stmt, error) {
	return s.PrepareContext(context.Background(), prepare, caller...)
}

// QueryContext -> execute the query sql statement
func (s *Way) QueryContext(ctx context.Context, query func(rows *sql.Rows) error, prepare string, args ...interface{}) error {
	stmt, err := s.PrepareContext(ctx, prepare)
	if err != nil {
		return err
	}
	defer stmt.Close()
	return stmt.QueryContext(ctx, query, args...)
}

// Query -> execute the query sql statement
func (s *Way) Query(query func(rows *sql.Rows) error, prepare string, args ...interface{}) error {
	return s.QueryContext(context.Background(), query, prepare, args...)
}

// ScanAllContext -> query and scan results through the mapping of column names and struct tags
func (s *Way) ScanAllContext(ctx context.Context, result interface{}, prepare string, args ...interface{}) error {
	return s.QueryContext(ctx, func(rows *sql.Rows) error {
		return ScanSliceStruct(rows, result, s.Tag)
	}, prepare, args...)
}

// ScanAll -> query and scan results
func (s *Way) ScanAll(result interface{}, prepare string, args ...interface{}) error {
	return s.ScanAllContext(context.Background(), result, prepare, args...)
}

// ExecContext -> execute the execute sql statement
func (s *Way) ExecContext(ctx context.Context, prepare string, args ...interface{}) (int64, error) {
	stmt, err := s.PrepareContext(ctx, prepare)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	return stmt.ExecContext(ctx, args...)
}

// Exec -> execute the execute sql statement
func (s *Way) Exec(prepare string, args ...interface{}) (int64, error) {
	return s.ExecContext(context.Background(), prepare, args...)
}

// getter -> query, execute the query sql statement without args, no prepared is used
func (s *Way) getter(ctx context.Context, query func(rows *sql.Rows) error, prepare string, caller ...Caller) (err error) {
	if query == nil || prepare == "" {
		return
	}
	op := &OfPrepare{
		txId:    s.txId,
		txMsg:   s.txMsg,
		prepare: prepare,
	}
	oa := &OfArgs{
		Args:    nil,
		StartAt: time.Now(),
	}
	var endAt time.Time
	defer func() {
		if endAt.IsZero() {
			oa.EndAt = time.Now()
		} else {
			oa.EndAt = endAt
		}
		oa.Error = err
		s.withLogger(op, oa)
	}()
	var rows *sql.Rows
	rows, err = s.caller(caller...).QueryContext(ctx, prepare)
	endAt = time.Now()
	if err != nil {
		return
	}
	defer rows.Close()
	err = query(rows)
	return
}

// setter -> execute, execute the execute sql statement without args, no prepared is used
func (s *Way) setter(ctx context.Context, prepare string, caller ...Caller) (rowsAffected int64, err error) {
	if prepare == "" {
		return
	}
	op := &OfPrepare{
		txId:    s.txId,
		txMsg:   s.txMsg,
		prepare: prepare,
	}
	oa := &OfArgs{
		Args:    nil,
		StartAt: time.Now(),
	}
	var endAt time.Time
	defer func() {
		if endAt.IsZero() {
			oa.EndAt = time.Now()
		} else {
			oa.EndAt = endAt
		}
		oa.Error = err
		s.withLogger(op, oa)
	}()
	var result sql.Result
	result, err = s.caller(caller...).ExecContext(ctx, prepare)
	endAt = time.Now()
	if err != nil {
		return
	}
	rowsAffected, err = result.RowsAffected()
	return
}

// GetterContext -> execute the query sql statement without args, no prepared is used
func (s *Way) GetterContext(ctx context.Context, query func(rows *sql.Rows) error, prepare string, caller ...Caller) (err error) {
	return s.getter(ctx, query, prepare, caller...)
}

// Getter -> execute the query sql statement without args, no prepared is used
func (s *Way) Getter(query func(rows *sql.Rows) error, prepare string, caller ...Caller) error {
	return s.GetterContext(context.Background(), query, prepare, caller...)
}

// SetterContext -> execute the execute sql statement without args, no prepared is used
func (s *Way) SetterContext(ctx context.Context, prepare string, caller ...Caller) (int64, error) {
	return s.setter(ctx, prepare, caller...)
}

// Setter -> execute the execute sql statement without args, no prepared is used
func (s *Way) Setter(prepare string, caller ...Caller) (int64, error) {
	return s.SetterContext(context.Background(), prepare, caller...)
}

// F -> quickly initialize a filter
func (s *Way) F(filter ...Filter) Filter {
	return NewFilter().Filter(filter...)
}

// Add -> create an instance that executes the INSERT sql statement
func (s *Way) Add(table string) *Add {
	return NewAdd(s).Table(table)
}

// Del -> create an instance that executes the DELETE sql statement
func (s *Way) Del(table string) *Del {
	return NewDel(s).Table(table)
}

// Mod -> create an instance that executes the UPDATE sql statement
func (s *Way) Mod(table string) *Mod {
	return NewMod(s).Table(table)
}

// Get -> create an instance that executes the SELECT sql statement
func (s *Way) Get(table ...string) *Get {
	return NewGet(s).Table(LastNotEmptyString(table))
}

// Identifier -> sql identifier
func (s *Way) Identifier(prefix ...string) *Identifier {
	return &Identifier{
		prefix: LastNotEmptyString(prefix),
	}
}

// WayWriterReader -> read and write separation
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
