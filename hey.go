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

const (
	EmptyString = ""
)

const (
	SqlDollar = "$"
	SqlPoint  = "."
	SqlSpace  = " "
	SqlStar   = "*"

	SqlAs       = "AS"
	SqlAsc      = "ASC"
	SqlDesc     = "DESC"
	SqlUnion    = "UNION"
	SqlUnionAll = "UNION ALL"

	SqlJoinInner = "INNER JOIN"
	SqlJoinLeft  = "LEFT JOIN"
	SqlJoinRight = "RIGHT JOIN"
	SqlJoinFull  = "FULL JOIN"

	SqlAnd = "AND"
	SqlOr  = "OR"

	SqlAvg   = "AVG"
	SqlMax   = "MAX"
	SqlMin   = "MIN"
	SqlSum   = "SUM"
	SqlCount = "COUNT"

	SqlNull = "NULL"

	SqlPlaceholder   = "?"
	SqlEqual         = "="
	SqlNotEqual      = "<>"
	SqlGreater       = ">"
	SqlGreaterEqual  = ">="
	SqlLessThan      = "<"
	SqlLessThanEqual = "<="
)

type Preparer interface {
	// SQL get the prepare sql statement and parameter list
	SQL() (prepare string, args []interface{})
}

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
	latest.fix = origin.fix
	latest.tag = origin.tag
	latest.log = origin.log
	latest.txLog = origin.txLog
	latest.txOpts = origin.txOpts
	latest.config = origin.config
	return latest
}

// putWay put *Way in the pool
func putWay(s *Way) {
	s.db = nil
	s.fix = nil
	s.tag = EmptyString
	s.log = nil
	s.txLog = nil
	s.txOpts = nil
	s.config = nil
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
	s.prepare = EmptyString
	s.stmt = nil
	poolStmt.Put(s)
}

// Config configure of Way
type Config struct {
	DeleteMustUseWhere bool
	UpdateMustUseWhere bool
}

var (
	DefaultConfig = &Config{
		DeleteMustUseWhere: true,
		UpdateMustUseWhere: true,
	}
)

// LogArgs record executed args of prepare
type LogArgs struct {
	Args    []interface{}
	StartAt time.Time
	EndAt   time.Time
	Error   error
}

// LogPrepareArgs record executed prepare args
type LogPrepareArgs struct {
	TxId    string
	TxMsg   string
	Prepare string
	Args    *LogArgs
}

// LogTransaction record executed transaction
type LogTransaction struct {
	TxId    string    // transaction id
	TxMsg   string    // transaction message
	StartAt time.Time // transaction start at
	EndAt   time.Time // transaction end at
	State   string    // transaction result COMMIT || ROLLBACK
	Error   error     // error
}

// Way quick insert, delete, update, select helper
type Way struct {
	db *sql.DB // the instance of the database connect pool

	fix func(string) string // fix prepare sql script before call prepare method

	tag string // bind struct tag and table column

	log func(log *LogPrepareArgs) // logger executed sql statement

	tx     *sql.Tx                   // the transaction instance
	txAt   *time.Time                // the transaction start at
	txId   string                    // the transaction unique id
	txMsg  string                    // the transaction message
	txLog  func(log *LogTransaction) // logger executed transaction
	txOpts *sql.TxOptions            // the transaction isolation level

	config *Config // configure of *Way
}

// Opts custom attribute value of *Way
type Opts func(s *Way)

// WithPrepare -> uses custom fix prepare
func WithPrepare(fix func(prepare string) string) Opts {
	return func(s *Way) { s.fix = fix }
}

// WithTag -> uses custom tag
func WithTag(tag string) Opts {
	return func(s *Way) { s.tag = tag }
}

// WithLogger -> uses custom logger
func WithLogger(log func(log *LogPrepareArgs)) Opts {
	return func(s *Way) { s.log = log }
}

// WithTxLogger -> uses custom transaction logger
func WithTxLogger(txLog func(log *LogTransaction)) Opts {
	return func(s *Way) { s.txLog = txLog }
}

// WithTxOpts -> uses custom global default transaction isolation level
func WithTxOpts(txOpts *sql.TxOptions) Opts {
	return func(s *Way) { s.txOpts = txOpts }
}

// WithConfig -> uses custom configure
func WithConfig(config *Config) Opts {
	return func(s *Way) { s.config = config }
}

// NewWay instantiate a helper
func NewWay(db *sql.DB, opts ...Opts) *Way {
	s := &Way{
		db:     db,
		tag:    DefaultTag,
		config: DefaultConfig,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// DB -> get the database connection pool object in the current instance
func (s *Way) DB() *sql.DB {
	return s.db
}

// Tag -> get tag value
func (s *Way) Tag() string {
	return s.tag
}

// begin -> open transaction
func (s *Way) begin(ctx context.Context, conn *sql.Conn) (err error) {
	if conn != nil {
		s.tx, err = conn.BeginTx(ctx, s.txOpts)
	} else {
		s.tx, err = s.db.BeginTx(ctx, s.txOpts)
	}
	if err != nil {
		return
	}
	now := time.Now()
	s.txAt = &now
	s.txId = fmt.Sprintf("%d%s%d%s%p", now.UnixNano(), SqlPoint, os.Getpid(), SqlPoint, s.tx)
	return
}

// commit -> commit transaction
func (s *Way) commit() (err error) {
	err = s.tx.Commit()
	s.tx, s.txAt, s.txId, s.txMsg = nil, nil, EmptyString, EmptyString
	return
}

// rollback -> rollback transaction
func (s *Way) rollback() (err error) {
	err = s.tx.Rollback()
	s.tx, s.txAt, s.txId, s.txMsg = nil, nil, EmptyString, EmptyString
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
	if s.txMsg == EmptyString {
		s.txMsg = msg
	}
	return s
}

// transaction -> execute sql statements in batches in a transaction When a transaction is nested, the inner transaction automatically uses the outer transaction object
func (s *Way) transaction(ctx context.Context, fc func(tx *Way) error, conn *sql.Conn) (err error) {
	if s.tx != nil {
		return fc(s)
	}

	way := getWay(s)
	defer putWay(way)

	if err = way.begin(ctx, conn); err != nil {
		return
	}

	log := &LogTransaction{
		TxId:    way.txId,
		StartAt: *(way.txAt),
	}

	ok := false
	defer func() {
		log.TxMsg = way.txMsg
		if err == nil && ok {
			err = way.commit()
			log.State = "COMMIT"
		} else {
			_ = way.rollback()
			log.State = "ROLLBACK"
		}
		log.Error = err
		if s.txLog != nil {
			log.EndAt = time.Now()
			s.txLog(log)
		}
	}()

	if err = fc(way); err != nil {
		return
	}
	ok = true
	return
}

// TxTryCtx -> call transaction multiple times
func (s *Way) TxTryCtx(ctx context.Context, fc func(tx *Way) error, times int, conn ...*sql.Conn) (err error) {
	if times < 1 {
		times = 1
	}
	var c *sql.Conn
	length := len(conn)
	for i := length - 1; i >= 0; i-- {
		if conn[i] != nil {
			c = conn[i]
			break
		}
	}
	for i := 0; i < times; i++ {
		if err = s.transaction(ctx, fc, c); err == nil {
			break
		}
	}
	return
}

// TxTry -> call transaction multiple times
func (s *Way) TxTry(fc func(tx *Way) error, times int, conn ...*sql.Conn) error {
	return s.TxTryCtx(context.Background(), fc, times, conn...)
}

// TxCtx -> call transaction once
func (s *Way) TxCtx(ctx context.Context, fc func(tx *Way) error, conn ...*sql.Conn) error {
	return s.TxTryCtx(ctx, fc, 1, conn...)
}

// Tx -> call transaction once
func (s *Way) Tx(fc func(tx *Way) error, conn ...*sql.Conn) error {
	return s.TxTryCtx(context.Background(), fc, 1, conn...)
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
func (s *Way) withLogger(log *LogPrepareArgs) {
	if s.log != nil {
		s.log(log)
	}
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
	log := &LogPrepareArgs{
		TxId:    s.way.txId,
		TxMsg:   s.way.txMsg,
		Prepare: s.prepare,
		Args:    &LogArgs{Args: args},
	}
	defer func() {
		log.Args.Error = err
		s.way.withLogger(log)
	}()
	var rows *sql.Rows
	log.Args.StartAt = time.Now()
	rows, err = s.stmt.QueryContext(ctx, args...)
	log.Args.EndAt = time.Now()
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	err = query(rows)
	return
}

// Query -> query prepared that can be called repeatedly
func (s *Stmt) Query(query func(rows *sql.Rows) error, args ...interface{}) error {
	return s.QueryContext(context.Background(), query, args...)
}

// ExecContext -> execute prepared that can be called repeatedly
func (s *Stmt) ExecContext(ctx context.Context, args ...interface{}) (rowsAffected int64, err error) {
	log := &LogPrepareArgs{
		TxId:    s.way.txId,
		TxMsg:   s.way.txMsg,
		Prepare: s.prepare,
		Args:    &LogArgs{Args: args},
	}
	defer func() {
		log.Args.Error = err
		s.way.withLogger(log)
	}()
	var result sql.Result
	log.Args.StartAt = time.Now()
	result, err = s.stmt.ExecContext(ctx, args...)
	log.Args.EndAt = time.Now()
	if err != nil {
		return
	}
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
		return ScanSliceStruct(rows, result, s.way.tag)
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
	if s.fix != nil {
		stmt.prepare = s.fix(prepare)
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
	defer func() { _ = stmt.Close() }()
	return stmt.QueryContext(ctx, query, args...)
}

// Query -> execute the query sql statement
func (s *Way) Query(query func(rows *sql.Rows) error, prepare string, args ...interface{}) error {
	return s.QueryContext(context.Background(), query, prepare, args...)
}

// ScanAllContext -> query and scan results through the mapping of column names and struct tags
func (s *Way) ScanAllContext(ctx context.Context, result interface{}, prepare string, args ...interface{}) error {
	return s.QueryContext(ctx, func(rows *sql.Rows) error {
		return ScanSliceStruct(rows, result, s.tag)
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
	defer func() { _ = stmt.Close() }()
	return stmt.ExecContext(ctx, args...)
}

// Exec -> execute the execute sql statement
func (s *Way) Exec(prepare string, args ...interface{}) (int64, error) {
	return s.ExecContext(context.Background(), prepare, args...)
}

// getter -> query, execute the query sql statement without args, no prepared is used
func (s *Way) getter(ctx context.Context, query func(rows *sql.Rows) error, prepare string, caller ...Caller) (err error) {
	if query == nil || prepare == EmptyString {
		return
	}
	log := &LogPrepareArgs{
		TxId:    s.txId,
		TxMsg:   s.txMsg,
		Prepare: prepare,
		Args:    &LogArgs{Args: nil},
	}
	defer func() {
		log.Args.Error = err
		s.withLogger(log)
	}()
	var rows *sql.Rows
	log.Args.StartAt = time.Now()
	rows, err = s.caller(caller...).QueryContext(ctx, prepare)
	log.Args.EndAt = time.Now()
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	err = query(rows)
	return
}

// setter -> execute, execute the execute sql statement without args, no prepared is used
func (s *Way) setter(ctx context.Context, prepare string, caller ...Caller) (rowsAffected int64, err error) {
	if prepare == "" {
		return
	}
	log := &LogPrepareArgs{
		TxId:    s.txId,
		TxMsg:   s.txMsg,
		Prepare: prepare,
		Args:    &LogArgs{Args: nil},
	}
	defer func() {
		log.Args.Error = err
		s.withLogger(log)
	}()
	var result sql.Result
	log.Args.StartAt = time.Now()
	result, err = s.caller(caller...).ExecContext(ctx, prepare)
	log.Args.EndAt = time.Now()
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
	return F().Filter(filter...)
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

// Ident -> sql identifier
func (s *Way) Ident(prefix ...string) *Ident {
	return &Ident{
		prefix: LastNotEmptyString(prefix),
	}
}

// AliasA sql identifier prefix a
func (s *Way) AliasA() *Ident {
	return s.Ident(AliasA)
}

// AliasB sql identifier prefix b
func (s *Way) AliasB() *Ident {
	return s.Ident(AliasB)
}

// AliasC sql identifier prefix c
func (s *Way) AliasC() *Ident {
	return s.Ident(AliasC)
}

// AliasD sql identifier prefix d
func (s *Way) AliasD() *Ident {
	return s.Ident(AliasD)
}

// AliasE sql identifier prefix e
func (s *Way) AliasE() *Ident {
	return s.Ident(AliasE)
}

// AliasF sql identifier prefix f
func (s *Way) AliasF() *Ident {
	return s.Ident(AliasF)
}

// AliasG sql identifier prefix g
func (s *Way) AliasG() *Ident {
	return s.Ident(AliasG)
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

// W -> for write
func (s *wayWriterReader) W() *Way {
	return s.writer[s.choose(s.writerLen)]
}

// R -> for read
func (s *wayWriterReader) R() *Way {
	return s.reader[s.choose(s.readerLen)]
}

// NewWayWriterReader -> read and write separated calls
func NewWayWriterReader(
	choose func(n int) int,
	writer []*Way,
	reader []*Way,
) (WayWriterReader, error) {
	if choose == nil {
		return nil, fmt.Errorf("hey: param choose is nil")
	}

	writerLen, readerLen := len(writer), len(reader)
	if writerLen == 0 || readerLen == 0 {
		return nil, fmt.Errorf("hey: both writer and reader should hold at least one element")
	}

	return &wayWriterReader{
		choose:    choose,
		writer:    writer,
		writerLen: writerLen,
		reader:    reader,
		readerLen: readerLen,
	}, nil
}
