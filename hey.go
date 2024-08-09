// Package hey is a helper that quickly responds to the results of insert, delete, update, select sql statements.
// You can also use hey to quickly build sql statements.
package hey

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultTag Mapping of default database column name and struct tag.
	DefaultTag = "db"

	// EmptyString Empty string value.
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

const (
	AliasA = "a"
	AliasB = "b"
	AliasC = "c"
	AliasD = "d"
	AliasE = "e"
	AliasF = "f"
	AliasG = "g"
)

type Commander interface {
	// SQL Get the SQL statement that can be executed and its corresponding parameter list.
	SQL() (prepare string, args []interface{})
}

var (
	// poolStmt *Stmt pool.
	poolStmt *sync.Pool
)

// init Initialize.
func init() {
	// initialize *Stmt
	poolStmt = &sync.Pool{}
	poolStmt.New = func() interface{} {
		return &Stmt{}
	}
}

// getStmt Get *Stmt from pool.
func getStmt() *Stmt {
	return poolStmt.Get().(*Stmt)
}

// putStmt Put *Stmt in the pool.
func putStmt(s *Stmt) {
	s.way = nil
	s.caller = nil
	s.prepare = EmptyString
	s.stmt = nil
	s.log = nil
	poolStmt.Put(s)
}

// Config Configure of Way.
type Config struct {
	// DeleteMustUseWhere Deletion of data must be filtered using conditions.
	DeleteMustUseWhere bool

	// UpdateMustUseWhere Updated data must be filtered using conditions.
	UpdateMustUseWhere bool
}

var (
	DefaultConfig = &Config{
		DeleteMustUseWhere: true,
		UpdateMustUseWhere: true,
	}
)

// LogArgs Record executed args of prepare.
type LogArgs struct {
	// Args SQL parameter list.
	Args []interface{}

	// StartAt The start time of the SQL statement.
	StartAt time.Time

	// EndAt The end time of the SQL statement.
	EndAt time.Time
}

// LogSQL Record executed prepare args.
type LogSQL struct {
	// TxId Transaction ID.
	TxId string

	// TxMsg Transaction descriptors.
	TxMsg string

	// Prepare Preprocess the SQL statements that are executed.
	Prepare string

	// Error An error encountered when executing SQL.
	Error error

	// Args SQL parameter list.
	Args *LogArgs
}

// LogTrans Record executed transaction.
type LogTrans struct {
	// Transaction id.
	TxId string

	// Transaction message.
	TxMsg string

	// Transaction start at.
	StartAt time.Time

	// Transaction end at.
	EndAt time.Time

	// Transaction result COMMIT || ROLLBACK.
	State string

	// Error.
	Error error
}

// PrepareArgs Statements to be executed and corresponding parameter list.
type PrepareArgs struct {
	Prepare string

	Args []interface{}
}

// Reader Separate read and write, when you distinguish between reading and writing, please do not use the same object for both reading and writing.
type Reader interface {
	// Read Get an object for read.
	Read() *Way
}

// Way Quick insert, delete, update, select helper.
type Way struct {
	// Custom properties: the instance of the database connect pool.
	db *sql.DB

	// Custom properties: fix prepare sql script before call prepare method.
	fix func(string) string

	// Custom properties: bind struct tag and table column.
	tag string

	// Custom properties: logger executed sql statement.
	log func(log *LogSQL)

	// The transaction instance.
	tx *sql.Tx

	// The transaction start at.
	txAt *time.Time

	// The transaction unique id.
	txId string

	// The transaction message.
	txMsg string

	// Custom properties: logger executed transaction.
	txLog func(log *LogTrans)

	// Custom properties: the transaction isolation level.
	txOpts *sql.TxOptions

	// Custom properties: configure of *Way.
	config *Config

	// Custom properties: scan query result.
	scan func(rows *sql.Rows, result interface{}, tag string) error

	// Custom properties: get a query-only object.
	reader Reader

	// Whether the current object is a query object, ignore current values when calling getWay and putWay.
	isRead *struct{}
}

// Opts Custom attribute value of *Way.
type Opts func(s *Way)

// WithPrepare -> Uses custom fix prepare.
func WithPrepare(fix func(prepare string) string) Opts {
	return func(s *Way) { s.fix = fix }
}

// WithTag -> Uses custom tag.
func WithTag(tag string) Opts {
	return func(s *Way) { s.tag = tag }
}

// WithLogger -> Uses custom logger.
func WithLogger(log func(log *LogSQL)) Opts {
	return func(s *Way) { s.log = log }
}

// WithTxLogger -> Uses custom transaction logger.
func WithTxLogger(txLog func(log *LogTrans)) Opts {
	return func(s *Way) { s.txLog = txLog }
}

// WithTxOpts -> Uses custom global default transaction isolation level.
func WithTxOpts(txOpts *sql.TxOptions) Opts {
	return func(s *Way) { s.txOpts = txOpts }
}

// WithConfig -> Uses custom configure.
func WithConfig(config *Config) Opts {
	return func(s *Way) { s.config = config }
}

// WithScan -> Uses scan for query data.
func WithScan(scan func(rows *sql.Rows, result interface{}, tag string) error) Opts {
	return func(s *Way) { s.scan = scan }
}

// WithReader -> uses reader for query.
func WithReader(reader Reader) Opts {
	return func(s *Way) { s.reader = reader }
}

// NewWay Instantiate a helper.
func NewWay(db *sql.DB, opts ...Opts) *Way {
	s := &Way{
		db:     db,
		tag:    DefaultTag,
		config: DefaultConfig,
		scan:   ScanSliceStruct,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// DB -> Get the database connection pool object in the current instance.
func (s *Way) DB() *sql.DB {
	return s.db
}

// Tag -> Get tag value.
func (s *Way) Tag() string {
	return s.tag
}

// Read -> Get an object for read.
func (s *Way) Read() *Way {
	if s.reader == nil {
		return s
	}
	readWay := s.reader.Read()
	if readWay.isRead == nil {
		readWay.isRead = &struct{}{}
	}
	return readWay
}

// IsRead -> Is an object for read?
func (s *Way) IsRead() bool {
	return s.isRead != nil
}

// begin -> Open transaction.
func (s *Way) begin(ctx context.Context, conn *sql.Conn, opts ...*sql.TxOptions) (err error) {
	opt := s.txOpts
	length := len(opts)
	for i := length - 1; i >= 0; i-- {
		if opts[i] != nil {
			opt = opts[i]
		}
	}
	if conn != nil {
		s.tx, err = conn.BeginTx(ctx, opt)
	} else {
		s.tx, err = s.db.BeginTx(ctx, opt)
	}
	if err != nil {
		return
	}
	now := time.Now()
	s.txAt = &now
	s.txId = fmt.Sprintf("%d%s%d%s%p", now.UnixNano(), SqlPoint, os.Getpid(), SqlPoint, s.tx)
	return
}

// commit -> Commit transaction.
func (s *Way) commit() (err error) {
	err = s.tx.Commit()
	s.tx, s.txAt, s.txId, s.txMsg = nil, nil, EmptyString, EmptyString
	return
}

// rollback -> Rollback transaction.
func (s *Way) rollback() (err error) {
	err = s.tx.Rollback()
	s.tx, s.txAt, s.txId, s.txMsg = nil, nil, EmptyString, EmptyString
	return
}

// TxNil -> Whether the current instance has not opened a transaction.
func (s *Way) TxNil() bool {
	return s.tx == nil
}

// TxMsg -> Set the prompt for the current transaction, can only be set once.
func (s *Way) TxMsg(msg string) *Way {
	if s.tx == nil {
		return s
	}
	if s.txMsg == EmptyString {
		s.txMsg = msg
	}
	return s
}

// TxArgs Optional args for begin a transaction.
type TxArgs struct {
	// Use the specified database connection, default nil.
	Conn *sql.Conn

	// Use the specified transaction isolation level, default *Way.txOpts.
	Opts *sql.TxOptions

	// Try calling transaction multiple times, default 1.
	Times int
}

// transaction -> Batch execute SQL through transactions.
func (s *Way) transaction(ctx context.Context, fc func(tx *Way) error, args *TxArgs) (err error) {
	if s.tx != nil {
		return fc(s)
	}
	tmp := *s
	way := &tmp
	if err = way.begin(ctx, args.Conn, args.Opts); err != nil {
		return
	}
	log := &LogTrans{
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

// TxTryCtx -> Batch execute SQL through transactions, In args, only the last transaction parameter is valid.
func (s *Way) TxTryCtx(ctx context.Context, fc func(tx *Way) error, args ...*TxArgs) (err error) {
	param := &TxArgs{
		Opts:  s.txOpts,
		Times: 1,
	}
	length := len(args)
	for i := length - 1; i >= 0; i-- {
		if args[i] == nil {
			continue
		}
		if args[i].Conn != nil {
			param.Conn = args[i].Conn
		}
		if args[i].Opts != nil {
			param.Opts = args[i].Opts
		}
		if args[i].Times > 0 {
			param.Times = args[i].Times
		}
		break
	}
	for i := 0; i < param.Times; i++ {
		if err = s.transaction(ctx, fc, param); err == nil {
			break
		}
	}
	return
}

// TxTry -> Batch execute SQL through transactions, In args, only the last transaction parameter is valid.
func (s *Way) TxTry(fc func(tx *Way) error, args ...*TxArgs) error {
	return s.TxTryCtx(context.Background(), fc, args...)
}

// Now -> Get current time, the transaction open status will get the same time.
func (s *Way) Now() time.Time {
	if s.TxNil() {
		return time.Now()
	}
	return *(s.txAt)
}

// ScanAll -> Iteratively scan from query results.
func (s *Way) ScanAll(rows *sql.Rows, fc func(rows *sql.Rows) error) error {
	return ScanAll(rows, fc)
}

// ScanOne -> Scan at most once from the query results.
func (s *Way) ScanOne(rows *sql.Rows, dest ...interface{}) error {
	return ScanOne(rows, dest...)
}

// Caller The implementation object is usually one of *sql.Conn, *sql.DB, *sql.Tx.
type Caller interface {
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)

	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// caller -> *sql.Conn(or other) first, *sql.Tx(s.tx) second, *sql.DB(s.db) last.
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

// logger -> Call logger.
func (s *Way) logger(log *LogSQL) {
	if s.log != nil {
		s.log(log)
	}
}

// Stmt Prepare handle.
type Stmt struct {
	way     *Way
	caller  Caller
	prepare string
	stmt    *sql.Stmt
	log     *LogSQL
}

// Close -> Close prepare handle.
func (s *Stmt) Close() (err error) {
	if s.stmt != nil {
		err = s.stmt.Close()
		putStmt(s) // successfully called
	}
	return
}

// QueryContext -> Query prepared, that can be called repeatedly.
func (s *Stmt) QueryContext(ctx context.Context, query func(rows *sql.Rows) error, args ...interface{}) error {
	s.log.Args = &LogArgs{
		Args: args,
	}
	defer s.way.logger(s.log)
	s.log.Args.StartAt = time.Now()
	rows, err := s.stmt.QueryContext(ctx, args...)
	s.log.Args.EndAt = time.Now()
	if err != nil {
		s.log.Error = err
		return err
	}
	defer func() { _ = rows.Close() }()
	s.log.Error = query(rows)
	return s.log.Error
}

// Query -> Query prepared, that can be called repeatedly.
func (s *Stmt) Query(query func(rows *sql.Rows) error, args ...interface{}) error {
	return s.QueryContext(context.Background(), query, args...)
}

// QueryRowContext -> Query prepared, that can be called repeatedly.
func (s *Stmt) QueryRowContext(ctx context.Context, query func(rows *sql.Row) error, args ...interface{}) error {
	s.log.Args = &LogArgs{
		Args: args,
	}
	defer s.way.logger(s.log)
	s.log.Args.StartAt = time.Now()
	row := s.stmt.QueryRowContext(ctx, args...)
	s.log.Args.EndAt = time.Now()
	s.log.Error = query(row)
	return s.log.Error
}

// QueryRow -> Query prepared, that can be called repeatedly.
func (s *Stmt) QueryRow(query func(rows *sql.Row) error, args ...interface{}) (err error) {
	return s.QueryRowContext(context.Background(), query, args...)
}

// ExecuteContext -> Execute prepared, that can be called repeatedly.
func (s *Stmt) ExecuteContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	s.log.Args = &LogArgs{
		Args: args,
	}
	defer s.way.logger(s.log)
	s.log.Args.StartAt = time.Now()
	result, err := s.stmt.ExecContext(ctx, args...)
	s.log.Args.EndAt = time.Now()
	s.log.Error = err
	return result, err
}

// Execute -> Execute prepared, that can be called repeatedly.
func (s *Stmt) Execute(args ...interface{}) (sql.Result, error) {
	return s.ExecuteContext(context.Background(), args...)
}

// ExecContext -> Execute prepared, that can be called repeatedly, return number of rows affected.
func (s *Stmt) ExecContext(ctx context.Context, args ...interface{}) (int64, error) {
	result, err := s.ExecuteContext(ctx, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// Exec -> Execute prepared, that can be called repeatedly, return number of rows affected.
func (s *Stmt) Exec(args ...interface{}) (int64, error) {
	return s.ExecContext(context.Background(), args...)
}

// TakeAllContext -> Query prepared and get all query results, that can be called repeatedly.
func (s *Stmt) TakeAllContext(ctx context.Context, result interface{}, args ...interface{}) error {
	return s.QueryContext(ctx, func(rows *sql.Rows) error { return s.way.scan(rows, result, s.way.tag) }, args...)
}

// TakeAll -> Query prepared and get all query results, that can be called repeatedly.
func (s *Stmt) TakeAll(result interface{}, args ...interface{}) error {
	return s.TakeAllContext(context.Background(), result, args...)
}

// PrepareContext -> Prepare sql statement, don't forget to call *Stmt.Close().
func (s *Way) PrepareContext(ctx context.Context, prepare string, caller ...Caller) (*Stmt, error) {
	stmt := getStmt()
	stmt.log = &LogSQL{}
	stmt.log.TxId, stmt.log.TxMsg = s.txId, s.txMsg
	defer func() {
		if stmt.stmt == nil {
			putStmt(stmt) // called when stmt.caller.PrepareContext() failed.
		}
	}()
	stmt.way = s
	stmt.caller = s.caller(caller...)
	stmt.prepare = prepare
	if s.fix != nil {
		stmt.prepare = s.fix(prepare)
	}
	stmt.log.Prepare = stmt.prepare
	tmp, err := stmt.caller.PrepareContext(ctx, stmt.prepare)
	if err != nil {
		stmt.log.Error = err
		s.logger(stmt.log)
		return nil, err
	}
	stmt.stmt = tmp
	return stmt, nil
}

// Prepare -> Prepare sql statement, don't forget to call *Stmt.Close().
func (s *Way) Prepare(prepare string, caller ...Caller) (*Stmt, error) {
	return s.PrepareContext(context.Background(), prepare, caller...)
}

// QueryContext -> Execute the query sql statement.
func (s *Way) QueryContext(ctx context.Context, query func(rows *sql.Rows) error, prepare string, args ...interface{}) error {
	stmt, err := s.PrepareContext(ctx, prepare)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	return stmt.QueryContext(ctx, query, args...)
}

// Query -> Execute the query sql statement.
func (s *Way) Query(query func(rows *sql.Rows) error, prepare string, args ...interface{}) error {
	return s.QueryContext(context.Background(), query, prepare, args...)
}

// QueryRowContext -> Execute sql statement and return a row data, usually INSERT, UPDATE, DELETE.
func (s *Way) QueryRowContext(ctx context.Context, query func(row *sql.Row) error, prepare string, args ...interface{}) error {
	stmt, err := s.PrepareContext(ctx, prepare)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	return stmt.QueryRowContext(ctx, query, args...)
}

// QueryRow -> Execute sql statement and return a row data, usually INSERT, UPDATE, DELETE.
func (s *Way) QueryRow(query func(row *sql.Row) error, prepare string, args ...interface{}) error {
	return s.QueryRowContext(context.Background(), query, prepare, args...)
}

// TakeAllContext -> Query prepared and get all query results, through the mapping of column names and struct tags.
func (s *Way) TakeAllContext(ctx context.Context, result interface{}, prepare string, args ...interface{}) error {
	return s.QueryContext(ctx, func(rows *sql.Rows) error { return s.scan(rows, result, s.tag) }, prepare, args...)
}

// TakeAll -> Query prepared and get all query results.
func (s *Way) TakeAll(result interface{}, prepare string, args ...interface{}) error {
	return s.TakeAllContext(context.Background(), result, prepare, args...)
}

// ExecuteContext -> Execute the execute sql statement.
func (s *Way) ExecuteContext(ctx context.Context, prepare string, args ...interface{}) (sql.Result, error) {
	stmt, err := s.PrepareContext(ctx, prepare)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stmt.Close() }()
	return stmt.ExecuteContext(ctx, args...)
}

// Execute -> Execute the execute sql statement.
func (s *Way) Execute(prepare string, args ...interface{}) (sql.Result, error) {
	return s.ExecuteContext(context.Background(), prepare, args...)
}

// ExecContext -> Execute the execute sql statement.
func (s *Way) ExecContext(ctx context.Context, prepare string, args ...interface{}) (int64, error) {
	stmt, err := s.PrepareContext(ctx, prepare)
	if err != nil {
		return 0, err
	}
	defer func() { _ = stmt.Close() }()
	return stmt.ExecContext(ctx, args...)
}

// Exec -> Execute the execute sql statement.
func (s *Way) Exec(prepare string, args ...interface{}) (int64, error) {
	return s.ExecContext(context.Background(), prepare, args...)
}

// getter -> Query, execute the query sql statement without args, no prepared is used.
func (s *Way) getter(ctx context.Context, query func(rows *sql.Rows) error, prepare string, caller ...Caller) error {
	if query == nil || prepare == EmptyString {
		return nil
	}
	log := &LogSQL{
		TxId:    s.txId,
		TxMsg:   s.txMsg,
		Prepare: prepare,
		Args:    &LogArgs{},
	}
	defer s.logger(log)
	log.Args.StartAt = time.Now()
	rows, err := s.caller(caller...).QueryContext(ctx, prepare)
	log.Args.EndAt = time.Now()
	if err != nil {
		log.Error = err
		return log.Error
	}
	defer func() { _ = rows.Close() }()
	log.Error = query(rows)
	return log.Error
}

// setter -> Execute, execute the execute sql statement without args, no prepared is used.
func (s *Way) setter(ctx context.Context, prepare string, caller ...Caller) (rowsAffected int64, err error) {
	if prepare == "" {
		return
	}
	log := &LogSQL{
		TxId:    s.txId,
		TxMsg:   s.txMsg,
		Prepare: prepare,
		Args:    &LogArgs{},
	}
	defer s.logger(log)
	log.Args.StartAt = time.Now()
	result, rer := s.caller(caller...).ExecContext(ctx, prepare)
	log.Args.EndAt = time.Now()
	if rer != nil {
		log.Error, err = rer, rer
		return
	}
	rowsAffected, err = result.RowsAffected()
	log.Error = err
	return
}

// GetterContext -> Execute the query sql statement without args, no prepared is used.
func (s *Way) GetterContext(ctx context.Context, query func(rows *sql.Rows) error, prepare string, caller ...Caller) (err error) {
	return s.getter(ctx, query, prepare, caller...)
}

// Getter -> Execute the query sql statement without args, no prepared is used.
func (s *Way) Getter(query func(rows *sql.Rows) error, prepare string, caller ...Caller) error {
	return s.GetterContext(context.Background(), query, prepare, caller...)
}

// SetterContext -> Execute the execute sql statement without args, no prepared is used.
func (s *Way) SetterContext(ctx context.Context, prepare string, caller ...Caller) (int64, error) {
	return s.setter(ctx, prepare, caller...)
}

// Setter -> Execute the execute sql statement without args, no prepared is used.
func (s *Way) Setter(prepare string, caller ...Caller) (int64, error) {
	return s.SetterContext(context.Background(), prepare, caller...)
}

// F -> Quickly initialize a filter.
func (s *Way) F(filter ...Filter) Filter {
	return F().Filter(filter...)
}

// Add -> Create an instance that executes the INSERT sql statement.
func (s *Way) Add(table string) *Add {
	return NewAdd(s).Table(table)
}

// Del -> Create an instance that executes the DELETE sql statement.
func (s *Way) Del(table string) *Del {
	return NewDel(s).Table(table)
}

// Mod -> Create an instance that executes the UPDATE sql statement.
func (s *Way) Mod(table string) *Mod {
	return NewMod(s).Table(table)
}

// Get -> Create an instance that executes the SELECT sql statement.
func (s *Way) Get(table ...string) *Get {
	return NewGet(s).Table(LastNotEmptyString(table))
}

// Ident -> SQL identifier.
func (s *Way) Ident(prefix ...string) *Ident {
	return &Ident{
		prefix: LastNotEmptyString(prefix),
	}
}

// AliasA SQL identifier prefix a.
func (s *Way) AliasA() *Ident {
	return s.Ident(AliasA)
}

// AliasB SQL identifier prefix b.
func (s *Way) AliasB() *Ident {
	return s.Ident(AliasB)
}

// AliasC SQL identifier prefix c.
func (s *Way) AliasC() *Ident {
	return s.Ident(AliasC)
}

// AliasD SQL identifier prefix d.
func (s *Way) AliasD() *Ident {
	return s.Ident(AliasD)
}

// AliasE SQL identifier prefix e.
func (s *Way) AliasE() *Ident {
	return s.Ident(AliasE)
}

// AliasF SQL identifier prefix f.
func (s *Way) AliasF() *Ident {
	return s.Ident(AliasF)
}

// AliasG SQL identifier prefix g.
func (s *Way) AliasG() *Ident {
	return s.Ident(AliasG)
}

// read Implement Reader.
type read struct {
	// reads Read list.
	reads []*Way

	// total Length of read list.
	total int

	// choose Gets a read-only object from the read list.
	choose func(n int) int
}

// Read Get an instance for querying.
func (s *read) Read() *Way {
	return s.reads[s.choose(s.total)]
}

// NewReader It is recommended that objects used for writing should not appear in reads.
func NewReader(choose func(n int) int, reads []*Way) Reader {
	if choose == nil {
		panic("hey: empty value of `choose`")
	}
	length := len(reads)
	if length == 0 {
		panic("hey: empty value of `reads`")
	}
	return &read{
		reads:  reads,
		total:  length,
		choose: choose,
	}
}

// ScanAll Iteratively scan from query results.
func ScanAll(rows *sql.Rows, fc func(rows *sql.Rows) error) (err error) {
	for rows.Next() {
		if err = fc(rows); err != nil {
			return
		}
	}
	return
}

// ScanOne Scan at most once from the query results.
func ScanOne(rows *sql.Rows, dest ...interface{}) error {
	if rows.Next() {
		return rows.Scan(dest...)
	}
	return nil
}

// tryFloat64 string or []byte to float64.
func tryFloat64(value interface{}) interface{} {
	if value == nil {
		return nil
	}
	switch val := value.(type) {
	case []byte:
		if f64, err := strconv.ParseFloat(string(val), 64); err == nil {
			return f64
		}
	case string:
		if f64, err := strconv.ParseFloat(val, 64); err == nil {
			return f64
		}
	}
	return value
}

// tryString []byte to string.
func tryString(value interface{}) interface{} {
	if value == nil {
		return nil
	}
	switch val := value.(type) {
	case []byte:
		return string(val)
	}
	return value
}

// adjustViewData Try to convert the text data type to a specific type that matches it.
func adjustViewData(columnType *sql.ColumnType) func(value interface{}) interface{} {
	databaseTypeName := columnType.DatabaseTypeName()
	databaseTypeNameUpper := strings.ToUpper(databaseTypeName)
	switch databaseTypeNameUpper {
	case "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL", "DOUBLE PRECISION", "NUMBER":
		return tryFloat64
	case "CHAR", "VARCHAR", "TEXT",
		"CHARACTER", "CHARACTER VARYING", "BPCHAR",
		"NCHAR", "NVARCHAR",
		"TINYTEXT", "MEDIUMTEXT", "LARGETEXT", "LONGTEXT",
		"TIMESTAMP", "DATE", "TIME", "DATETIME",
		"JSON":
		return tryString
	case "BYTEA",
		"BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":
		return nil
	}
	return nil
}

// ScanViewMap Scan query result to []map[string]interface{}, view query result.
func ScanViewMap(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	count := len(columns)
	var slices []map[string]interface{}
	for rows.Next() {
		tmp := make(map[string]interface{})
		scan := make([]interface{}, count)
		for i := range scan {
			scan[i] = new(interface{})
		}
		if err = rows.Scan(scan...); err != nil {
			return nil, err
		}
		for i, column := range columns {
			value := scan[i].(*interface{})
			tmp[column] = *value
		}
		slices = append(slices, tmp)
	}
	fcs := make(map[string]func(interface{}) interface{})
	for _, v := range types {
		if tmp := adjustViewData(v); tmp != nil {
			fcs[v.Name()] = tmp
		}
	}
	for column, call := range fcs {
		for index, temp := range slices {
			slices[index][column] = call(temp[column])
		}
	}
	return slices, nil
}

// ArgString Convert SQL statement parameters into text strings.
func ArgString(i interface{}) string {
	if i == nil {
		return SqlNull
	}
	t, v := reflect.TypeOf(i), reflect.ValueOf(i)
	k := t.Kind()
	for k == reflect.Ptr {
		if v.IsNil() {
			return SqlNull
		}
		t, v = t.Elem(), v.Elem()
		k = t.Kind()
	}
	// any base type to string
	tmp := v.Interface()
	switch k {
	case reflect.Bool:
		return fmt.Sprintf("%t", tmp)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return fmt.Sprintf("%d", tmp)
	case reflect.Float32, reflect.Float64:
		return fmt.Sprintf("%f", tmp)
	case reflect.String:
		return fmt.Sprintf("'%s'", tmp)
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return fmt.Sprintf("'%s'", tmp)
		}
	default:
	}
	return fmt.Sprintf("'%v'", tmp)
}

// PrepareString Merge executed SQL statements and parameters.
func PrepareString(prepare string, args []interface{}) string {
	count := len(args)
	if count == 0 {
		return prepare
	}
	index := 0
	origin := []byte(prepare)
	latest := getBuilder()
	defer putBuilder(latest)
	length := len(origin)
	byte63 := SqlPlaceholder[0]
	for i := 0; i < length; i++ {
		if origin[i] == byte63 && index < count {
			latest.WriteString(ArgString(args[index]))
			index++
		} else {
			latest.WriteByte(origin[i])
		}
	}
	return latest.String()
}
