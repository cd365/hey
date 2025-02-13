// Package hey is a helper that quickly responds to the results of insert, delete, update, select sql statements.
// You can also use hey to quickly build sql statements.
package hey

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/cd365/hey/v3/logger"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	DriverNameMysql    = "mysql"
	DriverNamePostgres = "postgres"
	DriverNameSqlite3  = "sqlite3"
)

const (
	// DefaultTag Mapping of default database column name and struct tag.
	DefaultTag = "db"

	// EmptyString Empty string value.
	EmptyString = ""
)

const (
	SqlPoint = "."
	SqlSpace = " "
	SqlStar  = "*"

	SqlAs       = "AS"
	SqlAsc      = "ASC"
	SqlDesc     = "DESC"
	SqlUnion    = "UNION"
	SqlUnionAll = "UNION ALL"

	SqlJoinInner = "INNER JOIN"
	SqlJoinLeft  = "LEFT JOIN"
	SqlJoinRight = "RIGHT JOIN"
	SqlJoinFull  = "FULL JOIN"
	SqlJoinCross = "CROSS JOIN"

	SqlAnd = "AND"
	SqlOr  = "OR"

	SqlNot  = "NOT"
	SqlNull = "NULL"

	SqlPlaceholder      = "?"
	SqlEqual            = "="
	SqlNotEqual         = "<>"
	SqlGreaterThan      = ">"
	SqlGreaterThanEqual = ">="
	SqlLessThan         = "<"
	SqlLessThanEqual    = "<="

	SqlAll = "ALL"
	SqlAny = "ANY"

	SqlLeftSmallBracket  = "("
	SqlRightSmallBracket = ")"

	SqlExpect    = "EXCEPT"
	SqlIntersect = "INTERSECT"

	SqlDistinct = "DISTINCT"
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

const (
	DefaultAliasNameCount = "counts"
)

// Cfg Configure of Way.
type Cfg struct {
	// Scan Scan data into structure.
	Scan func(rows *sql.Rows, result interface{}, tag string) error

	// ScanTag Scan data to tag mapping on structure.
	ScanTag string

	// Helper Helpers for handling different types of databases.
	Helper Helper

	// Replace Helpers for handling different types of databases.
	Replace *Replace

	// DeleteMustUseWhere Deletion of data must be filtered using conditions.
	DeleteMustUseWhere bool

	// UpdateMustUseWhere Updated data must be filtered using conditions.
	UpdateMustUseWhere bool

	// TransactionOptions Start transaction.
	TransactionOptions *sql.TxOptions

	// TransactionMaxDuration Maximum transaction execution time.
	TransactionMaxDuration time.Duration

	// WarnDuration SQL execution time warning threshold.
	WarnDuration time.Duration
}

// DefaultCfg default configure value.
func DefaultCfg() Cfg {
	return Cfg{
		Scan:                   ScanSliceStruct,
		ScanTag:                DefaultTag,
		Replace:                NewReplace(),
		DeleteMustUseWhere:     true,
		UpdateMustUseWhere:     true,
		TransactionMaxDuration: time.Second * 5,
		WarnDuration:           time.Millisecond * 200,
	}
}

// CmdLog Record executed prepare args.
type CmdLog struct {
	way *Way

	// prepare Preprocess the SQL statements that are executed.
	prepare string

	// args SQL parameter list.
	args *CmdLogRun

	// err An error encountered when executing SQL.
	err error
}

// CmdLogRun Record executed args of prepare.
type CmdLogRun struct {
	// args SQL parameter list.
	args []interface{}

	// startAt The start time of the SQL statement.
	startAt time.Time

	// endAt The end time of the SQL statement.
	endAt time.Time
}

func (s *Way) CmdLog(prepare string, args []interface{}) *CmdLog {
	return &CmdLog{
		way:     s,
		prepare: prepare,
		args: &CmdLogRun{
			startAt: time.Now(),
			args:    args,
		},
	}
}

func (s *CmdLog) Write() {
	if s.way.log == nil {
		return
	}
	if s.args.endAt.IsZero() {
		s.args.endAt = time.Now()
	}
	if s.way.transaction != nil {
		s.way.transaction.logCmd = append(s.way.transaction.logCmd, s)
		return
	}
	lg := s.way.log.Info()
	if s.err != nil {
		lg = s.way.log.Error()
		lg.Str("error", s.err.Error())
		lg.Str("script", PrepareString(s.way.cfg.Helper, s.prepare, s.args.args))
	} else {
		if s.args.endAt.Sub(s.args.startAt) > s.way.cfg.WarnDuration {
			lg = s.way.log.Warn()
			lg.Str("script", PrepareString(s.way.cfg.Helper, s.prepare, s.args.args))
		}
	}
	lg.Str("prepare", s.prepare)
	lg.Any("args", s.args.args)
	lg.Int64("start_at", s.args.startAt.UnixMilli())
	lg.Int64("end_at", s.args.endAt.UnixMilli())
	lg.Str("cost", s.args.endAt.Sub(s.args.startAt).String())
	lg.Send()
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
	cfg *Cfg

	db *sql.DB

	log *logger.Logger

	transaction *transaction

	reader Reader

	isRead bool
}

func (s *Way) GetCfg() *Cfg {
	return s.cfg
}

func (s *Way) SetCfg(cfg Cfg) *Way {
	if cfg.Scan == nil || cfg.ScanTag == "" || cfg.Helper == nil || cfg.Replace == nil || cfg.TransactionMaxDuration <= 0 || cfg.WarnDuration <= 0 {
		return s
	}
	s.cfg = &cfg
	return s
}

func (s *Way) GetDatabase() *sql.DB {
	return s.db
}

func (s *Way) SetDatabase(db *sql.DB) *Way {
	s.db = db
	return s
}

func (s *Way) GetLogger() *logger.Logger {
	return s.log
}

func (s *Way) SetLogger(l *logger.Logger) *Way {
	s.log = l
	return s
}

func (s *Way) GetReader() Reader {
	return s.reader
}

func (s *Way) SetReader(reader Reader) *Way {
	s.reader = reader
	return s
}

func (s *Way) Read() *Way {
	if s.reader == nil {
		return s
	}
	readWay := s.reader.Read()
	readWay.isRead = true
	return readWay
}

// IsRead -> Is an object for read?
func (s *Way) IsRead() bool {
	return s.isRead
}

func NewWay(db *sql.DB) *Way {
	cfg := DefaultCfg()
	return &Way{
		db:  db,
		cfg: &cfg,
	}
}

// begin -> Open transaction.
func (s *Way) begin(ctx context.Context, conn *sql.Conn, opts ...*sql.TxOptions) (tx *Way, err error) {
	tmp := *s
	tx = &tmp

	opt := tx.cfg.TransactionOptions
	length := len(opts)
	for i := length - 1; i >= 0; i-- {
		if opts[i] != nil {
			opt = opts[i]
			break
		}
	}

	tx.transaction = &transaction{
		way: tx,
	}
	if conn != nil {
		tx.transaction.tx, err = conn.BeginTx(ctx, opt)
	} else {
		tx.transaction.tx, err = tx.db.BeginTx(ctx, opt)
	}
	if err != nil {
		tx = nil
		return
	}
	tx.transaction.startAt = time.Now()
	tx.transaction.id = fmt.Sprintf("%d%s%d%s%p", tx.transaction.startAt.UnixNano(), SqlPoint, os.Getpid(), SqlPoint, tx.transaction)
	tx.transaction.start()
	return
}

// commit -> Commit transaction.
func (s *Way) commit() (err error) {
	tx := s.transaction
	tx.state = "COMMIT"
	defer tx.write()
	tx.err = tx.tx.Commit()
	s.transaction, err = nil, tx.err
	return
}

// rollback -> Rollback transaction.
func (s *Way) rollback() (err error) {
	tx := s.transaction
	tx.state = "ROLLBACK"
	defer tx.write()
	tx.err = tx.tx.Rollback()
	s.transaction, err = nil, tx.err
	return
}

// Begin -> Open transaction.
func (s *Way) Begin(ctx context.Context, opts ...*sql.TxOptions) (*Way, error) {
	return s.begin(ctx, nil, opts...)
}

// BeginConn -> Open transaction using *sql.Conn.
func (s *Way) BeginConn(ctx context.Context, conn *sql.Conn, opts ...*sql.TxOptions) (*Way, error) {
	return s.begin(ctx, conn, opts...)
}

// Commit -> Transaction commit.
func (s *Way) Commit() error {
	return s.commit()
}

// Rollback -> Transaction rollback.
func (s *Way) Rollback() error {
	return s.rollback()
}

// TransactionIsNil -> Is the transaction object empty?
func (s *Way) TransactionIsNil() bool {
	return s.transaction == nil
}

// TransactionMessage -> Set the prompt for the current transaction, can only be set once.
func (s *Way) TransactionMessage(message string) *Way {
	if s.transaction == nil {
		return s
	}
	if s.transaction.message == EmptyString {
		s.transaction.message = message
	}
	return s
}

// newTransaction -> Start a new transaction and execute a set of SQL statements atomically.
func (s *Way) newTransaction(ctx context.Context, fc func(tx *Way) error, conn *sql.Conn, opts ...*sql.TxOptions) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}

	timeout := time.Second * 8
	if s.cfg != nil && s.cfg.TransactionMaxDuration > 0 {
		timeout = s.cfg.TransactionMaxDuration
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var tx *Way
	tx, err = s.begin(ctxTimeout, conn, opts...)
	if err != nil {
		return
	}

	ok := false

	defer func() {
		if err == nil && ok {
			err = tx.commit()
		} else {
			err = tx.rollback()
		}
	}()

	if err = fc(tx); err != nil {
		return
	}

	ok = true

	return
}

// Transaction -> Atomically executes a set of SQL statements. If a transaction has been opened, the opened transaction instance will be used.
func (s *Way) Transaction(ctx context.Context, fc func(tx *Way) error, opts ...*sql.TxOptions) error {
	if !s.TransactionIsNil() {
		return fc(s)
	}
	return s.newTransaction(ctx, fc, nil, opts...)
}

// TransactionNew -> Starts a new transaction and executes a set of SQL statements atomically. Does not care whether the current transaction instance is open.
func (s *Way) TransactionNew(ctx context.Context, fc func(tx *Way) error, opts ...*sql.TxOptions) error {
	return s.newTransaction(ctx, fc, nil, opts...)
}

// TransactionRetry Starts a new transaction and executes a set of SQL statements atomically. Does not care whether the current transaction instance is open.
func (s *Way) TransactionRetry(ctx context.Context, retries int, fc func(tx *Way) error, opts ...*sql.TxOptions) (err error) {
	for i := 0; i < retries; i++ {
		if err = s.newTransaction(ctx, fc, nil, opts...); err == nil {
			break
		}
	}
	return
}

// Now -> Get current time, the transaction open status will get the same time.
func (s *Way) Now() time.Time {
	if s.TransactionIsNil() {
		return time.Now()
	}
	return s.transaction.startAt
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
	if s.transaction != nil {
		return s.transaction.tx
	}
	return s.db
}

// Stmt Prepare handle.
type Stmt struct {
	way     *Way
	caller  Caller
	prepare string
	stmt    *sql.Stmt
}

// Close -> Close prepare handle.
func (s *Stmt) Close() (err error) {
	if s.stmt != nil {
		err = s.stmt.Close()
	}
	return
}

// QueryContext -> Query prepared, that can be called repeatedly.
func (s *Stmt) QueryContext(ctx context.Context, query func(rows *sql.Rows) error, args ...interface{}) error {
	lg := s.way.CmdLog(s.prepare, args)
	defer lg.Write()
	rows, err := s.stmt.QueryContext(ctx, args...)
	lg.args.endAt = time.Now()
	if err != nil {
		lg.err = err
		return err
	}
	defer func() { _ = rows.Close() }()
	lg.err = query(rows)
	return lg.err
}

// Query -> Query prepared, that can be called repeatedly.
func (s *Stmt) Query(query func(rows *sql.Rows) error, args ...interface{}) error {
	return s.QueryContext(context.Background(), query, args...)
}

// QueryRowContext -> Query prepared, that can be called repeatedly.
func (s *Stmt) QueryRowContext(ctx context.Context, query func(rows *sql.Row) error, args ...interface{}) error {
	lg := s.way.CmdLog(s.prepare, args)
	defer lg.Write()
	row := s.stmt.QueryRowContext(ctx, args...)
	lg.args.endAt = time.Now()
	lg.err = query(row)
	return lg.err
}

// QueryRow -> Query prepared, that can be called repeatedly.
func (s *Stmt) QueryRow(query func(rows *sql.Row) error, args ...interface{}) (err error) {
	return s.QueryRowContext(context.Background(), query, args...)
}

// ExecuteContext -> Execute prepared, that can be called repeatedly.
func (s *Stmt) ExecuteContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	lg := s.way.CmdLog(s.prepare, args)
	defer lg.Write()
	result, err := s.stmt.ExecContext(ctx, args...)
	lg.args.endAt = time.Now()
	lg.err = err
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
	return s.QueryContext(ctx, func(rows *sql.Rows) error { return s.way.cfg.Scan(rows, result, s.way.cfg.ScanTag) }, args...)
}

// TakeAll -> Query prepared and get all query results, that can be called repeatedly.
func (s *Stmt) TakeAll(result interface{}, args ...interface{}) error {
	return s.TakeAllContext(context.Background(), result, args...)
}

// PrepareContext -> Prepare sql statement, don't forget to call *Stmt.Close().
func (s *Way) PrepareContext(ctx context.Context, prepare string, caller ...Caller) (stmt *Stmt, err error) {
	stmt = &Stmt{
		way:     s,
		caller:  s.caller(caller...),
		prepare: s.cfg.Helper.Prepare(prepare),
	}
	stmt.stmt, err = stmt.caller.PrepareContext(ctx, stmt.prepare)
	if err != nil {
		return nil, err
	}
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
	return s.QueryContext(ctx, func(rows *sql.Rows) error { return s.cfg.Scan(rows, result, s.cfg.ScanTag) }, prepare, args...)
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

// getter -> Query, execute the query sql statement with args, no prepared is used.
func (s *Way) getter(ctx context.Context, caller Caller, query func(rows *sql.Rows) error, prepare string, args ...interface{}) error {
	if query == nil || prepare == EmptyString {
		return nil
	}
	lg := s.CmdLog(prepare, args)
	defer lg.Write()
	rows, err := s.caller(caller).QueryContext(ctx, prepare, args...)
	lg.args.endAt = time.Now()
	if err != nil {
		lg.err = err
		return err
	}
	defer func() { _ = rows.Close() }()
	err = query(rows)
	lg.err = err
	return err
}

// setter -> Execute, execute the execute sql statement with args, no prepared is used.
func (s *Way) setter(ctx context.Context, caller Caller, prepare string, args ...interface{}) (rowsAffected int64, err error) {
	if prepare == "" {
		return
	}
	lg := s.CmdLog(prepare, args)
	defer lg.Write()
	result, rer := s.caller(caller).ExecContext(ctx, prepare, args...)
	lg.args.endAt = time.Now()
	if rer != nil {
		lg.err, err = rer, rer
		return
	}
	rowsAffected, err = result.RowsAffected()
	lg.err = err
	return
}

// GetterContext -> Execute the query sql statement with args, no prepared is used.
func (s *Way) GetterContext(ctx context.Context, caller Caller, query func(rows *sql.Rows) error, prepare string, args ...interface{}) (err error) {
	return s.getter(ctx, caller, query, prepare, args...)
}

// Getter -> Execute the query sql statement with args, no prepared is used.
func (s *Way) Getter(caller Caller, query func(rows *sql.Rows) error, prepare string, args ...interface{}) error {
	return s.GetterContext(context.Background(), caller, query, prepare, args...)
}

// SetterContext -> Execute the execute sql statement with args, no prepared is used.
func (s *Way) SetterContext(ctx context.Context, caller Caller, prepare string, args ...interface{}) (int64, error) {
	return s.setter(ctx, caller, prepare, args...)
}

// Setter -> Execute the execute sql statement with args, no prepared is used.
func (s *Way) Setter(caller Caller, prepare string, args ...interface{}) (int64, error) {
	return s.SetterContext(context.Background(), caller, prepare, args...)
}

// F -> Quickly initialize a filter.
func (s *Way) F(fs ...Filter) Filter {
	return F().New(fs...).Way(s)
}

// Add -> Create an instance that executes the INSERT sql statement.
func (s *Way) Add(table string) *Add {
	return NewAdd(s).Table(s.NameReplace(table))
}

// Del -> Create an instance that executes the DELETE sql statement.
func (s *Way) Del(table string) *Del {
	return NewDel(s).Table(s.NameReplace(table))
}

// Mod -> Create an instance that executes the UPDATE sql statement.
func (s *Way) Mod(table string) *Mod {
	return NewMod(s).Table(s.NameReplace(table))
}

// Get -> Create an instance that executes the SELECT sql statement.
func (s *Way) Get(table ...string) *Get {
	return NewGet(s).Table(s.NameReplace(LastNotEmptyString(table)))
}

// T Table empty alias
func (s *Way) T() *AdjustColumn {
	return NewAdjustColumn(s)
}

// TA Table alias `a`
func (s *Way) TA() *AdjustColumn {
	return NewAdjustColumn(s, AliasA)
}

// TB Table alias `b`
func (s *Way) TB() *AdjustColumn {
	return NewAdjustColumn(s, AliasB)
}

// TC Table alias `c`
func (s *Way) TC() *AdjustColumn {
	return NewAdjustColumn(s, AliasC)
}

// TD Table alias `d`
func (s *Way) TD() *AdjustColumn {
	return NewAdjustColumn(s, AliasD)
}

// TE Table alias `e`
func (s *Way) TE() *AdjustColumn {
	return NewAdjustColumn(s, AliasE)
}

// TF Table alias `f`
func (s *Way) TF() *AdjustColumn {
	return NewAdjustColumn(s, AliasF)
}

// TG Table alias `g`
func (s *Way) TG() *AdjustColumn {
	return NewAdjustColumn(s, AliasG)
}

// NameReplace Replace name.
func (s *Way) NameReplace(name string) string {
	if s.cfg.Replace != nil {
		return s.cfg.Replace.Get(name)
	}
	return name
}

// NameReplaces Replace names.
func (s *Way) NameReplaces(names []string) []string {
	if s.cfg.Replace != nil {
		return s.cfg.Replace.GetAll(names)
	}
	return names
}

// WindowFunc New a window function object.
func (s *Way) WindowFunc(alias ...string) *WindowFunc {
	return NewWindowFunc(s, alias...)
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
		tmp := make(map[string]interface{}, 32)
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
	fcs := make(map[string]func(interface{}) interface{}, 32)
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
func ArgString(helper Helper, i interface{}) string {
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
	// any base type to string.
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
	default:
		if bts, ok := tmp.([]byte); ok {
			if bts == nil {
				return SqlNull
			}
			return helper.BinaryDataToHexString(bts)
		}
		return fmt.Sprintf("'%v'", tmp)
	}
}

// PrepareString Merge executed SQL statements and parameters.
func PrepareString(helper Helper, prepare string, args []interface{}) string {
	count := len(args)
	if count == 0 {
		return prepare
	}
	index := 0
	origin := []byte(prepare)
	latest := getStringBuilder()
	defer putStringBuilder(latest)
	length := len(origin)
	questionMark := byte('?')
	for i := 0; i < length; i++ {
		if origin[i] == questionMark && index < count {
			latest.WriteString(ArgString(helper, args[index]))
			index++
		} else {
			latest.WriteByte(origin[i])
		}
	}
	return latest.String()
}
