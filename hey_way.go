// About *Way

package hey

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/cd365/hey/v6/cst"

	"github.com/cd365/logger/v9"
)

// hexEncodeToString Convert binary byte array to hexadecimal string.
func hexEncodeToString(values []byte) string {
	return hex.EncodeToString(values)
}

func binaryByteSliceToString(args []any) []any {
	for index, value := range args {
		if tmp, ok := value.([]byte); ok && tmp != nil {
			args[index] = hexEncodeToString(tmp)
		}
	}
	return args
}

// argValueToString Convert SQL parameter value to string.
func argValueToString(i any) string {
	if i == nil {
		return cst.NULL
	}
	t, v := reflect.TypeOf(i), reflect.ValueOf(i)
	k := t.Kind()
	for k == reflect.Pointer {
		if v.IsNil() {
			return cst.NULL
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
				return cst.NULL
			}
			return hexEncodeToString(bts)
		}
		return fmt.Sprintf("'%v'", tmp)
	}
}

// SQLToString Use parameter values to replace placeholders in SQL statements and build a visual SQL script.
// Warning: Binary byte slice will be converted to hexadecimal strings.
func SQLToString(script *SQL) string {
	if script == nil {
		return cst.Empty
	}
	counts := len(script.Args)
	if counts == 0 {
		return script.Prepare
	}
	index := 0
	origin := []byte(script.Prepare)
	result := poolGetStringBuilder()
	defer poolPutStringBuilder(result)
	length := len(origin)
	c63 := cst.Placeholder[0]
	for i := range length {
		if origin[i] == c63 && index < counts {
			result.WriteString(argValueToString(script.Args[index]))
			index++
		} else {
			result.WriteByte(origin[i])
		}
	}
	return result.String()
}

// DebugMaker Output SQL script to the specified output stream.
// Warning: Binary byte slice will be converted to hexadecimal strings.
type DebugMaker interface {
	// Debug Output SQL script to the specified output stream.
	// Warning: Binary byte slice will be converted to hexadecimal strings.
	Debug(maker Maker) DebugMaker

	GetLogger() *logger.Logger

	SetLogger(log *logger.Logger) DebugMaker
}

type debugMaker struct {
	log *logger.Logger
}

func NewDebugMaker() DebugMaker { return &debugMaker{} }

func (s *debugMaker) GetLogger() *logger.Logger {
	return s.log
}

func (s *debugMaker) SetLogger(log *logger.Logger) DebugMaker {
	s.log = log
	return s
}

func (s *debugMaker) Debug(maker Maker) DebugMaker {
	if s.log == nil {
		return s
	}
	script := maker.ToSQL()
	s.log.Debug().
		Str(cst.LogScript, SQLToString(script)).
		Str(cst.LogPrepare, script.Prepare).
		Any(cst.LogArgs, binaryByteSliceToString(script.Args)).
		Msg("debugger SQL script")
	return s
}

// transaction Information for transaction.
type transaction struct {
	startAt time.Time
	endAt   time.Time

	ctx context.Context

	err error

	way *Way

	tx *sql.Tx

	id      string
	message string

	state string

	sqlLog []*sqlLog
}

// start Logging when starting a transaction.
func (s *transaction) start() {
	if s.way.log == nil {
		return
	}
	lg := s.way.log.Info()
	lg.Str(cst.LogId, s.id)
	lg.Int64(cst.LogStartAt, s.startAt.UnixMilli())
	lg.Str(cst.LogState, cst.BEGIN)
	lg.Msg(cst.Empty)
}

// write Recording transaction logs.
func (s *transaction) write() {
	if s.way.log == nil {
		return
	}
	if s.endAt.IsZero() {
		s.endAt = time.Now()
	}
	for _, v := range s.sqlLog {
		lg := s.way.log.Info()
		if v.err != nil {
			lg = s.way.log.Error()
			lg.Str(cst.LogError, v.err.Error())
			lg.Str(cst.LogScript, SQLToString(NewSQL(v.prepare, v.args.args...)))
		} else {
			if v.args.endAt.Sub(v.args.startAt) > s.way.cfg.sqlWarnDuration {
				lg = s.way.log.Warn()
				lg.Str(cst.LogScript, SQLToString(NewSQL(v.prepare, v.args.args...)))
			}
		}
		lg.Str(cst.LogId, s.id)
		lg.Str(cst.LogMsg, s.message)
		lg.Str(cst.LogPrepare, v.prepare)
		lg.Any(cst.LogArgs, binaryByteSliceToString(v.args.args))
		lg.Int64(cst.LogStartAt, v.args.startAt.UnixMilli())
		lg.Int64(cst.LogEndAt, v.args.endAt.UnixMilli())
		lg.Str(cst.LogCost, v.args.endAt.Sub(v.args.startAt).String())
		lg.Msg(cst.Empty)
	}
	lg := s.way.log.Info()
	if s.err != nil {
		lg = s.way.log.Error()
		lg.Str(cst.LogError, s.err.Error())
	}
	lg.Str(cst.LogId, s.id)
	lg.Int64(cst.LogStartAt, s.startAt.UnixMilli())
	lg.Str(cst.LogState, s.state)
	lg.Int64(cst.LogEndAt, s.endAt.UnixMilli())
	lg.Str(cst.LogCost, s.endAt.Sub(s.startAt).String())
	lg.Msg(cst.Empty)
}

// Manual For handling different types of databases.
type Manual struct {
	// Prepare to adjust the SQL statement format to meet the current database SQL statement format.
	Prepare func(prepare string) string

	// Replacer SQL Identifier Replacer.
	Replacer Replacer

	// More custom methods can be added here to achieve the same function using different databases.
}

func Mysql() *Manual {
	manual := &Manual{}
	return manual
}

func Sqlite() *Manual {
	manual := &Manual{}
	return manual
}

func prepare63236(prepare string) string {
	var index int64
	latest := poolGetStringBuilder()
	defer poolPutStringBuilder(latest)
	origin := []byte(prepare)
	length := len(origin)
	c36 := cst.Dollar[0]      // $
	c63 := cst.Placeholder[0] // ?
	for i := range length {
		if origin[i] == c63 {
			index++
			latest.WriteByte(c36)
			latest.WriteString(strconv.FormatInt(index, 10))
		} else {
			latest.WriteByte(origin[i])
		}
	}
	return latest.String()
}

func Postgresql() *Manual {
	manual := &Manual{}
	manual.Prepare = prepare63236
	return manual
}

type config struct {
	// debugMaker For debug output SQL script.
	debugMaker DebugMaker

	// mapScanner Custom MapScan, Cannot be set to nil.
	mapScanner MapScanner

	// manual For handling different types of databases.
	manual *Manual

	// scan For scanning data into structure, Cannot be set to nil.
	scan func(rows *sql.Rows, result any, tag string) error

	// txOptions Start transaction options.
	txOptions *sql.TxOptions

	// scanTag Scan data to tag mapping on structure.
	scanTag string

	// tableMethodName Custom method name to get table name.
	tableMethodName string

	// txMaxDuration Maximum transaction execution time.
	txMaxDuration time.Duration

	// sqlWarnDuration SQL execution time warning threshold.
	sqlWarnDuration time.Duration

	// deleteRequireWhere Deletion of data must be filtered using conditions.
	deleteRequireWhere bool

	// updateRequireWhere Updated data must be filtered using conditions.
	updateRequireWhere bool
}

const (
	DefaultTag      = "db"
	TableMethodName = "TableName"
)

func configDefault() *config {
	return &config{
		mapScanner:         NewMapScanner(),
		scan:               RowsScan,
		scanTag:            DefaultTag,
		tableMethodName:    TableMethodName,
		deleteRequireWhere: true,
		updateRequireWhere: true,
		txMaxDuration:      time.Second * 5,
		sqlWarnDuration:    time.Millisecond * 200,
	}
}

type Option func(way *Way)

func WithDebugMaker(debugMaker DebugMaker) Option {
	return func(way *Way) {
		way.cfg.debugMaker = debugMaker
	}
}

func WithMapScanner(mapScanner MapScanner) Option {
	return func(way *Way) {
		if mapScanner != nil {
			way.cfg.mapScanner = mapScanner
		}
	}
}

func WithManual(manual *Manual) Option {
	return func(way *Way) {
		way.cfg.manual = manual
	}
}

func WithScan(scan func(rows *sql.Rows, result any, tag string) error) Option {
	return func(way *Way) {
		if scan != nil {
			way.cfg.scan = scan
		}
	}
}

func WithTxOptions(txOptions *sql.TxOptions) Option {
	return func(way *Way) {
		way.cfg.txOptions = txOptions
	}
}

func WithScanTag(scanTag string) Option {
	return func(way *Way) {
		way.cfg.scanTag = scanTag
	}
}

func WithTableMethodName(tableMethodName string) Option {
	return func(way *Way) {
		way.cfg.tableMethodName = tableMethodName
	}
}

func WithTxMaxDuration(txMaxDuration time.Duration) Option {
	return func(way *Way) {
		if txMaxDuration > 0 {
			way.cfg.txMaxDuration = txMaxDuration
		}
	}
}

func WithSqlWarnDuration(sqlWarnDuration time.Duration) Option {
	return func(way *Way) {
		if sqlWarnDuration > 0 {
			way.cfg.sqlWarnDuration = sqlWarnDuration
		}
	}
}

func WithDeleteRequireWhere(deleteRequireWhere bool) Option {
	return func(way *Way) {
		way.cfg.deleteRequireWhere = deleteRequireWhere
	}
}

func WithUpdateRequireWhere(updateRequireWhere bool) Option {
	return func(way *Way) {
		way.cfg.updateRequireWhere = updateRequireWhere
	}
}

func WithDatabase(db *sql.DB) Option {
	return func(way *Way) {
		way.db = db
	}
}

func WithLogger(log *logger.Logger) Option {
	return func(way *Way) {
		way.log = log
	}
}

func WithReader(reader Reader) Option {
	return func(way *Way) {
		way.reader = reader
	}
}

// sqlLog Record executed prepare args.
type sqlLog struct {
	err error // An error encountered when executing SQL.

	way *Way

	args *sqlLogRun // SQL parameter list.

	prepare string // Preprocess the SQL statements that are executed.
}

// sqlLogRun Record executed args of prepare.
type sqlLogRun struct {
	startAt time.Time // The start time of the SQL statement.

	endAt time.Time // The end time of the SQL statement.

	args []any // SQL parameter list.
}

func (s *Way) sqlLog(prepare string, args []any) *sqlLog {
	return &sqlLog{
		way:     s,
		prepare: prepare,
		args: &sqlLogRun{
			startAt: time.Now(),
			args:    args,
		},
	}
}

func (s *sqlLog) Write() {
	if s.way.log == nil {
		return
	}
	if s.args.endAt.IsZero() {
		s.args.endAt = time.Now()
	}
	if s.way.IsInTransaction() {
		s.way.transaction.sqlLog = append(s.way.transaction.sqlLog, s)
		return
	}
	lg := s.way.log.Info()
	if s.err != nil {
		lg = s.way.log.Error()
		lg.Str(cst.LogError, s.err.Error())
		lg.Str(cst.LogScript, SQLToString(NewSQL(s.prepare, s.args.args...)))
	} else {
		if s.args.endAt.Sub(s.args.startAt) > s.way.cfg.sqlWarnDuration {
			lg = s.way.log.Warn()
			lg.Str(cst.LogScript, SQLToString(NewSQL(s.prepare, s.args.args...)))
		}
	}
	lg.Str(cst.LogPrepare, s.prepare)
	lg.Any(cst.LogArgs, binaryByteSliceToString(s.args.args))
	lg.Int64(cst.LogStartAt, s.args.startAt.UnixMilli())
	lg.Int64(cst.LogEndAt, s.args.endAt.UnixMilli())
	lg.Str(cst.LogCost, s.args.endAt.Sub(s.args.startAt).String())
	lg.Msg(cst.Empty)
}

// Reader Separate read and write, when you distinguish between reading and writing, please do not use the same object for both reading and writing.
type Reader interface {
	// Read Get an object for read.
	Read() *Way
}

type Way struct {
	cfg *config

	db *sql.DB

	log *logger.Logger

	transaction *transaction

	reader Reader

	isRead bool
}

func NewWay(options ...Option) *Way {
	way := &Way{}
	way.cfg = configDefault()
	for _, option := range options {
		option(way)
	}
	return way
}

func (s *Way) Database() *sql.DB {
	return s.db
}

func (s *Way) Logger() *logger.Logger {
	return s.log
}

func (s *Way) Reader() Reader {
	return s.reader
}

func (s *Way) Read() *Way {
	if s.reader == nil {
		return s
	}
	result := s.reader.Read()
	result.isRead = true
	return result
}

// IsRead -> Is an object for read?
func (s *Way) IsRead() bool {
	return s.isRead
}

// Replace Get a single identifier mapping value, if it does not exist, return the original value.
func (s *Way) Replace(key string) string {
	manual := s.cfg.manual
	if manual == nil {
		return key
	}
	if manual.Replacer == nil {
		return key
	}
	return manual.Replacer.Get(key)
}

// ReplaceAll Get multiple identifier mapping values, return the original value if none exists.
func (s *Way) ReplaceAll(keys []string) []string {
	manual := s.cfg.manual
	if manual == nil {
		return keys
	}
	if manual.Replacer == nil {
		return keys
	}
	return manual.Replacer.GetAll(keys)
}

// begin -> Open transaction.
func (s *Way) begin(ctx context.Context, conn *sql.Conn, opts ...*sql.TxOptions) (tx *Way, err error) {
	if s.db == nil {
		return nil, ErrDatabaseIsNil
	}

	tmp := *s
	tx = &tmp

	opt := tx.cfg.txOptions
	length := len(opts)
	for i := length - 1; i >= 0; i-- {
		if opts[i] != nil {
			opt = opts[i]
			break
		}
	}

	tx.transaction = &transaction{
		ctx: ctx,
		way: tx,
	}
	if conn != nil {
		tx.transaction.tx, err = conn.BeginTx(ctx, opt)
	} else {
		tx.transaction.tx, err = tx.db.BeginTx(ctx, opt)
	}
	if err != nil {
		tx = nil
		return tx, err
	}
	tx.transaction.startAt = time.Now()
	tx.transaction.id = fmt.Sprintf("%d%s%d%s%p", tx.transaction.startAt.UnixNano(), cst.Point, os.Getpid(), cst.Point, tx.transaction)
	tx.transaction.start()
	return tx, err
}

// commit -> Commit transaction.
func (s *Way) commit() (err error) {
	if s.transaction == nil {
		return ErrTransactionIsNil
	}
	tx := s.transaction
	tx.state = cst.COMMIT
	defer tx.write()
	tx.err = tx.tx.Commit()
	s.transaction, err = nil, tx.err
	return err
}

// rollback -> Rollback transaction.
func (s *Way) rollback() (err error) {
	if s.transaction == nil {
		return ErrTransactionIsNil
	}
	tx := s.transaction
	tx.state = cst.ROLLBACK
	defer tx.write()
	tx.err = tx.tx.Rollback()
	s.transaction, err = nil, tx.err
	return err
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

// IsInTransaction -> Is the transaction currently in progress?
func (s *Way) IsInTransaction() bool {
	return s.transaction != nil
}

// TransactionMessage -> Set the prompt for the current transaction, can only be set once.
func (s *Way) TransactionMessage(message string) *Way {
	if s.transaction == nil {
		return s
	}
	if s.transaction.message == cst.Empty {
		s.transaction.message = message
	}
	return s
}

// newTransaction -> Start a new transaction and execute a set of SQL statements atomically.
func (s *Way) newTransaction(ctx context.Context, fc func(tx *Way) error, opts ...*sql.TxOptions) (err error) {
	if ctx == nil {
		timeout := time.Second * 8
		if s.cfg != nil && s.cfg.txMaxDuration > 0 {
			timeout = s.cfg.txMaxDuration
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		defer cancel()
	}

	tx := (*Way)(nil)
	tx, err = s.begin(ctx, nil, opts...)
	if err != nil {
		return err
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
		return err
	}

	ok = true

	return err
}

// Transaction -> Atomically executes a set of SQL statements. If a transaction has been opened, the opened transaction instance will be used.
func (s *Way) Transaction(ctx context.Context, fc func(tx *Way) error, opts ...*sql.TxOptions) error {
	if s.IsInTransaction() {
		return fc(s)
	}
	return s.newTransaction(ctx, fc, opts...)
}

// TransactionNew -> Starts a new transaction and executes a set of SQL statements atomically. Does not care whether the current transaction instance is open.
func (s *Way) TransactionNew(ctx context.Context, fc func(tx *Way) error, opts ...*sql.TxOptions) error {
	return s.newTransaction(ctx, fc, opts...)
}

// TransactionRetry Starts a new transaction and executes a set of SQL statements atomically. Does not care whether the current transaction instance is open.
func (s *Way) TransactionRetry(ctx context.Context, retries int, fc func(tx *Way) error, opts ...*sql.TxOptions) (err error) {
	for range retries {
		if err = s.newTransaction(ctx, fc, opts...); err == nil {
			break
		}
	}
	return err
}

// Now -> Get current time, the transaction open status will get the same time.
func (s *Way) Now() time.Time {
	if s.IsInTransaction() {
		return s.transaction.startAt
	}
	return time.Now()
}

// Stmt Prepare a handle.
type Stmt struct {
	way     *Way
	stmt    *sql.Stmt
	prepare string
}

// Close -> Close prepare a handle.
func (s *Stmt) Close() (err error) {
	if s.stmt != nil {
		err = s.stmt.Close()
	}
	return err
}

// Query -> Query prepared, that can be called repeatedly.
func (s *Stmt) Query(ctx context.Context, query func(rows *sql.Rows) error, args ...any) error {
	lg := s.way.sqlLog(s.prepare, args)
	defer lg.Write()
	rows, err := s.stmt.QueryContext(ctx, args...)
	lg.args.endAt = time.Now()
	if err != nil {
		lg.err = err
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
	lg.err = query(rows)
	return lg.err
}

// QueryRow -> Query prepared, that can be called repeatedly.
func (s *Stmt) QueryRow(ctx context.Context, query func(row *sql.Row) error, args ...any) error {
	lg := s.way.sqlLog(s.prepare, args)
	defer lg.Write()
	row := s.stmt.QueryRowContext(ctx, args...)
	lg.args.endAt = time.Now()
	lg.err = query(row)
	return lg.err
}

// Exec -> Execute prepared, that can be called repeatedly.
func (s *Stmt) Exec(ctx context.Context, args ...any) (sql.Result, error) {
	lg := s.way.sqlLog(s.prepare, args)
	defer lg.Write()
	result, err := s.stmt.ExecContext(ctx, args...)
	lg.args.endAt = time.Now()
	lg.err = err
	return result, err
}

// Execute -> Execute prepared, that can be called repeatedly, return number of rows affected.
func (s *Stmt) Execute(ctx context.Context, args ...any) (int64, error) {
	result, err := s.Exec(ctx, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// Scan -> Query prepared and get all query results, that can be called repeatedly.
func (s *Stmt) Scan(ctx context.Context, result any, args ...any) error {
	return s.Query(ctx, func(rows *sql.Rows) error {
		return s.way.cfg.scan(rows, result, s.way.cfg.scanTag)
	}, args...)
}

// Prepare -> Prepare SQL statement, remember to call *Stmt.Close().
func (s *Way) Prepare(ctx context.Context, prepare string) (stmt *Stmt, err error) {
	if s.db == nil {
		return nil, ErrDatabaseIsNil
	}

	stmt = &Stmt{
		way:     s,
		prepare: prepare,
	}
	if manual := s.cfg.manual; manual != nil && manual.Prepare != nil {
		stmt.prepare = manual.Prepare(prepare)
	}
	if s.IsInTransaction() {
		stmt.stmt, err = s.transaction.tx.PrepareContext(ctx, stmt.prepare)
	} else {
		stmt.stmt, err = s.db.PrepareContext(ctx, stmt.prepare)
	}
	if err != nil {
		if s.log != nil {
			s.log.Error().Str(cst.LogPrepare, stmt.prepare).Msg(err.Error())
		}
		return nil, err
	} else {
		if stmt.prepare == cst.Empty && s.log != nil {
			s.log.Warn().Str(cst.LogPrepare, stmt.prepare).Msg("prepare value is an empty string")
		}
	}
	return stmt, nil
}

// Query -> Execute the query sql statement.
func (s *Way) Query(ctx context.Context, maker Maker, query func(rows *sql.Rows) error) error {
	script := maker.ToSQL()
	if script.IsEmpty() {
		return ErrEmptyScript
	}
	stmt, err := s.Prepare(ctx, script.Prepare)
	if err != nil {
		return err
	}
	defer func() {
		_ = stmt.Close()
	}()
	return stmt.Query(ctx, query, script.Args...)
}

// QueryRow -> Execute SQL statement and return row data, usually INSERT, UPDATE, DELETE.
func (s *Way) QueryRow(ctx context.Context, maker Maker, query func(row *sql.Row) error) error {
	script := maker.ToSQL()
	if script.IsEmpty() {
		return ErrEmptyScript
	}
	stmt, err := s.Prepare(ctx, script.Prepare)
	if err != nil {
		return err
	}
	defer func() {
		_ = stmt.Close()
	}()
	return stmt.QueryRow(ctx, query, script.Args...)
}

// Scan -> Query prepared and get all query results, through the mapping of column names and struct tags.
func (s *Way) Scan(ctx context.Context, maker Maker, result any) error {
	return s.Query(ctx, maker, func(rows *sql.Rows) error { return s.cfg.scan(rows, result, s.cfg.scanTag) })
}

// MapScan -> Scanning the query results into []map[string]any.
func (s *Way) MapScan(ctx context.Context, maker Maker, adjusts ...AdjustColumnAnyValue) (result []map[string]any, err error) {
	err = s.Query(ctx, maker, func(rows *sql.Rows) error {
		result, err = s.cfg.mapScanner.Scan(rows, adjusts...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, err
}

// Exec -> Execute the execute sql statement.
func (s *Way) Exec(ctx context.Context, maker Maker) (sql.Result, error) {
	script := maker.ToSQL()
	if script.IsEmpty() {
		return nil, ErrEmptyScript
	}
	stmt, err := s.Prepare(ctx, script.Prepare)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = stmt.Close()
	}()
	return stmt.Exec(ctx, script.Args...)
}

// Execute -> Execute the execute sql statement.
func (s *Way) Execute(ctx context.Context, maker Maker) (int64, error) {
	script := maker.ToSQL()
	if script.IsEmpty() {
		return 0, ErrEmptyScript
	}
	stmt, err := s.Prepare(ctx, script.Prepare)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = stmt.Close()
	}()
	return stmt.Execute(ctx, script.Args...)
}

// MultiScan Execute multiple DQL statements.
func (s *Way) MultiScan(ctx context.Context, makers []Maker, results []any) (err error) {
	for index, maker := range makers {
		err = s.Scan(ctx, maker, results[index])
		if err != nil {
			break
		}
	}
	return err
}

// MultiExecute Execute multiple DML statements.
func (s *Way) MultiExecute(ctx context.Context, makers []Maker) (affectedRows int64, err error) {
	rows := int64(0)
	for _, maker := range makers {
		rows, err = s.Execute(ctx, maker)
		if err != nil {
			return affectedRows, err
		}
		affectedRows += rows
	}
	return affectedRows, nil
}

// MultiStmtScan Executing a DQL statement multiple times using the same prepared statement.
func (s *Way) MultiStmtScan(ctx context.Context, prepare string, lists [][]any, results []any) (err error) {
	if prepare == cst.Empty {
		return ErrEmptyScript
	}
	stmt := (*Stmt)(nil)
	stmt, err = s.Prepare(ctx, prepare)
	if err != nil {
		return err
	}
	defer func() {
		_ = stmt.Close()
	}()
	for index, value := range lists {
		err = stmt.Scan(ctx, results[index], value...)
		if err != nil {
			return err
		}
	}
	return nil
}

// MultiStmtExecute Executing a DML statement multiple times using the same prepared statement.
func (s *Way) MultiStmtExecute(ctx context.Context, prepare string, lists [][]any) (affectedRows int64, err error) {
	if prepare == cst.Empty {
		return 0, ErrEmptyScript
	}
	stmt := (*Stmt)(nil)
	stmt, err = s.Prepare(ctx, prepare)
	if err != nil {
		return affectedRows, err
	}
	defer func() {
		_ = stmt.Close()
	}()
	rows := int64(0)
	for _, args := range lists {
		rows, err = stmt.Execute(ctx, args...)
		if err != nil {
			return affectedRows, err
		}
		affectedRows += rows
	}
	return affectedRows, nil
}

// F -> Quickly initialize a filter.
func (s *Way) F(filters ...Filter) Filter {
	result := F().New(filters...)
	if manual := s.cfg.manual; manual != nil {
		result.SetReplacer(manual.Replacer)
	}
	return result
}

// V -> Prioritize the specified non-empty object, otherwise use the current object.
func (s *Way) V(values ...*Way) *Way {
	for i := len(values) - 1; i >= 0; i-- {
		if values[i] != nil {
			return values[i]
		}
	}
	return s
}

// Debug Debugging output SQL script.
func (s *Way) Debug(maker Maker) *Way {
	if s.cfg.debugMaker != nil {
		s.cfg.debugMaker.Debug(maker)
	}
	return s
}

// reader Implement Reader.
type reader struct {
	// choose Gets a read-only object from the read list.
	choose func(n int) int

	// reads Read list.
	reads []*Way

	// total Length of a read list.
	total int
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
	return &reader{
		reads:  reads,
		total:  length,
		choose: choose,
	}
}

// Read Get an instance for querying.
func (s *reader) Read() *Way {
	return s.reads[s.choose(s.total)]
}
