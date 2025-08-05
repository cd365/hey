// Package hey is a helper that quickly responds to the results of insert, delete, update, select SQL statements.
// You can also use hey to quickly build SQL statements.
package hey

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/cd365/logger/v9"
)

// Manual For handling different types of databases.
type Manual struct {
	// Prepare to adjust the SQL statement format to meet the current database SQL statement format.
	Prepare func(prepare string) string

	// Replace Helpers for handling different types of databases.
	Replace Replace

	// More custom methods can be added here to achieve the same function using different databases.
}

// NullDefaultValue Use defaultValue to replace NULL values.
func NullDefaultValue(prepare string, defaultValue string) string {
	return ConcatString(SqlCoalesce, SqlLeftSmallBracket, prepare, SqlConcat, defaultValue, SqlRightSmallBracket)
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
	latest := getStringBuilder()
	defer putStringBuilder(latest)
	origin := []byte(prepare)
	length := len(origin)
	dollar := byte('$')       // $
	questionMark := byte('?') // ?
	for i := range length {
		if origin[i] == questionMark {
			index++
			latest.WriteByte(dollar)
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

// Cfg Configure of Way.
type Cfg struct {
	// DeleteMustUseWhere Deletion of data must be filtered using conditions.
	DeleteMustUseWhere bool

	// UpdateMustUseWhere Updated data must be filtered using conditions.
	UpdateMustUseWhere bool

	// _ Memory alignment padding.
	_ [6]byte

	// Manual For handling different types of databases.
	Manual *Manual

	// Scan Scan data into structure.
	Scan func(rows *sql.Rows, result any, tag string) error

	// ScanTag Scan data to tag mapping on structure.
	ScanTag string

	// TransactionOptions Start transaction.
	TransactionOptions *sql.TxOptions

	// TransactionMaxDuration Maximum transaction execution time.
	TransactionMaxDuration time.Duration

	// WarnDuration SQL execution time warning threshold.
	WarnDuration time.Duration

	// Debugger Debug output SQL script.
	Debugger Debugger
}

// DefaultCfg default configure value.
func DefaultCfg() Cfg {
	return Cfg{
		Scan:                   RowsScan,
		ScanTag:                DefaultTag,
		DeleteMustUseWhere:     true,
		UpdateMustUseWhere:     true,
		TransactionMaxDuration: time.Second * 5,
		WarnDuration:           time.Millisecond * 200,
	}
}

// sqlLog Record executed prepare args.
type sqlLog struct {
	way *Way

	// prepare Preprocess the SQL statements that are executed.
	prepare string

	// args SQL parameter list.
	args *sqlLogRun

	// err An error encountered when executing SQL.
	err error
}

// sqlLogRun Record executed args of prepare.
type sqlLogRun struct {
	// args SQL parameter list.
	args []any

	// startAt The start time of the SQL statement.
	startAt time.Time

	// endAt The end time of the SQL statement.
	endAt time.Time
}

func (s *sqlLogRun) handleArgs() []any {
	return handleArgs(s.args)
}

// handleArgs Convert binary data to hexadecimal string.
func handleArgs(args []any) []any {
	for index, value := range args {
		if tmp, ok := value.([]byte); ok && tmp != nil {
			args[index] = hex.EncodeToString(tmp)
		}
	}
	return args
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
	if s.way.transaction != nil {
		s.way.transaction.sqlLog = append(s.way.transaction.sqlLog, s)
		return
	}
	lg := s.way.log.Info()
	if s.err != nil {
		lg = s.way.log.Error()
		lg.Str(logError, s.err.Error())
		lg.Str(logScript, prepareArgsToString(s.prepare, s.args.args))
	} else {
		if s.args.endAt.Sub(s.args.startAt) > s.way.cfg.WarnDuration {
			lg = s.way.log.Warn()
			lg.Str(logScript, prepareArgsToString(s.prepare, s.args.args))
		}
	}
	lg.Str(logPrepare, s.prepare)
	lg.Any(logArgs, s.args.handleArgs())
	lg.Int64(logStartAt, s.args.startAt.UnixMilli())
	lg.Int64(logEndAt, s.args.endAt.UnixMilli())
	lg.Str(logCost, s.args.endAt.Sub(s.args.startAt).String())
	lg.Msg(EmptyString)
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
	_      [7]byte // memory alignment padding
}

func (s *Way) GetCfg() *Cfg {
	return s.cfg
}

func (s *Way) SetCfg(cfg *Cfg) *Way {
	if cfg == nil || cfg.Scan == nil || cfg.ScanTag == EmptyString || cfg.Manual == nil || cfg.TransactionMaxDuration <= 0 || cfg.WarnDuration <= 0 {
		return s
	}
	s.cfg = cfg
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

	cfg.Manual = Postgresql()
	if drivers := sql.Drivers(); len(drivers) == 1 {
		switch drivers[0] {
		case "mysql":
			cfg.Manual = Mysql()
		case "sqlite", "sqlite3":
			cfg.Manual = Sqlite()
		default:
		}
	}

	cfg.Debugger = NewDebugger().SetLogger(logger.NewLogger(os.Stdout))

	way := &Way{
		db:  db,
		cfg: &cfg,
	}

	return way
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
		return
	}
	tx.transaction.startAt = time.Now()
	tx.transaction.id = fmt.Sprintf("%d%s%d%s%p", tx.transaction.startAt.UnixNano(), SqlPoint, os.Getpid(), SqlPoint, tx.transaction)
	tx.transaction.start()
	return
}

// commit -> Commit transaction.
func (s *Way) commit() (err error) {
	if s.transaction == nil {
		return errTransactionIsNil
	}
	tx := s.transaction
	tx.state = logTxCommit
	defer tx.write()
	tx.err = tx.tx.Commit()
	s.transaction, err = nil, tx.err
	return
}

// rollback -> Rollback transaction.
func (s *Way) rollback() (err error) {
	if s.transaction == nil {
		return errTransactionIsNil
	}
	tx := s.transaction
	tx.state = logTxRollback
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

// IsInTransaction -> Is the transaction currently in progress?
func (s *Way) IsInTransaction() bool {
	return s.transaction != nil
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
func (s *Way) newTransaction(ctx context.Context, fc func(tx *Way) error, opts ...*sql.TxOptions) (err error) {
	if ctx == nil {
		timeout := time.Second * 8
		if s.cfg != nil && s.cfg.TransactionMaxDuration > 0 {
			timeout = s.cfg.TransactionMaxDuration
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		defer cancel()
	}

	tx := (*Way)(nil)
	tx, err = s.begin(ctx, nil, opts...)
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
	return
}

// Now -> Get current time, the transaction open status will get the same time.
func (s *Way) Now() time.Time {
	if s.IsInTransaction() {
		return s.transaction.startAt
	}
	return time.Now()
}

// ScanAll -> Iteratively scan from query results.
func (s *Way) ScanAll(rows *sql.Rows, fc func(rows *sql.Rows) error) error {
	return ScanAll(rows, fc)
}

// ScanOne -> Scan at most once from the query results.
func (s *Way) ScanOne(rows *sql.Rows, dest ...any) error {
	return ScanOne(rows, dest...)
}

// Stmt Prepare a handle.
type Stmt struct {
	way     *Way
	prepare string
	stmt    *sql.Stmt
}

// Close -> Close prepare a handle.
func (s *Stmt) Close() (err error) {
	if s.stmt != nil {
		err = s.stmt.Close()
	}
	return
}

// QueryContext -> Query prepared, that can be called repeatedly.
func (s *Stmt) QueryContext(ctx context.Context, query func(rows *sql.Rows) error, args ...any) error {
	lg := s.way.sqlLog(s.prepare, args)
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
func (s *Stmt) Query(query func(rows *sql.Rows) error, args ...any) error {
	return s.QueryContext(context.Background(), query, args...)
}

// QueryRowContext -> Query prepared, that can be called repeatedly.
func (s *Stmt) QueryRowContext(ctx context.Context, query func(rows *sql.Row) error, args ...any) error {
	lg := s.way.sqlLog(s.prepare, args)
	defer lg.Write()
	row := s.stmt.QueryRowContext(ctx, args...)
	lg.args.endAt = time.Now()
	lg.err = query(row)
	return lg.err
}

// QueryRow -> Query prepared, that can be called repeatedly.
func (s *Stmt) QueryRow(query func(rows *sql.Row) error, args ...any) (err error) {
	return s.QueryRowContext(context.Background(), query, args...)
}

// ExecuteContext -> Execute prepared, that can be called repeatedly.
func (s *Stmt) ExecuteContext(ctx context.Context, args ...any) (sql.Result, error) {
	lg := s.way.sqlLog(s.prepare, args)
	defer lg.Write()
	result, err := s.stmt.ExecContext(ctx, args...)
	lg.args.endAt = time.Now()
	lg.err = err
	return result, err
}

// Execute -> Execute prepared, that can be called repeatedly.
func (s *Stmt) Execute(args ...any) (sql.Result, error) {
	return s.ExecuteContext(context.Background(), args...)
}

// ExecContext -> Execute prepared, that can be called repeatedly, return number of rows affected.
func (s *Stmt) ExecContext(ctx context.Context, args ...any) (int64, error) {
	result, err := s.ExecuteContext(ctx, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// Exec -> Execute prepared, that can be called repeatedly, return number of rows affected.
func (s *Stmt) Exec(args ...any) (int64, error) {
	return s.ExecContext(context.Background(), args...)
}

// TakeAllContext -> Query prepared and get all query results, that can be called repeatedly.
func (s *Stmt) TakeAllContext(ctx context.Context, result any, args ...any) error {
	return s.QueryContext(ctx, func(rows *sql.Rows) error { return s.way.cfg.Scan(rows, result, s.way.cfg.ScanTag) }, args...)
}

// TakeAll -> Query prepared and get all query results; that can be called repeatedly.
func (s *Stmt) TakeAll(result any, args ...any) error {
	return s.TakeAllContext(context.Background(), result, args...)
}

// PrepareContext -> Prepare SQL statement, remember to call *Stmt.Close().
func (s *Way) PrepareContext(ctx context.Context, prepare string) (stmt *Stmt, err error) {
	stmt = &Stmt{
		way:     s,
		prepare: prepare,
	}
	if tmp := s.cfg.Manual; tmp != nil && tmp.Prepare != nil {
		stmt.prepare = tmp.Prepare(prepare)
	}
	if s.IsInTransaction() {
		stmt.stmt, err = s.transaction.tx.PrepareContext(ctx, stmt.prepare)
	} else {
		stmt.stmt, err = s.db.PrepareContext(ctx, stmt.prepare)
	}
	if err != nil {
		if s.log != nil {
			s.log.Error().Str(logPrepare, stmt.prepare).Msg(err.Error())
		}
		return nil, err
	}
	return stmt, nil
}

// Prepare -> Prepare SQL statement, remember to call *Stmt.Close().
func (s *Way) Prepare(prepare string) (*Stmt, error) {
	return s.PrepareContext(context.Background(), prepare)
}

// QueryContext -> Execute the query sql statement.
func (s *Way) QueryContext(ctx context.Context, query func(rows *sql.Rows) error, prepare string, args ...any) error {
	stmt, err := s.PrepareContext(ctx, prepare)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	return stmt.QueryContext(ctx, query, args...)
}

// Query -> Execute the query sql statement.
func (s *Way) Query(query func(rows *sql.Rows) error, prepare string, args ...any) error {
	return s.QueryContext(context.Background(), query, prepare, args...)
}

// QueryRowContext -> Execute SQL statement and return row data, usually INSERT, UPDATE, DELETE.
func (s *Way) QueryRowContext(ctx context.Context, query func(row *sql.Row) error, prepare string, args ...any) error {
	stmt, err := s.PrepareContext(ctx, prepare)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	return stmt.QueryRowContext(ctx, query, args...)
}

// QueryRow -> Execute SQL statement and return row data, usually INSERT, UPDATE, DELETE.
func (s *Way) QueryRow(query func(row *sql.Row) error, prepare string, args ...any) error {
	return s.QueryRowContext(context.Background(), query, prepare, args...)
}

// TakeAllContext -> Query prepared and get all query results, through the mapping of column names and struct tags.
func (s *Way) TakeAllContext(ctx context.Context, result any, prepare string, args ...any) error {
	return s.QueryContext(ctx, func(rows *sql.Rows) error { return s.cfg.Scan(rows, result, s.cfg.ScanTag) }, prepare, args...)
}

// TakeAll -> Query prepared and get all query results.
func (s *Way) TakeAll(result any, prepare string, args ...any) error {
	return s.TakeAllContext(context.Background(), result, prepare, args...)
}

// ExecuteContext -> Execute the execute sql statement.
func (s *Way) ExecuteContext(ctx context.Context, prepare string, args ...any) (sql.Result, error) {
	stmt, err := s.PrepareContext(ctx, prepare)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stmt.Close() }()
	return stmt.ExecuteContext(ctx, args...)
}

// Execute -> Execute the execute sql statement.
func (s *Way) Execute(prepare string, args ...any) (sql.Result, error) {
	return s.ExecuteContext(context.Background(), prepare, args...)
}

// ExecContext -> Execute the execute sql statement.
func (s *Way) ExecContext(ctx context.Context, prepare string, args ...any) (int64, error) {
	stmt, err := s.PrepareContext(ctx, prepare)
	if err != nil {
		return 0, err
	}
	defer func() { _ = stmt.Close() }()
	return stmt.ExecContext(ctx, args...)
}

// Exec -> Execute the execute sql statement.
func (s *Way) Exec(prepare string, args ...any) (int64, error) {
	return s.ExecContext(context.Background(), prepare, args...)
}

/* Using Maker */

func (s *Way) MakerQueryContext(ctx context.Context, maker Maker, query func(rows *sql.Rows) error) error {
	if maker == nil {
		return nil
	}
	script := maker.ToSQL()
	if script.Empty() {
		return nil
	}
	return s.QueryContext(ctx, query, script.Prepare, script.Args...)
}

func (s *Way) MakerQuery(maker Maker, query func(rows *sql.Rows) error) error {
	if maker == nil {
		return nil
	}
	script := maker.ToSQL()
	if script.Empty() {
		return nil
	}
	return s.Query(query, script.Prepare, script.Args...)
}

func (s *Way) MakerQueryRowContext(ctx context.Context, maker Maker, query func(row *sql.Row) error) error {
	if maker == nil {
		return nil
	}
	script := maker.ToSQL()
	if script.Empty() {
		return nil
	}
	return s.QueryRowContext(ctx, query, script.Prepare, script.Args...)
}

func (s *Way) MakerQueryRow(maker Maker, query func(row *sql.Row) error) error {
	if maker == nil {
		return nil
	}
	script := maker.ToSQL()
	if script.Empty() {
		return nil
	}
	return s.QueryRow(query, script.Prepare, script.Args...)
}

func (s *Way) MakerTakeAllContext(ctx context.Context, maker Maker, result any) error {
	if maker == nil {
		return nil
	}
	script := maker.ToSQL()
	if script.Empty() {
		return nil
	}
	return s.TakeAllContext(ctx, result, script.Prepare, script.Args...)
}

func (s *Way) MakerTakeAll(maker Maker, result any) error {
	if maker == nil {
		return nil
	}
	script := maker.ToSQL()
	if script.Empty() {
		return nil
	}
	return s.TakeAll(result, script.Prepare, script.Args...)
}

func (s *Way) MakerExecuteContext(ctx context.Context, maker Maker) (sql.Result, error) {
	if maker == nil {
		return nil, nil
	}
	script := maker.ToSQL()
	if script.Empty() {
		return nil, nil
	}
	return s.ExecuteContext(ctx, script.Prepare, script.Args...)
}

func (s *Way) MakerExecute(maker Maker) (sql.Result, error) {
	if maker == nil {
		return nil, nil
	}
	script := maker.ToSQL()
	if script.Empty() {
		return nil, nil
	}
	return s.Execute(script.Prepare, script.Args...)
}

func (s *Way) MakerExecContext(ctx context.Context, maker Maker) (int64, error) {
	if maker == nil {
		return 0, nil
	}
	script := maker.ToSQL()
	if script.Empty() {
		return 0, nil
	}
	return s.ExecContext(ctx, script.Prepare, script.Args...)
}

func (s *Way) MakerExec(maker Maker) (int64, error) {
	if maker == nil {
		return 0, nil
	}
	script := maker.ToSQL()
	if script.Empty() {
		return 0, nil
	}
	return s.Exec(script.Prepare, script.Args...)
}

/* Batch Update */

func (s *Way) BatchUpdateContext(ctx context.Context, prepare string, argsList [][]any) (affectedRows int64, err error) {
	var stmt *Stmt
	stmt, err = s.PrepareContext(ctx, prepare)
	if err != nil {
		return affectedRows, err
	}
	defer func() { _ = stmt.Close() }()
	var rows int64
	for _, args := range argsList {
		if rows, err = stmt.Exec(args...); err != nil {
			return affectedRows, err
		} else {
			affectedRows += rows
		}
	}
	return affectedRows, nil
}

func (s *Way) BatchUpdate(prepare string, argsList [][]any) (affectedRows int64, err error) {
	return s.BatchUpdateContext(context.Background(), prepare, argsList)
}

// getter -> Query, execute the query SQL statement with args, no prepared is used.
func (s *Way) getter(ctx context.Context, query func(rows *sql.Rows) error, prepare string, args ...any) error {
	if query == nil || prepare == EmptyString {
		return nil
	}
	lg := s.sqlLog(prepare, args)
	defer lg.Write()
	var (
		err  error
		rows *sql.Rows
	)
	if s.IsInTransaction() {
		rows, err = s.transaction.tx.QueryContext(ctx, prepare, args...)
	} else {
		rows, err = s.db.QueryContext(ctx, prepare, args...)
	}
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

// setter -> Execute, execute the execute SQL statement with args, no prepared is used.
func (s *Way) setter(ctx context.Context, prepare string, args ...any) (rowsAffected int64, err error) {
	if prepare == EmptyString {
		return
	}
	lg := s.sqlLog(prepare, args)
	defer lg.Write()
	var result sql.Result
	if s.IsInTransaction() {
		result, err = s.transaction.tx.ExecContext(ctx, prepare, args...)
	} else {
		result, err = s.db.ExecContext(ctx, prepare, args...)
	}
	lg.args.endAt = time.Now()
	if err != nil {
		lg.err = err
		return
	}
	rowsAffected, err = result.RowsAffected()
	lg.err = err
	return
}

// GetterContext -> Execute the query SQL statement with args, no prepared is used.
func (s *Way) GetterContext(ctx context.Context, query func(rows *sql.Rows) error, prepare string, args ...any) (err error) {
	return s.getter(ctx, query, prepare, args...)
}

// Getter -> Execute the query SQL statement with args, no prepared is used.
func (s *Way) Getter(query func(rows *sql.Rows) error, prepare string, args ...any) error {
	return s.GetterContext(context.Background(), query, prepare, args...)
}

// SetterContext -> Execute the execute SQL statement with args, no prepared is used.
func (s *Way) SetterContext(ctx context.Context, prepare string, args ...any) (int64, error) {
	return s.setter(ctx, prepare, args...)
}

// Setter -> Execute the execute SQL statement with args, no prepared is used.
func (s *Way) Setter(prepare string, args ...any) (int64, error) {
	return s.SetterContext(context.Background(), prepare, args...)
}

// F -> Quickly initialize a filter.
func (s *Way) F(filters ...Filter) Filter {
	return F().New(filters...).SetWay(s)
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

// W -> The specified transaction object is used first, otherwise the current object is used.
func (s *Way) W(values ...*Way) *Way {
	for i := len(values) - 1; i >= 0; i-- {
		if values[i] != nil && values[i].IsInTransaction() {
			return values[i]
		}
	}
	return s
}

// Add -> Create an instance that executes the INSERT SQL statement.
func (s *Way) Add(table string) *Add {
	return NewAdd(s).Table(s.Replace(table))
}

// Del -> Create an instance that executes the DELETE SQL statement.
func (s *Way) Del(table string) *Del {
	return NewDel(s).Table(s.Replace(table))
}

// Mod -> Create an instance that executes the UPDATE SQL statement.
func (s *Way) Mod(table string) *Mod {
	return NewMod(s).Table(s.Replace(table))
}

// Get -> Create an instance that executes the SELECT SQL statement.
func (s *Way) Get(table ...string) *Get {
	return NewGet(s).Table(s.Replace(LastNotEmptyString(table)))
}

// AddOneReturnSequenceValue Insert a record and return the sequence value of the data (usually an auto-incrementing id value).
type AddOneReturnSequenceValue interface {
	Maker

	// Context Custom context.
	Context(ctx context.Context) AddOneReturnSequenceValue

	// Prepare You may need to modify the SQL statement to be executed.
	Prepare(prepare func(tmp *SQL)) AddOneReturnSequenceValue

	// Execute Customize the method to return the sequence value of inserted data.
	Execute(execute func(ctx context.Context, stmt *Stmt, args ...any) (sequenceValue int64, err error)) AddOneReturnSequenceValue

	// AddOne Insert a record and return the sequence value of the data (usually an auto-incrementing id value).
	AddOne() (int64, error)
}

type addOneReturnSequenceValue struct {
	ctx     context.Context
	way     *Way
	add     *SQL
	prepare func(tmp *SQL)
	execute func(ctx context.Context, stmt *Stmt, args ...any) (sequenceValue int64, err error)
}

// Context Custom context.
func (s *addOneReturnSequenceValue) Context(ctx context.Context) AddOneReturnSequenceValue {
	s.ctx = ctx
	return s
}

// Prepare You may need to modify the SQL statement to be executed.
func (s *addOneReturnSequenceValue) Prepare(prepare func(tmp *SQL)) AddOneReturnSequenceValue {
	s.prepare = prepare
	return s
}

// Execute Customize the method to return the sequence value of inserted data.
func (s *addOneReturnSequenceValue) Execute(execute func(ctx context.Context, stmt *Stmt, args ...any) (sequenceValue int64, err error)) AddOneReturnSequenceValue {
	s.execute = execute
	return s
}

// ToSQL Make SQL.
func (s *addOneReturnSequenceValue) ToSQL() *SQL {
	result := NewSQL(s.add.Prepare, s.add.Args...)
	if prepare := s.prepare; prepare != nil {
		prepare(result)
	}
	return result
}

// AddOne Insert a record and return the sequence value of the data (usually an auto-incrementing id value).
func (s *addOneReturnSequenceValue) AddOne() (int64, error) {
	create := s.ToSQL()
	ctx := s.ctx
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), s.way.cfg.TransactionMaxDuration)
		defer cancel()
	}
	stmt, err := s.way.PrepareContext(ctx, create.Prepare)
	if err != nil {
		return 0, err
	}
	defer func() { _ = stmt.Close() }()
	return s.execute(ctx, stmt, create.Args...)
}

// NewAddOne Insert one and get the last insert sequence value.
func (s *Way) NewAddOne(prepare string, args ...any) AddOneReturnSequenceValue {
	return &addOneReturnSequenceValue{
		way: s,
		add: NewSQL(prepare, args...),
	}
}

// NewAddOneMaker Insert one and get the last insert sequence value.
func (s *Way) NewAddOneMaker(insert Maker) AddOneReturnSequenceValue {
	add := insert.ToSQL()
	return s.NewAddOne(add.Prepare, add.Args...)
}

// NewInsertOnConflict Update if exists, insert if not.
func (s *Way) NewInsertOnConflict(prepare string, args ...any) SQLInsertOnConflict {
	return NewSQLInsertOnConflict(s, prepare, args...)
}

// NewInsertOnConflictMaker Update if exists, insert if not.
func (s *Way) NewInsertOnConflictMaker(insert Maker) SQLInsertOnConflict {
	add := insert.ToSQL()
	return s.NewInsertOnConflict(add.Prepare, add.Args...)
}

// T Table empty alias
func (s *Way) T() *TableColumn {
	return NewTableColumn(s)
}

// TA Table alias `a`
func (s *Way) TA() *TableColumn {
	return NewTableColumn(s, AliasA)
}

// TB Table alias `b`
func (s *Way) TB() *TableColumn {
	return NewTableColumn(s, AliasB)
}

// TC Table alias `c`
func (s *Way) TC() *TableColumn {
	return NewTableColumn(s, AliasC)
}

// TD Table alias `d`
func (s *Way) TD() *TableColumn {
	return NewTableColumn(s, AliasD)
}

// TE Table alias `e`
func (s *Way) TE() *TableColumn {
	return NewTableColumn(s, AliasE)
}

// TF Table alias `f`
func (s *Way) TF() *TableColumn {
	return NewTableColumn(s, AliasF)
}

// TG Table alias `g`
func (s *Way) TG() *TableColumn {
	return NewTableColumn(s, AliasG)
}

// Replace For a replacement key.
func (s *Way) Replace(key string) string {
	if tmp := s.cfg.Manual.Replace; tmp != nil {
		return tmp.Get(key)
	}
	return key
}

// Replaces For replacement keys.
func (s *Way) Replaces(keys []string) []string {
	if tmp := s.cfg.Manual.Replace; tmp != nil {
		return tmp.Gets(keys)
	}
	return keys
}

// WindowFunc New a window function object.
func (s *Way) WindowFunc(alias ...string) *WindowFunc {
	return NewWindowFunc(s, alias...)
}

// Debug Debugging output SQL script.
func (s *Way) Debug(maker Maker) *Way {
	if s.cfg.Debugger != nil {
		s.cfg.Debugger.Debug(maker)
	}
	return s
}

// read Implement Reader.
type read struct {
	// reads Read list.
	reads []*Way

	// total Length of a read list.
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
func ScanOne(rows *sql.Rows, dest ...any) error {
	if rows.Next() {
		return rows.Scan(dest...)
	}
	return nil
}

// tryFloat64 string or []byte to float64.
func tryFloat64(value any) any {
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
func tryString(value any) any {
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
func adjustViewData(columnType *sql.ColumnType) func(value any) any {
	databaseTypeName := columnType.DatabaseTypeName()
	databaseTypeNameUpper := strings.ToUpper(databaseTypeName)
	switch databaseTypeNameUpper {
	case "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL", "DOUBLE PRECISION", "NUMBER":
		return tryFloat64
	case "CHAR", "VARCHAR", "TEXT", "CHARACTER", "CHARACTER VARYING", "BPCHAR", "NCHAR", "NVARCHAR",
		"TINYTEXT", "MEDIUMTEXT", "LARGETEXT", "LONGTEXT", "TIMESTAMP", "DATE", "TIME", "DATETIME", "JSON":
		return tryString
	case "BYTEA",
		"BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":
		return nil
	default:
	}
	return nil
}

// ScanMap Scan query result to []map[string]any, view query result.
func ScanMap(rows *sql.Rows) ([]map[string]any, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	count := len(columns)
	result := make([]map[string]any, 0)
	for rows.Next() {
		scan := make([]any, count)
		for i := range scan {
			scan[i] = new(any)
		}
		if err = rows.Scan(scan...); err != nil {
			return nil, err
		}
		value := make(map[string]any, count)
		for index, column := range columns {
			if scan[index] == nil {
				value[column] = nil
			} else {
				value[column] = scan[index]
			}
		}
		result = append(result, value)
	}
	change := make(map[string]func(any) any, 8)
	for _, value := range types {
		if tmp := adjustViewData(value); tmp != nil {
			change[value.Name()] = tmp
		}
	}
	for column, action := range change {
		for index, value := range result {
			result[index][column] = action(value[column])
		}
	}
	return result, nil
}

// argValueToString Convert SQL statement parameters into text strings.
func argValueToString(i any) string {
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
			return hex.EncodeToString(bts)
		}
		return fmt.Sprintf("'%v'", tmp)
	}
}

// prepareArgsToString Merge executed SQL statements and parameters.
func prepareArgsToString(prepare string, args []any) string {
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
	for i := range length {
		if origin[i] == questionMark && index < count {
			latest.WriteString(argValueToString(args[index]))
			index++
		} else {
			latest.WriteByte(origin[i])
		}
	}
	return latest.String()
}

// Debugger Debug output SQL script.
type Debugger interface {
	// Debug Debug output SQL script
	Debug(maker Maker) Debugger

	GetLogger() *logger.Logger

	SetLogger(log *logger.Logger) Debugger
}

type debugger struct {
	log *logger.Logger
}

func (s *debugger) GetLogger() *logger.Logger {
	return s.log
}

func (s *debugger) SetLogger(log *logger.Logger) Debugger {
	s.log = log
	return s
}

func (s *debugger) Debug(maker Maker) Debugger {
	if maker == nil || s.log == nil {
		return s
	}
	script := maker.ToSQL()
	if script.Empty() {
		return s
	}
	scriptString := prepareArgsToString(script.Prepare, script.Args)
	s.log.Debug().Str(logScript, scriptString).Str(logPrepare, script.Prepare).Any(logArgs, handleArgs(script.Args)).Msg("debug SQL script")
	return s
}

func NewDebugger() Debugger { return &debugger{} }
