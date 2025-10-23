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
		return StrNull
	}
	t, v := reflect.TypeOf(i), reflect.ValueOf(i)
	k := t.Kind()
	for k == reflect.Pointer {
		if v.IsNil() {
			return StrNull
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
				return StrNull
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
		return StrEmpty
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
	c63 := Str63[0]
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

const (
	strTxBegin    = "BEGIN"
	strTxCommit   = "COMMIT"
	strTxRollback = "ROLLBACK"

	strId      = "id"
	strStartAt = "start_at"
	strEndAt   = "end_at"
	strState   = "state"
	strError   = "error"
	strScript  = "script"
	strMsg     = "msg"
	strPrepare = "prepare"
	strArgs    = "args"
	strCost    = "cost"
)

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
		Str(strScript, SQLToString(script)).
		Str(strPrepare, script.Prepare).
		Any(strArgs, binaryByteSliceToString(script.Args)).
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
	lg.Str(strId, s.id)
	lg.Int64(strStartAt, s.startAt.UnixMilli())
	lg.Str(strState, strTxBegin)
	lg.Msg(StrEmpty)
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
			lg.Str(strError, v.err.Error())
			lg.Str(strScript, SQLToString(NewSQL(v.prepare, v.args.args...)))
		} else {
			if v.args.endAt.Sub(v.args.startAt) > s.way.cfg.WarnDuration {
				lg = s.way.log.Warn()
				lg.Str(strScript, SQLToString(NewSQL(v.prepare, v.args.args...)))
			}
		}
		lg.Str(strId, s.id)
		lg.Str(strMsg, s.message)
		lg.Str(strPrepare, v.prepare)
		lg.Any(strArgs, binaryByteSliceToString(v.args.args))
		lg.Int64(strStartAt, v.args.startAt.UnixMilli())
		lg.Int64(strEndAt, v.args.endAt.UnixMilli())
		lg.Str(strCost, v.args.endAt.Sub(v.args.startAt).String())
		lg.Msg(StrEmpty)
	}
	lg := s.way.log.Info()
	if s.err != nil {
		lg = s.way.log.Error()
		lg.Str(strError, s.err.Error())
	}
	lg.Str(strId, s.id)
	lg.Int64(strStartAt, s.startAt.UnixMilli())
	lg.Str(strState, s.state)
	lg.Int64(strEndAt, s.endAt.UnixMilli())
	lg.Str(strCost, s.endAt.Sub(s.startAt).String())
	lg.Msg(StrEmpty)
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
	c36 := Str36[0] // $
	c63 := Str63[0] // ?
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

// Cfg Configure of Way.
type Cfg struct {
	// Debug For debug output SQL script.
	Debug DebugMaker

	// MapScan Custom MapScan.
	MapScan MapScanner

	// Manual For handling different types of databases.
	Manual *Manual

	// Scan For scanning data into structure.
	Scan func(rows *sql.Rows, result any, tag string) error

	// TransactionOptions Start transaction.
	TransactionOptions *sql.TxOptions

	// ScanTag Scan data to tag mapping on structure.
	ScanTag string

	// TableMethodName Custom method name to get table name.
	TableMethodName string

	// TransactionMaxDuration Maximum transaction execution time.
	TransactionMaxDuration time.Duration

	// WarnDuration SQL execution time warning threshold.
	WarnDuration time.Duration

	// DeleteMustUseWhere Deletion of data must be filtered using conditions.
	DeleteMustUseWhere bool

	// UpdateMustUseWhere Updated data must be filtered using conditions.
	UpdateMustUseWhere bool
}

// DefaultCfg default configure value.
func DefaultCfg() Cfg {
	return Cfg{
		MapScan:                NewMapScanner(),
		Scan:                   RowsScan,
		ScanTag:                StrDefaultTag,
		TableMethodName:        StrTableMethodName,
		DeleteMustUseWhere:     true,
		UpdateMustUseWhere:     true,
		TransactionMaxDuration: time.Second * 5,
		WarnDuration:           time.Millisecond * 200,
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
		lg.Str(strError, s.err.Error())
		lg.Str(strScript, SQLToString(NewSQL(s.prepare, s.args.args...)))
	} else {
		if s.args.endAt.Sub(s.args.startAt) > s.way.cfg.WarnDuration {
			lg = s.way.log.Warn()
			lg.Str(strScript, SQLToString(NewSQL(s.prepare, s.args.args...)))
		}
	}
	lg.Str(strPrepare, s.prepare)
	lg.Any(strArgs, binaryByteSliceToString(s.args.args))
	lg.Int64(strStartAt, s.args.startAt.UnixMilli())
	lg.Int64(strEndAt, s.args.endAt.UnixMilli())
	lg.Str(strCost, s.args.endAt.Sub(s.args.startAt).String())
	lg.Msg(StrEmpty)
}

// DatabaseReader Separate read and write, when you distinguish between reading and writing, please do not use the same object for both reading and writing.
type DatabaseReader interface {
	// Read Get an object for read.
	Read() *Way
}

// Way Quick insert, delete, update, select helper.
type Way struct {
	cfg *Cfg

	db *sql.DB

	log *logger.Logger

	transaction *transaction

	databaseReader DatabaseReader

	isRead bool
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

	cfg.Debug = NewDebugMaker().SetLogger(logger.NewLogger(os.Stdout))

	way := &Way{
		db:  db,
		cfg: &cfg,
	}

	return way
}

func (s *Way) GetCfg() *Cfg {
	return s.cfg
}

func (s *Way) SetCfg(cfg *Cfg) *Way {
	if cfg == nil || cfg.Scan == nil || cfg.ScanTag == StrEmpty || cfg.Manual == nil || cfg.TransactionMaxDuration <= 0 || cfg.WarnDuration <= 0 {
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

func (s *Way) SetLogger(log *logger.Logger) *Way {
	s.log = log
	return s
}

func (s *Way) GetDatabaseReader() DatabaseReader {
	return s.databaseReader
}

func (s *Way) SetDatabaseReader(reader DatabaseReader) *Way {
	s.databaseReader = reader
	return s
}

func (s *Way) Read() *Way {
	if s.databaseReader == nil {
		return s
	}
	result := s.databaseReader.Read()
	result.isRead = true
	return result
}

// IsRead -> Is an object for read?
func (s *Way) IsRead() bool {
	return s.isRead
}

// Replace Get a single identifier mapping value, if it does not exist, return the original value.
func (s *Way) Replace(key string) string {
	if tmp := s.cfg.Manual.Replacer; tmp != nil {
		return tmp.Get(key)
	}
	return key
}

// ReplaceAll Get multiple identifier mapping values, return the original value if none exists.
func (s *Way) ReplaceAll(keys []string) []string {
	if tmp := s.cfg.Manual.Replacer; tmp != nil {
		return tmp.GetAll(keys)
	}
	return keys
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
		return tx, err
	}
	tx.transaction.startAt = time.Now()
	tx.transaction.id = fmt.Sprintf("%d%s%d%s%p", tx.transaction.startAt.UnixNano(), StrPoint, os.Getpid(), StrPoint, tx.transaction)
	tx.transaction.start()
	return tx, err
}

// commit -> Commit transaction.
func (s *Way) commit() (err error) {
	if s.transaction == nil {
		return ErrTransactionIsNil
	}
	tx := s.transaction
	tx.state = strTxCommit
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
	tx.state = strTxRollback
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
	if s.transaction.message == StrEmpty {
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

// Fetch -> Query prepared and get all query results, that can be called repeatedly.
func (s *Stmt) Fetch(ctx context.Context, result any, args ...any) error {
	return s.Query(ctx, func(rows *sql.Rows) error {
		return s.way.cfg.Scan(rows, result, s.way.cfg.ScanTag)
	}, args...)
}

// Prepare -> Prepare SQL statement, remember to call *Stmt.Close().
func (s *Way) Prepare(ctx context.Context, prepare string) (stmt *Stmt, err error) {
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
			s.log.Error().Str(strPrepare, stmt.prepare).Msg(err.Error())
		}
		return nil, err
	} else {
		if stmt.prepare == StrEmpty && s.log != nil {
			s.log.Warn().Str(strPrepare, stmt.prepare).Msg("prepare value is an empty string")
		}
	}
	return stmt, nil
}

// Query -> Execute the query sql statement.
func (s *Way) Query(ctx context.Context, maker Maker, query func(rows *sql.Rows) error) error {
	script := maker.ToSQL()
	if script.IsEmpty() {
		return ErrEmptyPrepare
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
		return ErrEmptyPrepare
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

// Fetch -> Query prepared and get all query results, through the mapping of column names and struct tags.
func (s *Way) Fetch(ctx context.Context, maker Maker, result any) error {
	return s.Query(ctx, maker, func(rows *sql.Rows) error { return s.cfg.Scan(rows, result, s.cfg.ScanTag) })
}

// MapScan -> Scanning the query results into []map[string]any.
func (s *Way) MapScan(ctx context.Context, maker Maker, adjusts ...AdjustColumnAnyValue) (result []map[string]any, err error) {
	err = s.Query(ctx, maker, func(rows *sql.Rows) error {
		result, err = s.cfg.MapScan.Scan(rows, adjusts...)
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
		return nil, ErrEmptyPrepare
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
		return 0, ErrEmptyPrepare
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

// MultiFetch Execute multiple DQL statements.
func (s *Way) MultiFetch(ctx context.Context, makers []Maker, results []any) (err error) {
	for index, maker := range makers {
		err = s.Fetch(ctx, maker, results[index])
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

// MultiStmtFetch Executing a DQL statement multiple times using the same prepared statement.
func (s *Way) MultiStmtFetch(ctx context.Context, prepare string, lists [][]any, results []any) (err error) {
	if prepare == StrEmpty {
		return ErrEmptyPrepare
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
		err = stmt.Fetch(ctx, results[index], value...)
		if err != nil {
			return err
		}
	}
	return nil
}

// MultiStmtExecute Executing a DML statement multiple times using the same prepared statement.
func (s *Way) MultiStmtExecute(ctx context.Context, prepare string, lists [][]any) (affectedRows int64, err error) {
	if prepare == StrEmpty {
		return 0, ErrEmptyPrepare
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
	return F().New(filters...).SetReplacer(s.cfg.Manual.Replacer)
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
	if s.cfg.Debug != nil {
		s.cfg.Debug.Debug(maker)
	}
	return s
}

// databaseRead Implement DatabaseReader.
type databaseRead struct {
	// choose Gets a read-only object from the read list.
	choose func(n int) int

	// reads Read list.
	reads []*Way

	// total Length of a read list.
	total int
}

// NewDatabaseReader It is recommended that objects used for writing should not appear in reads.
func NewDatabaseReader(choose func(n int) int, reads []*Way) DatabaseReader {
	if choose == nil {
		panic("hey: empty value of `choose`")
	}
	length := len(reads)
	if length == 0 {
		panic("hey: empty value of `reads`")
	}
	return &databaseRead{
		reads:  reads,
		total:  length,
		choose: choose,
	}
}

// Read Get an instance for querying.
func (s *databaseRead) Read() *Way {
	return s.reads[s.choose(s.total)]
}
