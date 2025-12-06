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
)

// hexEncodeToString Convert binary byte array to hexadecimal string.
func hexEncodeToString(values []byte) string {
	return hex.EncodeToString(values)
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

// transaction Information for transaction.
type transaction struct {
	// ctx Context object.
	ctx context.Context

	// way Original *Way object.
	way *Way

	// tx Transaction object.
	tx *sql.Tx

	// track Tracking transaction.
	track *MyTrack

	// script List of SQL statements that have been executed within a transaction.
	script []*MyTrack
}

// write Recording transaction logs.
func (s *transaction) write() {
	track := s.way.track
	if track == nil {
		return
	}
	if s.track.TimeEnd.IsZero() {
		s.track.TimeEnd = time.Now()
	}
	defer track.Track(s.track.Context, s.track)
	for _, v := range s.script {
		v.Script = SQLToString(NewSQL(v.Prepare, v.Args...))
		v.TxId = s.track.TxId
		v.TxMsg = s.track.TxMsg
		v.TxState = s.track.TxState
		track.Track(v.Context, v)
	}
}

// Manual For handling different types of databases.
type Manual struct {
	// Replacer SQL Identifier Replacer.
	Replacer Replacer

	// Prepare to adjust the SQL statement format to meet the current database SQL statement format.
	Prepare func(prepare string) string

	// DatabaseType Database type value.
	DatabaseType cst.DatabaseType

	// More custom methods can be added here to achieve the same function using different databases.
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
	manual.DatabaseType = cst.Postgresql
	manual.Prepare = prepare63236
	return manual
}

func Sqlite() *Manual {
	manual := &Manual{}
	manual.DatabaseType = cst.Sqlite
	return manual
}

func Mysql() *Manual {
	manual := &Manual{}
	manual.DatabaseType = cst.Mysql
	return manual
}

type config struct {
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

	// maxLimit Check the maximum allowed LIMIT value; a value less than or equal to 0 will be unlimited.
	maxLimit int64

	// maxOffset Check the maximum allowed OFFSET value; a value less than or equal to 0 will be unlimited.
	maxOffset int64

	// defaultPageSize The default value of limit when querying data with the page parameter for pagination.
	defaultPageSize int64

	// txMaxDuration Maximum transaction execution time.
	txMaxDuration time.Duration

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
		maxLimit:           10000,
		maxOffset:          100000,
		defaultPageSize:    10,
		txMaxDuration:      time.Second * 5,
		deleteRequireWhere: true,
		updateRequireWhere: true,
	}
}

type Option func(way *Way)

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

func WithMaxLimit(maxLimit int64) Option {
	return func(way *Way) {
		if maxLimit >= 0 {
			way.cfg.maxLimit = maxLimit
		}
	}
}

func WithMaxOffset(maxOffset int64) Option {
	return func(way *Way) {
		if maxOffset >= 0 {
			way.cfg.maxOffset = maxOffset // The maximum range of data that can be queried is controlled by the business.
		}
	}
}

func WithDefaultPageSize(pageSize int64) Option {
	return func(way *Way) {
		if pageSize > 0 {
			way.cfg.defaultPageSize = pageSize
		}
	}
}

func WithTxMaxDuration(txMaxDuration time.Duration) Option {
	return func(way *Way) {
		if txMaxDuration > 0 {
			way.cfg.txMaxDuration = txMaxDuration
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

func WithTrack(track Track) Option {
	return func(way *Way) {
		way.track = track
	}
}

func WithReader(reader Reader) Option {
	return func(way *Way) {
		way.reader = reader
	}
}

// Reader Separate read and write, when you distinguish between reading and writing, please do not use the same object for both reading and writing.
type Reader interface {
	// Read Get an object for read.
	Read() *Way
}

type Way struct {
	// cfg Configuration information.
	cfg *config

	// db Database object.
	db *sql.DB

	// track Tracing SQL statements.
	track Track

	// transaction Transaction object.
	transaction *transaction

	// reader A *Way object used only for reading data.
	reader Reader

	// isRead Is the current object a read-only object?
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

func (s *Way) Manual() *Manual {
	return s.cfg.manual
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
	if s.track != nil {
		start := time.Now()
		tracked := trackTransaction(ctx, start)
		tracked.TxId = fmt.Sprintf("%d%s%d%s%p", start.UnixNano(), cst.Point, os.Getpid(), cst.Point, tx.transaction)
		tracked.TxState = cst.BEGIN
		tx.transaction.track = tracked
		s.track.Track(ctx, tracked)
	}
	return tx, err
}

// commit -> Commit transaction.
func (s *Way) commit() (err error) {
	if s.transaction == nil {
		return ErrTransactionIsNil
	}
	tx := s.transaction
	if tx.track != nil {
		tx.track.TxState = cst.COMMIT
	}
	defer func() {
		tx.write()
		s.transaction = nil
	}()
	err = tx.tx.Commit()
	if tx.track != nil {
		tx.track.Err = err
	}
	return err
}

// rollback -> Rollback transaction.
func (s *Way) rollback() (err error) {
	if s.transaction == nil {
		return ErrTransactionIsNil
	}
	tx := s.transaction
	if tx.track != nil {
		tx.track.TxState = cst.ROLLBACK
	}
	defer func() {
		tx.write()
		s.transaction = nil
	}()
	err = tx.tx.Rollback()
	if tx.track != nil {
		tx.track.Err = err
	}
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
	if s.transaction.track.TxMsg == cst.Empty {
		s.transaction.track.TxMsg = message
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
		return s.transaction.track.TimeStart
	}
	return time.Now()
}

// Stmt Prepare a handle.
type Stmt struct {
	way      *Way
	stmt     *sql.Stmt
	prepare  string
	prepared string
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
	var tracked *MyTrack
	if track := s.way.track; track != nil {
		tracked = trackSQL(ctx, s.prepare, args)
		defer tracked.write(track, s.way)
	}
	rows, err := s.stmt.QueryContext(ctx, args...)
	if tracked != nil {
		tracked.TimeEnd = time.Now()
		tracked.Err = err
	}
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
	err = query(rows)
	if tracked != nil {
		tracked.Err = err
	}
	return err
}

// QueryRow -> Query prepared, that can be called repeatedly.
func (s *Stmt) QueryRow(ctx context.Context, query func(row *sql.Row) error, args ...any) error {
	var tracked *MyTrack
	if track := s.way.track; track != nil {
		tracked = trackSQL(ctx, s.prepare, args)
		defer tracked.write(track, s.way)
	}
	row := s.stmt.QueryRowContext(ctx, args...)
	if tracked != nil {
		tracked.TimeEnd = time.Now()
	}
	err := query(row)
	if tracked != nil {
		tracked.Err = err
	}
	return err
}

// Exec -> Execute prepared, that can be called repeatedly.
func (s *Stmt) Exec(ctx context.Context, args ...any) (sql.Result, error) {
	var tracked *MyTrack
	if track := s.way.track; track != nil {
		tracked = trackSQL(ctx, s.prepare, args)
		defer tracked.write(track, s.way)
	}
	result, err := s.stmt.ExecContext(ctx, args...)
	if tracked != nil {
		tracked.TimeEnd = time.Now()
		tracked.Err = err
	}
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
		way:      s,
		prepare:  prepare,
		prepared: prepare,
	}
	if manual := s.cfg.manual; manual != nil && manual.Prepare != nil {
		stmt.prepared = manual.Prepare(prepare)
	}
	if s.IsInTransaction() {
		stmt.stmt, err = s.transaction.tx.PrepareContext(ctx, stmt.prepared)
	} else {
		stmt.stmt, err = s.db.PrepareContext(ctx, stmt.prepared)
	}
	if err != nil {
		return nil, err
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

// RowScan Scan a row of SQL results containing one or more columns.
func (s *Way) RowScan(dest ...any) func(row *sql.Row) error {
	return func(row *sql.Row) error {
		return row.Scan(dest...)
	}
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

// Exists -> Execute a query SQL statement to check if the data exists.
func (s *Way) Exists(ctx context.Context, maker Maker) (bool, error) {
	// SQL statement format: SELECT EXISTS ( subquery ) AS a
	// SELECT EXISTS ( SELECT 1 FROM example_table ) AS a
	// SELECT EXISTS ( SELECT 1 FROM example_table WHERE ( id > 0 ) ) AS a
	// SELECT EXISTS ( ( SELECT 1 FROM example_table WHERE ( column1 = 'value1' ) ) UNION ALL ( SELECT 1 FROM example_table WHERE ( column2 = 'value2' ) ) ) AS a

	// Database drivers typically return a boolean or integer value, where 0 indicates that the data does not exist and 1 indicates that the data exists.
	var result any

	err := s.Query(ctx, maker, func(rows *sql.Rows) error {
		for rows.Next() {
			err := rows.Scan(&result)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return false, err
	}

	switch value := result.(type) {
	case bool:
		return value, nil
	case int:
		return value != 0, nil
	case int8:
		return value != 0, nil
	case int16:
		return value != 0, nil
	case int32:
		return value != 0, nil
	case int64:
		return value != 0, nil
	case uint:
		return value != 0, nil
	case uint8:
		return value != 0, nil
	case uint16:
		return value != 0, nil
	case uint32:
		return value != 0, nil
	case uint64:
		return value != 0, nil
	default:
		return false, fmt.Errorf("unexpected type %T %v", result, result)
	}
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
	var stmt *Stmt
	defer func() {
		if stmt != nil {
			_ = stmt.Close()
		}
	}()
	for index, value := range lists {
		if stmt == nil {
			stmt, err = s.Prepare(ctx, prepare)
			if err != nil {
				return err
			}
		}
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
	var stmt *Stmt
	defer func() {
		if stmt != nil {
			_ = stmt.Close()
		}
	}()
	rows := int64(0)
	for _, args := range lists {
		if stmt == nil {
			stmt, err = s.Prepare(ctx, prepare)
			if err != nil {
				return affectedRows, err
			}
		}
		rows, err = stmt.Execute(ctx, args...)
		if err != nil {
			return affectedRows, err
		}
		affectedRows += rows
	}
	return affectedRows, nil
}

// GroupMultiStmtScan Call using the same prepared statement. Multi-statement query.
func (s *Way) GroupMultiStmtScan(ctx context.Context, queries []Maker, results []any) (err error) {
	length := len(queries)
	if length == 0 {
		return
	}
	prepare := make([]string, 0, 1<<1)
	args := make(map[string][][]any, 1<<1)
	scan := make(map[string][]any, 1<<1)
	for index, value := range queries {
		if value == nil {
			continue
		}
		script := value.ToSQL()
		if script == nil {
			continue
		}
		if script.IsEmpty() {
			return ErrEmptyScript
		}
		if _, ok := args[script.Prepare]; !ok {
			args[script.Prepare] = make([][]any, 0, 1)
			scan[script.Prepare] = make([]any, 0, 1)
			prepare = append(prepare, script.Prepare)
		}
		args[script.Prepare] = append(args[script.Prepare], script.Args)
		scan[script.Prepare] = append(scan[script.Prepare], results[index])
	}
	for _, value := range prepare {
		err = s.MultiStmtScan(ctx, value, args[value], scan[value])
		if err != nil {
			return
		}
	}
	return
}

// GroupMultiStmtExecute Call using the same prepared statement. Multi-statement insert and update.
func (s *Way) GroupMultiStmtExecute(ctx context.Context, executes []Maker) (affectedRows int64, err error) {
	length := len(executes)
	if length == 0 {
		return
	}
	prepare := make([]string, 0, 1<<1)
	args := make(map[string][][]any, 1<<1)
	for _, value := range executes {
		if value == nil {
			continue
		}
		script := value.ToSQL()
		if script == nil {
			continue
		}
		if script.IsEmpty() {
			err = ErrEmptyScript
			return
		}
		if _, ok := args[script.Prepare]; !ok {
			args[script.Prepare] = make([][]any, 0, 1)
			prepare = append(prepare, script.Prepare)
		}
		args[script.Prepare] = append(args[script.Prepare], script.Args)
	}
	rows := int64(0)
	for _, value := range prepare {
		rows, err = s.MultiStmtExecute(ctx, value, args[value])
		if err != nil {
			return
		}
		affectedRows += rows
	}
	return
}

// Debug Debugging output SQL script.
func (s *Way) Debug(maker Maker) *Way {
	if s.track == nil || maker == nil {
		return s
	}
	ctx := context.Background()
	s.track.Track(ctx, trackDebug(ctx, maker))
	return s
}

// F -> Quickly initialize a filter.
func (s *Way) F(filters ...Filter) Filter {
	result := F().New(filters...)
	if manual := s.cfg.manual; manual != nil {
		result.SetReplacer(manual.Replacer)
	}
	return result
}

// W -> Prioritize the specified non-nil object, otherwise use the current object.
func (s *Way) W(way *Way) *Way {
	if way != nil {
		return way
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

// V Get the currently used *Way object value.
type V interface {
	// V Get the currently used *Way object value.
	V() *Way
}

// W Use the non-nil value *Way.
type W interface {
	// W Use the non-nil value *Way.
	W(way *Way)
}
