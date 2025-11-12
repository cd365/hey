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

// transaction Information for transaction.
type transaction struct {
	ctx context.Context

	way *Way

	tx *sql.Tx

	track *MyTrack

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
		track.Track(s.ctx, v)
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
		deleteRequireWhere: true,
		updateRequireWhere: true,
		txMaxDuration:      time.Second * 5,
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
	cfg *config

	db *sql.DB

	track Track

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
	start := time.Now()
	track := s.trackTransaction(ctx, start)
	track.TxId = fmt.Sprintf("%d%s%d%s%p", start.UnixNano(), cst.Point, os.Getpid(), cst.Point, tx.transaction)
	track.TxState = cst.BEGIN
	tx.transaction.track = track
	if s.track != nil {
		s.track.Track(ctx, track)
	}
	return tx, err
}

// commit -> Commit transaction.
func (s *Way) commit() (err error) {
	if s.transaction == nil {
		return ErrTransactionIsNil
	}
	tx := s.transaction
	tx.track.TxState = cst.COMMIT
	defer tx.write()
	tx.track.Err = tx.tx.Commit()
	s.transaction, err = nil, tx.track.Err
	return err
}

// rollback -> Rollback transaction.
func (s *Way) rollback() (err error) {
	if s.transaction == nil {
		return ErrTransactionIsNil
	}
	tx := s.transaction
	tx.track.TxState = cst.ROLLBACK
	defer tx.write()
	tx.track.Err = tx.tx.Rollback()
	s.transaction, err = nil, tx.track.Err
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
	track := s.way.trackSQL(ctx, s.prepare, args)
	defer track.track(s.way)
	rows, err := s.stmt.QueryContext(ctx, args...)
	track.TimeEnd = time.Now()
	if err != nil {
		track.Err = err
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
	track.Err = query(rows)
	return track.Err
}

// QueryRow -> Query prepared, that can be called repeatedly.
func (s *Stmt) QueryRow(ctx context.Context, query func(row *sql.Row) error, args ...any) error {
	track := s.way.trackSQL(ctx, s.prepare, args)
	defer track.track(s.way)
	row := s.stmt.QueryRowContext(ctx, args...)
	track.TimeEnd = time.Now()
	track.Err = query(row)
	return track.Err
}

// Exec -> Execute prepared, that can be called repeatedly.
func (s *Stmt) Exec(ctx context.Context, args ...any) (sql.Result, error) {
	track := s.way.trackSQL(ctx, s.prepare, args)
	defer track.track(s.way)
	result, err := s.stmt.ExecContext(ctx, args...)
	track.TimeEnd = time.Now()
	track.Err = err
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
	if s.track == nil || maker == nil {
		return s
	}
	ctx := context.Background()
	s.track.Track(ctx, s.trackDebug(ctx, maker))
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
