// About *Way

package hey

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/cd365/hey/v7/cst"
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

	// InsertOneReturningId Insert a record and return the id value of the inserted data.
	InsertOneReturningId func(r SQLReturning)

	// DatabaseType Database type value.
	DatabaseType cst.DatabaseType

	// More custom methods can be added here to achieve the same function using different databases.
}

// InsertOneAndScanInsertId INSERT INTO xxx RETURNING id
func (s *Manual) InsertOneAndScanInsertId() func(r SQLReturning) {
	return func(r SQLReturning) {
		id := cst.Id
		if replace := s.Replacer; replace != nil {
			id = replace.Get(id)
		}
		r.Returning(id)
		r.SetExecute(r.QueryRowScan())
	}
}

// InsertOneGetLastInsertId INSERT INTO xxx; sql.Result
func (s *Manual) InsertOneGetLastInsertId() func(r SQLReturning) {
	return func(r SQLReturning) {
		r.SetExecute(r.LastInsertId())
	}
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

func Postgresql() Manual {
	manual := Manual{}
	manual.DatabaseType = cst.Postgresql
	manual.Prepare = prepare63236
	manual.InsertOneReturningId = manual.InsertOneAndScanInsertId()
	return manual
}

func Sqlite() Manual {
	manual := Manual{}
	manual.DatabaseType = cst.Sqlite
	manual.InsertOneReturningId = manual.InsertOneGetLastInsertId()
	return manual
}

func Mysql() Manual {
	manual := Manual{}
	manual.DatabaseType = cst.Mysql
	manual.InsertOneReturningId = manual.InsertOneGetLastInsertId()
	return manual
}

type Config struct {
	// Manual For handling different types of databases.
	Manual Manual

	// TxOptions Start transaction options.
	TxOptions *sql.TxOptions

	// MapScanner Custom MapScan, cannot be set to nil.
	MapScanner MapScanner

	// RowsScan For scanning data into structure, cannot be set to nil.
	RowsScan func(rows *sql.Rows, result any, tag string) error

	// NewSQLLabel Create SQLLabel, cannot be set to nil.
	NewSQLLabel func(way *Way) SQLLabel

	// NewSQLWith Create SQLWith, cannot be set to nil.
	NewSQLWith func(way *Way) SQLWith

	// NewSQLSelect Create SQLSelect, cannot be set to nil.
	NewSQLSelect func(way *Way) SQLSelect

	// NewSQLTable Create SQLAlias, cannot be set to nil.
	NewSQLTable func(way *Way, table any) SQLAlias

	// NewSQLJoin Create SQLJoin, cannot be set to nil.
	NewSQLJoin func(way *Way, query SQLSelect) SQLJoin

	// NewSQLJoinOn Create SQLJoinOn, cannot be set to nil.
	NewSQLJoinOn func(way *Way) SQLJoinOn

	// NewSQLFilter Create Filter, cannot be set to nil.
	NewSQLFilter func(way *Way) Filter

	// NewSQLGroupBy Create SQLGroupBy, cannot be set to nil.
	NewSQLGroupBy func(way *Way) SQLGroupBy

	// NewSQLWindow Create SQLWindow, cannot be set to nil.
	NewSQLWindow func(way *Way) SQLWindow

	// NewSQLOrderBy Create SQLOrderBy, cannot be set to nil.
	NewSQLOrderBy func(way *Way) SQLOrderBy

	// NewSQLLimit Create SQLLimit, cannot be set to nil.
	NewSQLLimit func(way *Way) SQLLimit

	// NewSQLInsert Create SQLInsert, cannot be set to nil.
	NewSQLInsert func(way *Way) SQLInsert

	// NewSQLValues Create SQLValues, cannot be set to nil.
	NewSQLValues func(way *Way) SQLValues

	// NewSQLReturning Create SQLReturning, cannot be set to nil.
	NewSQLReturning func(way *Way, insert Maker) SQLReturning

	// NewSQLOnConflict Create SQLOnConflict, cannot be set to nil.
	NewSQLOnConflict func(way *Way, insert Maker) SQLOnConflict

	// NewSQLOnConflictUpdateSet Create SQLOnConflictUpdateSet, cannot be set to nil.
	NewSQLOnConflictUpdateSet func(way *Way) SQLOnConflictUpdateSet

	// NewSQLUpdateSet Create SQLUpdateSet, cannot be set to nil.
	NewSQLUpdateSet func(way *Way) SQLUpdateSet

	// NewSQLCase Create SQLCase, cannot be set to nil.
	NewSQLCase func(way *Way) SQLCase

	// NewSQLWindowFuncFrame Create SQLWindowFuncFrame, cannot be set to nil.
	NewSQLWindowFuncFrame func(frame string) SQLWindowFuncFrame

	// NewSQLWindowFuncOver Create SQLWindowFuncOver, cannot be set to nil.
	NewSQLWindowFuncOver func(way *Way) SQLWindowFuncOver

	// NewMulti Create Multi, cannot be set to nil.
	NewMulti func(way *Way) Multi

	// NewQuantifier Create Quantifier, cannot be set to nil.
	NewQuantifier func(filter Filter) Quantifier

	// NewExtractFilter Create ExtractFilter, cannot be set to nil.
	NewExtractFilter func(filter Filter) ExtractFilter

	// NewTimeFilter Create TimeFilter, cannot be set to nil.
	NewTimeFilter func(filter Filter) TimeFilter

	// NewTableColumn Create TableColumn, cannot be set to nil.
	NewTableColumn func(way *Way, tableName ...string) TableColumn

	// ToSQLSelect Construct a query statement, cannot be set to nil.
	ToSQLSelect func(s MakeSQL) *SQL

	// ToSQLInsert Construct an insert statement, cannot be set to nil.
	ToSQLInsert func(s MakeSQL) *SQL

	// ToSQLDelete Construct a delete statement, cannot be set to nil.
	ToSQLDelete func(s MakeSQL) *SQL

	// ToSQLUpdate Construct an update statement, cannot be set to nil.
	ToSQLUpdate func(s MakeSQL) *SQL

	// ToSQLSelectExists Construct an exists statement, cannot be set to nil.
	ToSQLSelectExists func(s MakeSQL) *SQL

	// ToSQLSelectCount Construct a count statement, cannot be set to nil.
	ToSQLSelectCount func(s MakeSQL) *SQL

	// ScanTag Scan data to tag mapping on structure.
	ScanTag string

	// LabelDelimiter Delimiter between multiple labels.
	LabelDelimiter string

	// SuffixLabel SQL Statement suffix label.
	SuffixLabel string

	// TableMethodName Custom method name to get table name.
	TableMethodName string

	// InsertForbidColumn List of columns ignored when inserting data.
	InsertForbidColumn []string

	// UpdateForbidColumn List of columns ignored when updating data.
	UpdateForbidColumn []string

	// MaxLimit Check the maximum allowed LIMIT value; a value less than or equal to 0 will be unlimited.
	MaxLimit int64

	// MaxOffset Check the maximum allowed OFFSET value; a value less than or equal to 0 will be unlimited.
	MaxOffset int64

	// DefaultPageSize The default value of limit when querying data with the page parameter for pagination.
	DefaultPageSize int64

	// DeleteRequireWhere Deletion of data must be filtered using conditions.
	DeleteRequireWhere bool

	// UpdateRequireWhere Updated data must be filtered using conditions.
	UpdateRequireWhere bool
}

func (s *Config) fully(way *Way) bool {
	if way.db != nil {
		if s.Manual.DatabaseType == cst.Empty {
			return false
		}
		if s.MapScanner == nil {
			return false
		}
		if s.RowsScan == nil {
			return false
		}
		if s.ScanTag == cst.Empty {
			return false
		}
	}

	if s.NewSQLLabel == nil {
		return false
	}
	if s.NewSQLWith == nil {
		return false
	}
	if s.NewSQLSelect == nil {
		return false
	}
	if s.NewSQLTable == nil {
		return false
	}
	if s.NewSQLJoin == nil {
		return false
	}
	if s.NewSQLJoinOn == nil {
		return false
	}
	if s.NewSQLFilter == nil {
		return false
	}
	if s.NewSQLGroupBy == nil {
		return false
	}
	if s.NewSQLWindow == nil {
		return false
	}
	if s.NewSQLOrderBy == nil {
		return false
	}
	if s.NewSQLLimit == nil {
		return false
	}

	if s.NewSQLInsert == nil {
		return false
	}
	if s.NewSQLValues == nil {
		return false
	}
	if s.NewSQLReturning == nil {
		return false
	}
	if s.NewSQLOnConflict == nil {
		return false
	}
	if s.NewSQLOnConflictUpdateSet == nil {
		return false
	}

	if s.NewSQLUpdateSet == nil {
		return false
	}
	if s.NewSQLCase == nil {
		return false
	}
	if s.NewSQLWindowFuncFrame == nil {
		return false
	}
	if s.NewSQLWindowFuncOver == nil {
		return false
	}
	if s.NewMulti == nil {
		return false
	}
	if s.NewQuantifier == nil {
		return false
	}
	if s.NewExtractFilter == nil {
		return false
	}
	if s.NewTimeFilter == nil {
		return false
	}
	if s.NewTableColumn == nil {
		return false
	}

	if s.ToSQLSelect == nil {
		return false
	}
	if s.ToSQLInsert == nil {
		return false
	}
	if s.ToSQLDelete == nil {
		return false
	}
	if s.ToSQLUpdate == nil {
		return false
	}
	if s.ToSQLSelectExists == nil {
		return false
	}
	if s.ToSQLSelectCount == nil {
		return false
	}

	return true
}

const (
	DefaultTag      = "db"
	TableMethodName = "Table"
)

// ConfigDefault Default configuration.
// If there is no highly customized configuration, please use it and set the Manual property for the specific database.
func ConfigDefault() *Config {
	return &Config{
		MapScanner: NewMapScanner(),
		RowsScan:   RowsScan,

		NewSQLLabel:               newSQLLabel,
		NewSQLWith:                newSQLWith,
		NewSQLSelect:              newSQLSelect,
		NewSQLTable:               newSQLTable,
		NewSQLJoin:                newSQLJoin,
		NewSQLJoinOn:              newSQLJoinOn,
		NewSQLFilter:              newSQLFilter,
		NewSQLGroupBy:             newSQLGroupBy,
		NewSQLWindow:              newSQLWindow,
		NewSQLOrderBy:             newSQLOrderBy,
		NewSQLLimit:               newSQLLimit,
		NewSQLInsert:              newSQLInsert,
		NewSQLValues:              newSQLValues,
		NewSQLReturning:           newSQLReturning,
		NewSQLOnConflict:          newSQLOnConflict,
		NewSQLOnConflictUpdateSet: newSQLOnConflictUpdateSet,
		NewSQLUpdateSet:           newSQLUpdateSet,
		NewSQLCase:                NewSQLCase,
		NewSQLWindowFuncFrame:     NewSQLWindowFuncFrame,
		NewSQLWindowFuncOver:      NewSQLWindowFuncOver,
		NewMulti:                  NewMulti,
		NewQuantifier:             newQuantifier,
		NewExtractFilter:          newExtractFilter,
		NewTimeFilter:             newTimeFilter,
		NewTableColumn:            NewTableColumn,

		ToSQLSelect:       toSQLSelect,
		ToSQLInsert:       toSQLInsert,
		ToSQLDelete:       toSQLDelete,
		ToSQLUpdate:       toSQLUpdate,
		ToSQLSelectExists: toSQLSelectExists,
		ToSQLSelectCount:  toSQLSelectCount,

		ScanTag:            DefaultTag,
		LabelDelimiter:     cst.Comma,
		TableMethodName:    TableMethodName,
		InsertForbidColumn: []string{cst.Id},
		UpdateForbidColumn: []string{cst.Id},
		MaxLimit:           10000,
		MaxOffset:          100000,
		DefaultPageSize:    10,
		DeleteRequireWhere: true,
		UpdateRequireWhere: true,
	}
}

type Option func(way *Way)

func WithConfig(cfg *Config) Option {
	return func(way *Way) {
		if cfg != nil && cfg.fully(way) {
			way.cfg = cfg
		}
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
	// cfg Configuration information, cannot be set to nil.
	cfg *Config

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

// NewWay Create a *Way object.
func NewWay(options ...Option) *Way {
	way := &Way{}
	for _, option := range options {
		option(way)
	}
	if way.cfg == nil {
		WithConfig(ConfigDefault())(way)
	}
	return way
}

// Config Updating the returned object's properties will not affect the configuration values that have been set.
func (s *Way) Config() *Config {
	return s.cfg
}

func (s *Way) Database() *sql.DB {
	return s.db
}

func (s *Way) Track() Track {
	return s.track
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
	replace := s.cfg.Manual.Replacer
	if replace != nil {
		return replace.Get(key)
	}
	return key
}

// ReplaceAll Get multiple identifier mapping values, return the original value if none exists.
func (s *Way) ReplaceAll(keys []string) []string {
	replace := s.cfg.Manual.Replacer
	if replace != nil {
		return replace.GetAll(keys)
	}
	return keys
}

// begin -> Open transaction.
func (s *Way) begin(ctx context.Context, conn *sql.Conn, opts ...*sql.TxOptions) (tx *Way, err error) {
	if s.db == nil {
		err = ErrDatabaseIsNil
		return
	}

	tmp := *s
	tx = &tmp

	opt := tx.cfg.TxOptions
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
	if s.track != nil {
		start := time.Now()
		tracked := trackTransaction(ctx, start)
		tracked.TxId = fmt.Sprintf("%d%s%d%s%p", start.UnixNano(), cst.Point, os.Getpid(), cst.Point, tx.transaction)
		tracked.TxState = cst.BEGIN
		tx.transaction.track = tracked
		s.track.Track(ctx, tracked)
	}
	return
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
func (s *Way) newTransaction(ctx context.Context, fx func(tx *Way) error, opts ...*sql.TxOptions) (err error) {
	tx := (*Way)(nil)
	tx, err = s.begin(ctx, nil, opts...)
	if err != nil {
		return
	}

	ok := false

	defer func() {
		if err == nil && ok {
			if e := tx.commit(); e != nil {
				err = e
			}
		} else {
			if e := tx.rollback(); e != nil {
				if err == nil {
					err = e
				}
			}
		}
	}()

	if err = fx(tx); err != nil {
		return
	}

	ok = true

	return
}

// Transaction -> Atomically executes a set of SQL statements. If a transaction has been opened, the opened transaction instance will be used.
func (s *Way) Transaction(ctx context.Context, fx func(tx *Way) error, opts ...*sql.TxOptions) error {
	if s.IsInTransaction() {
		return fx(s)
	}
	return s.newTransaction(ctx, fx, opts...)
}

// TransactionNew -> Starts a new transaction and executes a set of SQL statements atomically. Does not care whether the current transaction instance is open.
func (s *Way) TransactionNew(ctx context.Context, fx func(tx *Way) error, opts ...*sql.TxOptions) error {
	return s.newTransaction(ctx, fx, opts...)
}

// TransactionRetry Starts a new transaction and executes a set of SQL statements atomically. Does not care whether the current transaction instance is open.
func (s *Way) TransactionRetry(ctx context.Context, retries int, fx func(tx *Way) error, opts ...*sql.TxOptions) (err error) {
	for range retries {
		if err = s.newTransaction(ctx, fx, opts...); err == nil {
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
	// way Original *Way object.
	way *Way

	// stmt A prepared statement for later queries or executions.
	stmt *sql.Stmt

	// prepare SQL prepare statement.
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
func (s *Stmt) Query(ctx context.Context, query func(rows *sql.Rows) error, args ...any) (err error) {
	var tracked *MyTrack
	if track := s.way.track; track != nil {
		tracked = trackSQL(ctx, s.prepare, args)
		defer tracked.write(track, s.way)
	}
	var rows *sql.Rows
	rows, err = s.stmt.QueryContext(ctx, args...)
	if tracked != nil {
		tracked.TimeEnd = time.Now()
		tracked.Err = err
	}
	if err != nil {
		return
	}
	defer func() {
		if e := rows.Close(); e != nil && err == nil {
			err = e
		}
	}()
	err = query(rows)
	if tracked != nil {
		tracked.Err = err
	}
	return
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
		return s.way.cfg.RowsScan(rows, result, s.way.cfg.ScanTag)
	}, args...)
}

// Prepare -> Prepare SQL statement, remember to call *Stmt.Close().
func (s *Way) Prepare(ctx context.Context, query string) (stmt *Stmt, err error) {
	if s.db == nil {
		return nil, ErrDatabaseIsNil
	}
	if query == cst.Empty {
		return nil, ErrEmptySqlStatement
	}
	stmt = &Stmt{
		way:     s,
		prepare: query,
	}
	if prepare := s.cfg.Manual.Prepare; prepare != nil {
		query = prepare(query)
	}
	if s.IsInTransaction() {
		stmt.stmt, err = s.transaction.tx.PrepareContext(ctx, query)
	} else {
		stmt.stmt, err = s.db.PrepareContext(ctx, query)
	}
	if err != nil {
		return nil, err
	}
	return stmt, nil
}

// RowsScan Scan one or more rows of SQL results containing one or more columns.
func (s *Way) RowsScan(result any) func(rows *sql.Rows) error {
	return func(rows *sql.Rows) error {
		return s.cfg.RowsScan(rows, result, s.cfg.ScanTag)
	}
}

// Query -> Execute the query sql statement.
func (s *Way) Query(ctx context.Context, maker Maker, query func(rows *sql.Rows) error) (err error) {
	script := maker.ToSQL()
	var stmt *Stmt
	stmt, err = s.Prepare(ctx, script.Prepare)
	if err != nil {
		return
	}
	defer func() {
		if e := stmt.Close(); e != nil && err == nil {
			err = e
		}
	}()
	err = stmt.Query(ctx, query, script.Args...)
	return
}

// RowScan Scan a row of SQL results containing one or more columns.
func (s *Way) RowScan(dest ...any) func(row *sql.Row) error {
	return func(row *sql.Row) error {
		return row.Scan(dest...)
	}
}

// QueryRow -> Execute SQL statement and return row data, usually INSERT, UPDATE, DELETE.
func (s *Way) QueryRow(ctx context.Context, maker Maker, query func(row *sql.Row) error) (err error) {
	script := maker.ToSQL()
	var stmt *Stmt
	stmt, err = s.Prepare(ctx, script.Prepare)
	if err != nil {
		return
	}
	defer func() {
		if e := stmt.Close(); e != nil && err == nil {
			err = e
		}
	}()
	err = stmt.QueryRow(ctx, query, script.Args...)
	return
}

// Scan -> Query prepared and get all query results, through the mapping of column names and struct tags.
func (s *Way) Scan(ctx context.Context, maker Maker, result any) error {
	return s.Query(ctx, maker, s.RowsScan(result))
}

// MapScan -> Scanning the query results into []map[string]any.
func (s *Way) MapScan(ctx context.Context, maker Maker, adjusts ...AdjustColumnAnyValue) (result []map[string]any, err error) {
	err = s.Query(ctx, maker, func(rows *sql.Rows) error {
		result, err = s.cfg.MapScanner.Scan(rows, adjusts...)
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
func (s *Way) Exec(ctx context.Context, maker Maker) (result sql.Result, err error) {
	script := maker.ToSQL()
	var stmt *Stmt
	stmt, err = s.Prepare(ctx, script.Prepare)
	if err != nil {
		return
	}
	defer func() {
		if e := stmt.Close(); e != nil && err == nil {
			err = e
		}
	}()
	result, err = stmt.Exec(ctx, script.Args...)
	return
}

// Execute -> Execute the execute sql statement.
func (s *Way) Execute(ctx context.Context, maker Maker) (affectedRows int64, err error) {
	result := (sql.Result)(nil)
	result, err = s.Exec(ctx, maker)
	if err != nil {
		return
	}
	affectedRows, err = result.RowsAffected()
	return
}

// MultiExecute Execute multiple DML statements.
func (s *Way) MultiExecute(ctx context.Context, makers []Maker) (affectedRows int64, err error) {
	rows := int64(0)
	for _, maker := range makers {
		rows, err = s.Execute(ctx, maker)
		if err != nil {
			return
		}
		affectedRows += rows
	}
	return
}

// MultiStmtQuery Executing a DQL statement multiple times using the same prepared statement.
// The exit conditions are as follows:
// 1. Context was canceled or timed out.
// 2. Abnormal execution process (unexpected error occurred).
// 3. Close the argsQueue channel.
func (s *Way) MultiStmtQuery(ctx context.Context, prepare string, argsQueue <-chan []any, query func(rows *sql.Rows) error) (err error) {
	var stmt *Stmt
	defer func() {
		if stmt != nil {
			if e := stmt.Close(); e != nil && err == nil {
				err = e
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case args, ok := <-argsQueue:
			if !ok {
				return
			}
			if stmt == nil {
				stmt, err = s.Prepare(ctx, prepare)
				if err != nil {
					return
				}
			}
			err = stmt.Query(ctx, query, args...)
			if err != nil {
				return
			}
		}
	}
}

// MultiStmtExecute Executing a DML statement multiple times using the same prepared statement.
// The exit conditions are as follows:
// 1. Context was canceled or timed out.
// 2. Abnormal execution process (unexpected error occurred).
// 3. Close the argsQueue channel.
func (s *Way) MultiStmtExecute(ctx context.Context, prepare string, argsQueue <-chan []any) (affectedRows int64, err error) {
	var stmt *Stmt
	defer func() {
		if stmt != nil {
			if e := stmt.Close(); e != nil && err == nil {
				err = e
			}
		}
	}()
	rows := int64(0)
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case args, ok := <-argsQueue:
			if !ok {
				return
			}
			if stmt == nil {
				stmt, err = s.Prepare(ctx, prepare)
				if err != nil {
					return
				}
			}
			rows, err = stmt.Execute(ctx, args...)
			if err != nil {
				return
			}
			affectedRows += rows
		}
	}
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
	return s.cfg.NewSQLFilter(s).Use(filters...)
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
		panic(errors.New("hey: nil value of `choose`"))
	}
	length := len(reads)
	if length == 0 {
		panic(errors.New("hey: empty value of `reads`"))
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

// Map Store key-value to the map.
type Map interface {
	// Get Getting the value corresponding to a key from the map.
	Get(key string) (value any, has bool)

	// Set Storing key-value to the map.
	Set(key string, value any) Map

	// Has Checking if the key exists in the map.
	Has(key string) bool

	// Del Deleting map key.
	Del(key string) Map

	// Map Getting the map value.
	Map() map[string]any

	// Len Getting the map length.
	Len() int

	// IsEmpty Is the map empty?
	IsEmpty() bool

	// ToEmpty Setting the map to empty value.
	ToEmpty() Map
}

type myMap struct {
	m map[string]any
}

func (s *myMap) Get(key string) (value any, has bool) {
	value, has = s.m[key]
	return
}

func (s *myMap) Set(key string, value any) Map {
	s.m[key] = value
	return s
}

func (s *myMap) Has(key string) bool {
	_, has := s.m[key]
	return has
}

func (s *myMap) Del(key string) Map {
	delete(s.m, key)
	return s
}

func (s *myMap) Map() map[string]any {
	return s.m
}

func (s *myMap) Len() int {
	return len(s.m)
}

func (s *myMap) IsEmpty() bool {
	return s.Len() == 0
}

func (s *myMap) ToEmpty() Map {
	s.m = make(map[string]any, 1<<1)
	return s
}

func NewMap() Map {
	return &myMap{
		m: make(map[string]any, 1<<1),
	}
}
