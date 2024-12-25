package hey

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

/**
 * database helper.
 **/

type Helper interface {
	// DriverName Get the driver name.
	DriverName() []byte

	// DataSourceName Get the data source name.
	DataSourceName() []byte

	// Identifier Database tag identifier.
	Identifier() string

	// Prepare Before executing preprocessing, adjust the preprocessing SQL format.
	Prepare(prepare string) string

	// Returning When inserting a SQL statement, the auto-increment column of the inserted data is returned.
	Returning() string

	// InsertReturningId Insert a SQL statement and return the auto-increment column value of the inserted data.
	InsertReturningId(ctx context.Context, way *Way, prepare string, args []interface{}) (columnValue int64, err error)

	// IfNull Set a default value when the query field value is NULL.
	IfNull(columnName string, columnDefaultValue string) string

	// BinaryDataToHexString Convert binary data to hexadecimal string.
	BinaryDataToHexString(binaryData []byte) string
}

// MysqlHelper helper for mysql.
type MysqlHelper struct {
	driverName     []byte
	dataSourceName []byte
}

func (s *MysqlHelper) DriverName() []byte {
	return s.driverName
}

func (s *MysqlHelper) DataSourceName() []byte {
	return s.dataSourceName
}

func (s *MysqlHelper) Identifier() string {
	return "`"
}

func (s *MysqlHelper) Prepare(prepare string) string {
	return prepare
}

func (s *MysqlHelper) Returning() string {
	return ""
}

func (s *MysqlHelper) InsertReturningId(ctx context.Context, way *Way, prepare string, args []interface{}) (int64, error) {
	if way.transaction != nil {
		lg := way.LogSql(prepare, args)
		defer lg.Write()
		stmt, err := way.transaction.tx.PrepareContext(ctx, prepare)
		if err != nil {
			return 0, err
		}
		defer func() { _ = stmt.Close() }()
		result, err := stmt.ExecContext(ctx, args...)
		if err != nil {
			return 0, err
		}
		return result.LastInsertId()
	}
	stmt, err := way.Prepare(prepare)
	if err != nil {
		return 0, err
	}
	defer func() { _ = stmt.Close() }()
	result, err := stmt.ExecuteContext(ctx, args...)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (s *MysqlHelper) IfNull(columnName string, columnDefaultValue string) string {
	return fmt.Sprintf("IFNULL(%s,%s)", columnName, columnDefaultValue)
}

func (s *MysqlHelper) BinaryDataToHexString(binaryData []byte) string {
	return fmt.Sprintf("UNHEX('%s')", hex.EncodeToString(binaryData))
}

func NewMysqlHelper() *MysqlHelper {
	return &MysqlHelper{}
}

// PostgresHelper helper for postgresql.
type PostgresHelper struct {
	driverName     []byte
	dataSourceName []byte
	returning      string
}

func (s *PostgresHelper) DriverName() []byte {
	return s.driverName
}

func (s *PostgresHelper) DataSourceName() []byte {
	return s.dataSourceName
}

func (s *PostgresHelper) Identifier() string {
	return `"`
}

func (s *PostgresHelper) Prepare(prepare string) string {
	var index int64
	latest := getStringBuilder()
	defer putStringBuilder(latest)
	origin := []byte(prepare)
	length := len(origin)
	dollar := byte('$')       // $
	questionMark := byte('?') // ?
	for i := 0; i < length; i++ {
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

func (s *PostgresHelper) SetReturning(returning string) *PostgresHelper {
	s.returning = returning
	return s
}

func (s *PostgresHelper) Returning() string {
	return s.returning
}

func (s *PostgresHelper) InsertReturningId(ctx context.Context, way *Way, prepare string, args []interface{}) (int64, error) {
	var id int64
	prepare = fmt.Sprintf("%s RETURNING %s", prepare, s.Returning())
	if way.transaction != nil {
		lg := way.LogSql(prepare, args)
		defer lg.Write()
		stmt, err := way.transaction.tx.PrepareContext(ctx, prepare)
		if err != nil {
			return 0, err
		}
		defer func() { _ = stmt.Close() }()
		err = stmt.QueryRowContext(ctx, args...).Scan(&id)
		return id, err
	}
	stmt, err := way.PrepareContext(ctx, prepare)
	if err != nil {
		return 0, err
	}
	defer func() { _ = stmt.Close() }()
	err = stmt.QueryRowContext(ctx, func(rows *sql.Row) error { return rows.Scan(&id) }, args...)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (s *PostgresHelper) IfNull(columnName string, columnDefaultValue string) string {
	return fmt.Sprintf("COALESCE(%s,%s)", columnName, columnDefaultValue)
}

func (s *PostgresHelper) BinaryDataToHexString(binaryData []byte) string {
	return fmt.Sprintf(`E'\\x%s'`, hex.EncodeToString(binaryData))
}

func NewPostgresHelper() *PostgresHelper {
	return &PostgresHelper{
		returning: "id",
	}
}

// Sqlite3Helper helper for sqlite3.
type Sqlite3Helper struct {
	driverName     []byte
	dataSourceName []byte
}

func (s *Sqlite3Helper) DriverName() []byte {
	return s.driverName
}

func (s *Sqlite3Helper) DataSourceName() []byte {
	return s.dataSourceName
}

func (s *Sqlite3Helper) Identifier() string {
	return "`"
}

func (s *Sqlite3Helper) Prepare(prepare string) string {
	return prepare
}

func (s *Sqlite3Helper) Returning() string {
	return ""
}

func (s *Sqlite3Helper) InsertReturningId(ctx context.Context, way *Way, prepare string, args []interface{}) (int64, error) {
	if way.transaction != nil {
		lg := way.LogSql(prepare, args)
		defer lg.Write()
		stmt, err := way.transaction.tx.PrepareContext(ctx, prepare)
		if err != nil {
			return 0, err
		}
		defer func() { _ = stmt.Close() }()
		result, err := stmt.ExecContext(ctx, args...)
		if err != nil {
			return 0, err
		}
		return result.LastInsertId()
	}
	stmt, err := way.Prepare(prepare)
	if err != nil {
		return 0, err
	}
	defer func() { _ = stmt.Close() }()
	result, err := stmt.ExecuteContext(ctx, args...)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (s *Sqlite3Helper) IfNull(columnName string, columnDefaultValue string) string {
	return fmt.Sprintf("COALESCE(%s,%s)", columnName, columnDefaultValue)
}

func (s *Sqlite3Helper) BinaryDataToHexString(binaryData []byte) string {
	return fmt.Sprintf(`X'%s'`, hex.EncodeToString(binaryData))
}

func NewSqlite3Helper() *Sqlite3Helper {
	return &Sqlite3Helper{}
}

// ColumnAdjust Adjust the column name according to the column name.
func ColumnAdjust(columns []string, adjust func(column string) string) {
	for index, column := range columns {
		columns[index] = adjust(column)
	}
}

// ColumnCommon Identifier contain column name.
func ColumnCommon(columns []string, identifier string) {
	double := fmt.Sprintf("%s%s", identifier, identifier)
	ColumnAdjust(columns, func(column string) string {
		after := fmt.Sprintf("%s%s%s", identifier, column, identifier)
		if strings.Contains(after, double) {
			return column
		}
		return after
	})
}

// ColumnPrefix Prefix column name.
func ColumnPrefix(columns []string, prefix string) {
	ColumnAdjust(columns, func(column string) string {
		return SqlPrefix(prefix, column)
	})
}

/**
 * sql identifier.
 **/

type Fields struct {
	way    *Way
	prefix string
}

// GetPrefix Get prefix value.
func (s *Fields) GetPrefix() string {
	return s.prefix
}

// SetPrefix Set prefix value.
func (s *Fields) SetPrefix(prefix string) *Fields {
	s.prefix = prefix
	return s
}

// Batch process field names according to slices.

// Adjust Adjusting the column name according to the column name.
func (s *Fields) Adjust(columns []string, adjust func(column string) string) {
	ColumnAdjust(columns, adjust)
}

// Common Identifier contain column name.
func (s *Fields) Common(columns []string) {
	ColumnCommon(columns, s.way.cfg.Helper.Identifier())
}

// The following methods rely on return values.

// Ordinary Add sql identifier to column names. column_name -> `column_name` || "column_name" || ...
func (s *Fields) Ordinary(column string) string {
	columns := []string{column}
	s.Common(columns)
	return columns[0]
}

// Name Prefix the column name with the table name. column_name -> a.column_name
func (s *Fields) Name(column string, prefixes ...string) string {
	if len(prefixes) == 1 && prefixes[0] == "" {
		return column
	}
	prefix := LastNotEmptyString(prefixes)
	if prefix == "" {
		prefix = s.prefix
	}
	return SqlPrefix(prefix, column)
}

// Names Batch set column name prefix. [column_name, ...] -> [a.column_name, ...]
func (s *Fields) Names(columns ...string) []string {
	ColumnPrefix(columns, s.prefix)
	return columns
}

// NamePrefix Sets the prefix name of a group of fields using the specified prefix name. [column_name, ...] -> [a.column_name, ...]
func (s *Fields) NamePrefix(prefix string, columns ...string) []string {
	if prefix == "" {
		prefix = s.prefix
	}
	ColumnPrefix(columns, prefix)
	return columns
}

// With Prefix the column name with the table name. column_name -> column_name -> `a`.`column_name`
func (s *Fields) With(column string, prefixes ...string) string {
	columns := []string{column}
	s.Common(columns)
	if len(prefixes) == 1 && prefixes[0] == "" {
		return columns[0]
	}
	prefix := LastNotEmptyString(prefixes)
	if prefix == "" {
		prefix = s.prefix
	}
	if prefix != "" {
		prefixName := []string{prefix}
		s.Common(prefixName)
		prefix = prefixName[0]
	}
	return SqlPrefix(prefix, columns[0])

}

// Withs Batch set column name prefix. ["column_name", ...] -> ["`column_name`", ...] OR ["column_name", ...] -> ["`a`.`column_name`", ...]
func (s *Fields) Withs(columns ...string) []string {
	for index, column := range columns {
		columns[index] = s.With(column, s.prefix)
	}
	return columns
}

// WithsPrefix Sets the prefix name of a group of fields using the specified prefix name. ["column_name", ...] -> ["`column_name`", ...] OR ["column_name", ...] -> ["`a`.`column_name`", ...]
func (s *Fields) WithsPrefix(prefix string, columns ...string) []string {
	if prefix == "" {
		prefix = s.prefix
	}
	for index, column := range columns {
		columns[index] = s.With(column, prefix)
	}
	return columns
}

// Alias Before calling this method, you should usually call the Normal or General method.
func (s *Fields) Alias(column string, alias ...string) string {
	if column == "" {
		return ""
	}
	aliasName := LastNotEmptyString(alias)
	if aliasName == "" {
		return column
	}
	if strings.Contains(column, s.way.cfg.Helper.Identifier()) {
		common := []string{aliasName}
		s.Common(common)
		aliasName = common[0]
	}
	return SqlAlias(column, aliasName)
}

// Sum SUM(column_name)
func (s *Fields) Sum(column string, alias ...string) string {
	columns := []string{column}
	s.Adjust(columns, func(column string) string {
		return fmt.Sprintf("SUM(%s)", column)
	})
	return s.Alias(columns[0], alias...)
}

// Max MAX(column_name)
func (s *Fields) Max(column string, alias ...string) string {
	columns := []string{column}
	s.Adjust(columns, func(column string) string {
		return fmt.Sprintf("MAX(%s)", column)
	})
	return s.Alias(columns[0], alias...)
}

// Min MIN(column_name)
func (s *Fields) Min(column string, alias ...string) string {
	columns := []string{column}
	s.Adjust(columns, func(column string) string {
		return fmt.Sprintf("MIN(%s)", column)
	})
	return s.Alias(columns[0], alias...)
}

// Avg AVG(column_name)
func (s *Fields) Avg(column string, alias ...string) string {
	columns := []string{column}
	s.Adjust(columns, func(column string) string {
		return fmt.Sprintf("AVG(%s)", column)
	})
	return s.Alias(columns[0], alias...)
}

// Count COUNT(column_name)
func (s *Fields) Count(column string, alias ...string) string {
	columns := []string{column}
	s.Adjust(columns, func(column string) string {
		return fmt.Sprintf("COUNT(%s)", column)
	})
	return s.Alias(columns[0], alias...)
}

// IfNull If the value is NULL, set the default value.
func (s *Fields) IfNull(column string, columnDefaultValue string, alias ...string) string {
	return s.Alias(s.way.cfg.Helper.IfNull(column, columnDefaultValue), alias...)
}

// IfNullSum IF_NULL(SUM(column),0)[ AS column_name]
func (s *Fields) IfNullSum(column string, alias ...string) string {
	return s.Alias(s.IfNull(s.Sum(column), "0"), alias...)
}

// IfNullMax IF_NULL(MAX(column),0)[ AS column_name]
func (s *Fields) IfNullMax(column string, alias ...string) string {
	return s.Alias(s.IfNull(s.Max(column), "0"), alias...)
}

// IfNullMin IF_NULL(MIN(column),0)[ AS column_name]
func (s *Fields) IfNullMin(column string, alias ...string) string {
	return s.Alias(s.IfNull(s.Min(column), "0"), alias...)
}

func NewFields(way *Way) *Fields {
	return &Fields{
		way: way,
	}
}

/**
 * sql window functions.
 **/

// WindowFunc sql window function.
type WindowFunc struct {
	// Helper Database helper.
	Helper Helper

	// withFunc The window function used.
	withFunc string

	// partition Setting up window partitions.
	partition []string

	// order Sorting data within a group.
	order []string

	// windowFrame Window frame clause. `ROWS` or `RANGE`.
	windowFrame string

	// alias Serial number column alias.
	alias string
}

// WithFunc Using custom function. for example: CUME_DIST(), PERCENT_RANK(), PERCENTILE_CONT(), PERCENTILE_DISC()...
func (s *WindowFunc) WithFunc(withFunc string) *WindowFunc {
	s.withFunc = withFunc
	return s
}

// RowNumber ROW_NUMBER() Assign a unique serial number to each row, in the order specified, starting with 1.
func (s *WindowFunc) RowNumber() *WindowFunc {
	return s.WithFunc("ROW_NUMBER()")
}

// Rank RANK() Assign a rank to each row, if there are duplicate values, the rank is skipped.
func (s *WindowFunc) Rank() *WindowFunc {
	return s.WithFunc("RANK()")
}

// DenseRank DENSE_RANK() Similar to RANK(), but does not skip rankings.
func (s *WindowFunc) DenseRank() *WindowFunc {
	return s.WithFunc("DENSE_RANK()")
}

// Ntile NTILE() Divide the rows in the window into n buckets and assign a bucket number to each row.
func (s *WindowFunc) Ntile(buckets int64) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("NTILE(%d)", buckets))
}

// Sum SUM() Returns the sum of all rows in the window.
func (s *WindowFunc) Sum(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("SUM(%s)", column))
}

// Max MAX() Returns the maximum value within the window.
func (s *WindowFunc) Max(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("MAX(%s)", column))
}

// Min MIN() Returns the minimum value within the window.
func (s *WindowFunc) Min(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("MIN(%s)", column))
}

// Avg AVG() Returns the average of all rows in the window.
func (s *WindowFunc) Avg(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("AVG(%s)", column))
}

// Count COUNT() Returns the number of rows in the window.
func (s *WindowFunc) Count(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("COUNT(%s)", column))
}

// Lag LAG() Returns the value of the row before the current row.
func (s *WindowFunc) Lag(column string, offset int64, defaultValue any) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("LAG(%s, %d, %s)", column, offset, ArgString(s.Helper, defaultValue)))
}

// Lead LEAD() Returns the value of a row after the current row.
func (s *WindowFunc) Lead(column string, offset int64, defaultValue any) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("LEAD(%s, %d, %s)", column, offset, ArgString(s.Helper, defaultValue)))
}

// NthValue NTH_VALUE() The Nth value can be returned according to the specified order. This is very useful when you need to get data at a specific position.
func (s *WindowFunc) NthValue(column string, LineNumber int64) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("NTH_VALUE(%s, %d)", column, LineNumber))
}

// FirstValue FIRST_VALUE() Returns the value of the first row in the window.
func (s *WindowFunc) FirstValue(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("FIRST_VALUE(%s)", column))
}

// LastValue LAST_VALUE() Returns the value of the last row in the window.
func (s *WindowFunc) LastValue(column string) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("LAST_VALUE(%s)", column))
}

// Partition The OVER clause defines window partitions so that the window function is calculated independently in each partition.
func (s *WindowFunc) Partition(column ...string) *WindowFunc {
	s.partition = append(s.partition, column...)
	return s
}

// Asc Define the sorting within the partition so that the window function is calculated in order.
func (s *WindowFunc) Asc(column string) *WindowFunc {
	s.order = append(s.order, fmt.Sprintf("%s %s", column, SqlAsc))
	return s
}

// Desc Define the sorting within the partition so that the window function is calculated in descending order.
func (s *WindowFunc) Desc(column string) *WindowFunc {
	s.order = append(s.order, fmt.Sprintf("%s %s", column, SqlDesc))
	return s
}

// WindowFrame Set custom window frame clause.
func (s *WindowFunc) WindowFrame(windowFrame string) *WindowFunc {
	s.windowFrame = windowFrame
	return s
}

// Alias Set the alias of the field that uses the window function.
func (s *WindowFunc) Alias(alias string) *WindowFunc {
	s.alias = alias
	return s
}

// Result Query column expressions.
func (s *WindowFunc) Result() string {
	if s.withFunc == EmptyString || s.partition == nil || s.order == nil || s.alias == EmptyString {
		panic("hey: the SQL window function parameters are incomplete.")
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(s.withFunc)
	b.WriteString(" OVER ( PARTITION BY ")
	b.WriteString(strings.Join(s.partition, ", "))
	b.WriteString(" ORDER BY ")
	b.WriteString(strings.Join(s.order, ", "))
	if s.windowFrame != "" {
		b.WriteString(" ")
		b.WriteString(s.windowFrame)
	}
	b.WriteString(" ) AS ")
	b.WriteString(s.alias)
	return b.String()
}

func NewWindowFunc(way *Way, alias ...string) *WindowFunc {
	return &WindowFunc{
		Helper: way.cfg.Helper,
		alias:  LastNotEmptyString(alias),
	}
}
