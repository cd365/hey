package hey

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

/**
 * database helper.
 **/

type Identifier interface {
	Identify() string
	AddIdentify(keys []string) []string
	DelIdentify(keys []string) []string
}

type identifier struct {
	identify   string
	add        map[string]string
	addRWMutex *sync.RWMutex
	del        map[string]string
	delRWMutex *sync.RWMutex
}

func (s *identifier) Identify() string {
	return s.identify
}

func (s *identifier) getAdd(key string) (value string, ok bool) {
	s.addRWMutex.RLock()
	defer s.addRWMutex.RUnlock()
	value, ok = s.add[key]
	return
}

func (s *identifier) setAdd(key string, value string) {
	s.addRWMutex.Lock()
	defer s.addRWMutex.Unlock()
	s.add[key] = value
	return
}

func (s *identifier) getDel(key string) (value string, ok bool) {
	s.delRWMutex.RLock()
	defer s.delRWMutex.RUnlock()
	value, ok = s.del[key]
	return
}

func (s *identifier) setDel(key string, value string) {
	s.delRWMutex.Lock()
	defer s.delRWMutex.Unlock()
	s.del[key] = value
	return
}

func (s *identifier) AddIdentify(keys []string) []string {
	length := len(keys)
	identify := s.identify
	if length == 0 || identify == EmptyString {
		return keys
	}
	result := make([]string, 0, length)
	for i := 0; i < length; i++ {
		if keys[i] == EmptyString {
			result = append(result, keys[i])
			continue
		}
		value, ok := s.getAdd(keys[i])
		if !ok {
			value = strings.ReplaceAll(keys[i], identify, EmptyString)
			value = strings.TrimSpace(value)
			values := strings.Split(value, SqlPoint)
			for k, v := range values {
				values[k] = fmt.Sprintf("%s%s%s", identify, v, identify)
			}
			value = strings.Join(values, SqlPoint)
			s.setAdd(keys[i], value)
		}
		result = append(result, value)
	}
	return result
}

func (s *identifier) DelIdentify(keys []string) []string {
	length := len(keys)
	identify := s.identify
	if length == 0 || identify == EmptyString {
		return keys
	}
	result := make([]string, length)
	for i := 0; i < length; i++ {
		value, ok := s.getDel(keys[i])
		if !ok {
			value = strings.ReplaceAll(keys[i], identify, EmptyString)
			s.setDel(keys[i], value)
		}
		result[i] = value
	}
	return result
}

func NewIdentifier(identify string) Identifier {
	return &identifier{
		identify:   identify,
		add:        make(map[string]string, 512),
		addRWMutex: &sync.RWMutex{},
		del:        make(map[string]string, 512),
		delRWMutex: &sync.RWMutex{},
	}
}

type Helper interface {
	// DriverName Get the driver name.
	DriverName() []byte

	// DataSourceName Get the data source name.
	DataSourceName() []byte

	// SetIdentifier Custom Identifier.
	SetIdentifier(identifier Identifier) Helper

	Identifier

	// SetPrepare Custom method.
	SetPrepare(prepare func(prepare string) string) Helper

	// Prepare Before executing preprocessing, adjust the preprocessing SQL format.
	Prepare(prepare string) string

	// SetIfNull Custom method.
	SetIfNull(ifNull func(columnName string, columnDefaultValue string) string) Helper

	// IfNull Set a default value when the query field value is NULL.
	IfNull(columnName string, columnDefaultValue string) string

	// SetBinaryDataToHexString Custom method.
	SetBinaryDataToHexString(binaryDataToHexString func(binaryData []byte) string) Helper

	// BinaryDataToHexString Convert binary data to hexadecimal string.
	BinaryDataToHexString(binaryData []byte) string
}

// MysqlHelper helper for mysql.
type MysqlHelper struct {
	driverName     []byte
	dataSourceName []byte

	Identifier

	prepare               func(prepare string) string
	ifNull                func(columnName string, columnDefaultValue string) string
	binaryDataToHexString func(binaryData []byte) string
}

func (s *MysqlHelper) DriverName() []byte {
	return s.driverName
}

func (s *MysqlHelper) DataSourceName() []byte {
	return s.dataSourceName
}

func (s *MysqlHelper) SetIdentifier(identifier Identifier) Helper {
	if identifier != nil {
		s.Identifier = identifier
	}
	return s
}

func (s *MysqlHelper) SetPrepare(prepare func(prepare string) string) Helper {
	if prepare != nil {
		s.prepare = prepare
	}
	return s
}

func (s *MysqlHelper) Prepare(prepare string) string {
	if s.prepare != nil {
		return s.prepare(prepare)
	}
	return prepare
}

func (s *MysqlHelper) SetIfNull(ifNull func(columnName string, columnDefaultValue string) string) Helper {
	if ifNull != nil {
		s.ifNull = ifNull
	}
	return s
}

func (s *MysqlHelper) IfNull(columnName string, columnDefaultValue string) string {
	if s.ifNull != nil {
		return s.ifNull(columnName, columnDefaultValue)
	}
	return fmt.Sprintf("IFNULL(%s,%s)", columnName, columnDefaultValue)
}

func (s *MysqlHelper) SetBinaryDataToHexString(binaryDataToHexString func(binaryData []byte) string) Helper {
	if binaryDataToHexString != nil {
		s.binaryDataToHexString = binaryDataToHexString
	}
	return s
}

func (s *MysqlHelper) BinaryDataToHexString(binaryData []byte) string {
	if s.binaryDataToHexString != nil {
		return s.binaryDataToHexString(binaryData)
	}
	return fmt.Sprintf("UNHEX('%s')", hex.EncodeToString(binaryData))
}

func NewMysqlHelper() *MysqlHelper {
	return &MysqlHelper{
		Identifier: NewIdentifier("`"),
	}
}

// PostgresHelper helper for postgresql.
type PostgresHelper struct {
	driverName     []byte
	dataSourceName []byte

	Identifier

	prepare               func(prepare string) string
	ifNull                func(columnName string, columnDefaultValue string) string
	binaryDataToHexString func(binaryData []byte) string
}

func (s *PostgresHelper) DriverName() []byte {
	return s.driverName
}

func (s *PostgresHelper) DataSourceName() []byte {
	return s.dataSourceName
}

func (s *PostgresHelper) SetIdentifier(identifier Identifier) Helper {
	if identifier != nil {
		s.Identifier = identifier
	}
	return s
}

func (s *PostgresHelper) SetPrepare(prepare func(prepare string) string) Helper {
	if prepare != nil {
		s.prepare = prepare
	}
	return s
}

func (s *PostgresHelper) Prepare(prepare string) string {
	if s.prepare != nil {
		return s.prepare(prepare)
	}
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

func (s *PostgresHelper) SetIfNull(ifNull func(columnName string, columnDefaultValue string) string) Helper {
	if ifNull != nil {
		s.ifNull = ifNull
	}
	return s
}

func (s *PostgresHelper) IfNull(columnName string, columnDefaultValue string) string {
	if s.ifNull != nil {
		return s.ifNull(columnName, columnDefaultValue)
	}
	return fmt.Sprintf("COALESCE(%s,%s)", columnName, columnDefaultValue)
}

func (s *PostgresHelper) SetBinaryDataToHexString(binaryDataToHexString func(binaryData []byte) string) Helper {
	if binaryDataToHexString != nil {
		s.binaryDataToHexString = binaryDataToHexString
	}
	return s
}

func (s *PostgresHelper) BinaryDataToHexString(binaryData []byte) string {
	if s.binaryDataToHexString != nil {
		return s.binaryDataToHexString(binaryData)
	}
	return fmt.Sprintf(`E'\\x%s'`, hex.EncodeToString(binaryData))
}

func NewPostgresHelper() *PostgresHelper {
	return &PostgresHelper{
		Identifier: NewIdentifier(`"`),
	}
}

// Sqlite3Helper helper for sqlite3.
type Sqlite3Helper struct {
	driverName     []byte
	dataSourceName []byte

	Identifier

	prepare               func(prepare string) string
	ifNull                func(columnName string, columnDefaultValue string) string
	binaryDataToHexString func(binaryData []byte) string
}

func (s *Sqlite3Helper) DriverName() []byte {
	return s.driverName
}

func (s *Sqlite3Helper) DataSourceName() []byte {
	return s.dataSourceName
}

func (s *Sqlite3Helper) SetIdentifier(identifier Identifier) Helper {
	if identifier != nil {
		s.Identifier = identifier
	}
	return s
}

func (s *Sqlite3Helper) SetPrepare(prepare func(prepare string) string) Helper {
	if prepare != nil {
		s.prepare = prepare
	}
	return s
}

func (s *Sqlite3Helper) Prepare(prepare string) string {
	if s.prepare != nil {
		return s.prepare(prepare)
	}
	return prepare
}

func (s *Sqlite3Helper) SetIfNull(ifNull func(columnName string, columnDefaultValue string) string) Helper {
	if ifNull != nil {
		s.ifNull = ifNull
	}
	return s
}

func (s *Sqlite3Helper) IfNull(columnName string, columnDefaultValue string) string {
	if s.ifNull != nil {
		return s.ifNull(columnName, columnDefaultValue)
	}
	return fmt.Sprintf("COALESCE(%s,%s)", columnName, columnDefaultValue)
}

func (s *Sqlite3Helper) SetBinaryDataToHexString(binaryDataToHexString func(binaryData []byte) string) Helper {
	if binaryDataToHexString != nil {
		s.binaryDataToHexString = binaryDataToHexString
	}
	return s
}

func (s *Sqlite3Helper) BinaryDataToHexString(binaryData []byte) string {
	if s.binaryDataToHexString != nil {
		return s.binaryDataToHexString(binaryData)
	}
	return fmt.Sprintf(`X'%s'`, hex.EncodeToString(binaryData))
}

func NewSqlite3Helper() *Sqlite3Helper {
	return &Sqlite3Helper{
		Identifier: NewIdentifier("`"),
	}
}

/**
 * sql identifier.
 **/

type AdjustColumn struct {
	alias string
	way   *Way
}

// Alias Get the alias name value.
func (s *AdjustColumn) Alias() string {
	return s.alias
}

// SetAlias Set the alias name value.
func (s *AdjustColumn) SetAlias(alias string) *AdjustColumn {
	s.alias = alias
	return s
}

// Adjust Batch adjust columns.
func (s *AdjustColumn) Adjust(adjust func(column string) string, columns ...string) []string {
	if adjust != nil {
		for index, column := range columns {
			columns[index] = adjust(column)
		}
	}
	return columns
}

// ColumnAll Add table name prefix to column names in batches.
func (s *AdjustColumn) ColumnAll(columns ...string) []string {
	if s.alias == EmptyString {
		return columns
	}
	prefix := fmt.Sprintf("%s%s", s.alias, SqlPoint)
	for index, column := range columns {
		if !strings.HasPrefix(column, prefix) {
			columns[index] = fmt.Sprintf("%s%s", prefix, column)
		}
	}
	return columns
}

// Column Add table name prefix to single column name, allowing column alias to be set.
func (s *AdjustColumn) Column(column string, aliases ...string) string {
	return SqlAlias(s.ColumnAll(column)[0], LastNotEmptyString(aliases))
}

// Sum SUM(column[, alias])
func (s *AdjustColumn) Sum(column string, aliases ...string) string {
	return SqlAlias(fmt.Sprintf("SUM(%s)", s.Column(column)), LastNotEmptyString(aliases))
}

// Max MAX(column[, alias])
func (s *AdjustColumn) Max(column string, aliases ...string) string {
	return SqlAlias(fmt.Sprintf("MAX(%s)", s.Column(column)), LastNotEmptyString(aliases))
}

// Min MIN(column[, alias])
func (s *AdjustColumn) Min(column string, aliases ...string) string {
	return SqlAlias(fmt.Sprintf("MIN(%s)", s.Column(column)), LastNotEmptyString(aliases))
}

// Avg AVG(column[, alias])
func (s *AdjustColumn) Avg(column string, aliases ...string) string {
	return SqlAlias(fmt.Sprintf("AVG(%s)", s.Column(column)), LastNotEmptyString(aliases))
}

// Count Example
// Count(): `COUNT(*) AS counts`
// Count("total"): `COUNT(*) AS total`
// Count("1", "total"): `COUNT(1) AS total`
// Count("id", "counts"): `COUNT(id) AS counts`
func (s *AdjustColumn) Count(counts ...string) string {
	count := "COUNT(*)"
	length := len(counts)
	if length == 0 {
		// using default expression: `COUNT(*) AS counts`
		return SqlAlias(count, s.way.cfg.Helper.AddIdentify([]string{DefaultAliasNameCount})[0])
	}
	if length == 1 && counts[0] != EmptyString {
		// only set alias name
		return SqlAlias(count, counts[0])
	}
	// set COUNT function parameters and alias name
	countAlias := s.way.cfg.Helper.AddIdentify([]string{DefaultAliasNameCount})[0]
	field := false
	for i := 0; i < length; i++ {
		if counts[i] == EmptyString {
			continue
		}
		if field {
			countAlias = counts[i]
			break
		}
		count, field = fmt.Sprintf("COUNT(%s)", counts[i]), true
	}
	return SqlAlias(count, countAlias)
}

// IfNull If the value is NULL, set the default value.
func (s *AdjustColumn) IfNull(column string, defaultValue string, aliases ...string) string {
	return SqlAlias(s.way.cfg.Helper.IfNull(s.Column(column), defaultValue), LastNotEmptyString(aliases))
}

// IfNullSum IF_NULL(SUM(column),0)[ AS column_name]
func (s *AdjustColumn) IfNullSum(column string, aliases ...string) string {
	return SqlAlias(s.IfNull(s.Sum(column), "0"), LastNotEmptyString(aliases))
}

// IfNullMax IF_NULL(MAX(column),0)[ AS column_name]
func (s *AdjustColumn) IfNullMax(column string, aliases ...string) string {
	return SqlAlias(s.IfNull(s.Max(column), "0"), LastNotEmptyString(aliases))
}

// IfNullMin IF_NULL(MIN(column),0)[ AS column_name]
func (s *AdjustColumn) IfNullMin(column string, aliases ...string) string {
	return SqlAlias(s.IfNull(s.Min(column), "0"), LastNotEmptyString(aliases))
}

// IfNullAvg IF_NULL(AVG(column),0)[ AS column_name]
func (s *AdjustColumn) IfNullAvg(column string, aliases ...string) string {
	return SqlAlias(s.IfNull(s.Avg(column), "0"), LastNotEmptyString(aliases))
}

func NewAdjustColumn(way *Way, aliases ...string) *AdjustColumn {
	return &AdjustColumn{
		alias: LastNotEmptyString(aliases),
		way:   way,
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

func NewWindowFunc(way *Way, aliases ...string) *WindowFunc {
	return &WindowFunc{
		Helper: way.cfg.Helper,
		alias:  LastNotEmptyString(aliases),
	}
}
