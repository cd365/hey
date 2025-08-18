package hey

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"maps"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cd365/logger/v9"
)

// DiscardDuplicate Slice deduplication.
func DiscardDuplicate[T comparable](discard func(tmp T) bool, dynamic ...T) (result []T) {
	length := len(dynamic)
	mp := make(map[T]*struct{}, length)
	ok := false
	result = make([]T, 0, length)
	for i := range length {
		if _, ok = mp[dynamic[i]]; ok {
			continue
		}
		if discard != nil {
			if discard(dynamic[i]) {
				continue
			}
		}
		mp[dynamic[i]] = &struct{}{}
		result = append(result, dynamic[i])
	}
	return
}

// MergeAssoc Merge maps.
func MergeAssoc[K comparable, V any](values ...map[K]V) map[K]V {
	length := len(values)
	result := make(map[K]V, 8)
	for i := range length {
		maps.Copy(result, values[i])
	}
	return result
}

// MergeArray Merge slices.
func MergeArray[V any](values ...[]V) []V {
	length := len(values)
	result := make([]V, 0)
	for i := range length {
		if i == 0 {
			result = values[i]
			continue
		}
		result = append(result, values[i]...)
	}
	return result
}

// AssocToAssoc Create a map based on another map.
func AssocToAssoc[K comparable, V any, X comparable, Y any](values map[K]V, fc func(k K, v V) (X, Y)) map[X]Y {
	if fc == nil {
		return nil
	}
	result := make(map[X]Y, len(values))
	for key, value := range values {
		k, v := fc(key, value)
		result[k] = v
	}
	return result
}

// AssocToArray Create a slice from a map.
func AssocToArray[K comparable, V any, W any](values map[K]V, fc func(k K, v V) W) []W {
	if fc == nil {
		return nil
	}
	length := len(values)
	result := make([]W, length)
	for index, value := range values {
		result = append(result, fc(index, value))
	}
	return result
}

// ArrayToAssoc Create a map from a slice.
func ArrayToAssoc[V any, K comparable, W any](values []V, fc func(v V) (K, W)) map[K]W {
	if fc == nil {
		return nil
	}
	length := len(values)
	result := make(map[K]W, length)
	for i := range length {
		k, v := fc(values[i])
		result[k] = v
	}
	return result
}

// ArrayToArray Create a slice from another slice.
func ArrayToArray[V any, W any](values []V, fc func(k int, v V) W) []W {
	if fc == nil {
		return nil
	}
	result := make([]W, len(values))
	for index, value := range values {
		result[index] = fc(index, value)
	}
	return result
}

// AssocDiscard Delete some elements from the map.
func AssocDiscard[K comparable, V any](values map[K]V, discard func(k K, v V) bool) map[K]V {
	if values == nil || discard == nil {
		return values
	}
	result := make(map[K]V, len(values))
	for index, value := range values {
		if !discard(index, value) {
			result[index] = value
		}
	}
	return result
}

// ArrayDiscard Delete some elements from a slice.
func ArrayDiscard[V any](values []V, discard func(k int, v V) bool) []V {
	if values == nil || discard == nil {
		return values
	}
	result := make([]V, 0, len(values))
	for index, value := range values {
		if !discard(index, value) {
			result = append(result, value)
		}
	}
	return result
}

type RowsScanMakeSliceLength string

const (
	MakerScanAllMakeSliceLength RowsScanMakeSliceLength = "maker_scan_all_make_slice_length"
)

// MakerScanAll Rows scan to any struct, based on struct scan data.
func MakerScanAll[V any](ctx context.Context, way *Way, maker Maker, scan func(rows *sql.Rows, v *V) error) ([]*V, error) {
	if way == nil || maker == nil || scan == nil {
		return make([]*V, 0), nil
	}
	script := maker.ToSQL()
	if script == nil || script.IsEmpty() {
		return make([]*V, 0), nil
	}
	length := 16
	if ctx == nil {
		ctx = context.Background()
	} else {
		if tmp := ctx.Value(MakerScanAllMakeSliceLength); tmp != nil {
			if val, ok := tmp.(int); ok && val > 0 && val <= 10000 {
				length = val
			}
		}
	}
	var err error
	var group []V
	result := make([]*V, 0, length)
	err = way.Query(ctx, script, func(rows *sql.Rows) error {
		index := 0
		for rows.Next() {
			if index == 0 {
				group = make([]V, length)
			}
			if err = scan(rows, &group[index]); err != nil {
				return err
			}
			result = append(result, &group[index])
			index++
			if index == length {
				index = 0
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// MakerScanOne Rows scan to any struct, based on struct scan data.
// Scan a piece of data, don't forget to use LIMIT 1 in your SQL statement.
func MakerScanOne[V any](ctx context.Context, way *Way, maker Maker, scan func(rows *sql.Rows, v *V) error) (*V, error) {
	lists, err := MakerScanAll(context.WithValue(ctx, MakerScanAllMakeSliceLength, 1), way, maker, scan)
	if err != nil {
		return nil, err
	}
	if len(lists) > 0 {
		return lists[0], nil
	}
	return nil, ErrNoRows
}

const (
	StrDefaultTag = "db"

	StrEmpty = ""
	Str36    = "$"
	Str37    = "%"
	Str39    = "'"
	Str43    = "+"
	Str45    = "-"
	Str47    = "/"
	Str63    = "?"
	StrComma = ","
	StrPoint = "."
	StrStar  = "*"

	StrSpace      = " "
	StrCommaSpace = ", "

	StrNull = "NULL"
	StrAs   = "AS"
	StrAsc  = "ASC"
	StrDesc = "DESC"

	StrUnion        = "UNION"
	StrUnionAll     = "UNION ALL"
	StrIntersect    = "INTERSECT"
	StrIntersectAll = "INTERSECT ALL"
	StrExpect       = "EXCEPT"
	StrExpectAll    = "EXCEPT ALL"

	StrJoinInner = "INNER JOIN"
	StrJoinLeft  = "LEFT JOIN"
	StrJoinRight = "RIGHT JOIN"

	StrAnd = "AND"
	StrOr  = "OR"
	StrNot = "NOT"

	StrPlaceholder   = "?"
	StrEqual         = "="
	StrNotEqual      = "<>"
	StrLessThan      = "<"
	StrLessThanEqual = "<="
	StrMoreThan      = ">"
	StrMoreThanEqual = ">="

	StrAll = "ALL"
	StrAny = "ANY"

	StrLeftSmallBracket  = "("
	StrRightSmallBracket = ")"

	StrCoalesce = "COALESCE"
	StrCount    = "COUNT"
	StrAvg      = "AVG"
	StrMax      = "MAX"
	StrMin      = "MIN"
	StrSum      = "SUM"

	StrDistinct = "DISTINCT"

	StrSelect    = "SELECT"
	StrInsert    = "INSERT"
	StrUpdate    = "UPDATE"
	StrDelete    = "DELETE"
	StrFrom      = "FROM"
	StrInto      = "INTO"
	StrValues    = "VALUES"
	StrSet       = "SET"
	StrWhere     = "WHERE"
	StrReturning = "RETURNING"

	StrBetween     = "BETWEEN"
	StrConflict    = "CONFLICT"
	StrDo          = "DO"
	StrExcluded    = "EXCLUDED"
	StrExists      = "EXISTS"
	StrGroupBy     = "GROUP BY"
	StrHaving      = "HAVING"
	StrIn          = "IN"
	StrIs          = "IS"
	StrLike        = "LIKE"
	StrLimit       = "LIMIT"
	StrNothing     = "NOTHING"
	StrOffset      = "OFFSET"
	StrOn          = "ON"
	StrOrderBy     = "ORDER BY"
	StrOver        = "OVER"
	StrPartitionBy = "PARTITION BY"
	StrUsing       = "USING"
	StrWith        = "WITH"
	StrRecursive   = "RECURSIVE"

	StrCase = "CASE"
	StrWhen = "WHEN"
	StrThen = "THEN"
	StrElse = "ELSE"
	StrEnd  = "END"

	StrRange = "RANGE"
	StrRows  = "ROWS"
)

type Err string

func (s Err) Error() string {
	return string(s)
}

const (
	// ErrEmptyPrepare The prepared value executed is an empty string.
	ErrEmptyPrepare = Err("hey: the prepared value executed is an empty string")

	// ErrNoRows Error no rows.
	ErrNoRows = Err("hey: no rows")

	// ErrNoRowsAffected Error no rows affected.
	ErrNoRowsAffected = Err("hey: no rows affected")

	// ErrTransactionIsNil Error transaction isn't started.
	ErrTransactionIsNil = Err("hey: transaction is nil")
)

// Maker Build SQL fragments or SQL statements, which may include corresponding parameter lists.
// Notice: The ToSQL method must return a non-nil value for *SQL.
type Maker interface {
	// ToSQL Construct SQL statements that may contain parameters, the return value cannot be nil.
	ToSQL() *SQL
}

// SQL Prepare SQL statements and parameter lists corresponding to placeholders.
type SQL struct {
	// Prepare SQL fragments or SQL statements, which may contain SQL placeholders.
	Prepare string

	// Args The corresponding parameter list of the placeholder list.
	Args []any
}

func NewSQL(prepare string, args ...any) *SQL {
	return &SQL{
		Prepare: prepare,
		Args:    args,
	}
}

func NewEmptySQL() *SQL {
	return NewSQL(StrEmpty)
}

// CopySQL Copy *SQL.
func CopySQL(src *SQL) (dst *SQL) {
	dst = NewEmptySQL()
	dst.Prepare = src.Prepare
	dst.Args = make([]any, len(src.Args))
	copy(dst.Args, src.Args)
	return dst
}

// Copy Make a copy.
func (s *SQL) Copy() *SQL {
	return CopySQL(s)
}

// IsEmpty Used to determine whether the current SQL fragments or SQL statements is empty string.
func (s *SQL) IsEmpty() bool {
	return s.Prepare == StrEmpty
}

// ToEmpty Set Prepare, Args to empty value.
func (s *SQL) ToEmpty() *SQL {
	s.Prepare, s.Args = StrEmpty, nil
	return s
}

// ToSQL Implementing the Maker interface.
func (s *SQL) ToSQL() *SQL {
	return s
}

// Guess the given parameter value and convert it into SQL script and parameter lists.
func any2sql(i any) *SQL {
	result := NewEmptySQL()
	switch value := i.(type) {
	case bool:
		result = NewSQL(fmt.Sprintf("%t", value))
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		result = NewSQL(fmt.Sprintf("%d", value))
	case float32:
		result = NewSQL(strconv.FormatFloat(float64(value), 'f', -1, 64))
	case float64:
		result = NewSQL(strconv.FormatFloat(value, 'f', -1, 64))
	case string:
		result.Prepare = value
	case *SQL:
		if value != nil {
			result = value
		}
	case Maker:
		if tmp := value.ToSQL(); tmp != nil {
			result = tmp
		}
	default:
		v := reflect.ValueOf(i)
		t := v.Type()
		k := t.Kind()
		for k == reflect.Ptr {
			if v.IsNil() {
				return result
			}
			v = v.Elem()
			t = v.Type()
			k = t.Kind()
		}
		switch k {
		case reflect.Bool, reflect.Float32, reflect.Float64, reflect.String,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return any2sql(v.Interface())
		default: // Other types of values are invalid.
		}
	}
	return result
}

// nil1any2sql Prioritize converting nil to NULL.
func nil1any2sql(i any) *SQL {
	if i == nil {
		return NewSQL(StrNull)
	}
	return any2sql(i)
}

var poolStringBuilder = &sync.Pool{
	New: func() any { return &strings.Builder{} },
}

func poolGetStringBuilder() *strings.Builder {
	return poolStringBuilder.Get().(*strings.Builder)
}

func poolPutStringBuilder(s *strings.Builder) {
	s.Reset()
	poolStringBuilder.Put(s)
}

// JoinMaker Concatenate multiple SQL scripts and their parameter lists using a specified delimiter.
func JoinMaker(separator string, makers ...Maker) Maker {
	script := NewEmptySQL()
	builder := poolGetStringBuilder()
	defer poolPutStringBuilder(builder)
	num := 0
	for _, element := range makers {
		if element == nil {
			continue
		}
		shard := element.ToSQL()
		if shard != nil && !shard.IsEmpty() {
			num++
			if num > 1 {
				builder.WriteString(separator)
			}
			builder.WriteString(shard.Prepare)
			script.Args = append(script.Args, shard.Args...)
		}
	}
	script.Prepare = builder.String()
	return script
}

func headBody(head any, body ...any) []any {
	result := make([]any, 0, len(body)+1)
	result = append(result, head)
	result = append(result, body...)
	return result
}

// JoinSQL Use `separator` to concatenate multiple SQL scripts and parameters.
func JoinSQL(separator string, values ...any) *SQL {
	length := len(values)
	makers := make([]Maker, length)
	for i := range length {
		makers[i] = any2sql(values[i])
	}
	return JoinMaker(separator, makers...).ToSQL()
}

// JoinSQLComma Use StrComma to concatenate multiple SQL scripts and parameters.
func JoinSQLComma(values ...any) *SQL {
	return JoinSQL(StrComma, values...)
}

// JoinSQLEmpty Use StrEmpty to concatenate multiple SQL scripts and parameters.
func JoinSQLEmpty(values ...any) *SQL {
	return JoinSQL(StrEmpty, values...)
}

// JoinSQLSpace Use StrSpace to concatenate multiple SQL scripts and parameters.
func JoinSQLSpace(values ...any) *SQL {
	return JoinSQL(StrSpace, values...)
}

// JoinSQLCommaSpace Use StrCommaSpace to concatenate multiple SQL scripts and parameters.
func JoinSQLCommaSpace(values ...any) *SQL {
	return JoinSQL(StrCommaSpace, values...)
}

// FuncSQL Building SQL functions.
func FuncSQL(funcName string, funcArgs ...any) *SQL {
	if funcName == StrEmpty {
		return NewEmptySQL()
	}
	values := make([]any, 0, len(funcArgs))
	for _, arg := range funcArgs {
		if tmp := any2sql(arg); !tmp.IsEmpty() {
			values = append(values, tmp)
		}
	}
	return JoinSQLEmpty(
		any2sql(funcName),
		any2sql(StrLeftSmallBracket),
		JoinSQLComma(values...),
		any2sql(StrRightSmallBracket),
	).ToSQL()
}

// Coalesce Building SQL function COALESCE.
func Coalesce(values ...any) *SQL {
	return FuncSQL(StrCoalesce, values...)
}

// Count Building SQL function COUNT.
func Count(values ...any) *SQL {
	return FuncSQL(StrCount, values...)
}

// Avg Building SQL function AVG.
func Avg(values ...any) *SQL {
	return FuncSQL(StrAvg, values...)
}

// Max Building SQL function MAX.
func Max(values ...any) *SQL {
	return FuncSQL(StrMax, values...)
}

// Min Building SQL function MIN.
func Min(values ...any) *SQL {
	return FuncSQL(StrMin, values...)
}

// Sum Building SQL function SUM.
func Sum(values ...any) *SQL {
	return FuncSQL(StrSum, values...)
}

// Func Building SQL functions.
func (s *Way) Func(funcName string, funcArgs ...any) *SQL {
	return FuncSQL(funcName, funcArgs...)
}

// replacer Implementing the Replacer interface.
type replacer struct {
	replaces map[string]string
}

func NewReplacer() Replacer {
	return &replacer{
		replaces: make(map[string]string, 1<<8),
	}
}

func (s *replacer) Get(key string) string {
	if value, ok := s.replaces[key]; ok {
		return value
	}
	return key
}

func (s *replacer) Set(key string, value string) Replacer {
	s.replaces[key] = value
	return s
}

func (s *replacer) Del(key string) Replacer {
	delete(s.replaces, key)
	return s
}

func (s *replacer) Map() map[string]string {
	result := make(map[string]string, len(s.replaces))
	maps.Copy(result, s.replaces)
	return result
}

func (s *replacer) GetAll(keys []string) []string {
	length := len(keys)
	result := make([]string, length)
	for i := range keys {
		result[i] = s.Get(keys[i])
	}
	return result
}

// SQLAlias SQL script + alias name.
type SQLAlias interface {
	Maker

	// GetSQL Get SQL statement.
	GetSQL() *SQL

	// SetSQL Set SQL statement.
	SetSQL(script *SQL) SQLAlias

	// GetAlias Get alias name value.
	GetAlias() string

	// SetAlias Set alias name value.
	SetAlias(alias string) SQLAlias

	// IsEmpty Verify whether the SQL statement is empty.
	IsEmpty() bool

	// ToEmpty Set both the SQL statement and the alias to empty values.
	ToEmpty() SQLAlias
}

type sqlAlias struct {
	way    *Way
	script *SQL
	alias  string
}

func newSqlAlias(script any, aliases ...string) *sqlAlias {
	return &sqlAlias{
		script: any2sql(script),
		alias:  LastNotEmptyString(aliases),
	}
}

func NewSQLAlias(script any, aliases ...string) SQLAlias {
	return newSqlAlias(script, aliases...)
}

func (s *Way) Alias(script any, aliases ...string) SQLAlias {
	return newSqlAlias(script, aliases...).v(s)
}

func (s *sqlAlias) v(way *Way) SQLAlias {
	s.way = way
	return s
}

func (s *sqlAlias) rep(key string) string {
	if s.way == nil {
		return key
	}
	return s.way.Replace(key)
}

func (s *sqlAlias) GetSQL() *SQL {
	return s.script
}

func (s *sqlAlias) SetSQL(script *SQL) SQLAlias {
	if script == nil {
		script = NewEmptySQL()
	}
	s.script = script
	return s
}

func (s *sqlAlias) GetAlias() string {
	return s.alias
}

func (s *sqlAlias) SetAlias(alias string) SQLAlias {
	s.alias = alias
	return s
}

func (s *sqlAlias) IsEmpty() bool {
	return s.script.IsEmpty()
}

func (s *sqlAlias) ToEmpty() SQLAlias {
	s.script.ToEmpty()
	s.alias = StrEmpty
	return s
}

func (s *sqlAlias) ToSQL() *SQL {
	if s.IsEmpty() {
		return NewEmptySQL()
	}
	result := CopySQL(s.script)
	if s.way != nil {
		result.Prepare = s.rep(s.script.Prepare)
	}
	alias := s.alias
	if alias == StrEmpty {
		return result
	}
	aliases := make(map[string]*struct{}, 1<<2)
	aliases[Strings(StrSpace, alias)] = nil
	asAlias := Strings(StrSpace, StrAs, StrSpace, alias)
	aliases[asAlias] = nil
	if replace := s.rep(alias); alias != replace {
		aliases[Strings(StrSpace, replace)] = nil
		asAlias = Strings(StrSpace, StrAs, StrSpace, replace)
		aliases[asAlias] = nil
	}
	has := false
	for suffix := range aliases {
		if strings.HasSuffix(result.Prepare, suffix) {
			has = true
			break
		}
	}
	if !has {
		result.Prepare = Strings(result.Prepare, asAlias)
	}
	return result
}

// Strings Concatenate multiple strings in sequence.
func Strings(elements ...string) string {
	builder := poolGetStringBuilder()
	defer poolPutStringBuilder(builder)
	length := len(elements)
	for i := range length {
		builder.WriteString(elements[i])
	}
	return builder.String()
}

// Prefix Add SQL prefix name; if the prefix exists, it will not be added.
func Prefix(prefix string, name string) string {
	if prefix == StrEmpty || strings.Contains(name, StrPoint) {
		return name
	}
	return Strings(prefix, StrPoint, name)
}

// ParcelPrepare Parcel the SQL statement. `subquery` => ( `subquery` )
func ParcelPrepare(prepare string) string {
	if prepare == StrEmpty {
		return prepare
	}
	return Strings(StrLeftSmallBracket, StrSpace, prepare, StrSpace, StrRightSmallBracket)
}

// ParcelSQL Parcel the SQL statement. `subquery` => ( `subquery` )
func ParcelSQL(script *SQL) *SQL {
	if script == nil || script.IsEmpty() {
		return NewEmptySQL()
	}
	result := script.Copy()
	prepare := strings.TrimSpace(result.Prepare)
	if prepare == StrEmpty {
		return result.ToEmpty()
	}
	if prepare[0] != StrLeftSmallBracket[0] {
		result.Prepare = ParcelPrepare(prepare)
	}
	return result
}

// ParcelFilter Parcel the SQL filter statement. `SQL_FILTER_STATEMENT` => ( `SQL_FILTER_STATEMENT` )
func ParcelFilter(tmp Filter) Filter {
	if tmp.IsEmpty() {
		return tmp
	}
	if num := tmp.Num(); num != 1 {
		return tmp
	}
	return tmp.New().And(ParcelSQL(tmp.ToSQL()))
}

// ParcelCancelPrepare Cancel parcel the SQL statement. ( `subquery` ) => `subquery` OR ( ( `subquery` ) ) => ( `subquery` )
func ParcelCancelPrepare(prepare string) string {
	prepare = strings.TrimSpace(prepare)
	prepare = strings.TrimPrefix(prepare, Strings(StrLeftSmallBracket, StrSpace))
	return strings.TrimSuffix(prepare, Strings(StrSpace, StrRightSmallBracket))
}

// ParcelCancelSQL Cancel parcel the SQL statement. ( `subquery` ) => `subquery` OR ( ( `subquery` ) ) => ( `subquery` )
func ParcelCancelSQL(script *SQL) *SQL {
	if script == nil || script.IsEmpty() {
		return NewEmptySQL()
	}
	result := script.Copy()
	result.Prepare = ParcelCancelPrepare(result.Prepare)
	return result
}

// UnionSQL *SQL1, *SQL2, *SQL3 ... => ( ( QUERY_A ) UNION ( QUERY_B ) UNION ( QUERY_C )... )
func UnionSQL(scripts ...*SQL) *SQL {
	result := make([]any, len(scripts))
	for index, value := range scripts {
		result[index] = ParcelSQL(value)
	}
	return JoinSQL(Strings(StrSpace, StrUnion, StrSpace), result...)
}

// UnionAllSQL *SQL1, *SQL2, *SQL3 ... => ( ( QUERY_A ) UNION ALL ( QUERY_B ) UNION ALL ( QUERY_C )... )
func UnionAllSQL(scripts ...*SQL) *SQL {
	result := make([]any, len(scripts))
	for index, value := range scripts {
		result[index] = ParcelSQL(value)
	}
	return JoinSQL(Strings(StrSpace, StrUnionAll, StrSpace), result...)
}

// IntersectSQL *SQL1, *SQL2, *SQL3 ... => ( ( QUERY_A ) INTERSECT ( QUERY_B ) INTERSECT ( QUERY_C )... )
func IntersectSQL(scripts ...*SQL) *SQL {
	result := make([]any, len(scripts))
	for index, value := range scripts {
		result[index] = ParcelSQL(value)
	}
	return JoinSQL(Strings(StrSpace, StrIntersect, StrSpace), result...)
}

// IntersectAllSQL *SQL1, *SQL2, *SQL3 ... => ( ( QUERY_A ) INTERSECT ALL ( QUERY_B ) INTERSECT ALL ( QUERY_C )... )
func IntersectAllSQL(scripts ...*SQL) *SQL {
	result := make([]any, len(scripts))
	for index, value := range scripts {
		result[index] = ParcelSQL(value)
	}
	return JoinSQL(Strings(StrSpace, StrIntersectAll, StrSpace), result...)
}

// ExceptSQL *SQL1, *SQL2, *SQL3 ... => ( ( QUERY_A ) EXCEPT ( QUERY_B ) EXCEPT ( QUERY_C )... )
func ExceptSQL(scripts ...*SQL) *SQL {
	result := make([]any, len(scripts))
	for index, value := range scripts {
		result[index] = ParcelSQL(value)
	}
	return JoinSQL(Strings(StrSpace, StrExpect, StrSpace), result...)
}

// ExceptAllSQL *SQL1, *SQL2, *SQL3 ... => ( ( QUERY_A ) EXCEPT ALL ( QUERY_B ) EXCEPT ALL ( QUERY_C )... )
func ExceptAllSQL(scripts ...*SQL) *SQL {
	result := make([]any, len(scripts))
	for index, value := range scripts {
		result[index] = ParcelSQL(value)
	}
	return JoinSQL(Strings(StrSpace, StrExpectAll, StrSpace), result...)
}

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
	for k == reflect.Ptr {
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

// MustAffectedRows At least one row is affected.
func MustAffectedRows(affectedRows int64, err error) error {
	if err != nil {
		return err
	}
	if affectedRows <= 0 {
		return ErrNoRowsAffected
	}
	return nil
}

// LastNotEmptyString Get last not empty string, return empty string if it does not exist.
func LastNotEmptyString(sss []string) string {
	for i := len(sss) - 1; i >= 0; i-- {
		if sss[i] != StrEmpty {
			return sss[i]
		}
	}
	return StrEmpty
}

// poolInsertByStruct Insert with struct{}, *struct{}, []struct, []*struct{}, *[]struct{}, *[]*struct{}.
var poolInsertByStruct = &sync.Pool{
	New: func() any { return &insertByStruct{} },
}

func poolGetInsertByStruct() *insertByStruct {
	tmp := poolInsertByStruct.Get().(*insertByStruct)
	tmp.allow = make(map[string]*struct{}, 1<<5)
	tmp.except = make(map[string]*struct{}, 1<<5)
	tmp.used = make(map[string]*struct{}, 1<<3)
	return tmp
}

func poolPutInsertByStruct(b *insertByStruct) {
	b.tag = StrEmpty
	b.allow = nil
	b.except = nil
	b.used = nil
	b.structReflectType = nil
	poolInsertByStruct.Put(b)
}

/* Implement scanning *sql.Rows data into STRUCT or []STRUCT */

var poolBindScanStruct = &sync.Pool{
	New: func() any { return &bindScanStruct{} },
}

func poolGetBindScanStruct() *bindScanStruct {
	return poolBindScanStruct.Get().(*bindScanStruct).init()
}

func poolPutBindScanStruct(b *bindScanStruct) {
	poolBindScanStruct.Put(b.free())
}

// bindScanStruct Bind the receiving object with the query result.
type bindScanStruct struct {
	// direct Store root struct properties.
	direct map[string]int

	// indirect Store non-root struct properties, such as anonymous attribute structure and named attribute structure.
	indirect map[string][]int

	// structType All used struct types, including the root struct.
	structType map[reflect.Type]*struct{}
}

func (s *bindScanStruct) free() *bindScanStruct {
	s.direct = nil
	s.indirect = nil
	s.structType = nil
	return s
}

func (s *bindScanStruct) init() *bindScanStruct {
	s.direct = make(map[string]int, 1<<4)
	s.indirect = make(map[string][]int, 1<<4)
	s.structType = make(map[reflect.Type]*struct{}, 1<<1)
	return s
}

// binding Match the binding according to the structure `db` tag and the query column name.
// Please ensure that the type of refStructType must be `reflect.Struct`.
func (s *bindScanStruct) binding(refStructType reflect.Type, depth []int, tag string) {
	if _, ok := s.structType[refStructType]; ok {
		// prevent structure loop nesting.
		return
	}

	s.structType[refStructType] = &struct{}{}

	length := refStructType.NumField()

	for i := range length {

		attribute := refStructType.Field(i)

		if !attribute.IsExported() {
			continue
		}

		tagValue := attribute.Tag.Get(tag) // table column name
		if tagValue == "-" {
			continue
		}

		// anonymous structure, or named structure.
		at := attribute.Type
		atk := at.Kind()
		switch atk {
		case reflect.Struct:
			dst := depth[:]
			dst = append(dst, i)
			s.binding(at, dst, tag)
			continue
		case reflect.Ptr:
			if at.Elem().Kind() == reflect.Struct {
				dst := depth[:]
				dst = append(dst, i)
				s.binding(at.Elem(), dst, tag)
				continue
			}
		default:
		}

		if tagValue == StrEmpty {
			// database columns are usually named with underscores, so the `db` tag is not set. No column mapping is done here.
			continue
		}

		// root structure attribute.
		if depth == nil {
			s.direct[tagValue] = i
			continue
		}

		// another structure attributes, nested anonymous structure or named structure.
		if _, ok := s.indirect[tagValue]; !ok {
			dst := depth[:]
			dst = append(dst, i)
			s.indirect[tagValue] = dst
		}

	}
}

// Prepare The preparatory work before executing rows.Scan.
// Find the pointer of the corresponding field from the reflection value of the receiving object, and bind it.
func (s *bindScanStruct) prepare(columns []string, rowsScan []any, indirect reflect.Value, length int) error {
	for i := range length {
		index, ok := s.direct[columns[i]]
		if ok {
			// root structure.
			field := indirect.Field(index)
			if !field.CanAddr() || !field.CanSet() {
				return fmt.Errorf("hey: column `%s` cann't set value", columns[i])
			}
			if field.Kind() == reflect.Ptr && field.IsNil() {
				field.Set(reflect.New(field.Type()).Elem())
				rowsScan[i] = field.Addr().Interface()
				continue
			}
			rowsScan[i] = field.Addr().Interface()
			continue
		}
		// non-root structure, parsing multi-layer structures.
		indexChain, ok := s.indirect[columns[i]]
		if !ok {
			// unable to find mapping property for current field Use *[]byte instead to receive.
			rowsScan[i] = new([]byte)
			continue
		}
		total := len(indexChain)
		if total < 2 {
			return fmt.Errorf("hey: unable to determine field `%s` mapping", columns[i])
		}
		lists := make([]reflect.Value, total)
		lists[0] = indirect
		for serial := range total {
			using := lists[serial]
			if serial+1 < total {
				// Middle layer structures.
				next := using.Field(indexChain[serial])
				if next.Type().Kind() == reflect.Ptr {
					if next.IsNil() {
						next.Set(reflect.New(next.Type().Elem()))
					}
					next = next.Elem()
				}
				lists[serial+1] = next
				continue
			}

			// The struct where the current column is located.
			field := using.Field(indexChain[serial])
			if !field.CanAddr() || !field.CanSet() {
				return fmt.Errorf("hey: column `%s` cann't set value, multi-level", columns[i])
			}
			if field.Kind() == reflect.Ptr && field.IsNil() {
				field.Set(reflect.New(field.Type()).Elem())
				rowsScan[i] = field.Addr().Interface()
				continue
			}
			rowsScan[i] = field.Addr().Interface()
		}
	}
	return nil
}

// RowsScan Scan the query result set into the receiving object. Support type *AnyStruct, **AnyStruct, *[]AnyStruct, *[]*AnyStruct, **[]AnyStruct, **[]*AnyStruct ...
func RowsScan(rows *sql.Rows, result any, tag string) error {
	refType, refValue := reflect.TypeOf(result), reflect.ValueOf(result)

	depth1 := 0
	refType1 := refType
	kind1 := refType1.Kind()
	for kind1 == reflect.Ptr {
		depth1++
		refType1 = refType1.Elem()
		kind1 = refType1.Kind()
	}

	if depth1 == 0 {
		return fmt.Errorf("hey: the receiving parameter needs to be pointer, yours is `%s`", refType.String())
	}

	// Is the value a nil pointer?
	if refValue.IsNil() {
		return fmt.Errorf("hey: the receiving parameter value is nil")
	}

	switch kind1 {
	case reflect.Slice:
	case reflect.Struct:
	default:
		return fmt.Errorf("hey: the receiving parameter type needs to be slice or struct, yours is `%s`", refType.String())
	}

	// Query one, remember to use LIMIT 1 in your SQL statement.
	if kind1 == reflect.Struct {
		refStructType := refType1
		if rows.Next() {
			b := poolGetBindScanStruct()
			defer poolPutBindScanStruct(b)
			b.binding(refStructType, nil, tag)
			columns, err := rows.Columns()
			if err != nil {
				return err
			}
			length := len(columns)
			rowsScan := make([]any, length)
			refStructPtr := reflect.New(refStructType)
			refStructVal := reflect.Indirect(refStructPtr)
			if err = b.prepare(columns, rowsScan, refStructVal, length); err != nil {
				return err
			}
			if err = rows.Scan(rowsScan...); err != nil {
				return err
			}
			switch depth1 {
			case 1:
				refValue.Elem().Set(refStructVal)
				return nil
			default:
				current := refValue.Elem()
				for current.Kind() == reflect.Ptr {
					if current.IsNil() {
						tmp := reflect.New(current.Type().Elem())
						current.Set(tmp)
					}
					current = current.Elem()
				}
				if !refStructVal.Type().AssignableTo(current.Type()) {
					return fmt.Errorf("`%s` cannot assign to `%s`", refStructVal.Type().String(), current.Type().String())
				}
				current.Set(refStructVal)
			}
			return nil
		}
		return ErrNoRows
	}

	// Query multiple items.
	depth2 := 0

	// the type of slice elements.
	sliceItemType := refType1.Elem()

	// slice element struct type.
	refStructType := sliceItemType

	kind2 := refStructType.Kind()
	for kind2 == reflect.Ptr {
		depth2++
		refStructType = refStructType.Elem()
		kind2 = refStructType.Kind()
	}

	if kind2 != reflect.Struct {
		return fmt.Errorf("hey: the basic type of slice elements must be a struct, yours is `%s`", refStructType.String())
	}

	// initialize slice object.
	refValueSlice := refValue
	for i := 0; i < depth1; i++ {
		if refValueSlice.IsNil() {
			tmp := reflect.New(refValueSlice.Type().Elem())
			refValueSlice.Set(tmp)
		}
		refValueSlice = refValueSlice.Elem()
	}

	var (
		b        *bindScanStruct
		columns  []string
		err      error
		length   int
		rowsScan []any
	)

	defer func() {
		if b != nil {
			poolPutBindScanStruct(b)
		}
	}()

	for rows.Next() {

		if b == nil {
			// initialize variable values
			b = poolGetBindScanStruct()
			b.binding(refStructType, nil, tag)

			columns, err = rows.Columns()
			if err != nil {
				return err
			}
			length = len(columns)
			rowsScan = make([]any, length)
		}

		refStructPtr := reflect.New(refStructType)
		refStructVal := reflect.Indirect(refStructPtr)
		if err = b.prepare(columns, rowsScan, refStructVal, length); err != nil {
			return err
		}
		if err = rows.Scan(rowsScan...); err != nil {
			return err
		}
		switch depth2 {
		case 0:
			refValueSlice = reflect.Append(refValueSlice, refStructVal)
		case 1:
			refValueSlice = reflect.Append(refValueSlice, refStructPtr)
		default:
			leap := refStructPtr
			for i := 1; i < depth2; i++ {
				tmp := reflect.New(leap.Type()) // creates a pointer to the current type
				tmp.Elem().Set(leap)            // assign the current value to the new pointer
				leap = tmp                      // update to a new pointer
			}
			refValueSlice = reflect.Append(refValueSlice, leap)
		}
	}

	current := refValue.Elem()
	for current.Kind() == reflect.Ptr {
		if current.IsNil() {
			tmp := reflect.New(current.Type().Elem())
			current.Set(tmp)
		}
		current = current.Elem()
	}
	if !refValueSlice.Type().AssignableTo(current.Type()) {
		return fmt.Errorf("`%s` cannot assign to `%s`", refValueSlice.Type().String(), current.Type().String())
	}
	current.Set(refValueSlice)

	return nil
}

func basicTypeValue(value any) any {
	if value == nil {
		return nil
	}
	t, v := reflect.TypeOf(value), reflect.ValueOf(value)
	k := t.Kind()
	for k == reflect.Ptr {
		if v.IsNil() {
			for k == reflect.Ptr {
				t = t.Elem()
				k = t.Kind()
			}
			switch k {
			case reflect.String:
				return StrEmpty
			case reflect.Bool:
				return false
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
				reflect.Float32, reflect.Float64:
				return 0
			default:
				return reflect.Indirect(reflect.New(t)).Interface()
			}
		}
		t, v = t.Elem(), v.Elem()
		k = t.Kind()
	}
	return v.Interface()
}

type insertByStruct struct {
	// struct reflects type, make sure it is the same structure type.
	structReflectType reflect.Type

	// only allowed fields Hash table.
	allow map[string]*struct{}

	// ignored fields Hash table.
	except map[string]*struct{}

	// already existing fields Hash table.
	used map[string]*struct{}

	// struct tag name value used as table.column name.
	tag string
}

// setExcept Filter the list of unwanted fields, prioritize method calls structFieldsValues() and structValues().
func (s *insertByStruct) setExcept(except []string) {
	for _, field := range except {
		s.except[field] = &struct{}{}
	}
}

// setAllow Only allowed fields, prioritize method calls structFieldsValues() and structValues().
func (s *insertByStruct) setAllow(allow []string) {
	for _, field := range allow {
		s.allow[field] = &struct{}{}
	}
}

func panicSliceElementTypesAreInconsistent() {
	panic("hey: slice element types are inconsistent")
}

// structFieldsValues Checkout fields, values.
func (s *insertByStruct) structFieldsValues(structReflectValue reflect.Value, allowed bool) (fields []string, values []any) {
	reflectType := structReflectValue.Type()
	if s.structReflectType == nil {
		s.structReflectType = reflectType
	}
	if s.structReflectType != reflectType {
		panicSliceElementTypesAreInconsistent()
	}
	length := reflectType.NumField()
	for i := range length {
		field := reflectType.Field(i)
		if !field.IsExported() {
			continue
		}

		valueIndexField := structReflectValue.Field(i)
		valueIndexFieldKind := valueIndexField.Kind()
		for valueIndexFieldKind == reflect.Ptr {
			if valueIndexField.IsNil() {
				break
			}
			valueIndexField = valueIndexField.Elem()
			valueIndexFieldKind = valueIndexField.Kind()
		}
		if valueIndexFieldKind == reflect.Struct {
			tmpFields, tmpValues := s.structFieldsValues(valueIndexField, allowed)
			fields = append(fields, tmpFields...)
			values = append(values, tmpValues...)
			continue
		}

		valueIndexFieldTag := field.Tag.Get(s.tag)
		if valueIndexFieldTag == StrEmpty || valueIndexFieldTag == "-" {
			continue
		}
		if _, ok := s.except[valueIndexFieldTag]; ok {
			continue
		}
		if _, ok := s.used[valueIndexFieldTag]; ok {
			continue
		}
		if allowed {
			if _, ok := s.allow[valueIndexFieldTag]; !ok {
				continue
			}
		}
		s.used[valueIndexFieldTag] = &struct{}{}

		fields = append(fields, valueIndexFieldTag)
		values = append(values, basicTypeValue(valueIndexField.Interface()))
	}
	return
}

// structValues Checkout values.
func (s *insertByStruct) structValues(structReflectValue reflect.Value, allowed bool) (values []any) {
	reflectType := structReflectValue.Type()
	if s.structReflectType != nil && s.structReflectType != reflectType {
		panicSliceElementTypesAreInconsistent()
	}
	length := reflectType.NumField()
	for i := range length {
		field := reflectType.Field(i)
		if !field.IsExported() {
			continue
		}

		valueIndexField := structReflectValue.Field(i)
		valueIndexFieldKind := valueIndexField.Kind()
		for valueIndexFieldKind == reflect.Ptr {
			if valueIndexField.IsNil() {
				break
			}
			valueIndexField = valueIndexField.Elem()
			valueIndexFieldKind = valueIndexField.Kind()
		}
		if valueIndexFieldKind == reflect.Struct {
			values = append(values, s.structValues(valueIndexField, allowed)...)
			continue
		}

		valueIndexFieldTag := field.Tag.Get(s.tag)
		if valueIndexFieldTag == StrEmpty || valueIndexFieldTag == "-" {
			continue
		}
		if _, ok := s.except[valueIndexFieldTag]; ok {
			continue
		}
		if allowed {
			if _, ok := s.allow[valueIndexFieldTag]; !ok {
				continue
			}
		}

		values = append(values, basicTypeValue(valueIndexField.Interface()))
	}
	return
}

// Insert Object should be one of struct{}, *struct{}, []struct, []*struct{}, *[]struct{}, *[]*struct{}.
func (s *insertByStruct) Insert(object any, tag string, except []string, allow []string) (fields []string, values [][]any) {
	if object == nil || tag == StrEmpty {
		return
	}

	s.tag = tag
	s.setExcept(except)

	allowed := len(allow) > 0
	if allowed {
		s.setAllow(allow)
	}

	reflectValue := reflect.ValueOf(object)
	kind := reflectValue.Kind()
	for ; kind == reflect.Ptr; kind = reflectValue.Kind() {
		reflectValue = reflectValue.Elem()
	}

	if kind == reflect.Struct {
		values = make([][]any, 1)
		fields, values[0] = s.structFieldsValues(reflectValue, allowed)
	}

	if kind == reflect.Slice {
		sliceLength := reflectValue.Len()
		values = make([][]any, sliceLength)
		for i := range sliceLength {
			indexValue := reflectValue.Index(i)
			for indexValue.Kind() == reflect.Ptr {
				indexValue = indexValue.Elem()
			}
			if indexValue.Kind() != reflect.Struct {
				continue
			}
			if i == 0 {
				fields, values[i] = s.structFieldsValues(indexValue, allowed)
			} else {
				values[i] = s.structValues(indexValue, allowed)
			}
		}
	}
	return
}

// StructInsert Object should be one of struct{}, *struct{}, []struct, []*struct{}, *[]struct{}, *[]*struct{}.
// Get fields and values based on struct tag.
func StructInsert(object any, tag string, except []string, allow []string) (fields []string, values [][]any) {
	b := poolGetInsertByStruct()
	defer poolPutInsertByStruct(b)
	fields, values = b.Insert(object, tag, except, allow)
	return
}

// StructModify Object should be one of anyStruct, *anyStruct get the fields and values that need to be modified.
func StructModify(object any, tag string, except ...string) (fields []string, values []any) {
	if object == nil || tag == StrEmpty {
		return
	}
	ofType := reflect.TypeOf(object)
	ofValue := reflect.ValueOf(object)
	ofKind := ofType.Kind()
	for ; ofKind == reflect.Ptr; ofKind = ofType.Kind() {
		ofType = ofType.Elem()
		ofValue = ofValue.Elem()
	}
	if ofKind != reflect.Struct {
		return
	}
	excepted := make(map[string]*struct{}, 1<<5)
	for _, field := range except {
		excepted[field] = &struct{}{}
	}

	length := ofType.NumField()

	exists := make(map[string]*struct{}, length)
	fields = make([]string, 0, length)
	values = make([]any, 0, length)

	last := 0
	fieldsIndex := make(map[string]int, 1<<5)

	add := func(field string, value any) {
		if _, ok := exists[field]; ok {
			values[fieldsIndex[field]] = value
			return
		}
		exists[field] = &struct{}{}
		fields = append(fields, field)
		values = append(values, value)
		fieldsIndex[field] = last
		last++
	}

	for i := range length {
		field := ofType.Field(i)

		fieldType := field.Type
		fieldKind := fieldType.Kind()
		pointerDepth := 0
		for fieldKind == reflect.Pointer {
			pointerDepth++
			fieldType = fieldType.Elem()
			fieldKind = fieldType.Kind()
		}

		if fieldKind == reflect.Struct {
			fieldValue := ofValue.Field(i)
			isNil := false
			for j := 0; j < pointerDepth; j++ {
				if isNil = fieldValue.IsNil(); isNil {
					break
				}
				fieldValue = fieldValue.Elem()
			}
			if isNil {
				continue
			}
			tmpFields, tmpValues := StructModify(fieldValue.Interface(), tag, except...)
			for index, tmpField := range tmpFields {
				add(tmpField, tmpValues[index])
			}
			continue
		}

		column := field.Tag.Get(tag)
		if column == StrEmpty || column == "-" {
			continue
		}
		if _, ok := excepted[column]; ok {
			continue
		}

		fieldValue := ofValue.Field(i)

		// any
		if pointerDepth == 0 {
			add(column, fieldValue.Interface())
			continue
		}

		// *any
		if pointerDepth == 1 {
			if !fieldValue.IsNil() {
				add(column, fieldValue.Elem().Interface())
			}
			continue
		}

		// ***...any
		for index := pointerDepth; index > 1; index-- {
			if index == 2 {
				if fieldValue.IsNil() {
					break
				}
				if fieldValue = fieldValue.Elem(); !fieldValue.IsNil() {
					add(column, fieldValue.Elem().Interface())
				}
				break
			}
			if fieldValue.IsNil() {
				break
			}
			fieldValue = fieldValue.Elem()
		}
	}
	return
}

// StructObtain Object should be one of anyStruct, *anyStruct for get all fields and values.
func StructObtain(object any, tag string, except ...string) (fields []string, values []any) {
	if object == nil || tag == StrEmpty {
		return
	}
	ofType := reflect.TypeOf(object)
	ofValue := reflect.ValueOf(object)
	ofKind := ofType.Kind()
	for ; ofKind == reflect.Ptr; ofKind = ofType.Kind() {
		ofType = ofType.Elem()
		ofValue = ofValue.Elem()
	}
	if ofKind != reflect.Struct {
		return
	}
	excepted := make(map[string]*struct{}, 1<<5)
	for _, field := range except {
		excepted[field] = &struct{}{}
	}

	length := ofType.NumField()

	exists := make(map[string]*struct{}, length)
	fields = make([]string, 0, length)
	values = make([]any, 0, length)

	last := 0
	fieldsIndex := make(map[string]int, 1<<5)

	add := func(field string, value any) {
		if _, ok := exists[field]; ok {
			values[fieldsIndex[field]] = value
			return
		}
		exists[field] = &struct{}{}
		fields = append(fields, field)
		values = append(values, value)
		fieldsIndex[field] = last
		last++
	}

	for i := range length {
		field := ofType.Field(i)

		fieldType := field.Type
		fieldKind := fieldType.Kind()
		pointerDepth := 0
		for fieldKind == reflect.Pointer {
			pointerDepth++
			fieldType = fieldType.Elem()
			fieldKind = fieldType.Kind()
		}

		if fieldKind == reflect.Struct {
			fieldValue := ofValue.Field(i)
			isNil := false
			for j := 0; j < pointerDepth; j++ {
				if fieldValue.IsNil() {
					isNil = true
					break
				}
				fieldValue = fieldValue.Elem()
			}
			if isNil {
				continue
			}
			tmpFields, tmpValues := StructObtain(fieldValue.Interface(), tag, except...)
			for index, tmpField := range tmpFields {
				add(tmpField, tmpValues[index])
			}
			continue
		}

		column := field.Tag.Get(tag)
		if column == StrEmpty || column == "-" {
			continue
		}
		if _, ok := excepted[column]; ok {
			continue
		}

		add(column, ofValue.Field(i).Interface())
	}
	return
}

// StructUpdate Compare origin and latest for update.
func StructUpdate(origin any, latest any, tag string, except ...string) (fields []string, values []any) {
	if origin == nil || latest == nil || tag == StrEmpty {
		return
	}

	originFields, originValues := StructObtain(origin, tag, except...)
	latestFields, latestValues := StructModify(latest, tag, except...)

	storage := make(map[string]any, len(originFields))
	for k, v := range originFields {
		storage[v] = originValues[k]
	}

	exists := make(map[string]*struct{}, 1<<5)
	fields = make([]string, 0)
	values = make([]any, 0)

	last := 0
	fieldsIndex := make(map[string]int, 1<<5)

	add := func(field string, value any) {
		if _, ok := exists[field]; ok {
			values[fieldsIndex[field]] = value
			return
		}
		exists[field] = &struct{}{}
		fields = append(fields, field)
		values = append(values, value)
		fieldsIndex[field] = last
		last++
	}

	for k, v := range latestFields {
		if _, ok := storage[v]; !ok {
			continue
		}
		if latestValues[k] == nil {
			continue
		}
		bvo, bvl := basicTypeValue(storage[v]), basicTypeValue(latestValues[k])
		if reflect.DeepEqual(bvo, bvl) {
			continue
		}
		add(v, bvl)
	}

	return
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
		lg.Str(strId, s.id).Str(strMsg, s.message)
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

	// Manual For handling different types of databases.
	Manual *Manual

	// Scan For scanning data into structure.
	Scan func(rows *sql.Rows, result any, tag string) error

	// TransactionOptions Start transaction.
	TransactionOptions *sql.TxOptions

	// ScanTag Scan data to tag mapping on structure.
	ScanTag string

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
		Scan:                   RowsScan,
		ScanTag:                StrDefaultTag,
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
		return
	}
	tx.transaction.startAt = time.Now()
	tx.transaction.id = fmt.Sprintf("%d%s%d%s%p", tx.transaction.startAt.UnixNano(), StrPoint, os.Getpid(), StrPoint, tx.transaction)
	tx.transaction.start()
	return
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
	return
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
	return
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
	defer func() { _ = rows.Close() }()
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
	return s.Query(ctx, func(rows *sql.Rows) error { return s.way.cfg.Scan(rows, result, s.way.cfg.ScanTag) }, args...)
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
	stmt, err := s.Prepare(ctx, script.Prepare)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	return stmt.Query(ctx, query, script.Args...)
}

// QueryRow -> Execute SQL statement and return row data, usually INSERT, UPDATE, DELETE.
func (s *Way) QueryRow(ctx context.Context, maker Maker, query func(row *sql.Row) error) error {
	script := maker.ToSQL()
	stmt, err := s.Prepare(ctx, script.Prepare)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
	return stmt.QueryRow(ctx, query, script.Args...)
}

// Fetch -> Query prepared and get all query results, through the mapping of column names and struct tags.
func (s *Way) Fetch(ctx context.Context, maker Maker, result any) error {
	return s.Query(ctx, maker, func(rows *sql.Rows) error { return s.cfg.Scan(rows, result, s.cfg.ScanTag) })
}

// Exec -> Execute the execute sql statement.
func (s *Way) Exec(ctx context.Context, maker Maker) (sql.Result, error) {
	script := maker.ToSQL()
	stmt, err := s.Prepare(ctx, script.Prepare)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stmt.Close() }()
	return stmt.Exec(ctx, script.Args...)
}

// Execute -> Execute the execute sql statement.
func (s *Way) Execute(ctx context.Context, maker Maker) (int64, error) {
	script := maker.ToSQL()
	stmt, err := s.Prepare(ctx, script.Prepare)
	if err != nil {
		return 0, err
	}
	defer func() { _ = stmt.Close() }()
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
	return
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
	stmt := (*Stmt)(nil)
	stmt, err = s.Prepare(ctx, prepare)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()
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
	stmt := (*Stmt)(nil)
	stmt, err = s.Prepare(ctx, prepare)
	if err != nil {
		return affectedRows, err
	}
	defer func() { _ = stmt.Close() }()
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

// W -> The specified transaction object is used first, otherwise the current object is used.
func (s *Way) W(values ...*Way) *Way {
	for i := len(values) - 1; i >= 0; i-- {
		if values[i] != nil && values[i].IsInTransaction() {
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
	change := make(map[string]func(any) any, 1<<3)
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

// ToEmpty Sets the property value of an object to empty value.
type ToEmpty interface {
	// ToEmpty Sets the property value of an object to empty value.
	ToEmpty()
}

// SQLComment Constructing SQL statement comments.
type SQLComment interface {
	Maker

	ToEmpty

	// Comment Add comment.
	Comment(comment string) SQLComment
}

type sqlComment struct {
	comment []string
}

func newSqlComment() *sqlComment {
	return &sqlComment{
		comment: make([]string, 0, 1<<1),
	}
}

func (s *sqlComment) ToEmpty() {
	s.comment = nil
}

func (s *sqlComment) Comment(comment string) SQLComment {
	if comment == StrEmpty {
		return s
	}
	s.comment = append(s.comment, comment)
	return s
}

func (s *sqlComment) ToSQL() *SQL {
	if len(s.comment) == 0 {
		return NewEmptySQL()
	}
	return NewSQL(Strings("/*", strings.Join(s.comment, StrComma), "*/"))
}

/*
-- CTE
WITH [RECURSIVE]
	cte_name1 [(column_name11, column_name12, ...)] AS ( SELECT ... )
	[, cte_name2 [(column_name21, column_name22, ...)] AS ( SELECT ... ) ]
SELECT ... FROM cte_name1 ...

-- EXAMPLE RECURSIVE CTE:
-- postgres
WITH RECURSIVE sss AS (
	SELECT id, pid, name, 1 AS level, name::TEXT AS path FROM employee WHERE pid = 0
	UNION ALL
	SELECT e.id, e.pid, e.name, f.level + 1, f.path || ' -> ' || e.name FROM employee e INNER JOIN sss f ON e.pid = f.id
)
SELECT * FROM sss ORDER BY level ASC, id DESC
*/

// SQLWith CTE: Common Table Expression.
type SQLWith interface {
	Maker

	ToEmpty

	// Recursive Recursion or cancellation of recursion.
	Recursive() SQLWith

	// Set Setting common table expression.
	Set(alias string, maker Maker, columns ...string) SQLWith

	// Del Removing common table expression.
	Del(alias string) SQLWith
}

type sqlWith struct {
	column map[string][]string

	prepare map[string]Maker

	alias []string

	recursive bool
}

func newSqlWith() *sqlWith {
	return &sqlWith{
		column:  make(map[string][]string, 1<<1),
		prepare: make(map[string]Maker, 1<<1),
		alias:   make([]string, 0, 1<<1),
	}
}

func (s *sqlWith) ToEmpty() {
	s.column = make(map[string][]string, 1<<1)
	s.prepare = make(map[string]Maker, 1<<1)
	s.alias = make([]string, 0, 1<<1)
	s.recursive = false
}

func (s *sqlWith) IsEmpty() bool {
	return len(s.alias) == 0
}

func (s *sqlWith) ToSQL() *SQL {
	if s.IsEmpty() {
		return NewSQL(StrEmpty)
	}
	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)
	b.WriteString(StrWith)
	b.WriteString(StrSpace)
	if s.recursive {
		b.WriteString(StrRecursive)
		b.WriteString(StrSpace)
	}
	result := NewEmptySQL()
	for index, alias := range s.alias {
		if index > 0 {
			b.WriteString(StrCommaSpace)
		}
		script := s.prepare[alias]
		b.WriteString(alias)
		b.WriteString(StrSpace)
		if columns := s.column[alias]; len(columns) > 0 {
			// Displays the column alias that defines the CTE, overwriting the original column name of the query result.
			b.WriteString(StrLeftSmallBracket)
			b.WriteString(StrSpace)
			b.WriteString(strings.Join(columns, StrCommaSpace))
			b.WriteString(StrSpace)
			b.WriteString(StrRightSmallBracket)
			b.WriteString(StrSpace)
		}
		b.WriteString(Strings(StrAs, StrSpace, StrLeftSmallBracket, StrSpace))
		tmp := script.ToSQL()
		b.WriteString(tmp.Prepare)
		b.WriteString(Strings(StrSpace, StrRightSmallBracket))
		result.Args = append(result.Args, tmp.Args...)
	}
	result.Prepare = b.String()
	return result
}

func (s *sqlWith) Recursive() SQLWith {
	s.recursive = !s.recursive
	return s
}

func (s *sqlWith) Set(alias string, maker Maker, columns ...string) SQLWith {
	if alias == StrEmpty || maker == nil || maker.ToSQL().IsEmpty() {
		return s
	}
	if _, ok := s.prepare[alias]; !ok {
		s.alias = append(s.alias, alias)
	}
	s.column[alias] = columns
	s.prepare[alias] = maker
	return s
}

func (s *sqlWith) Del(alias string) SQLWith {
	if alias == StrEmpty {
		return s
	}
	if _, ok := s.prepare[alias]; !ok {
		return s
	}
	keeps := make([]string, 0, len(s.alias))
	for _, tmp := range s.alias {
		if tmp != alias {
			keeps = append(keeps, tmp)
		}
	}
	s.alias = keeps
	delete(s.column, alias)
	delete(s.prepare, alias)
	return s
}

// SQLSelect Build the query column set.
type SQLSelect interface {
	Maker

	ToEmpty

	// Distinct DISTINCT column1, column2, column3 ...
	Distinct() SQLSelect

	// Add Put Maker to the query list.
	Add(maker Maker) SQLSelect

	// Del Delete some columns from the query list. If not specified, delete all.
	Del(columns ...string) SQLSelect

	// Has Does the column exist in the query list?
	Has(column string) bool

	// Len Query list length.
	Len() int

	// Get Query list and its corresponding column parameter list.
	Get() (columns []string, args map[int][]any)

	// Set Query list and its corresponding column parameter list.
	Set(columns []string, args map[int][]any) SQLSelect

	// Select Add one or more query lists. If no parameter is provided, all existing query lists will be deleted.
	Select(columns ...any) SQLSelect
}

type sqlSelect struct {
	columnsMap map[string]int

	columnsArgs map[int][]any

	way *Way

	columns []string

	// distinct Allows multiple columns to be deduplicated, such as: DISTINCT column1, column2, column3 ...
	distinct bool
}

func newSqlSelect(way *Way) *sqlSelect {
	return &sqlSelect{
		columnsMap:  make(map[string]int, 1<<5),
		columnsArgs: make(map[int][]any, 1<<5),
		way:         way,
		columns:     make([]string, 0, 1<<5),
	}
}

func (s *sqlSelect) ToEmpty() {
	s.columns = make([]string, 0, 1<<5)
	s.columnsMap = make(map[string]int, 1<<5)
	s.columnsArgs = make(map[int][]any)
	s.distinct = false
}

func (s *sqlSelect) IsEmpty() bool {
	return len(s.columns) == 0
}

func (s *sqlSelect) ToSQL() *SQL {
	length := len(s.columns)
	if length == 0 {
		prepare := StrStar
		if s.distinct {
			prepare = Strings(StrDistinct, StrSpace, prepare)
		}
		return NewSQL(prepare)
	}
	script := NewSQL(StrEmpty)
	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)
	if s.distinct {
		b.WriteString(StrDistinct)
		b.WriteString(StrSpace)
	}
	columns := make([]string, 0, length)
	for i := range length {
		args, ok := s.columnsArgs[i]
		if !ok {
			continue
		}
		columns = append(columns, s.columns[i])
		script.Args = append(script.Args, args...)
	}
	b.WriteString(strings.Join(s.way.ReplaceAll(columns), StrCommaSpace))
	script.Prepare = b.String()
	return script
}

func (s *sqlSelect) Distinct() SQLSelect {
	s.distinct = !s.distinct
	return s
}

func (s *sqlSelect) Has(column string) bool {
	_, ok := s.columnsMap[column]
	return ok
}

func (s *sqlSelect) add(column string, args ...any) *sqlSelect {
	if column == StrEmpty {
		return s
	}
	index, ok := s.columnsMap[column]
	if ok {
		s.columnsArgs[index] = args
		return s
	}
	index = len(s.columns)
	s.columns = append(s.columns, column)
	s.columnsMap[column] = index
	s.columnsArgs[index] = args
	return s
}

func (s *sqlSelect) Add(maker Maker) SQLSelect {
	if maker == nil {
		return s
	}
	script := maker.ToSQL()
	if script.IsEmpty() {
		return s
	}
	return s.add(script.Prepare, script.Args...)
}

func (s *sqlSelect) AddAll(columns ...string) SQLSelect {
	index := len(s.columns)
	for _, column := range columns {
		if column == StrEmpty {
			continue
		}
		if _, ok := s.columnsMap[column]; ok {
			continue
		}
		s.columns = append(s.columns, column)
		s.columnsMap[column] = index
		s.columnsArgs[index] = nil
		index++
	}
	return s
}

func (s *sqlSelect) Del(columns ...string) SQLSelect {
	if columns == nil {
		s.ToEmpty()
		return s
	}
	deletes := make(map[int]*struct{}, len(columns))
	for _, column := range columns {
		if column == StrEmpty {
			continue
		}
		index, ok := s.columnsMap[column]
		if !ok {
			continue
		}
		deletes[index] = nil
	}
	length := len(s.columns)
	result := make([]string, 0, length)
	for index, column := range s.columns {
		if _, ok := deletes[index]; ok {
			delete(s.columnsMap, column)
			delete(s.columnsArgs, index)
		} else {
			result = append(result, column)
		}
	}
	s.columns = result
	return s
}

func (s *sqlSelect) Len() int {
	return len(s.columns)
}

func (s *sqlSelect) Get() ([]string, map[int][]any) {
	return s.columns, s.columnsArgs
}

func (s *sqlSelect) Set(columns []string, args map[int][]any) SQLSelect {
	columnsMap := make(map[string]int, len(columns))
	for i, column := range columns {
		columnsMap[column] = i
		if _, ok := args[i]; !ok {
			args[i] = nil
		}
	}
	s.columns, s.columnsMap, s.columnsArgs = columns, columnsMap, args
	return s
}

func (s *sqlSelect) use(columns ...SQLSelect) SQLSelect {
	length := len(columns)
	for i := range length {
		tmp := columns[i]
		if tmp == nil {
			continue
		}
		cols, args := tmp.Get()
		for index, value := range cols {
			s.add(value, args[index]...)
		}
	}
	return s
}

func (s *sqlSelect) Select(columns ...any) SQLSelect {
	if len(columns) == 0 {
		return s.Del()
	}
	for _, value := range columns {
		switch v := value.(type) {
		case string:
			s.add(v)
		case []string:
			for _, column := range v {
				s.add(column)
			}
		case *SQL:
			s.Add(v)
		case []*SQL:
			for _, w := range v {
				s.Add(w)
			}
		case SQLSelect:
			s.use(v)
		case []SQLSelect:
			s.use(v...)
		case Maker:
			s.Add(v)
		case []Maker:
			for _, maker := range v {
				s.Add(maker)
			}
		default:
		}
	}
	return s
}

// SQLJoinOn Construct the connection query conditions.
type SQLJoinOn interface {
	Maker

	// Equal Use equal value JOIN ON condition.
	Equal(table1alias string, table1column string, table2alias string, table2column string) SQLJoinOn

	// On Append custom conditions to the ON statement or use custom conditions on the ON statement to associate tables.
	On(on func(f Filter)) SQLJoinOn

	// Using Use USING instead of ON.
	Using(columns ...string) SQLJoinOn
}

type sqlJoinOn struct {
	way *Way

	on Filter

	usings []string
}

func newSQLJoinOn(way *Way) *sqlJoinOn {
	return &sqlJoinOn{
		way: way,
		on:  way.F(),
	}
}

func (s *sqlJoinOn) Equal(alias1 string, column1 string, alias2 string, column2 string) SQLJoinOn {
	s.on.And(JoinSQLSpace(Prefix(alias1, column1), StrEqual, Prefix(alias2, column2)))
	return s
}

func (s *sqlJoinOn) On(on func(f Filter)) SQLJoinOn {
	if on != nil {
		on(s.on)
	}
	return s
}

func (s *sqlJoinOn) Using(columns ...string) SQLJoinOn {
	columns = DiscardDuplicate(func(tmp string) bool { return tmp == StrEmpty }, columns...)
	if len(columns) > 0 {
		s.usings = columns
	}
	return s
}

func (s *sqlJoinOn) ToSQL() *SQL {
	// JOIN ON
	if s.on != nil && !s.on.IsEmpty() {
		script := s.on.ToSQL()
		script.Prepare = Strings(StrOn, StrSpace, script.Prepare)
		return script
	}
	// JOIN USING
	if length := len(s.usings); length > 0 {
		using := make([]string, 0, length)
		for _, column := range s.usings {
			if column != StrEmpty {
				using = append(using, column)
			}
		}
		if len(using) > 0 {
			using = s.way.ReplaceAll(using)
			prepare := Strings(StrUsing, StrSpace, StrLeftSmallBracket, StrSpace, strings.Join(using, StrCommaSpace), StrSpace, StrRightSmallBracket)
			return NewSQL(prepare)
		}
	}
	return NewEmptySQL()
}

type SQLJoinAssoc func(table1alias string, table2alias string) SQLJoinOn

type sqlJoinSchema struct {
	joinTable SQLAlias

	condition Maker

	joinType string
}

// SQLJoin Build a join query.
type SQLJoin interface {
	Maker

	ToEmpty

	// GetTable Get join query the main table.
	GetTable() SQLAlias

	// SetTable Set join query the main table.
	SetTable(table SQLAlias) SQLJoin

	// Table Create a table for join query.
	Table(table any, alias string) SQLAlias

	// On Set the join query conditions.
	On(on func(on SQLJoinOn, table1alias string, table2alias string)) SQLJoinAssoc

	// Using The conditions for the join query use USING.
	Using(columns ...string) SQLJoinAssoc

	// OnEqual The join query conditions uses table1.column = table2.column.
	OnEqual(table1column string, table2column string) SQLJoinAssoc

	// Join Use the join type to set the table join relationship, if the table1 value is nil, use the main table.
	Join(join string, table1 SQLAlias, table2 SQLAlias, on SQLJoinAssoc) SQLJoin

	// InnerJoin Set the table join relationship, if the table1 value is nil, use the main table.
	InnerJoin(table1 SQLAlias, table2 SQLAlias, on SQLJoinAssoc) SQLJoin

	// LeftJoin Set the table join relationship, if the table1 value is nil, use the main table.
	LeftJoin(table1 SQLAlias, table2 SQLAlias, on SQLJoinAssoc) SQLJoin

	// RightJoin Set the table join relationship, if the table1 value is nil, use the main table.
	RightJoin(table1 SQLAlias, table2 SQLAlias, on SQLJoinAssoc) SQLJoin

	// Select Add a column list to the query based on the table alias or table name prefix.
	Select(table SQLAlias, columns ...string) []string
}

type sqlJoin struct {
	table SQLAlias

	sqlSelect *sqlSelect

	way *Way

	joins []*sqlJoinSchema
}

func newSqlJoin(way *Way) *sqlJoin {
	tmp := &sqlJoin{
		sqlSelect: newSqlSelect(way),
		way:       way,
		joins:     make([]*sqlJoinSchema, 0, 1<<1),
	}
	return tmp
}

func (s *sqlJoin) ToEmpty() {
	s.joins = make([]*sqlJoinSchema, 0, 1<<1)
	s.sqlSelect.ToEmpty()
	s.table = nil
}

func (s *sqlJoin) GetTable() SQLAlias {
	return s.table
}

func (s *sqlJoin) SetTable(table SQLAlias) SQLJoin {
	s.table = table
	return s
}

func (s *sqlJoin) ToSQL() *SQL {
	script := s.sqlSelect.ToSQL()
	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)
	b.WriteString(Strings(StrSelect, StrSpace))
	b.WriteString(script.Prepare)
	b.WriteString(Strings(StrSpace, StrFrom, StrSpace))
	master := s.table.ToSQL()
	b.WriteString(master.Prepare)
	b.WriteString(StrSpace)
	script.Args = append(script.Args, master.Args...)
	for index, tmp := range s.joins {
		if tmp == nil {
			continue
		}
		if index > 0 {
			b.WriteString(StrSpace)
		}
		b.WriteString(tmp.joinType)
		right := tmp.joinTable.ToSQL()
		b.WriteString(StrSpace)
		b.WriteString(right.Prepare)
		script.Args = append(script.Args, right.Args...)
		if tmp.condition != nil {
			if on := tmp.condition.ToSQL(); on != nil && !on.IsEmpty() {
				b.WriteString(StrSpace)
				b.WriteString(on.Prepare)
				script.Args = append(script.Args, on.Args...)
			}
		}
	}
	script.Prepare = b.String()
	return script
}

func (s *sqlJoin) Table(table any, alias string) SQLAlias {
	return newSqlAlias(table).v(s.way).SetAlias(alias)
}

// On For `... JOIN ON ...`
func (s *sqlJoin) On(on func(o SQLJoinOn, alias1 string, alias2 string)) SQLJoinAssoc {
	return func(alias1 string, alias2 string) SQLJoinOn {
		return newSQLJoinOn(s.way).On(func(o Filter) {
			newAssoc := newSQLJoinOn(s.way)
			on(newAssoc, alias1, alias2)
			newAssoc.On(func(f Filter) { o.Use(f) })
		})
	}
}

// Using For `... JOIN USING ...`
func (s *sqlJoin) Using(columns ...string) SQLJoinAssoc {
	return func(alias1 string, alias2 string) SQLJoinOn {
		return newSQLJoinOn(s.way).Using(columns...)
	}
}

// OnEqual For `... JOIN ON ... = ... [...]`
func (s *sqlJoin) OnEqual(column1 string, column2 string) SQLJoinAssoc {
	if column1 == StrEmpty || column2 == StrEmpty {
		return nil
	}
	return func(alias1 string, alias2 string) SQLJoinOn {
		return newSQLJoinOn(s.way).On(func(f Filter) {
			f.CompareEqual(Prefix(alias1, column1), Prefix(alias2, column2))
		})
	}
}

func (s *sqlJoin) Join(joinType string, table1 SQLAlias, table2 SQLAlias, on SQLJoinAssoc) SQLJoin {
	if joinType == StrEmpty {
		joinType = StrJoinInner
	}
	if table1 == nil || table1.ToSQL().IsEmpty() {
		table1 = s.table
	}
	if table2 == nil || table2.ToSQL().IsEmpty() {
		return s
	}
	join := &sqlJoinSchema{
		joinType:  joinType,
		joinTable: table2,
	}
	if on != nil {
		join.condition = on(table1.GetAlias(), table2.GetAlias())
	}
	s.joins = append(s.joins, join)
	return s
}

func (s *sqlJoin) InnerJoin(table1 SQLAlias, table2 SQLAlias, on SQLJoinAssoc) SQLJoin {
	return s.Join(StrJoinInner, table1, table2, on)
}

func (s *sqlJoin) LeftJoin(table1 SQLAlias, table2 SQLAlias, on SQLJoinAssoc) SQLJoin {
	return s.Join(StrJoinLeft, table1, table2, on)
}

func (s *sqlJoin) RightJoin(table1 SQLAlias, table2 SQLAlias, on SQLJoinAssoc) SQLJoin {
	return s.Join(StrJoinRight, table1, table2, on)
}

func (s *sqlJoin) Select(table SQLAlias, columns ...string) []string {
	alias := table.GetAlias()
	if alias == StrEmpty {
		alias = table.GetSQL().Prepare
	}
	return s.way.T().SetAlias(alias).ColumnAll(columns...)
}

// SQLGroupBy Build GROUP BY statements.
type SQLGroupBy interface {
	Maker

	ToEmpty

	// Group Set the grouping column, allowing string, []string, *SQL, []*SQL, Maker, []Maker.
	Group(group ...any) SQLGroupBy

	// Having Set the conditions filter HAVING statement after GROUP BY.
	Having(having func(having Filter)) SQLGroupBy
}

type sqlGroupBy struct {
	having Filter

	groupColumnsMap map[string]int

	way *Way

	groupColumnsArgs map[string][]any

	groupColumns []string
}

func newSqlGroupBy(way *Way) *sqlGroupBy {
	return &sqlGroupBy{
		having:           way.F(),
		groupColumnsMap:  make(map[string]int, 1<<1),
		way:              way,
		groupColumnsArgs: make(map[string][]any, 1<<1),
		groupColumns:     make([]string, 0, 1<<1),
	}
}

func (s *sqlGroupBy) ToEmpty() {
	s.having = s.way.F()
	s.groupColumns = make([]string, 0, 1<<1)
	s.groupColumnsMap = make(map[string]int, 1<<1)
	s.groupColumnsArgs = make(map[string][]any, 1<<1)
}

func (s *sqlGroupBy) IsEmpty() bool {
	return len(s.groupColumns) == 0
}

func (s *sqlGroupBy) ToSQL() *SQL {
	script := NewEmptySQL()
	if s.IsEmpty() {
		return script
	}
	groupBy := strings.Join(s.way.ReplaceAll(s.groupColumns), StrCommaSpace)
	groupByArgs := make([]any, 0)
	for _, column := range s.groupColumns {
		groupByArgs = append(groupByArgs, s.groupColumnsArgs[column]...)
	}
	script.Prepare = Strings(StrGroupBy, StrSpace, groupBy)
	script.Args = groupByArgs
	if s.having == nil || s.having.IsEmpty() {
		return script
	}
	return JoinSQLSpace(script, StrHaving, ParcelFilter(s.having))
}

func (s *sqlGroupBy) add(script *SQL) *sqlGroupBy {
	if script == nil || script.IsEmpty() {
		return s
	}
	column, args := strings.TrimSpace(script.Prepare), script.Args
	if _, ok := s.groupColumnsMap[column]; ok {
		return s
	}
	index := len(s.groupColumns)
	s.groupColumns = append(s.groupColumns, column)
	s.groupColumnsMap[column] = index
	s.groupColumnsArgs[column] = args
	return s
}

func (s *sqlGroupBy) Group(group ...any) SQLGroupBy {
	for _, value := range group {
		switch v := value.(type) {
		case string:
			s.add(NewSQL(v))
		case []string:
			for _, w := range v {
				s.add(NewSQL(w))
			}
		case *SQL:
			s.add(v)
		case []*SQL:
			for _, w := range v {
				s.add(w)
			}
		case Maker:
			if v != nil {
				s.add(v.ToSQL())
			}
		case []Maker:
			for _, w := range v {
				if w != nil {
					s.add(w.ToSQL())
				}
			}
		default:
		}
	}
	return s
}

func (s *sqlGroupBy) Having(having func(having Filter)) SQLGroupBy {
	if having != nil {
		having(s.having)
	}
	return s
}

// SQLOrderBy Build ORDER BY statements.
type SQLOrderBy interface {
	Maker

	ToEmpty

	// Asc Build column1 ASC, column2 ASC, column3 ASC...
	Asc(columns ...string) SQLOrderBy

	// Desc Build column1 DESC, column2 DESC, column3 DESC...
	Desc(columns ...string) SQLOrderBy
}

type sqlOrderBy struct {
	orderMap map[string]int

	way *Way

	orderBy []string
}

func newSqlOrderBy(way *Way) *sqlOrderBy {
	return &sqlOrderBy{
		orderMap: make(map[string]int, 1<<1),
		way:      way,
		orderBy:  make([]string, 0, 1<<1),
	}
}

func (s *sqlOrderBy) ToEmpty() {
	s.orderBy = make([]string, 0, 1<<1)
	s.orderMap = make(map[string]int, 1<<1)
}

func (s *sqlOrderBy) IsEmpty() bool {
	return len(s.orderBy) == 0
}

func (s *sqlOrderBy) ToSQL() *SQL {
	script := NewSQL(StrEmpty)
	if s.IsEmpty() {
		return script
	}
	script.Prepare = Strings(Strings(StrOrderBy, StrSpace, strings.Join(s.orderBy, StrCommaSpace)))
	return script
}

func (s *sqlOrderBy) add(category string, columns ...string) SQLOrderBy {
	if category == StrEmpty {
		return s
	}
	index := len(s.orderBy)
	for _, column := range columns {
		if column == StrEmpty {
			continue
		}
		if _, ok := s.orderMap[column]; ok {
			continue
		}
		s.orderMap[column] = index
		index++
		order := s.way.Replace(column)
		order = Strings(order, StrSpace, category)
		s.orderBy = append(s.orderBy, order)
	}
	return s
}

func (s *sqlOrderBy) Asc(columns ...string) SQLOrderBy {
	return s.add(StrAsc, columns...)
}

func (s *sqlOrderBy) Desc(columns ...string) SQLOrderBy {
	return s.add(StrDesc, columns...)
}

// SQLLimit Build LIMIT n[ OFFSET m] statements.
type SQLLimit interface {
	Maker

	ToEmpty

	// Limit SQL LIMIT.
	Limit(limit int64) SQLLimit

	// Offset SQL OFFSET.
	Offset(offset int64) SQLLimit

	// Page SQL LIMIT and OFFSET.
	Page(page int64, limit ...int64) SQLLimit
}

type sqlLimit struct {
	limit *int64

	offset *int64
}

func newSqlLimit() *sqlLimit {
	return &sqlLimit{}
}

func (s *sqlLimit) ToEmpty() {
	s.limit = nil
	s.offset = nil
}

func (s *sqlLimit) IsEmpty() bool {
	return s.limit == nil
}

func (s *sqlLimit) ToSQL() *SQL {
	script := NewSQL(StrEmpty)
	if s.IsEmpty() {
		return script
	}
	makers := make([]any, 0, 1<<2)
	makers = append(makers, any2sql(StrLimit))
	makers = append(makers, any2sql(*s.limit))
	if s.offset != nil && *s.offset >= 0 {
		makers = append(makers, any2sql(StrOffset))
		makers = append(makers, any2sql(*s.offset))
	}
	return JoinSQLSpace(makers...)
}

func (s *sqlLimit) Limit(limit int64) SQLLimit {
	if limit > 0 {
		s.limit = &limit
	}
	return s
}

func (s *sqlLimit) Offset(offset int64) SQLLimit {
	if offset > 0 {
		s.offset = &offset
	}
	return s
}

func (s *sqlLimit) Page(page int64, limit ...int64) SQLLimit {
	if page <= 0 {
		return s
	}
	for i := len(limit) - 1; i >= 0; i-- {
		if limit[i] > 0 {
			s.Limit(limit[i]).Offset((page - 1) * limit[i])
			break
		}
	}
	return s
}

// Limiter limit and offset.
type Limiter interface {
	GetLimit() int64

	GetOffset() int64
}

// SQLValues Build INSERT-VALUES statements.
type SQLValues interface {
	Maker

	ToEmpty

	IsEmpty() bool

	// Subquery The inserted data is a subquery.
	Subquery(subquery Maker) SQLValues

	// Values The inserted data of VALUES.
	Values(values ...[]any) SQLValues
}

type sqlValues struct {
	subquery Maker

	values [][]any
}

func newSqlValues() *sqlValues {
	return &sqlValues{
		values: make([][]any, 1),
	}
}

func (s *sqlValues) ToEmpty() {
	s.subquery = nil
	s.values = make([][]any, 1)
}

func (s *sqlValues) IsEmpty() bool {
	return s.subquery == nil && (len(s.values) == 0 || len(s.values[0]) == 0)
}

func (s *sqlValues) valuesToSQL(values [][]any) *SQL {
	script := NewEmptySQL()
	count := len(values)
	if count == 0 {
		return script
	}
	length := len(values[0])
	if length == 0 {
		return script
	}
	line := make([]string, length)
	script.Args = make([]any, 0, count*length)
	for i := range length {
		line[i] = StrPlaceholder
	}
	value := ParcelPrepare(strings.Join(line, StrCommaSpace))
	rows := make([]string, count)
	for i := range count {
		script.Args = append(script.Args, values[i]...)
		rows[i] = value
	}
	script.Prepare = strings.Join(rows, StrCommaSpace)
	return script
}

func (s *sqlValues) ToSQL() *SQL {
	if s.subquery != nil {
		return s.subquery.ToSQL()
	}
	return s.valuesToSQL(s.values)
}

func (s *sqlValues) Subquery(subquery Maker) SQLValues {
	if subquery != nil {
		if script := subquery.ToSQL(); script.IsEmpty() {
			return s
		}
	}
	s.subquery = subquery
	return s
}

func (s *sqlValues) Values(values ...[]any) SQLValues {
	s.values = values
	return s
}

// SQLReturning Build INSERT INTO xxx RETURNING xxx
type SQLReturning interface {
	Maker

	ToEmpty

	// Prepare When constructing a SQL statement that insert a row of data and return the id,
	// you may need to adjust the SQL statement, such as adding `RETURNING id` to the end of the insert statement.
	Prepare(prepare func(tmp *SQL)) SQLReturning

	// Returning Set the RETURNING statement to return one or more columns.
	Returning(columns ...string) SQLReturning

	// Execute When constructing a SQL statement that inserts a row of data and returns the id,
	// get the id value of the inserted row (this may vary depending on the database driver)
	Execute(execute func(ctx context.Context, stmt *Stmt, args ...any) (id int64, err error)) SQLReturning
}

type sqlReturning struct {
	way     *Way
	insert  *SQL
	prepare func(tmp *SQL)
	execute func(ctx context.Context, stmt *Stmt, args ...any) (id int64, err error)
}

func newReturning(way *Way, insert *SQL) *sqlReturning {
	return &sqlReturning{
		way:    way,
		insert: insert,
	}
}

func (s *sqlReturning) ToEmpty() {
	s.insert = NewEmptySQL()
	s.prepare = nil
	s.execute = nil
}

// Prepare You may need to modify the SQL statement to be executed.
func (s *sqlReturning) Prepare(prepare func(tmp *SQL)) SQLReturning {
	s.prepare = prepare
	return s
}

// Returning Set the RETURNING statement to return one or more columns.
func (s *sqlReturning) Returning(columns ...string) SQLReturning {
	columns = ArrayDiscard(columns, func(k int, v string) bool { return strings.TrimSpace(v) == StrEmpty })
	if len(columns) == 0 {
		columns = []string{StrStar}
	}
	return s.Prepare(func(tmp *SQL) {
		tmp.Prepare = JoinSQLSpace(tmp.Prepare, StrReturning, JoinSQLCommaSpace(AnyAny(columns)...)).Prepare
	})
}

// ToSQL Make SQL.
func (s *sqlReturning) ToSQL() *SQL {
	result := s.insert.Copy()
	if prepare := s.prepare; prepare != nil {
		prepare(result)
	}
	return result
}

// Execute Customize the method to return the sequence value of inserted data.
func (s *sqlReturning) Execute(execute func(ctx context.Context, stmt *Stmt, args ...any) (id int64, err error)) SQLReturning {
	s.execute = execute
	return s
}

// SQLUpdateSet Build UPDATE-SET statements.
type SQLUpdateSet interface {
	Maker

	ToEmpty

	Len() int

	// Forbid Set a list of columns that cannot be updated.
	Forbid(columns ...string) SQLUpdateSet

	// GetForbid Get a list of columns that are prohibited from updating.
	GetForbid() []string

	// Select Set columns that only allow updates, not including defaults.
	Select(columns ...string) SQLUpdateSet

	// Set Update column assignment.
	Set(column string, value any) SQLUpdateSet

	// Decr Update column decrement.
	Decr(column string, decr any) SQLUpdateSet

	// Incr Update column increment.
	Incr(column string, incr any) SQLUpdateSet

	// SetMap Update column assignment by map.
	SetMap(columnValue map[string]any) SQLUpdateSet

	// SetSlice Update column assignment by slice.
	SetSlice(columns []string, values []any) SQLUpdateSet

	// Update Parse the given update data and assign the update value.
	Update(update any) SQLUpdateSet

	// Compare Compare struct assignment update.
	Compare(old, new any, except ...string) SQLUpdateSet

	// Default Set the default columns that need to be updated, such as update timestamp.
	Default(column string, value any) SQLUpdateSet

	// Remove Delete a column-value.
	Remove(columns ...string) SQLUpdateSet

	// GetUpdate Get a list of existing updates.
	GetUpdate() ([]string, [][]any)

	// SetUpdate Delete the existing update list and set the update list.
	SetUpdate(updates []string, params [][]any) SQLUpdateSet
}

type sqlUpdateSet struct {
	forbidSet map[string]*struct{}

	exists map[string][]string // column => expression lists

	onlyAllow map[string]*struct{} // Set columns that only allow updates.

	updateMap map[string]int

	way *Way

	defaults *sqlUpdateSet

	updateExpr []string

	updateArgs [][]any
}

func (s *sqlUpdateSet) init() {
	s.forbidSet = make(map[string]*struct{}, 1<<3)
	s.exists = make(map[string][]string, 1<<3)
	s.updateMap = make(map[string]int, 1<<3)
	s.updateExpr = make([]string, 0, 1<<3)
	s.updateArgs = make([][]any, 0, 1<<3)
}

func (s *sqlUpdateSet) toEmpty() {
	s.forbidSet = make(map[string]*struct{}, 1<<3)
	s.exists = make(map[string][]string, 1<<3)
	s.onlyAllow = nil
	s.updateMap = make(map[string]int, 1<<3)
	s.updateExpr = make([]string, 0, 1<<3)
	s.updateArgs = make([][]any, 0, 1<<3)
}

func newSqlUpdateSet(way *Way) *sqlUpdateSet {
	result := &sqlUpdateSet{
		way: way,
	}
	defaults := &sqlUpdateSet{
		way: way,
	}
	result.init()
	defaults.init()
	result.defaults = defaults
	return result
}

func (s *sqlUpdateSet) ToEmpty() {
	s.toEmpty()
	s.defaults.toEmpty()
}

func (s *sqlUpdateSet) IsEmpty() bool {
	return len(s.updateExpr) == 0
}

func (s *sqlUpdateSet) ToSQL() *SQL {
	script := NewSQL(StrEmpty)
	if s.IsEmpty() {
		return script
	}

	length := len(s.updateExpr)
	if length == 0 {
		return script
	}

	lengths := length + 8
	updates := make([]string, lengths)
	copy(updates, s.updateExpr)
	params := make([][]any, lengths)
	copy(params, s.updateArgs)

	defaultUpdates := s.defaults.updateExpr
	if len(defaultUpdates) > 0 {
		for index, defaultUpdate := range defaultUpdates {
			if _, ok := s.updateMap[defaultUpdate]; !ok {
				updates = append(updates, defaultUpdate)
				params = append(params, s.defaults.updateArgs[index])
			}
		}
	}

	script.Prepare = strings.Join(updates, StrCommaSpace)
	for _, tmp := range params {
		script.Args = append(script.Args, tmp...)
	}
	return script
}

func (s *sqlUpdateSet) beautifyExpr(update string) string {
	update = strings.TrimSpace(update)
	for strings.Contains(update, "  ") {
		update = strings.ReplaceAll(update, "  ", StrSpace)
	}
	return update
}

func (s *sqlUpdateSet) exprArgs(value *SQL) SQLUpdateSet {
	if value == nil || value.IsEmpty() {
		return s
	}
	update := s.beautifyExpr(value.Prepare)
	if update == StrEmpty {
		return s
	}
	index, ok := s.updateMap[update]
	if ok {
		s.updateExpr[index], s.updateArgs[index] = update, value.Args
		return s
	}
	s.updateMap[update] = len(s.updateExpr)
	s.updateExpr = append(s.updateExpr, update)
	s.updateArgs = append(s.updateArgs, value.Args)
	return s
}

func (s *sqlUpdateSet) Len() int {
	return len(s.updateExpr)
}

func (s *sqlUpdateSet) Forbid(columns ...string) SQLUpdateSet {
	for _, column := range columns {
		s.forbidSet[column] = nil
		s.defaults.forbidSet[column] = nil
	}
	return s
}

func (s *sqlUpdateSet) GetForbid() []string {
	columns := make([]string, 0, len(s.forbidSet))
	for column := range s.forbidSet {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return columns
}

func (s *sqlUpdateSet) Select(columns ...string) SQLUpdateSet {
	onlyAllow := make(map[string]*struct{}, len(columns))
	for _, column := range columns {
		onlyAllow[column] = nil
	}
	if s.onlyAllow == nil {
		s.onlyAllow = make(map[string]*struct{}, 1<<3)
	}
	maps.Copy(s.onlyAllow, onlyAllow)
	return s
}

func (s *sqlUpdateSet) columnUpdate(column string, script *SQL) SQLUpdateSet {
	if s.onlyAllow != nil {
		if _, ok := s.onlyAllow[column]; !ok {
			return s
		}
	}
	s.exists[column] = append(s.exists[column], script.Prepare)
	return s.exprArgs(script)
}

func (s *sqlUpdateSet) Set(column string, value any) SQLUpdateSet {
	if _, ok := s.forbidSet[column]; ok {
		return s
	}
	replace := s.way.Replace(column)
	script := NewSQL(fmt.Sprintf("%s = %s", replace, StrPlaceholder), value)
	return s.columnUpdate(column, script)
}

func (s *sqlUpdateSet) Decr(column string, decrement any) SQLUpdateSet {
	if _, ok := s.forbidSet[column]; ok {
		return s
	}
	replace := s.way.Replace(column)
	script := NewSQL(fmt.Sprintf("%s = %s - %s", replace, replace, StrPlaceholder), decrement)
	return s.columnUpdate(column, script)
}

func (s *sqlUpdateSet) Incr(column string, increment any) SQLUpdateSet {
	if _, ok := s.forbidSet[column]; ok {
		return s
	}
	replace := s.way.Replace(column)
	script := NewSQL(fmt.Sprintf("%s = %s + %s", replace, replace, StrPlaceholder), increment)
	return s.columnUpdate(column, script)
}

func (s *sqlUpdateSet) SetMap(columnValue map[string]any) SQLUpdateSet {
	columns := make([]string, 0, len(columnValue))
	for column := range columnValue {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	for column := range columnValue {
		s.Set(column, columnValue[column])
	}
	return s
}

// SetSlice SET column = value by slice, require len(columns) == len(values).
func (s *sqlUpdateSet) SetSlice(columns []string, values []any) SQLUpdateSet {
	for index, column := range columns {
		s.Set(column, values[index])
	}
	return s
}

// Update Value of update should be one of anyStruct, *anyStruct, map[string]any.
func (s *sqlUpdateSet) Update(update any) SQLUpdateSet {
	if columnValue, ok := update.(map[string]any); ok {
		columns := make([]string, 0, len(columnValue))
		for column := range columnValue {
			columns = append(columns, column)
		}
		sort.Strings(columns)
		for _, column := range columns {
			s.Set(column, columnValue[column])
		}
		return s
	}
	if tmp, ok := update.(*SQL); ok {
		return s.exprArgs(tmp)
	}
	if tmp, ok := update.(Maker); ok {
		return s.exprArgs(tmp.ToSQL())
	}
	return s.SetSlice(StructModify(update, s.way.cfg.ScanTag))
}

// Compare For compare old and new to automatically calculate the need to update columns.
func (s *sqlUpdateSet) Compare(old, new any, except ...string) SQLUpdateSet {
	return s.SetSlice(StructUpdate(old, new, s.way.cfg.ScanTag, except...))
}

func (s *sqlUpdateSet) Default(column string, value any) SQLUpdateSet {
	column = strings.TrimSpace(column)
	if column == StrEmpty {
		return s
	}
	if _, ok := s.forbidSet[column]; ok {
		return s
	}
	replace := s.way.Replace(column)
	script := NewSQL(fmt.Sprintf("%s = %s", replace, StrPlaceholder), value)
	if _, ok := s.updateMap[script.Prepare]; ok {
		return s
	}
	s.defaults.columnUpdate(column, script)
	return s
}

func (s *sqlUpdateSet) Remove(columns ...string) SQLUpdateSet {
	s.Forbid(columns...)
	removes := make(map[string]*struct{}, 1<<3)
	for _, column := range columns {
		if tmp, ok := s.exists[column]; ok {
			removes = MergeAssoc(removes, ArrayToAssoc(tmp, func(v string) (string, *struct{}) { return v, nil }))
		}
	}
	dropExpr := make(map[string]*struct{}, 1<<3)
	dropArgs := make(map[int]*struct{}, 1<<3)
	for index, value := range s.updateExpr {
		if _, ok := removes[value]; ok {
			dropExpr[value] = nil
			dropArgs[index] = nil
		}
	}
	updateExpr := ArrayDiscard(s.updateExpr, func(k int, v string) bool {
		_, ok := dropExpr[v]
		return ok
	})

	updateArgs := ArrayDiscard(s.updateArgs, func(k int, v []any) bool {
		_, ok := dropArgs[k]
		return ok
	})
	updateMap := AssocDiscard(s.updateMap, func(k string, v int) bool {
		_, ok := dropExpr[k]
		return ok
	})
	s.updateExpr, s.updateArgs, s.updateMap = updateExpr, updateArgs, updateMap
	if s.defaults != nil {
		s.defaults.Remove(columns...)
	}
	return s
}

func (s *sqlUpdateSet) GetUpdate() ([]string, [][]any) {
	return s.updateExpr, s.updateArgs
}

func (s *sqlUpdateSet) SetUpdate(updates []string, params [][]any) SQLUpdateSet {
	s.ToEmpty()
	for index, value := range updates {
		script := NewSQL(value, params[index]...)
		s.exprArgs(script)
	}
	return s
}

// SQLOnConflictUpdateSet Implement the following SQL statement:
// INSERT INTO ... ON CONFLICT ( column_a[, column_b, column_c...] ) DO UPDATE SET column1 = EXCLUDED.column1, column2 = EXCLUDED.column2, column3 = EXCLUDED.column3, column4 = 'fixed value' ...
type SQLOnConflictUpdateSet interface {
	SQLUpdateSet

	// Excluded Construct the update expression column1 = EXCLUDED.column1, column2 = EXCLUDED.column2, column3 = EXCLUDED.column3 ...
	// This is how the `new` data is accessed that causes the conflict.
	Excluded(columns ...string) SQLOnConflictUpdateSet
}

type sqlOnConflictUpdateSet struct {
	SQLUpdateSet

	way *Way
}

func newSqlOnConflictUpdateSet(way *Way) SQLOnConflictUpdateSet {
	tmp := &sqlOnConflictUpdateSet{
		way: way,
	}
	tmp.SQLUpdateSet = newSqlUpdateSet(way)
	return tmp
}

func (s *sqlOnConflictUpdateSet) Excluded(columns ...string) SQLOnConflictUpdateSet {
	for _, column := range columns {
		tmp := s.way.Replace(column)
		s.Update(NewSQL(Strings(tmp, StrSpace, StrEqual, StrSpace, StrExcluded, StrPoint, tmp)))
	}
	return s
}

// SQLOnConflict Implement the following SQL statement:
// INSERT INTO ... ON CONFLICT (column_a[, column_b, column_c...]) DO NOTHING /* If a conflict occurs, the insert operation is ignored. */
// INSERT INTO ... ON CONFLICT (column_a[, column_b, column_c...]) DO UPDATE SET column1 = EXCLUDED.column1, column2 = EXCLUDED.column2, column3 = EXCLUDED.column3, column4 = 'fixed value' ... /* If a conflict occurs, the existing row is updated with the new value */
type SQLOnConflict interface {
	Maker

	ToEmpty

	// OnConflict The column causing the conflict, such as a unique key or primary key, which can be a single column or multiple columns.
	OnConflict(conflicts ...string) SQLOnConflict

	// Do The SQL statement that needs to be executed when a data conflict occurs. By default, nothing is done.
	Do(maker Maker) SQLOnConflict

	// DoUpdateSet SQL update statements executed when data conflicts occur.
	DoUpdateSet(fc func(u SQLOnConflictUpdateSet)) SQLOnConflict
}

type sqlOnConflict struct {
	onConflictsDoUpdateSet SQLOnConflictUpdateSet

	way *Way

	insert Maker

	onConflictsDo Maker

	onConflicts []string
}

func newSqlOnConflict(way *Way, insert Maker) *sqlOnConflict {
	return &sqlOnConflict{
		way:    way,
		insert: insert,
	}
}

func (s *sqlOnConflict) ToEmpty() {
	s.onConflictsDoUpdateSet = nil
	s.insert = nil
	s.onConflictsDo = nil
	s.onConflicts = make([]string, 0, 1<<1)
}

func (s *sqlOnConflict) OnConflict(onConflicts ...string) SQLOnConflict {
	s.onConflicts = onConflicts
	return s
}

func (s *sqlOnConflict) Do(maker Maker) SQLOnConflict {
	s.onConflictsDo = maker
	return s
}

func (s *sqlOnConflict) DoUpdateSet(fc func(u SQLOnConflictUpdateSet)) SQLOnConflict {
	tmp := s.onConflictsDoUpdateSet
	if tmp == nil {
		s.onConflictsDoUpdateSet = newSqlOnConflictUpdateSet(s.way)
		tmp = s.onConflictsDoUpdateSet
	}
	fc(tmp)
	return s
}

func (s *sqlOnConflict) ToSQL() *SQL {
	script := NewSQL(StrEmpty)
	if s.insert == nil || s.insert.ToSQL().IsEmpty() || len(s.onConflicts) == 0 {
		return script
	}
	insert := s.insert.ToSQL()
	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)
	b.WriteString(insert.Prepare)
	script.Args = append(script.Args, insert.Args...)
	b.WriteString(Strings(StrSpace, StrOn, StrSpace, StrConflict, StrSpace))
	b.WriteString(ParcelPrepare(strings.Join(s.way.ReplaceAll(s.onConflicts), StrCommaSpace)))
	b.WriteString(StrSpace)
	b.WriteString(StrDo)
	b.WriteString(StrSpace)
	prepare, args := StrNothing, make([]any, 0)
	if onConflictsDo := s.onConflictsDo; onConflictsDo != nil {
		if tmp := onConflictsDo.ToSQL(); tmp != nil && !tmp.IsEmpty() {
			prepare, args = tmp.Prepare, tmp.Args[:]
		}
	}
	if prepare == StrNothing && s.onConflictsDoUpdateSet != nil && s.onConflictsDoUpdateSet.Len() > 0 {
		update := s.onConflictsDoUpdateSet.ToSQL()
		b1 := poolGetStringBuilder()
		defer poolPutStringBuilder(b1)
		b.WriteString(Strings(StrUpdate, StrSpace, StrSet, StrSpace))
		b1.WriteString(update.Prepare)
		prepare, args = b1.String(), update.Args[:]
	}
	b.WriteString(prepare)
	script.Args = append(script.Args, args...)
	script.Prepare = b.String()
	return script
}

// SQLInsert Build INSERT statements.
type SQLInsert interface {
	Maker

	ToEmpty

	// Table Insert data into the target table.
	Table(table Maker) SQLInsert

	// Forbid When inserting data, it is forbidden to set certain columns, such as: auto-increment id.
	Forbid(columns ...string) SQLInsert

	// GetForbid Get a list of columns that have been prohibited from insertion.
	GetForbid() []string

	// Select Set the columns to allow inserts only, not including defaults.
	Select(columns ...string) SQLInsert

	// Column Set the inserted column list. An empty value will delete the set field list.
	Column(columns ...string) SQLInsert

	// Values Set the list of values to be inserted.
	Values(values ...[]any) SQLInsert

	// ColumnValue Set a single column and value.
	ColumnValue(column string, value any) SQLInsert

	// Create Parses the given insert data and sets the insert data.
	Create(create any) SQLInsert

	// Default Set the default column for inserted data, such as the creation timestamp.
	Default(column string, value any) SQLInsert

	// Remove Delete a column-value.
	Remove(columns ...string) SQLInsert

	// Returning Insert a piece of data and get the auto-increment value.
	Returning(fc func(r SQLReturning)) SQLInsert

	// GetColumn Get the list of inserted columns that have been set.
	GetColumn(excludes ...string) []string

	// OnConflict When inserting data, set the execution logic when there is a conflict.
	OnConflict(fc func(o SQLOnConflict)) SQLInsert
}

type sqlInsert struct {
	forbidSet  map[string]*struct{}
	way        *Way
	table      *SQL
	onlyAllow  map[string]*struct{} // Set the columns to allow inserts only.
	columns    *sqlSelect
	values     *sqlValues
	returning  *sqlReturning
	onConflict *sqlOnConflict
	defaults   *sqlInsert
}

func (s *sqlInsert) init() {
	s.forbidSet = make(map[string]*struct{}, 1<<3)
	s.table = NewEmptySQL()
	s.columns = newSqlSelect(s.way)
	s.values = newSqlValues()
	s.returning = newReturning(s.way, NewEmptySQL())
	s.onConflict = newSqlOnConflict(s.way, NewEmptySQL())
}

func (s *sqlInsert) toEmpty() {
	s.forbidSet = make(map[string]*struct{}, 1<<3)
	s.onlyAllow = nil
	s.columns.ToEmpty()
	s.values.ToEmpty()
	s.returning.ToEmpty()
	s.onConflict.ToEmpty()
}

func newSqlInsert(way *Way) *sqlInsert {
	result := &sqlInsert{
		way: way,
	}
	defaults := &sqlInsert{
		way: way,
	}
	result.init()
	defaults.init()
	result.defaults = defaults
	return result
}

func (s *sqlInsert) ToEmpty() {
	s.toEmpty()
	s.defaults.toEmpty()
}

func (s *sqlInsert) ToSQL() *SQL {
	makers := []any{NewSQL(StrInsert), NewSQL(StrInto), s.table}

	columns1, params1 := s.columns.Get()
	values1 := make([][]any, len(s.values.values))
	copy(values1, s.values.values)

	columns, values := make([]string, len(columns1)), make([][]any, len(values1))
	copy(columns, columns1)
	copy(values, values1)
	params := make(map[int][]any, len(params1))
	maps.Copy(params, params1)
	if len(columns) > 0 {
		if len(values) > 0 {
			// add default columns and values.
			defaultColumns, defaultParams := s.defaults.columns.Get()
			defaultValues := make([][]any, len(s.defaults.values.values))
			copy(defaultValues, s.defaults.values.values)
			defaultColumnsLength := len(defaultColumns)
			defaultValuesLength := len(defaultValues)
			if defaultColumnsLength > 0 && defaultValuesLength > 0 && defaultColumnsLength == len(defaultValues[0]) {
				had := make(map[string]*struct{}, len(columns))
				for _, column := range columns {
					had[column] = nil
				}
				for index, column := range defaultColumns {
					if _, ok := had[column]; ok {
						continue
					}
					columns = append(columns, column)
					params[len(columns)] = defaultParams[index]
					for i := range values {
						values[i] = append(values[i], defaultValues[index]...)
					}
				}
			}
		}
		makers = append(makers, NewSQL(StrLeftSmallBracket))
		makers = append(makers, NewSQL(strings.Join(columns, StrCommaSpace)))
		makers = append(makers, NewSQL(StrRightSmallBracket))
	}

	ok := false

	subquery := s.values.subquery
	if subquery != nil {
		if script := subquery.ToSQL(); !script.IsEmpty() {
			makers = append(makers, script)
			ok = true
		}
	}

	if !ok {
		if len(values) > 0 {
			makers = append(makers, NewSQL(StrValues))
			makers = append(makers, s.values.valuesToSQL(values))
			ok = true
		}
	}

	if !ok {
		return NewEmptySQL()
	}

	if s.returning != nil && s.returning.execute != nil {
		s.returning.insert = JoinSQLSpace(makers...)
		if script := s.returning.ToSQL(); !script.IsEmpty() {
			return script
		}
	}

	if s.onConflict != nil && len(s.onConflict.onConflicts) > 0 {
		s.onConflict.insert = JoinSQLSpace(makers...)
		if script := s.onConflict.ToSQL(); !script.IsEmpty() {
			return script
		}
	}

	return JoinSQLSpace(makers...)
}

func (s *sqlInsert) Table(table Maker) SQLInsert {
	if table == nil {
		return s
	}
	script := table.ToSQL()
	if script.IsEmpty() {
		return s
	}
	s.table = script
	return s
}

func (s *sqlInsert) Forbid(columns ...string) SQLInsert {
	for _, column := range columns {
		s.forbidSet[column] = nil
		s.defaults.forbidSet[column] = nil
	}
	return s
}

func (s *sqlInsert) GetForbid() []string {
	columns := make([]string, 0, len(s.forbidSet))
	for column := range s.forbidSet {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return columns
}

func (s *sqlInsert) Select(columns ...string) SQLInsert {
	onlyAllow := make(map[string]*struct{}, len(columns))
	for _, column := range columns {
		onlyAllow[column] = nil
	}
	if s.onlyAllow == nil {
		s.onlyAllow = make(map[string]*struct{}, 1<<3)
	}
	maps.Copy(s.onlyAllow, onlyAllow)
	return s
}

func (s *sqlInsert) Column(columns ...string) SQLInsert {
	if len(columns) == 0 {
		return s
	}
	s.columns.ToEmpty() // Clear previous columns.
	s.columns.Select(columns)
	return s
}

func (s *sqlInsert) Values(values ...[]any) SQLInsert {
	if len(values) == 0 {
		return s
	}
	s.values.Values(values...)
	return s
}

func (s *sqlInsert) ColumnValue(column string, value any) SQLInsert {
	if _, ok := s.forbidSet[column]; ok {
		return s
	}
	if s.columns.Has(column) {
		return s
	}
	if s.onlyAllow != nil {
		if _, ok := s.onlyAllow[column]; !ok {
			return s
		}
	}
	s.columns.AddAll(column)
	for index := range s.values.values {
		s.values.values[index] = append(s.values.values[index], value)
	}
	return s
}

// Create value of creation should be one of struct{}, *struct{}, map[string]any, []struct, []*struct{}, *[]struct{}, *[]*struct{}.
func (s *sqlInsert) Create(create any) SQLInsert {
	if columnValue, ok := create.(map[string]any); ok {
		length := len(columnValue)
		if length == 0 {
			return s
		}
		columns := make([]string, 0, length)
		for column := range columnValue {
			if _, ok := s.forbidSet[column]; ok {
				continue
			}
			columns = append(columns, column)
		}
		sort.Strings(columns)
		for _, column := range columns {
			s.ColumnValue(column, columnValue[column])
		}
		return s
	}
	columns, values := StructInsert(create, s.way.cfg.ScanTag, nil, nil)
	removes := make(map[int]*struct{}, len(columns))
	for index, column := range columns {
		if _, ok := s.forbidSet[column]; ok {
			removes[index] = nil
		}
	}
	if len(removes) > 0 {
		columns = ArrayDiscard(columns, func(k int, v string) bool {
			_, ok := removes[k]
			return ok
		})
		for index, value := range values {
			values[index] = ArrayDiscard(value, func(k int, v any) bool {
				_, ok := removes[k]
				return ok
			})
		}
	}
	if s.onlyAllow != nil {
		indexes := make(map[int]*struct{}, 1<<3)
		for index, column := range columns {
			if _, ok := s.onlyAllow[column]; ok {
				indexes[index] = nil
			}
		}
		columns = ArrayDiscard(columns, func(k int, v string) bool {
			_, ok := indexes[k]
			return ok
		})
		for index, value := range values {
			values[index] = ArrayDiscard(value, func(k int, v any) bool {
				_, ok := indexes[k]
				return ok
			})
		}
	}
	return s.Column(columns...).Values(values...)
}

func (s *sqlInsert) Default(column string, value any) SQLInsert {
	s.defaults.ColumnValue(column, value)
	return s
}

func (s *sqlInsert) Remove(columns ...string) SQLInsert {
	fields, params := s.columns.Get()
	values := s.values.values
	length1, length2 := len(fields), len(values)
	if length1 == 0 || length2 == 0 {
		return s
	}
	ok := true
	for _, value := range values {
		if length1 != len(value) {
			ok = false
			break
		}
	}
	if !ok {
		return s
	}

	removes := make(map[string]*struct{}, len(columns))
	for _, column := range columns {
		removes[column] = nil
	}
	assoc := make(map[int]*struct{}, length1)
	for index, field := range fields {
		if _, ok = removes[field]; ok {
			assoc[index] = nil
		}
	}
	if len(assoc) == 0 {
		return s
	}
	fields = ArrayDiscard(fields, func(k int, v string) bool {
		if _, ok = assoc[k]; ok {
			delete(params, k)
		}
		return ok
	})
	s.columns.Del().Set(fields, params)
	for index, value := range values {
		s.values.values[index] = ArrayDiscard(value, func(k int, v any) bool {
			_, ok = assoc[k]
			return ok
		})
	}
	if s.defaults != nil {
		s.defaults.Remove(columns...)
	}
	return s
}

func (s *sqlInsert) Returning(fc func(r SQLReturning)) SQLInsert {
	if s.returning == nil {
		s.returning = newReturning(s.way, NewEmptySQL())
	}
	fc(s.returning)
	return s
}

func (s *sqlInsert) GetColumn(excludes ...string) []string {
	lengths := len(excludes)
	columns, _ := s.columns.Get()
	if lengths == 0 {
		return columns
	}
	discard := ArrayToAssoc(excludes, func(v string) (string, *struct{}) { return v, nil })
	columns = ArrayDiscard(columns, func(k int, v string) bool {
		_, ok := discard[v]
		return ok
	})
	return columns
}

func (s *sqlInsert) OnConflict(fc func(o SQLOnConflict)) SQLInsert {
	fc(s.onConflict)
	return s
}

// Table Represents a SQL table operation, such as SELECT, INSERT, UPDATE, DELETE.
type Table struct {
	way *Way

	comment *sqlComment
	with    *sqlWith
	selects *sqlSelect
	table   *sqlAlias
	joins   *sqlJoin
	where   Filter
	groupBy *sqlGroupBy
	orderBy *sqlOrderBy
	limit   *sqlLimit

	insert    *sqlInsert
	updateSet *sqlUpdateSet
}

// Table Create a *Table object to execute SELECT, INSERT, UPDATE, and DELETE statements.
func (s *Way) Table(table any) *Table {
	return &Table{
		way:       s,
		comment:   newSqlComment(),
		with:      newSqlWith(),
		selects:   newSqlSelect(s),
		table:     newSqlAlias(table),
		joins:     newSqlJoin(s),
		where:     s.F(),
		groupBy:   newSqlGroupBy(s),
		orderBy:   newSqlOrderBy(s),
		limit:     newSqlLimit(),
		insert:    newSqlInsert(s),
		updateSet: newSqlUpdateSet(s),
	}
}

// ToEmpty Do not reset table.
func (s *Table) ToEmpty() *Table {
	s.comment = newSqlComment()
	s.with = newSqlWith()
	s.selects = newSqlSelect(s.way)
	s.joins = newSqlJoin(s.way)
	s.where = s.way.F()
	s.groupBy = newSqlGroupBy(s.way)
	s.orderBy = newSqlOrderBy(s.way)
	s.limit = newSqlLimit()
	s.insert = newSqlInsert(s.way)
	s.updateSet = newSqlUpdateSet(s.way)
	return s
}

// Comment SQL statement notes.
func (s *Table) Comment(comment string) *Table {
	s.comment.Comment(comment)
	return s
}

// With Custom common table expression (CTE).
func (s *Table) With(fc func(w SQLWith)) *Table {
	fc(s.with)
	return s
}

// Select Add one or more query lists. If no parameter is provided, all existing query lists will be deleted.
func (s *Table) Select(selects ...any) *Table {
	s.selects.Select(selects...)
	return s
}

// Table Set the table name, or possibly a subquery with an alias.
func (s *Table) Table(table any) *Table {
	if table == nil {
		s.table = newSqlAlias(StrEmpty)
		return s
	}
	if tmp, ok := table.(*sqlAlias); ok && tmp != nil {
		s.table = tmp
		return s
	}
	s.table = newSqlAlias(table)
	return s
}

// Alias Set the table alias name.
func (s *Table) Alias(alias string) *Table {
	s.table.SetAlias(alias)
	return s
}

// Join Custom join query.
func (s *Table) Join(fc func(j SQLJoin)) *Table {
	fc(s.joins)
	return s
}

// Where Set the WHERE condition.
func (s *Table) Where(fc func(f Filter)) *Table {
	fc(s.where)
	return s
}

// Group Set up grouping.
func (s *Table) Group(fc func(g SQLGroupBy)) *Table {
	fc(s.groupBy)
	return s
}

// Asc Sort ascending.
func (s *Table) Asc(column string) *Table {
	s.orderBy.Asc(column)
	return s
}

// Desc Sort descending.
func (s *Table) Desc(column string) *Table {
	s.orderBy.Desc(column)
	return s
}

// Limit Set the maximum number of query result sets.
func (s *Table) Limit(limit int64) *Table {
	s.limit.Limit(limit)
	return s
}

// Offset Set the offset of the query target data.
func (s *Table) Offset(offset int64) *Table {
	s.limit.Offset(offset)
	return s
}

// Limiter Set limit and offset at the same time.
func (s *Table) Limiter(limiter Limiter) *Table {
	if limiter == nil {
		return s
	}
	return s.Limit(limiter.GetLimit()).Offset(limiter.GetOffset())
}

// Page Pagination query, page number + page limit.
func (s *Table) Page(page int64, limit ...int64) *Table {
	s.limit.Page(page, limit...)
	return s
}

// AboutInsert About inserting data.
func (s *Table) AboutInsert(fc func(i SQLInsert)) *Table {
	fc(s.insert)
	return s
}

// AboutUpdateSet About updating data.
func (s *Table) AboutUpdateSet(fc func(f Filter, u SQLUpdateSet)) *Table {
	fc(s.where, s.updateSet)
	return s
}

// ToSelect Build SELECT statement.
func (s *Table) ToSelect() *SQL {
	lists := make([]any, 0, 16)
	lists = append(lists, s.comment, s.with, StrSelect, s.selects, StrFrom, s.table, s.joins)
	if !s.where.IsEmpty() {
		lists = append(lists, StrWhere, s.where)
	}
	lists = append(lists, s.groupBy, s.orderBy, s.limit)
	return JoinSQLSpace(lists...).ToSQL()
}

// ToInsert Build INSERT statement.
func (s *Table) ToInsert() *SQL {
	insert := s.insert
	if insert.table == nil || insert.table.IsEmpty() {
		insert.table = s.table.ToSQL()
	}
	return JoinSQLSpace(s.comment, insert).ToSQL()
}

// ToUpdate Build UPDATE statement.
func (s *Table) ToUpdate() *SQL {
	lists := make([]any, 0, 16)
	lists = append(lists, s.comment, s.with, StrUpdate, s.table, s.joins)
	lists = append(lists, StrSet, s.updateSet)
	if !s.where.IsEmpty() {
		lists = append(lists, StrWhere, s.where)
	}
	lists = append(lists, s.orderBy, s.limit)
	return JoinSQLSpace(lists...).ToSQL()
}

// ToDelete Build DELETE statement.
func (s *Table) ToDelete() *SQL {
	lists := make([]any, 0, 16)
	lists = append(lists, s.comment, s.with, StrDelete, StrFrom, s.table, s.joins)
	if !s.where.IsEmpty() {
		lists = append(lists, StrWhere, s.where)
	}
	lists = append(lists, s.orderBy, s.limit)
	return JoinSQLSpace(lists...).ToSQL()
}

// Query Execute a SELECT statement.
func (s *Table) Query(ctx context.Context, query func(rows *sql.Rows) error) error {
	return s.way.Query(ctx, s.ToSelect(), query)
}

// Count Total number of statistics.
func (s *Table) Count(ctx context.Context, counts ...string) (int64, error) {
	if len(counts) == 0 {
		counts = []string{
			Strings("COUNT(*)", StrSpace, StrAs, StrSpace, s.way.Replace(DefaultAliasNameCount)),
		}
	}
	count := int64(0)
	lists := make([]any, 0, 16)
	lists = append(lists, s.comment, s.with, StrSelect, newSqlSelect(s.way).AddAll(counts...), StrFrom, s.table, s.joins)
	if !s.where.IsEmpty() {
		lists = append(lists, StrWhere, s.where)
	}
	script := JoinSQLSpace(lists...).ToSQL()
	err := s.way.Query(ctx, script, func(rows *sql.Rows) error {
		for rows.Next() {
			if err := rows.Scan(&count); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// Fetch Scan data into result by reflect.
func (s *Table) Fetch(ctx context.Context, result any) error {
	return s.way.Fetch(ctx, s.ToSelect(), result)
}

// Insert Execute an INSERT INTO statement.
func (s *Table) Insert(ctx context.Context) (int64, error) {
	script := s.ToInsert()
	if insert := s.insert; insert != nil {
		if returning := insert.returning; returning != nil && returning.execute != nil {
			stmt, err := s.way.Prepare(ctx, script.Prepare)
			if err != nil {
				return 0, err
			}
			return returning.execute(ctx, stmt, script.Args...)
		}
	}
	return s.way.Execute(ctx, script)
}

// Update Execute an UPDATE statement.
func (s *Table) Update(ctx context.Context) (int64, error) {
	if s.way.cfg.UpdateMustUseWhere && (s.where == nil || s.where.IsEmpty()) {
		return 0, nil
	}
	return s.way.Execute(ctx, s.ToUpdate())
}

// Delete Execute a DELETE statement.
func (s *Table) Delete(ctx context.Context) (int64, error) {
	if s.way.cfg.DeleteMustUseWhere && (s.where == nil || s.where.IsEmpty()) {
		return 0, nil
	}
	return s.way.Execute(ctx, s.ToDelete())
}

// Create Quickly insert data into the table.
func (s *Table) Create(ctx context.Context, create any) (int64, error) {
	return s.AboutInsert(func(i SQLInsert) { i.Create(create) }).Insert(ctx)
}

// Modify Quickly update data in the table.
func (s *Table) Modify(ctx context.Context, modify any) (int64, error) {
	return s.AboutUpdateSet(func(f Filter, u SQLUpdateSet) { u.Update(modify) }).Update(ctx)
}

type TableColumn struct {
	way   *Way
	alias string
}

func NewTableColumn(way *Way, aliases ...string) *TableColumn {
	tmp := &TableColumn{
		way: way,
	}
	tmp.alias = LastNotEmptyString(aliases)
	return tmp
}

// T Table empty alias
func (s *Way) T() *TableColumn {
	return NewTableColumn(s)
}

// Alias Get the alias name value.
func (s *TableColumn) Alias() string {
	return s.alias
}

// SetAlias Set the alias name value.
func (s *TableColumn) SetAlias(alias string) *TableColumn {
	s.alias = alias
	return s
}

// Adjust Batch adjust columns.
func (s *TableColumn) Adjust(adjust func(column string) string, columns ...string) []string {
	if adjust != nil {
		for index, column := range columns {
			columns[index] = s.way.Replace(adjust(column))
		}
	}
	return columns
}

// ColumnAll Add table name prefix to column names in batches.
func (s *TableColumn) ColumnAll(columns ...string) []string {
	if s.alias == StrEmpty {
		return s.way.ReplaceAll(columns)
	}
	alias := s.way.Replace(s.alias)
	result := make([]string, len(columns))
	for index, column := range columns {
		result[index] = Prefix(alias, s.way.Replace(column))
	}
	return result
}

// columnAlias Set an alias for the column.
// "column_name + alias_name" -> "column_name"
// "column_name + alias_name" -> "column_name AS alias_name"
func (s *TableColumn) columnAlias(column string, aliases ...string) string {
	if alias := LastNotEmptyString(aliases); alias != StrEmpty {
		return newSqlAlias(column).v(s.way).SetAlias(alias).ToSQL().Prepare
	}
	return column
}

// Column Add table name prefix to single column name, allowing column alias to be set.
func (s *TableColumn) Column(column string, aliases ...string) string {
	return s.columnAlias(s.ColumnAll(column)[0], aliases...)
}

// Sum SUM(column[, alias])
func (s *TableColumn) Sum(column string, aliases ...string) string {
	return s.columnAlias(Sum(s.Column(column)).Prepare, aliases...)
}

// Max MAX(column[, alias])
func (s *TableColumn) Max(column string, aliases ...string) string {
	return s.columnAlias(Max(s.Column(column)).Prepare, aliases...)
}

// Min MIN(column[, alias])
func (s *TableColumn) Min(column string, aliases ...string) string {
	return s.columnAlias(Min(s.Column(column)).Prepare, aliases...)
}

// Avg AVG(column[, alias])
func (s *TableColumn) Avg(column string, aliases ...string) string {
	return s.columnAlias(Avg(s.Column(column)).Prepare, aliases...)
}

const DefaultAliasNameCount = "counts"

// Count Example
// Count(): COUNT(*) AS `counts`
// Count("total"): COUNT(*) AS `total`
// Count("1", "total"): COUNT(1) AS `total`
// Count("id", "counts"): COUNT(`id`) AS `counts`
func (s *TableColumn) Count(counts ...string) string {
	count := "COUNT(*)"
	length := len(counts)
	if length == 0 {
		// using default expression: COUNT(*) AS `counts`
		return s.columnAlias(count, s.way.Replace(DefaultAliasNameCount))
	}
	if length == 1 && counts[0] != StrEmpty {
		// only set alias name
		return s.columnAlias(count, s.way.Replace(counts[0]))
	}
	// set COUNT function parameters and alias name
	countAlias := s.way.Replace(DefaultAliasNameCount)
	column := false
	for i := range length {
		if counts[i] == StrEmpty {
			continue
		}
		if column {
			countAlias = s.way.Replace(counts[i])
			break
		}
		count, column = fmt.Sprintf("COUNT(%s)", s.Column(counts[i])), true
	}
	return s.columnAlias(count, countAlias)
}

// aggregate Perform an aggregate function on the column and set a default value to replace NULL values.
func (s *TableColumn) aggregate(column string, defaultValue any, aliases ...string) string {
	return s.columnAlias(Coalesce(s.Column(column), defaultValue).Prepare, aliases...)
}

// SUM COALESCE(SUM(column) ,0)[ AS column_alias_name]
func (s *TableColumn) SUM(column string, aliases ...string) string {
	return s.aggregate(s.Sum(column), 0, aliases...)
}

// MAX COALESCE(MAX(column) ,0)[ AS column_alias_name]
func (s *TableColumn) MAX(column string, aliases ...string) string {
	return s.aggregate(s.Max(column), 0, aliases...)
}

// MIN COALESCE(MIN(column) ,0)[ AS column_alias_name]
func (s *TableColumn) MIN(column string, aliases ...string) string {
	return s.aggregate(s.Min(column), 0, aliases...)
}

// AVG COALESCE(AVG(column) ,0)[ AS column_alias_name]
func (s *TableColumn) AVG(column string, aliases ...string) string {
	return s.aggregate(s.Avg(column), 0, aliases...)
}

/*
SQL WINDOW FUNCTION

function_name() OVER (
	[PARTITION BY column1, column2, ...]
	[ORDER BY column3 [ASC|DESC], ...]
	[RANGE | ROWS frame_specification]
) [AS alias_name]

RANGE | ROWS BETWEEN frame_start AND frame_end
Possible values for frame_start AND frame_end are:
	1. UNBOUNDED PRECEDING
	2. n PRECEDING
	3. CURRENT ROW
	4. n FOLLOWING
	5. UNBOUNDED FOLLOWING

ROWS UNBOUNDED PRECEDING <=> ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
... RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW ...
*/

// SQLWindowFuncFrame Define a window based on a start and end, the end position can be omitted and SQL defaults to the current row.
// Allows independent use of custom SQL statements and parameters as values for ROWS or RANGE statements.
type SQLWindowFuncFrame interface {
	Maker

	// SQL Custom SQL statement.
	SQL(value any) SQLWindowFuncFrame

	// UnboundedPreceding Start of partition.
	UnboundedPreceding() *SQL

	// NPreceding First n rows.
	NPreceding(n int) *SQL

	// CurrentRow Current row.
	CurrentRow() *SQL

	// NFollowing After n rows.
	NFollowing(n int) *SQL

	// UnboundedFollowing End of partition.
	UnboundedFollowing() *SQL

	// Between Build BETWEEN start AND end.
	Between(fc func(ff SQLWindowFuncFrame) (*SQL, *SQL)) SQLWindowFuncFrame
}

// sqlWindowFuncFrame Implementing the SQLWindowFuncFrame interface.
type sqlWindowFuncFrame struct {
	// script Custom SQL statement, priority is higher than prepare + args.
	script *SQL
	// frame Value is `RANGE` or `ROWS`.
	frame string
}

func NewSQLWindowFuncFrame(frame string) SQLWindowFuncFrame {
	return &sqlWindowFuncFrame{
		frame: frame,
	}
}

func (s *sqlWindowFuncFrame) ToSQL() *SQL {
	if s.frame == StrEmpty {
		return NewSQL(StrEmpty)
	}
	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)
	b.WriteString(s.frame)
	b.WriteString(StrSpace)

	if script := s.script; script != nil {
		if !script.IsEmpty() {
			b.WriteString(script.Prepare)
			script.Prepare = b.String()
			return script
		}
	}
	return NewEmptySQL()
}

func (s *sqlWindowFuncFrame) SQL(value any) SQLWindowFuncFrame {
	s.script = any2sql(value)
	return s
}

func (s *sqlWindowFuncFrame) UnboundedPreceding() *SQL {
	return any2sql("UNBOUNDED PRECEDING")
}

func (s *sqlWindowFuncFrame) NPreceding(n int) *SQL {
	return any2sql(fmt.Sprintf("%d PRECEDING", n))
}

func (s *sqlWindowFuncFrame) CurrentRow() *SQL {
	return any2sql("CURRENT ROW")
}

func (s *sqlWindowFuncFrame) NFollowing(n int) *SQL {
	return any2sql(fmt.Sprintf("%d FOLLOWING", n))
}

func (s *sqlWindowFuncFrame) UnboundedFollowing() *SQL {
	return any2sql("UNBOUNDED FOLLOWING")
}

func (s *sqlWindowFuncFrame) Between(fc func(ff SQLWindowFuncFrame) (*SQL, *SQL)) SQLWindowFuncFrame {
	if fc != nil {
		if start, end := fc(s); start != nil && end != nil && !start.IsEmpty() && !end.IsEmpty() {
			s.SQL(JoinSQLSpace(StrBetween, start, StrAnd, end))
		}
	}
	return s
}

// WindowFunc SQL window function.
type WindowFunc struct {
	way *Way

	// window The window function used.
	window *SQL

	// frameRows Define the window based on the physical row number, accurately control the number of rows (such as the first 2 rows and the last 3 rows).
	frameRange *SQL

	// frameRows Defines a window based on a range of values, including all rows with the same ORDER BY column value; suitable for handling scenarios with equal values (such as time ranges).
	frameRows *SQL

	// alias Serial number column alias.
	alias string

	// partition Setting up window partitions.
	partition []string

	// order Sorting data within a group.
	order []string
}

func NewWindowFunc(way *Way, aliases ...string) *WindowFunc {
	return &WindowFunc{
		way:   way,
		alias: LastNotEmptyString(aliases),
	}
}

func (s *Way) WindowFunc(alias string) *WindowFunc {
	return NewWindowFunc(s, alias)
}

// Window Using custom function. for example: CUME_DIST(), PERCENT_RANK(), PERCENTILE_CONT(), PERCENTILE_DISC()...
func (s *WindowFunc) Window(funcName string, funcArgs ...any) *WindowFunc {
	s.window = FuncSQL(funcName, funcArgs...)
	return s
}

// RowNumber ROW_NUMBER() Assign a unique serial number to each row, in the order specified, starting with 1.
func (s *WindowFunc) RowNumber() *WindowFunc {
	return s.Window("ROW_NUMBER")
}

// Rank RANK() Assign a rank to each row, if there are duplicate values, the rank is skipped.
func (s *WindowFunc) Rank() *WindowFunc {
	return s.Window("RANK")
}

// DenseRank DENSE_RANK() Similar to RANK(), but does not skip rankings.
func (s *WindowFunc) DenseRank() *WindowFunc {
	return s.Window("DENSE_RANK")
}

// PercentRank PERCENT_RANK()
func (s *WindowFunc) PercentRank() *WindowFunc {
	return s.Window("PERCENT_RANK")
}

// CumeDist CUME_DIST()
func (s *WindowFunc) CumeDist() *WindowFunc {
	return s.Window("CUME_DIST")
}

// Sum SUM() Returns the sum of all rows in the window.
func (s *WindowFunc) Sum(column string) *WindowFunc {
	return s.Window("SUM", s.way.Replace(column))
}

// Max MAX() Returns the maximum value within the window.
func (s *WindowFunc) Max(column string) *WindowFunc {
	return s.Window("MAX", s.way.Replace(column))
}

// Min MIN() Returns the minimum value within the window.
func (s *WindowFunc) Min(column string) *WindowFunc {
	return s.Window("MIN", s.way.Replace(column))
}

// Avg AVG() Returns the average of all rows in the window.
func (s *WindowFunc) Avg(column string) *WindowFunc {
	return s.Window("AVG", s.way.Replace(column))
}

// Count COUNT() Returns the number of rows in the window.
func (s *WindowFunc) Count(columns ...string) *WindowFunc {
	column := StrStar
	for i := len(columns) - 1; i >= 0; i-- {
		if columns[i] != StrEmpty {
			column = s.way.Replace(columns[i])
			break
		}
	}
	return s.Window("COUNT", column)
}

// Lag LAG() Returns the value of the row before the current row.
func (s *WindowFunc) Lag(column string, args ...any) *WindowFunc {
	return s.Window("LAG", headBody(s.way.Replace(column), args...))
}

// Lead LEAD() Returns the value of a row after the current row.
func (s *WindowFunc) Lead(column string, args ...any) *WindowFunc {
	return s.Window("LEAD", headBody(s.way.Replace(column), args...))
}

// NTile N-TILE Divide the rows in the window into n buckets and assign a bucket number to each row.
func (s *WindowFunc) NTile(buckets int64, args ...any) *WindowFunc {
	return s.Window("NTILE", headBody(buckets, args...))
}

// FirstValue FIRST_VALUE() Returns the value of the first row in the window.
func (s *WindowFunc) FirstValue(column string) *WindowFunc {
	return s.Window("FIRST_VALUE", headBody(s.way.Replace(column)))
}

// LastValue LAST_VALUE() Returns the value of the last row in the window.
func (s *WindowFunc) LastValue(column string) *WindowFunc {
	return s.Window("LAST_VALUE", headBody(s.way.Replace(column)))
}

// NthValue NTH_VALUE() The Nth value can be returned according to the specified order. This is very useful when you need to get data at a specific position.
func (s *WindowFunc) NthValue(column string, args ...any) *WindowFunc {
	return s.Window("NTH_VALUE", headBody(s.way.Replace(column), args...))
}

// Partition The OVER clause defines window partitions so that the window function is calculated independently in each partition.
func (s *WindowFunc) Partition(column ...string) *WindowFunc {
	s.partition = append(s.partition, s.way.ReplaceAll(column)...)
	return s
}

// Asc Define the sorting within the partition so that the window function is calculated in order.
func (s *WindowFunc) Asc(column string) *WindowFunc {
	s.order = append(s.order, fmt.Sprintf("%s %s", s.way.Replace(column), StrAsc))
	return s
}

// Desc Define the sorting within the partition so that the window function is calculated in descending order.
func (s *WindowFunc) Desc(column string) *WindowFunc {
	s.order = append(s.order, fmt.Sprintf("%s %s", s.way.Replace(column), StrDesc))
	return s
}

// Range Define the window based on the physical row number, accurately control the number of rows (such as the first 2 rows and the last 3 rows).
func (s *WindowFunc) Range(fc func(frame SQLWindowFuncFrame)) *WindowFunc {
	if fc != nil {
		frame := NewSQLWindowFuncFrame(StrRange)
		fc(frame)
		if tmp := frame.ToSQL(); tmp != nil && !tmp.IsEmpty() {
			s.frameRange, s.frameRows = tmp, nil
		}
	}
	return s
}

// Rows Defines a window based on a range of values, including all rows with the same ORDER BY column value; suitable for handling scenarios with equal values (such as time ranges).
func (s *WindowFunc) Rows(fc func(frame SQLWindowFuncFrame)) *WindowFunc {
	if fc != nil {
		frame := NewSQLWindowFuncFrame(StrRows)
		fc(frame)
		if tmp := frame.ToSQL(); tmp != nil && !tmp.IsEmpty() {
			s.frameRows, s.frameRange = tmp, nil
		}
	}
	return s
}

// Alias Set the alias of the column that uses the window function.
func (s *WindowFunc) Alias(alias string) *WindowFunc {
	s.alias = alias
	return s
}

func (s *WindowFunc) ToSQL() *SQL {
	result, window := NewSQL(StrEmpty), s.window
	if window == nil || window.IsEmpty() {
		return result
	}

	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)

	b.WriteString(window.Prepare)
	result.Args = append(result.Args, window.Args...)
	b.WriteString(Strings(StrSpace, StrOver, StrSpace, StrLeftSmallBracket))

	num := 0
	if len(s.partition) > 0 {
		num++
		b.WriteString(Strings(StrSpace, StrPartitionBy, StrSpace))
		b.WriteString(strings.Join(s.partition, StrCommaSpace))
	}
	if len(s.order) > 0 {
		num++
		b.WriteString(Strings(StrSpace, StrOrderBy, StrSpace))
		b.WriteString(strings.Join(s.order, StrCommaSpace))
	}
	if frame := s.frameRange; frame != nil {
		if tmp := frame.ToSQL(); tmp != nil && !tmp.IsEmpty() {
			num++
			b.WriteString(Strings(StrSpace, tmp.Prepare))
			result.Args = append(result.Args, tmp.Args...)
		}
	}
	if frame := s.frameRows; frame != nil {
		if tmp := frame.ToSQL(); tmp != nil && !tmp.IsEmpty() {
			num++
			b.WriteString(Strings(StrSpace, tmp.Prepare))
			result.Args = append(result.Args, tmp.Args...)
		}
	}

	if num > 0 {
		b.WriteString(StrSpace)
	}
	b.WriteString(StrRightSmallBracket)
	result.Prepare = b.String()
	return newSqlAlias(result).v(s.way).SetAlias(s.alias).ToSQL()
}

/* CASE [xxx] WHEN xxx THEN XXX [WHEN yyy THEN YYY] [ELSE xxx] END [AS xxx] */

func sqlCaseString(value string) string {
	return fmt.Sprintf("'%s'", value)
}

// sqlCaseSQLToPrepareArgs Get SQL script and args.
func sqlCaseSQLToPrepareArgs(script *SQL) (string, []any) {
	if script == nil {
		return StrNull, nil
	}
	if prepare := sqlCaseString(StrEmpty); script.Prepare == StrEmpty || script.Prepare == prepare {
		script.Prepare, script.Args = prepare, nil
	}
	return script.Prepare, script.Args
}

// SQLCase Implementing SQL CASE.
type SQLCase interface {
	Maker

	// V Set the go string as a SQL string.
	V(value string) string

	// Alias Set alias name.
	Alias(alias string) SQLCase

	// Case SQL CASE.
	Case(value any) SQLCase

	// WhenThen Add WHEN xxx THEN xxx.
	WhenThen(when, then any) SQLCase

	// Else SQL CASE xxx ELSE xxx.
	Else(value any) SQLCase
}

type sqlCase struct {
	// sqlCase CASE value , value is optional.
	sqlCase *SQL

	// sqlElse ELSE value , value is optional.
	sqlElse *SQL

	way *Way

	// alias Alias-name for CASE , value is optional.
	alias string

	// whenThen WHEN xxx THEN xxx [WHEN xxx THEN xxx] ...
	whenThen []*SQL
}

func NewSQLCase(way *Way) SQLCase {
	return &sqlCase{
		way: way,
	}
}

func (s *Way) Case() SQLCase {
	return NewSQLCase(s)
}

// V Set the go string as a SQL string.
func (s *sqlCase) V(value string) string {
	return sqlCaseString(value)
}

// ToSQL Build CASE Statement.
func (s *sqlCase) ToSQL() *SQL {
	script := NewSQL(StrEmpty)
	if len(s.whenThen) == 0 {
		return script
	}
	whenThen := JoinSQLSpace(AnyAny(s.whenThen)...).ToSQL()
	if whenThen.IsEmpty() {
		return script
	}
	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)
	b.WriteString(StrCase)
	if tmp := s.sqlCase; tmp != nil && !tmp.IsEmpty() {
		b.WriteString(StrSpace)
		prepare, args := sqlCaseSQLToPrepareArgs(tmp)
		b.WriteString(prepare)
		script.Args = append(script.Args, args...)
	}
	b.WriteString(StrSpace)
	b.WriteString(whenThen.Prepare)
	script.Args = append(script.Args, whenThen.Args...)
	if tmp := s.sqlElse; tmp != nil && !tmp.IsEmpty() {
		b.WriteString(StrSpace)
		b.WriteString(StrElse)
		b.WriteString(StrSpace)
		prepare, args := sqlCaseSQLToPrepareArgs(tmp)
		b.WriteString(prepare)
		script.Args = append(script.Args, args...)
	}
	b.WriteString(StrSpace)
	b.WriteString(StrEnd)
	script.Prepare = b.String()
	return newSqlAlias(script).v(s.way).SetAlias(s.alias).ToSQL()
}

// Alias Set the alias of the CASE.
func (s *sqlCase) Alias(alias string) SQLCase {
	s.alias = alias
	return s
}

// Case SQL CASE xxx.
func (s *sqlCase) Case(value any) SQLCase {
	s.sqlCase = nil1any2sql(value)
	return s
}

// WhenThen Add WHEN xxx THEN xxx.
func (s *sqlCase) WhenThen(when, then any) SQLCase {
	s.whenThen = append(s.whenThen, JoinSQLSpace(StrWhen, nil1any2sql(when), StrThen, nil1any2sql(then)))
	return s
}

// Else SQL ELSE xxx.
func (s *sqlCase) Else(value any) SQLCase {
	s.sqlElse = nil1any2sql(value)
	return s
}
