package hey

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"reflect"
	"strconv"
	"strings"
	"sync"
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
	return result
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
		return nil, Err("hey: unexpected parameter value")
	}
	script := maker.ToSQL()
	if script == nil || script.IsEmpty() {
		return nil, ErrEmptyPrepare
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
	StrDefaultTag      = "db"
	StrTableMethodName = "TableName"

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
		for k == reflect.Pointer {
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

func firstNext(first any, next ...any) []any {
	result := make([]any, 0, len(next)+1)
	result = append(result, first)
	result = append(result, next...)
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

// Replacer SQL Identifier Replacer.
// All identifier mapping relationships should be set before the program is initialized.
// They cannot be set again while the program is running to avoid concurrent reading and writing of the map.
type Replacer interface {
	Get(key string) string

	Set(key string, value string) Replacer

	Del(key string) Replacer

	Map() map[string]string

	GetAll(keys []string) []string
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
	SetSQL(script any) SQLAlias

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

func Alias(script any, aliases ...string) SQLAlias {
	return newSqlAlias(script, aliases...)
}

func (s *Way) Alias(script any, aliases ...string) SQLAlias {
	return newSqlAlias(script, aliases...).v(s)
}

func (s *sqlAlias) v(way *Way) *sqlAlias {
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

func (s *sqlAlias) SetSQL(script any) SQLAlias {
	if script == nil {
		s.script = NewEmptySQL()
	} else {
		s.script = any2sql(script)
	}
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
	script := tmp.ToSQL()
	script.Prepare = ParcelPrepare(script.Prepare)
	return tmp.New().And(script)
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
		case reflect.Pointer:
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
			if field.Kind() == reflect.Pointer && field.IsNil() {
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
				if next.Type().Kind() == reflect.Pointer {
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
			if field.Kind() == reflect.Pointer && field.IsNil() {
				field.Set(reflect.New(field.Type()).Elem())
				rowsScan[i] = field.Addr().Interface()
				continue
			}
			rowsScan[i] = field.Addr().Interface()
		}
	}
	return nil
}

// RowsScan Scan the query result set into the receiving object. Support type *AnyStruct, **AnyStruct, *[]AnyStruct, *[]*AnyStruct, **[]AnyStruct, **[]*AnyStruct, *[]int, *[]float64, *[]string ...
func RowsScan(rows *sql.Rows, result any, tag string) error {
	refType, refValue := reflect.TypeOf(result), reflect.ValueOf(result)

	depth1 := 0
	refType1 := refType
	kind1 := refType1.Kind()
	for kind1 == reflect.Pointer {
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
				for current.Kind() == reflect.Pointer {
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

	// slice element type.
	refItemType := sliceItemType

	kind2 := refItemType.Kind()
	for kind2 == reflect.Pointer {
		depth2++
		refItemType = refItemType.Elem()
		kind2 = refItemType.Kind()
	}

	isSingle := false
	isStruct := false
	switch kind2 {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		isSingle = true
	case reflect.Struct:
		isStruct = true
	default:
	}

	if !(isSingle || isStruct) {
		return fmt.Errorf("hey: the basic type of slice elements must be a struct, int, float64, string, *struct, *int, *float64, *string... yours is `%s`", refItemType.String())
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

	if isSingle {
		for rows.Next() {
			item := reflect.New(sliceItemType)
			next := item
			for range depth2 {
				if next = next.Elem(); next.IsNil() {
					next = reflect.New(next.Type())
				}
			}
			if err := rows.Scan(item.Interface()); err != nil {
				return err
			}
			refValueSlice = reflect.Append(refValueSlice, reflect.Indirect(item))
		}
	}

	if isStruct {
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
				b.binding(refItemType, nil, tag)

				columns, err = rows.Columns()
				if err != nil {
					return err
				}
				length = len(columns)
				rowsScan = make([]any, length)
			}

			refStructPtr := reflect.New(refItemType)
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
	}

	// Set the value of the slice.
	current := refValue.Elem()
	for current.Kind() == reflect.Pointer {
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
	for k == reflect.Pointer {
		if v.IsNil() {
			for k == reflect.Pointer {
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
		for valueIndexFieldKind == reflect.Pointer {
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
	return fields, values
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
		for valueIndexFieldKind == reflect.Pointer {
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
	return values
}

// Insert Object should be one of struct{}, *struct{}, []struct, []*struct{}, *[]struct{}, *[]*struct{}.
func (s *insertByStruct) Insert(object any, tag string, except []string, allow []string) (fields []string, values [][]any) {
	if object == nil || tag == StrEmpty {
		return fields, values
	}

	s.tag = tag
	s.setExcept(except)

	allowed := len(allow) > 0
	if allowed {
		s.setAllow(allow)
	}

	reflectValue := reflect.ValueOf(object)
	kind := reflectValue.Kind()
	for ; kind == reflect.Pointer; kind = reflectValue.Kind() {
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
			for indexValue.Kind() == reflect.Pointer {
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
	return fields, values
}

// StructInsert Object should be one of struct{}, *struct{}, []struct, []*struct{}, *[]struct{}, *[]*struct{}.
// Get fields and values based on struct tag.
func StructInsert(object any, tag string, except []string, allow []string) (fields []string, values [][]any) {
	b := poolGetInsertByStruct()
	defer poolPutInsertByStruct(b)
	fields, values = b.Insert(object, tag, except, allow)
	return fields, values
}

// StructModify Object should be one of anyStruct, *anyStruct get the fields and values that need to be modified.
func StructModify(object any, tag string, except ...string) (fields []string, values []any) {
	if object == nil || tag == StrEmpty {
		return fields, values
	}
	ofType := reflect.TypeOf(object)
	ofValue := reflect.ValueOf(object)
	ofKind := ofType.Kind()
	for ; ofKind == reflect.Pointer; ofKind = ofType.Kind() {
		ofType = ofType.Elem()
		ofValue = ofValue.Elem()
	}
	if ofKind != reflect.Struct {
		return fields, values
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
	return fields, values
}

// StructObtain Object should be one of anyStruct, *anyStruct for get all fields and values.
func StructObtain(object any, tag string, except ...string) (fields []string, values []any) {
	if object == nil || tag == StrEmpty {
		return fields, values
	}
	ofType := reflect.TypeOf(object)
	ofValue := reflect.ValueOf(object)
	ofKind := ofType.Kind()
	for ; ofKind == reflect.Pointer; ofKind = ofType.Kind() {
		ofType = ofType.Elem()
		ofValue = ofValue.Elem()
	}
	if ofKind != reflect.Struct {
		return fields, values
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
	return fields, values
}

// StructUpdate Compare origin and latest for update.
func StructUpdate(origin any, latest any, tag string, except ...string) (fields []string, values []any) {
	if origin == nil || latest == nil || tag == StrEmpty {
		return fields, values
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

	return fields, values
}

// ExecuteScript Execute SQL script.
func ExecuteScript(ctx context.Context, db *sql.DB, execute string, args ...any) error {
	if execute = strings.TrimSpace(execute); execute == StrEmpty {
		return nil
	}
	if _, err := db.ExecContext(ctx, execute, args...); err != nil {
		return err
	}
	return nil
}

// DropTable DROP TABLE. Data is priceless! You should back up your data before calling this function unless you are very sure what you are doing.
func DropTable(ctx context.Context, db *sql.DB, tables ...string) error {
	for _, table := range tables {
		if table = strings.TrimSpace(table); table == StrEmpty {
			continue
		}
		if err := ExecuteScript(ctx, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table)); err != nil {
			return err
		}
	}
	return nil
}

// TruncateTable TRUNCATE TABLE. Data is priceless! You should back up your data before calling this function unless you are very sure what you are doing.
func TruncateTable(ctx context.Context, db *sql.DB, tables ...string) error {
	for _, table := range tables {
		if table = strings.TrimSpace(table); table == StrEmpty {
			continue
		}
		if err := ExecuteScript(ctx, db, fmt.Sprintf("TRUNCATE TABLE %s", table)); err != nil {
			return err
		}
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
	return s.Window("LAG", firstNext(s.way.Replace(column), args...))
}

// Lead LEAD() Returns the value of a row after the current row.
func (s *WindowFunc) Lead(column string, args ...any) *WindowFunc {
	return s.Window("LEAD", firstNext(s.way.Replace(column), args...))
}

// NTile N-TILE Divide the rows in the window into n buckets and assign a bucket number to each row.
func (s *WindowFunc) NTile(buckets int64, args ...any) *WindowFunc {
	return s.Window("NTILE", firstNext(buckets, args...))
}

// FirstValue FIRST_VALUE() Returns the value of the first row in the window.
func (s *WindowFunc) FirstValue(column string) *WindowFunc {
	return s.Window("FIRST_VALUE", firstNext(s.way.Replace(column)))
}

// LastValue LAST_VALUE() Returns the value of the last row in the window.
func (s *WindowFunc) LastValue(column string) *WindowFunc {
	return s.Window("LAST_VALUE", firstNext(s.way.Replace(column)))
}

// NthValue NTH_VALUE() The Nth value can be returned according to the specified order. This is very useful when you need to get data at a specific position.
func (s *WindowFunc) NthValue(column string, args ...any) *WindowFunc {
	return s.Window("NTH_VALUE", firstNext(s.way.Replace(column), args...))
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
