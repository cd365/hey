// Construct SQL statements

package hey

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/cd365/hey/v6/cst"
)

// SQL Prepare SQL statements and parameter lists corresponding to placeholders.
type SQL struct {
	// Prepare SQL fragments or SQL statements, which may contain SQL placeholders.
	Prepare string

	// Args The corresponding parameter list of the placeholder list.
	Args []any
}

// Maker Build SQL fragments or SQL statements, which may include corresponding parameter lists.
// Notice: The ToSQL method must return a non-nil value for *SQL.
type Maker interface {
	// ToSQL Construct SQL statements that may contain parameters, the return value cannot be nil.
	ToSQL() *SQL
}

// CloneSQL Clone *SQL.
func CloneSQL(src *SQL) *SQL {
	dst := NewEmptySQL()
	dst.Prepare = src.Prepare
	dst.Args = make([]any, len(src.Args))
	copy(dst.Args, src.Args)
	return dst
}

func NewSQL(prepare string, args ...any) *SQL {
	return &SQL{
		Prepare: prepare,
		Args:    args,
	}
}

func NewEmptySQL() *SQL {
	return NewSQL(cst.Empty)
}

// Clone For clone the current object.
func (s *SQL) Clone() *SQL {
	return CloneSQL(s)
}

// IsEmpty Used to determine whether the current SQL fragments or SQL statements is empty string.
func (s *SQL) IsEmpty() bool {
	return s.Prepare == cst.Empty
}

// ToEmpty Set Prepare, Args to empty value.
func (s *SQL) ToEmpty() *SQL {
	s.Prepare, s.Args = cst.Empty, nil
	return s
}

// ToSQL Implementing the Maker interface.
func (s *SQL) ToSQL() *SQL {
	return s
}

// AnyToSQL Convert values of any type into SQL expressions or SQL statements.
func AnyToSQL(i any) *SQL {
	if i == nil {
		return NewEmptySQL()
	}
	switch value := i.(type) {
	case bool:
		return NewSQL(fmt.Sprintf("%t", value))
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return NewSQL(fmt.Sprintf("%d", value))
	case float32:
		return NewSQL(strconv.FormatFloat(float64(value), 'f', -1, 64))
	case float64:
		return NewSQL(strconv.FormatFloat(value, 'f', -1, 64))
	case string:
		return NewSQL(value)
	case *SQL:
		if value != nil {
			return value
		}
	case Maker:
		if tmp := value.ToSQL(); tmp != nil {
			return tmp
		}
	default:
		v := reflect.ValueOf(i)
		t := v.Type()
		k := t.Kind()
		for k == reflect.Pointer {
			if v.IsNil() {
				return NewEmptySQL()
			}
			v = v.Elem()
			t = v.Type()
			k = t.Kind()
		}
		switch k {
		case reflect.Bool, reflect.Float32, reflect.Float64, reflect.String,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return AnyToSQL(v.Interface())
		default: // Other types of values are invalid.

		}
	}
	return NewEmptySQL()
}

// JoinMaker Concatenate multiple SQL scripts and their parameter lists using a specified delimiter.
func JoinMaker(elems []Maker, sep string) *SQL {
	script := NewEmptySQL()
	builder := poolGetStringBuilder()
	defer poolPutStringBuilder(builder)
	num := 0
	for _, element := range elems {
		if element == nil {
			continue
		}
		shard := element.ToSQL()
		if shard.IsEmpty() {
			continue
		}
		num++
		if num > 1 {
			builder.WriteString(sep)
		}
		builder.WriteString(shard.Prepare)
		if shard.Args != nil {
			script.Args = append(script.Args, shard.Args...)
		}
	}
	script.Prepare = builder.String()
	return script
}

// JoinSQL Use `separator` to concatenate multiple SQL scripts and parameters.
func JoinSQL(elems []any, sep string) *SQL {
	length := len(elems)
	makers := make([]Maker, length)
	for i := range length {
		makers[i] = AnyToSQL(elems[i])
	}
	return JoinMaker(makers, sep)
}

// JoinSQLComma Use "," to concatenate multiple SQL scripts and parameters.
func JoinSQLComma(values ...any) *SQL {
	return JoinSQL(values, cst.Comma)
}

// JoinSQLEmpty Use "" to concatenate multiple SQL scripts and parameters.
func JoinSQLEmpty(values ...any) *SQL {
	return JoinSQL(values, cst.Empty)
}

// JoinSQLSpace Use " " to concatenate multiple SQL scripts and parameters.
func JoinSQLSpace(values ...any) *SQL {
	return JoinSQL(values, cst.Space)
}

// JoinSQLCommaSpace Use ", " to concatenate multiple SQL scripts and parameters.
func JoinSQLCommaSpace(values ...any) *SQL {
	return JoinSQL(values, cst.CommaSpace)
}

// JoinSQLSemicolon Use ";" to concatenate multiple SQL scripts and parameters.
func JoinSQLSemicolon(values ...any) *SQL {
	return JoinSQL(values, cst.Semicolon)
}

func FuncSQL(funcName string, funcArgs ...any) *SQL {
	funcName = strings.TrimSpace(funcName)
	if funcName == cst.Empty {
		return NewEmptySQL()
	}
	values := make([]any, 0, len(funcArgs))
	for _, arg := range funcArgs {
		tmp := AnyToSQL(arg)
		if tmp.IsEmpty() {
			continue
		}
		values = append(values, tmp)
	}
	return JoinSQLEmpty(
		AnyToSQL(funcName),
		AnyToSQL(cst.LeftParenthesis),
		JoinSQLComma(values...),
		AnyToSQL(cst.RightParenthesis),
	)
}

func Coalesce(values ...any) *SQL {
	return FuncSQL(cst.COALESCE, values...)
}

func Count(values ...any) *SQL {
	return FuncSQL(cst.COUNT, values...)
}

func Avg(values ...any) *SQL {
	return FuncSQL(cst.AVG, values...)
}

func Max(values ...any) *SQL {
	return FuncSQL(cst.MAX, values...)
}

func Min(values ...any) *SQL {
	return FuncSQL(cst.MIN, values...)
}

func Sum(values ...any) *SQL {
	return FuncSQL(cst.SUM, values...)
}

func (s *Way) Func(funcName string, funcArgs ...any) *SQL {
	return FuncSQL(funcName, funcArgs...)
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
		return nil, ErrEmptyScript
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

var poolStringBuilder = &sync.Pool{
	New: func() any {
		return &strings.Builder{}
	},
}

func poolGetStringBuilder() *strings.Builder {
	return poolStringBuilder.Get().(*strings.Builder)
}

func poolPutStringBuilder(s *strings.Builder) {
	s.Reset()
	poolStringBuilder.Put(s)
}

// Prefix Add SQL prefix name; if the prefix exists, it will not be added.
func Prefix(prefix string, name string) string {
	if prefix == cst.Empty || strings.Contains(name, cst.Point) {
		return name
	}
	return JoinString(prefix, cst.Point, name)
}

// ParcelPrepare Parcel the SQL statement. `subquery` => ( `subquery` )
func ParcelPrepare(prepare string) string {
	if prepare == cst.Empty {
		return prepare
	}
	return JoinString(cst.LeftParenthesis, cst.Space, prepare, cst.Space, cst.RightParenthesis)
}

// ParcelSQL Parcel the SQL statement. `subquery` => ( `subquery` )
func ParcelSQL(script *SQL) *SQL {
	if script == nil || script.IsEmpty() {
		return NewEmptySQL()
	}
	result := script.Clone()
	prepare := strings.TrimSpace(result.Prepare)
	if prepare == cst.Empty {
		return result.ToEmpty()
	}
	if prepare[0] != cst.LeftParenthesis[0] {
		result.Prepare = ParcelPrepare(prepare)
	}
	return result
}

// ParcelCancelPrepare Cancel parcel the SQL statement. ( `subquery` ) => `subquery` OR ( ( `subquery` ) ) => ( `subquery` )
func ParcelCancelPrepare(prepare string) string {
	prepare = strings.TrimSpace(prepare)
	prepare = strings.TrimPrefix(prepare, JoinString(cst.LeftParenthesis, cst.Space))
	return strings.TrimSuffix(prepare, JoinString(cst.Space, cst.LeftParenthesis))
}

// ParcelCancelSQL Cancel parcel the SQL statement. ( `subquery` ) => `subquery` OR ( ( `subquery` ) ) => ( `subquery` )
func ParcelCancelSQL(script *SQL) *SQL {
	if script == nil || script.IsEmpty() {
		return NewEmptySQL()
	}
	result := script.Clone()
	result.Prepare = ParcelCancelPrepare(result.Prepare)
	return result
}

// parcelSingleFilter Parcel single Filter.
func parcelSingleFilter(tmp Filter) *SQL {
	if tmp == nil || tmp.IsEmpty() {
		return NewEmptySQL()
	}
	script := tmp.ToSQL()
	if script == nil || script.IsEmpty() {
		return NewEmptySQL()
	}
	if tmp.Num() == 1 && script.Prepare[0] != cst.LeftParenthesis[0] {
		script.Prepare = ParcelPrepare(script.Prepare)
	}
	return script
}

// RowsTable Concatenate one or more objects into a SQL table statement.
// ["id", "name"] + [{"id":1,"name":"name1"}, {"id":2,"name":"name2"}, {"id":3,"name":"name3"} ... ]
// ==>
// [( SELECT 1 AS id, NULL AS name ) + ( SELECT 2, 'name2' ) + ( SELECT NULL, 'name3' ) ... ]
func RowsTable(columns []string, rows func() [][]any, table func(values ...*SQL) *SQL) *SQL {
	if rows == nil || table == nil {
		return NewEmptySQL()
	}
	length := len(columns)
	if length == 0 {
		return NewEmptySQL()
	}
	var script *SQL
	result := make([]*SQL, 0)
	for serial, values := range rows() {
		if len(values) != length {
			return NewEmptySQL()
		}
		fields := make([]any, length)
		for index, value := range values {
			if value == nil {
				script = NewSQL(cst.NULL)
			} else {
				script = AnyToSQL(value)
				if script.IsEmpty() {
					script.Prepare = SQLString(script.Prepare)
				}
			}
			if serial == 0 {
				script.Prepare = JoinString(script.Prepare, cst.Space, cst.AS, cst.Space, columns[index])
			}
			fields[index] = script
		}
		result = append(result, JoinSQLSpace(cst.SELECT, JoinSQLCommaSpace(fields...)))
	}
	return table(result...)
}

// UnionSQL *SQL1, *SQL2, *SQL3 ... => ( QUERY_A ) UNION ( QUERY_B ) UNION ( QUERY_C )...
func UnionSQL(scripts ...*SQL) *SQL {
	result := make([]any, len(scripts))
	for index, value := range scripts {
		result[index] = ParcelSQL(value)
	}
	return JoinSQL(result, JoinString(cst.Space, cst.UNION, cst.Space))
}

// UnionAllSQL *SQL1, *SQL2, *SQL3 ... => ( QUERY_A ) UNION ALL ( QUERY_B ) UNION ALL ( QUERY_C )...
func UnionAllSQL(scripts ...*SQL) *SQL {
	result := make([]any, len(scripts))
	for index, value := range scripts {
		result[index] = ParcelSQL(value)
	}
	return JoinSQL(result, JoinString(cst.Space, cst.UNION, cst.Space, cst.ALL, cst.Space))
}

// IntersectSQL *SQL1, *SQL2, *SQL3 ... => ( QUERY_A ) INTERSECT ( QUERY_B ) INTERSECT ( QUERY_C )...
func IntersectSQL(scripts ...*SQL) *SQL {
	result := make([]any, len(scripts))
	for index, value := range scripts {
		result[index] = ParcelSQL(value)
	}
	return JoinSQL(result, JoinString(cst.Space, cst.INTERSECT, cst.Space))
}

// IntersectAllSQL *SQL1, *SQL2, *SQL3 ... => ( QUERY_A ) INTERSECT ALL ( QUERY_B ) INTERSECT ALL ( QUERY_C )...
func IntersectAllSQL(scripts ...*SQL) *SQL {
	result := make([]any, len(scripts))
	for index, value := range scripts {
		result[index] = ParcelSQL(value)
	}
	return JoinSQL(result, JoinString(cst.Space, cst.INTERSECT, cst.Space, cst.ALL, cst.Space))
}

// ExceptSQL *SQL1, *SQL2, *SQL3 ... => ( QUERY_A ) EXCEPT ( QUERY_B ) EXCEPT ( QUERY_C )...
func ExceptSQL(scripts ...*SQL) *SQL {
	result := make([]any, len(scripts))
	for index, value := range scripts {
		result[index] = ParcelSQL(value)
	}
	return JoinSQL(result, JoinString(cst.Space, cst.EXCEPT, cst.Space))
}

// ExceptAllSQL *SQL1, *SQL2, *SQL3 ... => ( QUERY_A ) EXCEPT ALL ( QUERY_B ) EXCEPT ALL ( QUERY_C )...
func ExceptAllSQL(scripts ...*SQL) *SQL {
	result := make([]any, len(scripts))
	for index, value := range scripts {
		result[index] = ParcelSQL(value)
	}
	return JoinSQL(result, JoinString(cst.Space, cst.EXCEPT, cst.Space, cst.ALL, cst.Space))
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
		script: AnyToSQL(script),
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
		s.script = AnyToSQL(script)
	}
	return s
}

func (s *sqlAlias) GetAlias() string {
	if s.alias == cst.Empty {
		return s.script.Prepare
	}
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
	s.alias = cst.Empty
	return s
}

func (s *sqlAlias) ToSQL() *SQL {
	if s.IsEmpty() {
		return NewEmptySQL()
	}
	result := CloneSQL(s.script)
	if s.way != nil {
		result.Prepare = s.rep(s.script.Prepare)
	}
	alias := s.alias
	if alias == cst.Empty {
		return result
	}
	aliases := make(map[string]*struct{}, 1<<2)
	aliases[JoinString(cst.Space, alias)] = nil
	asAlias := JoinString(cst.Space, cst.AS, cst.Space, alias)
	aliases[asAlias] = nil
	if replace := s.rep(alias); alias != replace {
		aliases[JoinString(cst.Space, replace)] = nil
		asAlias = JoinString(cst.Space, cst.AS, cst.Space, replace)
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
		result.Prepare = JoinString(result.Prepare, asAlias)
	}
	return result
}

/* Implement scanning *sql.Rows data into STRUCT or []STRUCT */

var poolBindScanStruct = &sync.Pool{
	New: func() any {
		return &bindScanStruct{}
	},
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

	s.structType[refStructType] = nil

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

		if tagValue == cst.Empty {
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
			return fmt.Errorf("hey: unable to determine column `%s` mapping", columns[i])
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

	kind1SliceElem := reflect.Invalid // []byte
	oneRowOneColumn := false

	switch kind1 {
	case reflect.Slice:
		kind1SliceElem = refType1.Elem().Kind()
	case reflect.Struct:
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Bool, reflect.Float32, reflect.Float64, reflect.String, reflect.Interface:
		oneRowOneColumn = true
	default:
		return fmt.Errorf("hey: unsupported parameter type `%s`", refType.String())
	}

	// Query one row one column; or directly query a specific value, like: SELECT VERSION()
	if oneRowOneColumn || (kind1 == reflect.Slice && kind1SliceElem == reflect.Uint8) {
		for i := 0; i < depth1; i++ {
			if refValue.IsNil() {
				refValue.Set(reflect.New(refValue.Type().Elem()))
			}
		}
		for rows.Next() {
			if err := rows.Scan(result); err != nil {
				return err
			}
		}
		return nil
	}

	// Query one row, remember to use LIMIT 1 in your SQL statement.
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

	// Query multiple rows.
	depth2 := 0

	// The type of slice elements.
	sliceItemType := refType1.Elem()

	// Slice element type.
	refItemType := sliceItemType

	kind2 := refItemType.Kind()
	for kind2 == reflect.Pointer {
		depth2++
		refItemType = refItemType.Elem()
		kind2 = refItemType.Kind()
	}

	// Check the element type of the slice.
	isSimple := false
	isStruct := false
	switch kind2 {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64,
		reflect.String:
		isSimple = true
	case reflect.Struct:
		isStruct = true
	default:
		return fmt.Errorf("hey: unsupported slice element type `%s`", refItemType.String())
	}

	// Initialize slice object.
	refValueSlice := refValue
	for i := 0; i < depth1; i++ {
		if refValueSlice.IsNil() {
			tmp := reflect.New(refValueSlice.Type().Elem())
			refValueSlice.Set(tmp)
		}
		refValueSlice = refValueSlice.Elem()
	}

	/* Prepare scan data according to the type of slice element. */

	// Query single column.
	if isSimple {
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

	// Query multiple columns.
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

var poolObjectInsert = &sync.Pool{
	New: func() any {
		return &objectInsert{}
	},
}

func poolGetObjectInsert() *objectInsert {
	tmp := poolObjectInsert.Get().(*objectInsert)
	tmp.allow = make(map[string]*struct{}, 1<<5)
	tmp.except = make(map[string]*struct{}, 1<<5)
	tmp.used = make(map[string]*struct{}, 1<<3)
	return tmp
}

func poolPutObjectInsert(b *objectInsert) {
	b.tag = cst.Empty
	b.allow = nil
	b.except = nil
	b.used = nil
	poolObjectInsert.Put(b)
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
				return cst.Empty
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

type objectInsert struct {
	// only allowed columns Hash table.
	allow map[string]*struct{}

	// ignored columns Hash table.
	except map[string]*struct{}

	// already existing columns Hash table.
	used map[string]*struct{}

	// struct tag name value used as table.column name.
	tag string
}

// setExcept Filter the list of unwanted columns, prioritize method calls structColumnValue() and structValue().
func (s *objectInsert) setExcept(except []string) {
	for _, column := range except {
		s.except[column] = nil
	}
}

// setAllow Only allowed columns, prioritize method calls structColumnValue() and structValue().
func (s *objectInsert) setAllow(allow []string) {
	for _, column := range allow {
		s.allow[column] = nil
	}
}

// structColumnValue Checkout columns, values.
func (s *objectInsert) structColumnValue(structReflectValue reflect.Value, allowed bool) (columns []string, values []any) {
	reflectType := structReflectValue.Type()
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
			tmpColumns, tmpValues := s.structColumnValue(valueIndexField, allowed)
			columns = append(columns, tmpColumns...)
			values = append(values, tmpValues...)
			continue
		}

		valueIndexFieldTag := field.Tag.Get(s.tag)
		if valueIndexFieldTag == cst.Empty || valueIndexFieldTag == "-" {
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
		s.used[valueIndexFieldTag] = nil

		columns = append(columns, valueIndexFieldTag)
		values = append(values, basicTypeValue(valueIndexField.Interface()))
	}
	return columns, values
}

// structValue Checkout struct values.
func (s *objectInsert) structValue(structReflectValue reflect.Value, allowed bool) (values []any) {
	reflectType := structReflectValue.Type()
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
			values = append(values, s.structValue(valueIndexField, allowed)...)
			continue
		}

		valueIndexFieldTag := field.Tag.Get(s.tag)
		if valueIndexFieldTag == cst.Empty || valueIndexFieldTag == "-" {
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

// Insert Object should be one of map[string]any, []map[string]any, struct{}, *struct{}, []struct, []*struct{}, *[]struct{}, *[]*struct{}.
func (s *objectInsert) Insert(object any, tag string, except []string, allow []string) (columns []string, values [][]any) {
	if object == nil {
		return columns, values
	}

	s.setExcept(except)

	allowed := len(allow) > 0
	if allowed {
		s.setAllow(allow)
	}

	// map[string]any
	if tmp, ok := object.(map[string]any); ok {
		length := len(tmp)
		if length == 0 {
			return columns, values
		}
		columns = make([]string, 0, length)
		for column := range tmp {
			if allowed {
				if _, ok = s.allow[column]; !ok {
					continue
				}
			}
			if _, ok = s.except[column]; ok {
				continue
			}
			columns = append(columns, column)
		}
		sort.Strings(columns)
		values = make([][]any, 1)
		values[0] = make([]any, 0, length)
		for _, column := range columns {
			values[0] = append(values[0], tmp[column])
		}
		return columns, values
	}

	// []map[string]any
	if tmp, ok := object.([]map[string]any); ok {
		length := len(tmp)
		if length == 0 {
			return columns, values
		}
		columns = make([]string, 0, length)
		for column := range tmp[0] {
			if allowed {
				if _, ok = s.allow[column]; !ok {
					continue
				}
			}
			if _, ok = s.except[column]; ok {
				continue
			}
			columns = append(columns, column)
		}
		count := len(columns)
		if count == 0 {
			return columns, values
		}
		sort.Strings(columns)
		values = make([][]any, 0, length)
		for _, value := range tmp {
			tmpValues := make([]any, 0, count)
			for _, column := range columns {
				tmpValue, exist := value[column]
				if exist {
					tmpValues = append(tmpValues, tmpValue)
				} else {
					tmpValues = append(tmpValues, nil)
				}
			}
			values = append(values, tmpValues)
		}
		return columns, values
	}

	if tag == cst.Empty {
		return columns, values
	}
	s.tag = tag

	reflectValue := reflect.ValueOf(object)
	kind := reflectValue.Kind()
	for ; kind == reflect.Pointer; kind = reflectValue.Kind() {
		reflectValue = reflectValue.Elem()
	}

	if kind == reflect.Struct {
		values = make([][]any, 1)
		columns, values[0] = s.structColumnValue(reflectValue, allowed)
		return
	}

	if kind == reflect.Slice {
		sliceLength := reflectValue.Len()
		values = make([][]any, sliceLength)
		var indexValueType reflect.Type
		for i := range sliceLength {
			indexValue := reflectValue.Index(i)
			if indexValueType == nil {
				indexValueType = indexValue.Type()
			} else {
				if indexValueType != indexValue.Type() {
					panic("hey: slice element types are inconsistent")
				}
			}
			for indexValue.Kind() == reflect.Pointer {
				indexValue = indexValue.Elem()
			}
			indexValueKind := indexValue.Kind()
			if indexValueKind == reflect.Struct {
				if i == 0 {
					columns, values[i] = s.structColumnValue(indexValue, allowed)
				} else {
					values[i] = s.structValue(indexValue, allowed)
				}
			} else {
				if indexValueKind != reflect.Interface {
					panic(fmt.Sprintf("hey: unsupported data type %T", indexValue.Interface()))
				}
				value := indexValue.Interface()
				indexValue = reflect.ValueOf(value)
				indexValueKind = indexValue.Kind()
				for ; indexValueKind == reflect.Pointer; indexValueKind = indexValue.Kind() {
					indexValue = indexValue.Elem()
				}
				if indexValueKind != reflect.Struct {
					panic(fmt.Sprintf("hey: unsupported data type %T", value))
				}
				if i == 0 {
					columns, values[i] = s.structColumnValue(indexValue, allowed)
				} else {
					values[i] = s.structValue(indexValue, allowed)
				}
			}
		}
		return
	}

	return
}

// ObjectInsert Object should be one of map[string]any, []map[string]any, struct{}, *struct{}, []struct, []*struct{}, *[]struct{}, *[]*struct{}.
func ObjectInsert(object any, tag string, except []string, allow []string) (columns []string, values [][]any) {
	i := poolGetObjectInsert()
	defer poolPutObjectInsert(i)
	return i.Insert(object, tag, except, allow)
}

// ObjectModify Object should be one of map[string]any, anyStruct, *anyStruct get the columns and values that need to be modified.
func ObjectModify(object any, tag string, except ...string) (columns []string, values []any) {
	if object == nil {
		return columns, values
	}

	excepted := make(map[string]*struct{}, 1<<3)
	for _, column := range except {
		excepted[column] = nil
	}

	if columnValue, ok := object.(map[string]any); ok {
		columns = make([]string, 0, len(columnValue))
		for column := range columnValue {
			if _, ok = excepted[column]; ok {
				continue
			}
			columns = append(columns, column)
		}
		sort.Strings(columns)
		values = make([]any, len(columns))
		for index, column := range columns {
			values[index] = columnValue[column]
		}
		return columns, values
	}

	if tag == cst.Empty {
		return columns, values
	}

	ofType := reflect.TypeOf(object)
	ofValue := reflect.ValueOf(object)
	ofKind := ofType.Kind()
	for ; ofKind == reflect.Pointer; ofKind = ofType.Kind() {
		ofType = ofType.Elem()
		ofValue = ofValue.Elem()
	}
	switch ofKind {
	case reflect.Struct:
	case reflect.Interface:
		columns, values = ObjectModify(ofValue.Interface(), tag, except...)
		return columns, values
	default:
		return columns, values
	}

	length := ofType.NumField()

	exists := make(map[string]*struct{}, length)
	columns = make([]string, 0, length)
	values = make([]any, 0, length)

	last := 0
	columnsIndex := make(map[string]int, 1<<3)

	add := func(field string, value any) {
		if _, ok := exists[field]; ok {
			values[columnsIndex[field]] = value
			return
		}
		exists[field] = nil
		columns = append(columns, field)
		values = append(values, value)
		columnsIndex[field] = last
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
			tmpFields, tmpValues := ObjectModify(fieldValue.Interface(), tag, except...)
			for index, tmpField := range tmpFields {
				add(tmpField, tmpValues[index])
			}
			continue
		}

		column := field.Tag.Get(tag)
		if column == cst.Empty || column == "-" {
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

	return columns, values
}

// ObjectObtain Object should be one of map[string]any, anyStruct, *anyStruct for get all columns and values.
func ObjectObtain(object any, tag string, except ...string) (columns []string, values []any) {
	if object == nil {
		return columns, values
	}

	excepted := make(map[string]*struct{}, 1<<3)
	for _, column := range except {
		excepted[column] = nil
	}

	if columnValue, ok := object.(map[string]any); ok {
		columns = make([]string, 0, len(columnValue))
		for column := range columnValue {
			if _, ok = excepted[column]; ok {
				continue
			}
			columns = append(columns, column)
		}
		sort.Strings(columns)
		values = make([]any, len(columns))
		for index, column := range columns {
			values[index] = columnValue[column]
		}
		return columns, values
	}

	if tag == cst.Empty {
		return columns, values
	}

	ofType := reflect.TypeOf(object)
	ofValue := reflect.ValueOf(object)
	ofKind := ofType.Kind()
	for ; ofKind == reflect.Pointer; ofKind = ofType.Kind() {
		ofType = ofType.Elem()
		ofValue = ofValue.Elem()
	}
	switch ofKind {
	case reflect.Struct:
	case reflect.Interface:
		columns, values = ObjectModify(ofValue.Interface(), tag, except...)
		return columns, values
	default:
		return columns, values
	}

	length := ofType.NumField()

	exists := make(map[string]*struct{}, length)
	columns = make([]string, 0, length)
	values = make([]any, 0, length)

	last := 0
	columnsIndex := make(map[string]int, 1<<5)

	add := func(field string, value any) {
		if _, ok := exists[field]; ok {
			values[columnsIndex[field]] = value
			return
		}
		exists[field] = nil
		columns = append(columns, field)
		values = append(values, value)
		columnsIndex[field] = last
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
			tmpFields, tmpValues := ObjectObtain(fieldValue.Interface(), tag, except...)
			for index, tmpField := range tmpFields {
				add(tmpField, tmpValues[index])
			}
			continue
		}

		column := field.Tag.Get(tag)
		if column == cst.Empty || column == "-" {
			continue
		}
		if _, ok := excepted[column]; ok {
			continue
		}

		add(column, ofValue.Field(i).Interface())
	}
	return columns, values
}

// StructUpdate Compare origin and latest for update.
func StructUpdate(origin any, latest any, tag string, except ...string) (columns []string, values []any) {
	if origin == nil || latest == nil || tag == cst.Empty {
		return columns, values
	}

	originColumns, originValues := ObjectObtain(origin, tag, except...)
	latestColumns, latestValues := ObjectModify(latest, tag, except...)

	storage := make(map[string]any, len(originColumns))
	for k, v := range originColumns {
		storage[v] = originValues[k]
	}

	exists := make(map[string]*struct{}, 1<<5)
	columns = make([]string, 0)
	values = make([]any, 0)

	last := 0
	columnsIndex := make(map[string]int, 1<<5)

	add := func(column string, value any) {
		if _, ok := exists[column]; ok {
			values[columnsIndex[column]] = value
			return
		}
		exists[column] = nil
		columns = append(columns, column)
		values = append(values, value)
		columnsIndex[column] = last
		last++
	}

	for k, v := range latestColumns {
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

	return columns, values
}

// ExecuteScript Execute SQL script.
func ExecuteScript(ctx context.Context, db *sql.DB, execute string, args ...any) error {
	if execute = strings.TrimSpace(execute); execute == cst.Empty {
		return ErrEmptyScript
	}
	result, err := db.ExecContext(ctx, execute, args...)
	if err != nil {
		return err
	}
	if _, err = result.RowsAffected(); err != nil {
		return err
	}
	return nil
}

// DropTable DROP TABLE. Data is priceless! You should back up your data before calling this function unless you are very sure what you are doing.
func DropTable(ctx context.Context, db *sql.DB, tables ...string) error {
	for _, table := range tables {
		if table = strings.TrimSpace(table); table == cst.Empty {
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
		if table = strings.TrimSpace(table); table == cst.Empty {
			continue
		}
		if err := ExecuteScript(ctx, db, fmt.Sprintf("TRUNCATE TABLE %s", table)); err != nil {
			return err
		}
	}
	return nil
}

type AdjustColumnAnyValue func(columnTypes []*sql.ColumnType, results []map[string]any) error

// MapScanner Scanning the query results into []map[string]any.
type MapScanner interface {
	// AdjustColumnAnyValue Customize the default method for adjusting column values.
	AdjustColumnAnyValue(adjust AdjustColumnAnyValue) MapScanner

	// Scan Scanning the query results into []map[string]any.
	// You can define a column value adjustment method with a higher priority
	// than the default through the adjusts parameter.
	Scan(rows *sql.Rows, adjusts ...AdjustColumnAnyValue) ([]map[string]any, error)
}

// mapScan Implementing the MapScanner interface.
type mapScan struct {
	adjustColumnValue AdjustColumnAnyValue
}

func (s *mapScan) AdjustColumnAnyValue(adjust AdjustColumnAnyValue) MapScanner {
	if adjust != nil {
		s.adjustColumnValue = adjust
	}
	return s
}

func (s *mapScan) Scan(rows *sql.Rows, adjusts ...AdjustColumnAnyValue) ([]map[string]any, error) {
	adjust := s.adjustColumnValue
	for i := len(adjusts) - 1; i >= 0; i-- {
		if adjusts[i] != nil {
			adjust = adjusts[i]
			break
		}
	}
	return QuickScan(rows, adjust)
}

// NewMapScanner Exposes a default implementation of the MapScanner interface.
func NewMapScanner() MapScanner {
	return &mapScan{
		adjustColumnValue: adjustColumnValueDefault,
	}
}

// tryFloat string or []byte to float64.
func tryFloat(value any) any {
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

// adjustColumnValueDefault Try to convert the text data type to a specific type that matches it.
func adjustColumnValueDefault(columnTypes []*sql.ColumnType, results []map[string]any) error {
	for _, v := range columnTypes {
		column := v.Name()
		databaseTypeName := v.DatabaseTypeName()
		databaseTypeNameUpper := strings.ToUpper(databaseTypeName)
		switch databaseTypeNameUpper {
		case "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL", "DOUBLE PRECISION", "NUMBER":
			for i := range results {
				if _, ok := results[i][column]; ok {
					results[i][column] = tryFloat(results[i][column])
				}
			}
		case "CHAR", "VARCHAR", "TEXT", "CHARACTER", "CHARACTER VARYING", "BPCHAR", "NCHAR", "NVARCHAR",
			"TINYTEXT", "MEDIUMTEXT", "LARGETEXT", "LONGTEXT", "TIMESTAMP", "DATE", "TIME", "DATETIME", "JSON":
			for i := range results {
				if _, ok := results[i][column]; ok {
					results[i][column] = tryString(results[i][column])
				}
			}
		case "BYTEA", "BINARY", "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB":

		default:

		}
	}
	return nil
}

// QuickScan Quickly scan query results into []map[string]any.
func QuickScan(
	rows *sql.Rows,
	adjustColumnValue func(columnTypes []*sql.ColumnType, results []map[string]any) error,
) ([]map[string]any, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	length := len(columns)
	results := make([]map[string]any, 0, 1)
	for rows.Next() {
		dest := make([]any, length)
		for i := range dest {
			dest[i] = new(any)
		}
		if err = rows.Scan(dest...); err != nil {
			return nil, err
		}
		item := make(map[string]any, length)
		for i := range length {
			item[columns[i]] = dest[i]
		}
		results = append(results, item)
	}
	if adjustColumnValue == nil {
		err = adjustColumnValueDefault(columnTypes, results)
	} else {
		err = adjustColumnValue(columnTypes, results)
	}
	if err != nil {
		return nil, err
	}
	return results, nil
}

// TableColumn Add the prefix "table_name." before the column name.
// Allow the table name to be empty, which makes it possible to replace column names or construct SQL statements based on column names.
type TableColumn interface {
	// Table Get the current table name.
	Table() string

	// Column Add table name prefix to single column name, allowing column alias to be set.
	Column(column string, alias ...string) string

	// ColumnAll Add table name prefix to column names in batches.
	ColumnAll(columnAll ...string) []string

	// ColumnSQL Single column to *SQL.
	ColumnSQL(column string, alias ...string) *SQL

	// ColumnAllSQL Multiple columns to *SQL.
	ColumnAllSQL(columnAll ...string) *SQL
}

type tableColumn struct {
	way       *Way
	tableName string
}

func NewTableColumn(way *Way, tableName ...string) TableColumn {
	tmp := &tableColumn{
		way: way,
	}
	tmp.tableName = LastNotEmptyString(tableName)
	return tmp
}

// Table Get the current table name.
func (s *tableColumn) Table() string {
	return s.tableName
}

// columnAlias Set an alias for the column.
// "[prefix.]column_name" -> "column_name"
// "[prefix.]column_name + alias_name" -> "column_name AS column_alias_name"
func (s *tableColumn) columnAlias(column string, alias ...string) string {
	if aliasName := LastNotEmptyString(alias); aliasName != cst.Empty {
		return newSqlAlias(column).v(s.way).SetAlias(aliasName).ToSQL().Prepare
	}
	return column
}

// Column Add table name prefix to single column name, allowing column alias to be set.
func (s *tableColumn) Column(column string, alias ...string) string {
	return s.columnAlias(s.ColumnAll(column)[0], alias...)
}

// ColumnAll Add table name prefix to column names in batches.
func (s *tableColumn) ColumnAll(columnAll ...string) []string {
	if s.tableName == cst.Empty {
		return s.way.ReplaceAll(columnAll)
	}
	table := s.way.Replace(s.tableName)
	result := make([]string, len(columnAll))
	for index, column := range columnAll {
		result[index] = Prefix(table, s.way.Replace(column))
	}
	return result
}

// ColumnSQL Single column to *SQL.
func (s *tableColumn) ColumnSQL(column string, alias ...string) *SQL {
	return NewSQL(s.Column(column, alias...))
}

// ColumnAllSQL Multiple columns to *SQL.
func (s *tableColumn) ColumnAllSQL(columnAll ...string) *SQL {
	columnAll = s.ColumnAll(columnAll...)
	valuesAll := make([]any, len(columnAll))
	for i := range columnAll {
		valuesAll[i] = columnAll[i]
	}
	return JoinSQLCommaSpace(valuesAll...)
}

// T Register a shortcut method T to quickly create a TableColumn instance.
func (s *Way) T(tableName ...string) TableColumn {
	return NewTableColumn(s, tableName...)
}

/*
SQL WINDOW FUNCTION

function_name() OVER (
	[PARTITION BY column1, column2, ...]
	[ORDER BY column3 [ASC|DESC], ...]
	[GROUPS | RANGE | ROWS frame_specification]
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

	// frame Value is one of `GROUPS`, `RANGE`, `ROWS`.
	frame string
}

func NewSQLWindowFuncFrame(frame string) SQLWindowFuncFrame {
	return &sqlWindowFuncFrame{
		frame: frame,
	}
}

func (s *sqlWindowFuncFrame) ToSQL() *SQL {
	if s.frame == cst.Empty {
		return NewSQL(cst.Empty)
	}
	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)
	b.WriteString(s.frame)
	b.WriteString(cst.Space)

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
	s.script = AnyToSQL(value)
	return s
}

func (s *sqlWindowFuncFrame) UnboundedPreceding() *SQL {
	return AnyToSQL("UNBOUNDED PRECEDING")
}

func (s *sqlWindowFuncFrame) NPreceding(n int) *SQL {
	return AnyToSQL(fmt.Sprintf("%d PRECEDING", n))
}

func (s *sqlWindowFuncFrame) CurrentRow() *SQL {
	return AnyToSQL("CURRENT ROW")
}

func (s *sqlWindowFuncFrame) NFollowing(n int) *SQL {
	return AnyToSQL(fmt.Sprintf("%d FOLLOWING", n))
}

func (s *sqlWindowFuncFrame) UnboundedFollowing() *SQL {
	return AnyToSQL("UNBOUNDED FOLLOWING")
}

func (s *sqlWindowFuncFrame) Between(fc func(ff SQLWindowFuncFrame) (*SQL, *SQL)) SQLWindowFuncFrame {
	if fc != nil {
		if start, end := fc(s); start != nil && end != nil && !start.IsEmpty() && !end.IsEmpty() {
			s.SQL(JoinSQLSpace(cst.BETWEEN, start, cst.AND, end))
		}
	}
	return s
}

type SQLWindowFuncOver interface {
	Maker

	ToEmpty

	// Over Set the OVER statement.
	Over(script Maker) SQLWindowFuncOver

	// Partition The OVER clause defines window partitions so that the window function is calculated independently in each partition.
	Partition(column ...string) SQLWindowFuncOver

	// Asc Define the sorting within the partition so that the window function is calculated in order.
	Asc(column string) SQLWindowFuncOver

	// Desc Define the sorting within the partition so that the window function is calculated in descending order.
	Desc(column string) SQLWindowFuncOver

	// Groups Define the window based on the sort column values.
	Groups(fc func(f SQLWindowFuncFrame)) SQLWindowFuncOver

	// Range Define the window based on the physical row number, accurately control the number of rows (such as the first 2 rows and the last 3 rows).
	Range(fc func(f SQLWindowFuncFrame)) SQLWindowFuncOver

	// Rows Defines a window based on a range of values, including all rows with the same ORDER BY column value; suitable for handling scenarios with equal values (such as time ranges).
	Rows(fc func(f SQLWindowFuncFrame)) SQLWindowFuncOver
}

type sqlWindowFuncOver struct {
	way *Way

	// over The over statement.
	over *SQL

	// frameGroups Define the window based on the sort column values.
	frameGroups *SQL

	// frameRange Define the window based on the physical row number, accurately control the number of rows (such as the first 2 rows and the last 3 rows).
	frameRange *SQL

	// frameRows Defines a window based on a range of values, including all rows with the same ORDER BY column value; suitable for handling scenarios with equal values (such as time ranges).
	frameRows *SQL

	// partition Setting up window partitions.
	partition []string

	// order Sorting data within a group.
	order []string
}

func NewSQLWindowFuncOver(way *Way) SQLWindowFuncOver {
	return &sqlWindowFuncOver{
		way: way,
	}
}

func (s *sqlWindowFuncOver) ToEmpty() {
	s.over = nil
	s.frameGroups = nil
	s.frameRange = nil
	s.frameRows = nil
	s.partition = nil
	s.order = nil
}

func (s *sqlWindowFuncOver) ToSQL() *SQL {
	if s.over != nil && !s.over.IsEmpty() {
		return s.over
	}
	result := NewEmptySQL()
	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)
	b.WriteString(cst.LeftParenthesis)
	num := 0
	if len(s.partition) > 0 {
		num++
		b.WriteString(JoinString(cst.Space, cst.PARTITION, cst.Space, cst.BY, cst.Space))
		b.WriteString(strings.Join(s.partition, cst.CommaSpace))
	}
	if len(s.order) > 0 {
		num++
		b.WriteString(JoinString(cst.Space, cst.ORDER, cst.Space, cst.BY, cst.Space))
		b.WriteString(strings.Join(s.order, cst.CommaSpace))
	}
	if frame := s.frameGroups; frame != nil {
		if tmp := frame.ToSQL(); tmp != nil && !tmp.IsEmpty() {
			num++
			b.WriteString(JoinString(cst.Space, tmp.Prepare))
			result.Args = append(result.Args, tmp.Args...)
		}
	}
	if frame := s.frameRange; frame != nil {
		if tmp := frame.ToSQL(); tmp != nil && !tmp.IsEmpty() {
			num++
			b.WriteString(JoinString(cst.Space, tmp.Prepare))
			result.Args = append(result.Args, tmp.Args...)
		}
	}
	if frame := s.frameRows; frame != nil {
		if tmp := frame.ToSQL(); tmp != nil && !tmp.IsEmpty() {
			num++
			b.WriteString(JoinString(cst.Space, tmp.Prepare))
			result.Args = append(result.Args, tmp.Args...)
		}
	}

	if num > 0 {
		b.WriteString(cst.Space)
	}
	b.WriteString(cst.RightParenthesis)
	result.Prepare = b.String()
	return result
}

func (s *sqlWindowFuncOver) Over(script Maker) SQLWindowFuncOver {
	if script == nil {
		return s
	}
	value := script.ToSQL()
	if value == nil || value.IsEmpty() {
		return s
	}
	over := value.Clone()
	over.Prepare = strings.TrimSpace(over.Prepare)
	if over.Prepare == cst.Empty {
		return s
	}
	if strings.Index(over.Prepare, cst.Space) > 0 && over.Prepare[0] != cst.LeftParenthesis[0] {
		over.Prepare = ParcelPrepare(over.Prepare)
	}
	s.over = over
	return s
}

func (s *sqlWindowFuncOver) Partition(column ...string) SQLWindowFuncOver {
	s.partition = append(s.partition, s.way.ReplaceAll(column)...)
	return s
}

func (s *sqlWindowFuncOver) Asc(column string) SQLWindowFuncOver {
	s.order = append(s.order, fmt.Sprintf("%s %s", s.way.Replace(column), cst.ASC))
	return s
}

func (s *sqlWindowFuncOver) Desc(column string) SQLWindowFuncOver {
	s.order = append(s.order, fmt.Sprintf("%s %s", s.way.Replace(column), cst.DESC))
	return s
}

func (s *sqlWindowFuncOver) Groups(fc func(f SQLWindowFuncFrame)) SQLWindowFuncOver {
	if fc != nil {
		frame := NewSQLWindowFuncFrame(cst.GROUPS)
		fc(frame)
		if tmp := frame.ToSQL(); tmp != nil && !tmp.IsEmpty() {
			s.frameGroups = tmp
			s.frameRange = nil
			s.frameRows = nil
		}
	}
	return s
}

func (s *sqlWindowFuncOver) Range(fc func(f SQLWindowFuncFrame)) SQLWindowFuncOver {
	if fc != nil {
		frame := NewSQLWindowFuncFrame(cst.RANGE)
		fc(frame)
		if tmp := frame.ToSQL(); tmp != nil && !tmp.IsEmpty() {
			s.frameGroups = nil
			s.frameRange = tmp
			s.frameRows = nil
		}
	}
	return s
}

func (s *sqlWindowFuncOver) Rows(fc func(f SQLWindowFuncFrame)) SQLWindowFuncOver {
	if fc != nil {
		frame := NewSQLWindowFuncFrame(cst.ROWS)
		fc(frame)
		if tmp := frame.ToSQL(); tmp != nil && !tmp.IsEmpty() {
			s.frameGroups = nil
			s.frameRange = nil
			s.frameRows = tmp
		}
	}
	return s
}

// WindowFunc SQL window function.
type WindowFunc struct {
	way *Way

	// window The window function used.
	window *SQL

	// over The over statement.
	over SQLWindowFuncOver

	// alias Serial number column alias.
	alias string
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
	column := cst.Asterisk
	for i := len(columns) - 1; i >= 0; i-- {
		if columns[i] != cst.Empty {
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

// OverFunc Define the OVER clause.
func (s *WindowFunc) OverFunc(fc func(o SQLWindowFuncOver)) *WindowFunc {
	if fc == nil {
		return s
	}
	if s.over == nil {
		s.over = NewSQLWindowFuncOver(s.way)
	}
	fc(s.over)
	return s
}

// Over Define the OVER clause.
func (s *WindowFunc) Over(prepare string, args ...any) *WindowFunc {
	if prepare = strings.TrimSpace(prepare); prepare == cst.Empty {
		return s
	}
	return s.OverFunc(func(o SQLWindowFuncOver) {
		o.Over(NewSQL(prepare, args...))
	})
}

// Alias Set the alias of the column that uses the window function.
func (s *WindowFunc) Alias(alias string) *WindowFunc {
	s.alias = alias
	return s
}

func (s *WindowFunc) ToSQL() *SQL {
	result, window := NewSQL(cst.Empty), s.window
	if window == nil || window.IsEmpty() {
		return result
	}

	b := poolGetStringBuilder()
	defer poolPutStringBuilder(b)

	b.WriteString(window.Prepare)
	result.Args = append(result.Args, window.Args...)
	b.WriteString(JoinString(cst.Space, cst.OVER, cst.Space))

	if s.over == nil {
		b.WriteString(cst.LeftParenthesis)
		b.WriteString(cst.RightParenthesis)
	} else {
		script := s.over.ToSQL()
		if script == nil || script.IsEmpty() {
			b.WriteString(cst.LeftParenthesis)
			b.WriteString(cst.RightParenthesis)
		} else {
			b.WriteString(script.Prepare)
			result.Args = append(result.Args, script.Args...)
		}
	}

	result.Prepare = b.String()
	return newSqlAlias(result).v(s.way).SetAlias(s.alias).ToSQL()
}
