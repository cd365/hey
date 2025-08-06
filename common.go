package hey

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

const (
	// DefaultTag Mapping of default database column name and struct tag.
	DefaultTag = "db"

	// EmptyString Empty string value.
	EmptyString = ""
)

const (
	SqlConcat = ", "
	SqlPoint  = "."
	SqlSpace  = " "
	SqlStar   = "*"

	SqlAs       = "AS"
	SqlAsc      = "ASC"
	SqlDesc     = "DESC"
	SqlUnion    = "UNION"
	SqlUnionAll = "UNION ALL"

	SqlJoinInner = "INNER JOIN"
	SqlJoinLeft  = "LEFT JOIN"
	SqlJoinRight = "RIGHT JOIN"
	SqlJoinFull  = "FULL JOIN"
	SqlJoinCross = "CROSS JOIN"

	SqlAnd = "AND"
	SqlOr  = "OR"

	SqlNot  = "NOT"
	SqlNull = "NULL"

	SqlPlaceholder   = "?"
	SqlEqual         = "="
	SqlNotEqual      = "<>"
	SqlLessThan      = "<"
	SqlLessThanEqual = "<="
	SqlMoreThan      = ">"
	SqlMoreThanEqual = ">="

	SqlAll = "ALL"
	SqlAny = "ANY"

	SqlLeftSmallBracket  = "("
	SqlRightSmallBracket = ")"

	SqlExpect    = "EXCEPT"
	SqlIntersect = "INTERSECT"

	SqlCoalesce = "COALESCE"

	SqlDistinct = "DISTINCT"

	SqlSelect = "SELECT"
	SqlInsert = "INSERT"
	SqlUpdate = "UPDATE"
	SqlDelete = "DELETE"
	SqlFrom   = "FROM"
	SqlInto   = "INTO"
	SqlValues = "VALUES"
	SqlSet    = "SET"
	SqlWhere  = "WHERE"

	SqlBetween     = "BETWEEN"
	SqlConflict    = "CONFLICT"
	SqlDo          = "DO"
	SqlExcluded    = "EXCLUDED"
	SqlExists      = "EXISTS"
	SqlGroupBy     = "GROUP BY"
	SqlHaving      = "HAVING"
	SqlIn          = "IN"
	SqlIs          = "IS"
	SqlLike        = "LIKE"
	SqlLimit       = "LIMIT"
	SqlNothing     = "NOTHING"
	SqlOffset      = "OFFSET"
	SqlOn          = "ON"
	SqlOrderBy     = "ORDER BY"
	SqlOver        = "OVER"
	SqlPartitionBy = "PARTITION BY"
	SqlUsing       = "USING"
	SqlWith        = "WITH"

	SqlCase = "CASE"
	SqlWhen = "WHEN"
	SqlThen = "THEN"
	SqlElse = "ELSE"
	SqlEnd  = "END"

	SqlRange = "RANGE"
	SqlRows  = "ROWS"
)

const (
	AliasA = "a"
	AliasB = "b"
	AliasC = "c"
	AliasD = "d"
	AliasE = "e"
	AliasF = "f"
	AliasG = "g"
)

const (
	DefaultAliasNameCount = "counts"
)

type sqlError string

func (s sqlError) Error() string {
	return string(s)
}

const (
	// ErrNoRows Error no rows.
	ErrNoRows = sqlError("hey: no rows")

	// ErrNoRowsAffected Error no rows affected.
	ErrNoRowsAffected = sqlError("hey: no rows affected")

	// errTransactionIsNil Error transaction isn't started.
	errTransactionIsNil = sqlError("hey: transaction is nil")
)

const (
	State0 = 0
	State1 = 1

	StateY = "Y"
	StateN = "N"

	StateYes = "YES"
	StateNo  = "NO"

	StateOn  = "ON"
	StateOff = "OFF"
)

type RowsScanMakeSliceLength string

const (
	MakerScanAllMakeSliceLength RowsScanMakeSliceLength = "maker_scan_all_make_slice_length"
)

var (
	stringBuilder = &sync.Pool{
		New: func() any {
			return &strings.Builder{}
		},
	}
)

func getStringBuilder() *strings.Builder {
	return stringBuilder.Get().(*strings.Builder)
}

func putStringBuilder(b *strings.Builder) {
	b.Reset()
	stringBuilder.Put(b)
}

// Empty Check if an object value is empty.
type Empty interface {
	Empty() bool
}

// Maker Construct an SQL statement, which may contain placeholders and their placeholder parameter lists.
type Maker interface {
	ToSQL() *SQL
}

type SQL struct {
	// Prepare SQL statement.
	Prepare string

	// Args SQL statement corresponding parameter lists.
	Args []any
}

func (s *SQL) Clean() *SQL {
	s.Prepare, s.Args = EmptyString, nil
	return s
}

func (s *SQL) Empty() bool {
	return s.Prepare == EmptyString
}

func (s *SQL) ToSQL() *SQL {
	return s
}

func NewSQL(prepare string, args ...any) *SQL {
	return &SQL{
		Prepare: prepare,
		Args:    args,
	}
}

// EmptyMaker Check whether the result (SQL statement) of Maker is empty.
func EmptyMaker(maker Maker) bool {
	if maker == nil {
		return true
	}
	return maker.ToSQL().Empty()
}

// JoinMaker Use the specified separator to splice multiple Makers.
func JoinMaker(elements []Maker, separators ...string) Maker {
	separator := SqlSpace
	for i := len(separators) - 1; i >= 0; i-- {
		if separator != separators[i] {
			separator = separators[i]
			break
		}
	}
	result := NewSQL(EmptyString)
	b := getStringBuilder()
	defer putStringBuilder(b)
	num := 0
	for _, maker := range elements {
		if maker == nil {
			continue
		}
		tmp := maker.ToSQL()
		if tmp == nil || tmp.Empty() {
			continue
		}
		num++
		if num > 1 {
			b.WriteString(separator)
		}
		b.WriteString(tmp.Prepare)
		if tmp.Args != nil {
			result.Args = append(result.Args, tmp.Args...)
		}
	}
	result.Prepare = b.String()
	return result
}

func Makers(elements ...Maker) []Maker {
	return elements
}

// Replace For replacement identifiers in SQL statements. Replace by default, concurrent reads and writes are not safe.
// If you need concurrent read and write security, you can implement Replace by yourself.
type Replace interface {
	Get(key string) string

	Set(key string, value string) Replace

	Del(key string) Replace

	Map() map[string]string

	Use(mapping map[string]string) Replace

	// Gets Batch getting.
	Gets(keys []string) []string

	// Sets Batch setting.
	Sets(mapping map[string]string) Replace
}

type replace struct {
	maps map[string]string
}

func (s *replace) Get(key string) string {
	value, ok := s.maps[key]
	if ok {
		return value
	}
	return key
}

func (s *replace) Set(key string, value string) Replace {
	s.maps[key] = value
	return s
}

func (s *replace) Del(key string) Replace {
	delete(s.maps, key)
	return s
}

func (s *replace) Map() map[string]string {
	return s.maps
}

func (s *replace) Use(mapping map[string]string) Replace {
	s.maps = mapping
	return s
}

func (s *replace) Gets(keys []string) []string {
	length := len(keys)
	if length == 0 {
		return keys
	}
	replaced := make([]string, length)
	for i := range length {
		if value, ok := s.maps[keys[i]]; ok {
			replaced[i] = value
		} else {
			replaced[i] = keys[i]
		}
	}
	return replaced
}

func (s *replace) Sets(mapping map[string]string) Replace {
	for key, value := range mapping {
		s.Set(key, value)
	}
	return s
}

func NewReplace() Replace {
	return &replace{
		maps: make(map[string]string, 256),
	}
}

var (
	// insertByStructPool insert with struct{}, *struct{}, []struct, []*struct{}, *[]struct{}, *[]*struct{}.
	insertByStructPool = &sync.Pool{
		New: func() any {
			return &insertByStruct{}
		},
	}
)

// getInsertByStruct get *insertByStruct from pool.
func getInsertByStruct() *insertByStruct {
	tmp := insertByStructPool.Get().(*insertByStruct)
	tmp.allow = make(map[string]*struct{}, 32)
	tmp.except = make(map[string]*struct{}, 32)
	tmp.used = make(map[string]*struct{}, 8)
	return tmp
}

// putInsertByStruct put *insertByStruct in the pool.
func putInsertByStruct(b *insertByStruct) {
	b.tag = EmptyString
	b.allow = nil
	b.except = nil
	b.used = nil
	b.structReflectType = nil
	insertByStructPool.Put(b)
}

// MustAffectedRows at least one row is affected.
func MustAffectedRows(affectedRows int64, err error) error {
	if err != nil {
		return err
	}
	if affectedRows <= 0 {
		return ErrNoRowsAffected
	}
	return nil
}

// LastNotEmptyString get last not empty string, return empty string if it does not exist.
func LastNotEmptyString(sss []string) string {
	for i := len(sss) - 1; i >= 0; i-- {
		if sss[i] != EmptyString {
			return sss[i]
		}
	}
	return EmptyString
}

// DiscardDuplicate discard duplicate element.
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

/* Implement scanning *sql.Rows data into STRUCT or []STRUCT */

var (
	bindScanStructPool = &sync.Pool{
		New: func() any {
			return &bindScanStruct{}
		},
	}
)

func getBindScanStruct() *bindScanStruct {
	return bindScanStructPool.Get().(*bindScanStruct).init()
}

func putBindScanStruct(b *bindScanStruct) {
	bindScanStructPool.Put(b.free())
}

// bindScanStruct bind the receiving object with the query result.
type bindScanStruct struct {
	// store root struct properties.
	direct map[string]int

	// store non-root struct properties, such as anonymous attribute structure and named attribute structure.
	indirect map[string][]int

	// all used struct types, including the root struct.
	structType map[reflect.Type]*struct{}
}

func (s *bindScanStruct) free() *bindScanStruct {
	s.direct = nil
	s.indirect = nil
	s.structType = nil
	return s
}

func (s *bindScanStruct) init() *bindScanStruct {
	s.direct = make(map[string]int, 16)
	s.indirect = make(map[string][]int, 16)
	s.structType = make(map[reflect.Type]*struct{}, 2)
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

		if tagValue == EmptyString {
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
			b := getBindScanStruct()
			defer putBindScanStruct(b)
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
			putBindScanStruct(b)
		}
	}()

	for rows.Next() {

		if b == nil {
			// initialize variable values
			b = getBindScanStruct()
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

/* Implement scanning *sql.Rows data into STRUCT or []STRUCT */

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
				return EmptyString
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

// setExcept filter the list of unwanted fields, prioritize method calls structFieldsValues() and structValues().
func (s *insertByStruct) setExcept(except []string) {
	for _, field := range except {
		s.except[field] = &struct{}{}
	}
}

// setAllow only allowed fields, prioritize method calls structFieldsValues() and structValues().
func (s *insertByStruct) setAllow(allow []string) {
	for _, field := range allow {
		s.allow[field] = &struct{}{}
	}
}

func panicSliceElementTypesAreInconsistent() {
	panic("hey: slice element types are inconsistent")
}

// structFieldsValues checkout fields, values.
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
		if valueIndexFieldTag == EmptyString || valueIndexFieldTag == "-" {
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

// structValues checkout values.
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
		if valueIndexFieldTag == EmptyString || valueIndexFieldTag == "-" {
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

// Insert object should be one of struct{}, *struct{}, []struct, []*struct{}, *[]struct{}, *[]*struct{}.
func (s *insertByStruct) Insert(object any, tag string, except []string, allow []string) (fields []string, values [][]any) {
	if object == nil || tag == EmptyString {
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

// StructInsert object should be one of struct{}, *struct{}, []struct, []*struct{}, *[]struct{}, *[]*struct{}.
// get fields and values based on struct tag.
func StructInsert(object any, tag string, except []string, allow []string) (fields []string, values [][]any) {
	b := getInsertByStruct()
	defer putInsertByStruct(b)
	fields, values = b.Insert(object, tag, except, allow)
	return
}

// StructModify object should be one of anyStruct, *anyStruct get the fields and values that need to be modified.
func StructModify(object any, tag string, except ...string) (fields []string, values []any) {
	if object == nil || tag == EmptyString {
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
	excepted := make(map[string]*struct{}, 32)
	for _, field := range except {
		excepted[field] = &struct{}{}
	}

	length := ofType.NumField()

	exists := make(map[string]*struct{}, length)
	fields = make([]string, 0, length)
	values = make([]any, 0, length)

	last := 0
	fieldsIndex := make(map[string]int, 32)

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
		if column == EmptyString || column == "-" {
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

// StructObtain object should be one of anyStruct, *anyStruct for get all fields and values.
func StructObtain(object any, tag string, except ...string) (fields []string, values []any) {
	if object == nil || tag == EmptyString {
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
	excepted := make(map[string]*struct{}, 32)
	for _, field := range except {
		excepted[field] = &struct{}{}
	}

	length := ofType.NumField()

	exists := make(map[string]*struct{}, length)
	fields = make([]string, 0, length)
	values = make([]any, 0, length)

	last := 0
	fieldsIndex := make(map[string]int, 32)

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
		if column == EmptyString || column == "-" {
			continue
		}
		if _, ok := excepted[column]; ok {
			continue
		}

		add(column, ofValue.Field(i).Interface())
	}
	return
}

// StructUpdate compare origin and latest for update.
func StructUpdate(origin any, latest any, tag string, except ...string) (fields []string, values []any) {
	if origin == nil || latest == nil || tag == EmptyString {
		return
	}

	originFields, originValues := StructObtain(origin, tag, except...)
	latestFields, latestValues := StructModify(latest, tag, except...)

	storage := make(map[string]any, len(originFields))
	for k, v := range originFields {
		storage[v] = originValues[k]
	}

	exists := make(map[string]*struct{}, 32)
	fields = make([]string, 0)
	values = make([]any, 0)

	last := 0
	fieldsIndex := make(map[string]int, 32)

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

// ConcatString concatenate string.
func ConcatString(sss ...string) string {
	b := getStringBuilder()
	defer putStringBuilder(b)
	length := len(sss)
	for i := range length {
		b.WriteString(sss[i])
	}
	return b.String()
}

// SqlAlias sql alias name.
func SqlAlias(name string, alias string) string {
	if alias == EmptyString {
		return name
	}
	return ConcatString(name, SqlSpace, SqlAs, SqlSpace, alias)
}

// SqlPrefix add SQL prefix name; if the prefix exists, it will not be added.
func SqlPrefix(prefix string, name string) string {
	if prefix == EmptyString || strings.Contains(name, SqlPoint) {
		return name
	}
	return ConcatString(prefix, SqlPoint, name)
}

// ParcelPrepare Parcel the SQL statement. `subquery` => ( `subquery` )
func ParcelPrepare(prepare string) string {
	if prepare == EmptyString {
		return prepare
	}
	return ConcatString(SqlLeftSmallBracket, SqlSpace, prepare, SqlSpace, SqlRightSmallBracket)
}

// ParcelMaker Parcel the SQL statement. `subquery` => ( `subquery` )
func ParcelMaker(maker Maker) Maker {
	if maker == nil {
		return maker
	}
	script := maker.ToSQL()
	if script.Empty() {
		return maker
	}
	script.Prepare = strings.TrimSpace(script.Prepare)
	if script.Prepare[0] != SqlLeftSmallBracket[0] {
		script.Prepare = ParcelPrepare(script.Prepare)
	}
	return NewSQL(script.Prepare, script.Args...)
}

// ParcelFilter Parcel the SQL filter statement. `SQL_FILTER_STATEMENT` => ( `SQL_FILTER_STATEMENT` )
func ParcelFilter(tmp Filter) Filter {
	if tmp == nil {
		return tmp
	}

	if num := tmp.Num(); num != 1 {
		return tmp
	}

	script := tmp.ToSQL()
	if !strings.HasPrefix(script.Prepare, SqlLeftSmallBracket) {
		script.Prepare = ParcelPrepare(script.Prepare)
	}
	return tmp.New().And(script.Prepare, script.Args...)
}

// ParcelCancelPrepare Cancel parcel the SQL statement. ( `subquery` ) => `subquery` OR ( ( `subquery` ) ) => ( `subquery` )
func ParcelCancelPrepare(prepare string) string {
	prepare = strings.TrimSpace(prepare)
	prepare = strings.TrimPrefix(prepare, ConcatString(SqlLeftSmallBracket, SqlSpace))
	return strings.TrimSuffix(prepare, ConcatString(SqlSpace, SqlRightSmallBracket))
}

// ParcelCancelMaker Cancel parcel the SQL statement. ( `subquery` ) => `subquery` OR ( ( `subquery` ) ) => ( `subquery` )
func ParcelCancelMaker(maker Maker) Maker {
	if maker == nil {
		return maker
	}
	script := maker.ToSQL()
	if script == nil {
		return maker
	}
	return NewSQL(ParcelCancelPrepare(script.Prepare), script.Args...)
}

// UnionMaker MakerA, MakerB, MakerC ... => ( ( QUERY_A ) UNION ( QUERY_B ) UNION ( QUERY_C ) ... )
func UnionMaker(elements ...Maker) Maker {
	return JoinMaker(elements, ConcatString(SqlSpace, SqlUnion, SqlSpace))
}

// UnionAllMaker MakerA, MakerB, MakerC ... => ( ( QUERY_A ) UNION ALL ( QUERY_B ) UNION ALL ( QUERY_C ) ... )
func UnionAllMaker(elements ...Maker) Maker {
	return JoinMaker(elements, ConcatString(SqlSpace, SqlUnionAll, SqlSpace))
}

// ExceptMaker MakerA, MakerB ... => ( ( QUERY_A ) EXCEPT ( QUERY_B ) ... )
func ExceptMaker(elements ...Maker) Maker {
	return JoinMaker(elements, ConcatString(SqlSpace, SqlExpect, SqlSpace))
}

// IntersectMaker MakerA, MakerB ... => ( ( QUERY_A ) INTERSECT ( QUERY_B ) ... )
func IntersectMaker(elements ...Maker) Maker {
	return JoinMaker(elements, ConcatString(SqlSpace, SqlIntersect, SqlSpace))
}

// MakerScanAll Rows scan to any struct, based on struct scan data.
func MakerScanAll[V any](ctx context.Context, way *Way, maker Maker, scan func(rows *sql.Rows, v *V) error) ([]*V, error) {
	if maker == nil || scan == nil {
		return make([]*V, 0), nil
	}
	script := maker.ToSQL()
	if script == nil || script.Empty() {
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
	result := make([]*V, 0, length)
	err = way.QueryContext(ctx, func(rows *sql.Rows) error {
		index := 0
		values := make([]V, length)
		for rows.Next() {
			if err = scan(rows, &values[index]); err != nil {
				return err
			}
			result = append(result, &values[index])
			index++
			if index == length {
				index, values = 0, make([]V, length)
			}
		}
		return nil
	}, script.Prepare, script.Args...)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// MakerScanOne Rows scan to any struct, based on struct scan data.
func MakerScanOne[V any](ctx context.Context, way *Way, maker Maker, scan func(rows *sql.Rows, v *V) error) (*V, error) {
	if get, ok := maker.(*Get); ok && get != nil {
		maker = get.Limit(1)
	}
	lists, err := MakerScanAll(context.WithValue(ctx, MakerScanAllMakeSliceLength, 1), way, maker, scan)
	if err != nil {
		return nil, err
	}
	if len(lists) > 0 {
		return lists[0], nil
	}
	return nil, ErrNoRows
}

func MergeAssoc[K comparable, V any](values ...map[K]V) map[K]V {
	length := len(values)
	result := make(map[K]V, 8)
	for i := range length {
		if i == 0 {
			if values[i] != nil {
				result = values[i]
			}
			continue
		}
		for k, v := range values[i] {
			result[k] = v
		}
	}
	return result
}

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

// SQLTable Used to construct expressions that can use table aliases and their corresponding parameter lists.
type SQLTable interface {
	Empty

	Maker

	// Alias Setting aliases for script statements.
	Alias(alias string) SQLTable

	// GetAlias Getting aliases for script statements.
	GetAlias() string
}

type sqlTable struct {
	table *SQL

	alias string
}

func (s *sqlTable) Empty() bool {
	return s.table == nil || s.table.Empty()
}

func (s *sqlTable) ToSQL() *SQL {
	if s.Empty() {
		return NewSQL(EmptyString)
	}
	table := NewSQL(s.table.Prepare, s.table.Args...)
	if s.alias != EmptyString {
		table.Prepare = SqlAlias(table.Prepare, s.alias)
	}
	return table
}

func (s *sqlTable) Alias(alias string) SQLTable {
	// Allow setting empty values.
	s.alias = alias
	return s
}

func (s *sqlTable) GetAlias() string {
	return s.alias
}

func NewSQLTable(prepare string, args []any) SQLTable {
	return &sqlTable{
		table: NewSQL(prepare, args...),
	}
}

func NewSQLTableGet(alias string, get *Get) SQLTable {
	script := ParcelMaker(get).ToSQL()
	return NewSQLTable(script.Prepare, script.Args).Alias(alias)
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
	Empty

	Maker

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

func NewSQLWith() SQLWith {
	return &sqlWith{
		alias:   make([]string, 0, 2),
		column:  make(map[string][]string, 2),
		prepare: make(map[string]Maker, 2),
	}
}

func (s *sqlWith) Empty() bool {
	return len(s.alias) == 0
}

func (s *sqlWith) ToSQL() *SQL {
	if s.Empty() {
		return NewSQL(EmptyString)
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(SqlWith)
	b.WriteString(SqlSpace)
	if s.recursive {
		b.WriteString("RECURSIVE")
		b.WriteString(SqlSpace)
	}
	var args []any
	for index, alias := range s.alias {
		if index > 0 {
			b.WriteString(SqlConcat)
		}
		script := s.prepare[alias]
		b.WriteString(alias)
		b.WriteString(SqlSpace)
		if columns := s.column[alias]; len(columns) > 0 {
			// Displays the column alias that defines the CTE, overwriting the original column name of the query result.
			b.WriteString(SqlLeftSmallBracket)
			b.WriteString(SqlSpace)
			b.WriteString(strings.Join(columns, SqlConcat))
			b.WriteString(SqlSpace)
			b.WriteString(SqlRightSmallBracket)
			b.WriteString(SqlSpace)
		}
		b.WriteString(ConcatString(SqlAs, SqlSpace, SqlLeftSmallBracket, SqlSpace))
		tmp := script.ToSQL()
		b.WriteString(tmp.Prepare)
		b.WriteString(ConcatString(SqlSpace, SqlRightSmallBracket))
		args = append(args, tmp.Args...)
	}
	return NewSQL(b.String(), args...)
}

func (s *sqlWith) Recursive() SQLWith {
	s.recursive = !s.recursive
	return s
}

func (s *sqlWith) Set(alias string, maker Maker, columns ...string) SQLWith {
	if alias == EmptyString || EmptyMaker(maker) {
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
	if alias == EmptyString {
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

// SQLSelect Used to build the list of columns to be queried.
type SQLSelect interface {
	Empty

	Maker

	Distinct() SQLSelect

	Index(column string) int

	Exists(column string) bool

	Add(column string, args ...any) SQLSelect

	AddAll(columns ...string) SQLSelect

	AddMaker(makers ...Maker) SQLSelect

	DelAll(columns ...string) SQLSelect

	Len() int

	Get() ([]string, map[int][]any)

	Set(columns []string, columnsArgs map[int][]any) SQLSelect

	Use(sqlSelect ...SQLSelect) SQLSelect

	// Queried Get all columns of the query results.
	Queried(excepts ...string) []string
}

type sqlSelect struct {
	columnsMap map[string]int

	columnsArgs map[int][]any

	way *Way

	columns []string

	// distinct Allows multiple columns to be deduplicated, such as: DISTINCT column1, column2, column3 ...
	distinct bool
}

func NewSQLSelect(way *Way) SQLSelect {
	return &sqlSelect{
		columns:     make([]string, 0, 16),
		columnsMap:  make(map[string]int, 16),
		columnsArgs: make(map[int][]any),
		way:         way,
	}
}

func (s *sqlSelect) Empty() bool {
	return len(s.columns) == 0
}

func (s *sqlSelect) ToSQL() *SQL {
	length := len(s.columns)
	if length == 0 {
		prepare := SqlStar
		if s.distinct {
			prepare = ConcatString(SqlDistinct, SqlSpace, prepare)
		}
		return NewSQL(prepare)
	}
	script := NewSQL(EmptyString)
	b := getStringBuilder()
	defer putStringBuilder(b)
	if s.distinct {
		b.WriteString(SqlDistinct)
		b.WriteString(SqlSpace)
	}
	columns := make([]string, 0, length)
	for i := range length {
		args, ok := s.columnsArgs[i]
		if !ok {
			continue
		}
		columns = append(columns, s.columns[i])
		if args != nil {
			script.Args = append(script.Args, args...)
		}
	}
	b.WriteString(strings.Join(s.way.Replaces(columns), SqlConcat))
	script.Prepare = b.String()
	return script
}

func (s *sqlSelect) Distinct() SQLSelect {
	s.distinct = !s.distinct
	return s
}

func (s *sqlSelect) Index(column string) int {
	index, ok := s.columnsMap[column]
	if !ok {
		return -1
	}
	return index
}

func (s *sqlSelect) Exists(column string) bool {
	return s.Index(column) >= 0
}

func (s *sqlSelect) Add(column string, args ...any) SQLSelect {
	if column == EmptyString {
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

func (s *sqlSelect) AddAll(columns ...string) SQLSelect {
	index := len(s.columns)
	for _, column := range columns {
		if column == EmptyString {
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

func (s *sqlSelect) AddMaker(makers ...Maker) SQLSelect {
	for _, value := range makers {
		if value != nil {
			if script := value.ToSQL(); script != nil && !script.Empty() {
				s.Add(script.Prepare, script.Args...)
			}
		}
	}
	return s
}

func (s *sqlSelect) DelAll(columns ...string) SQLSelect {
	if columns == nil {
		s.columns = make([]string, 0, 16)
		s.columnsMap = make(map[string]int, 16)
		s.columnsArgs = make(map[int][]any)
		return s
	}
	deleteIndex := make(map[int]*struct{}, len(columns))
	for _, column := range columns {
		if column == EmptyString {
			continue
		}
		index, ok := s.columnsMap[column]
		if !ok {
			continue
		}
		deleteIndex[index] = &struct{}{}
	}
	length := len(s.columns)
	result := make([]string, 0, length)
	for index, column := range s.columns {
		if _, ok := deleteIndex[index]; ok {
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

func (s *sqlSelect) Set(columns []string, columnsArgs map[int][]any) SQLSelect {
	columnsMap := make(map[string]int, len(columns))
	for i, column := range columns {
		columnsMap[column] = i
		if _, ok := columnsArgs[i]; !ok {
			columnsArgs[i] = nil
		}
	}
	s.columns, s.columnsMap, s.columnsArgs = columns, columnsMap, columnsArgs
	return s
}

func (s *sqlSelect) Use(sqlSelect ...SQLSelect) SQLSelect {
	length := len(sqlSelect)
	for i := range length {
		tmp := sqlSelect[i]
		if tmp == nil {
			continue
		}
		cols, args := tmp.Get()
		for index, value := range cols {
			s.Add(value, args[index]...)
		}
	}
	return s
}

// Queried Get all columns of the query results.
func (s *sqlSelect) Queried(excepts ...string) []string {
	star := []string{SqlStar}
	cols := s.columns[:]
	length := len(cols)
	if length == 0 {
		return star
	}
	lists := make([]string, 0, length)
	for i := range length {
		column := strings.TrimSpace(cols[i])
		index := strings.LastIndex(column, SqlSpace)
		if index >= 0 {
			column = column[index+1:]
			if strings.Contains(column, SqlRightSmallBracket) {
				return star
			}
			lists = append(lists, column)
		} else {
			column = strings.TrimSuffix(column, SqlPoint)
			index = strings.LastIndex(column, SqlPoint)
			if index >= 0 {
				lists = append(lists, column[index+1:])
			} else {
				lists = append(lists, column)
			}
		}
	}
	totals := len(excepts)
	if totals == 0 {
		return lists
	}
	result := make([]string, 0, length)
	exists := make(map[string]*struct{}, totals)
	for _, except := range excepts {
		except = strings.TrimSpace(except)
		except = strings.TrimSuffix(except, SqlPoint)
		if index := strings.LastIndex(except, SqlPoint); index >= 0 {
			except = except[index+1:]
		}
		exists[except] = &struct{}{}
	}
	for _, column := range lists {
		if _, ok := exists[column]; !ok {
			result = append(result, column)
		}
	}
	return result
}

type SQLJoinOn interface {
	Maker

	// Equal Use equal value JOIN ON condition.
	Equal(leftAlias string, leftColumn string, rightAlias string, rightColumn string) SQLJoinOn

	// Filter Append custom conditions to the ON statement or use custom conditions on the ON statement to associate tables.
	Filter(fc func(f Filter)) SQLJoinOn

	// Using Use USING instead of ON.
	Using(using ...string) SQLJoinOn
}

type sqlJoinOn struct {
	way *Way

	on Filter

	using []string
}

func (s *sqlJoinOn) Equal(leftAlias string, leftColumn string, rightAlias string, rightColumn string) SQLJoinOn {
	s.on.And(ConcatString(SqlPrefix(leftAlias, leftColumn), SqlSpace, SqlEqual, SqlSpace, SqlPrefix(rightAlias, rightColumn)))
	return s
}

func (s *sqlJoinOn) Filter(fc func(f Filter)) SQLJoinOn {
	if fc != nil {
		fc(s.on)
	}
	return s
}

func (s *sqlJoinOn) Using(using ...string) SQLJoinOn {
	using = DiscardDuplicate(func(tmp string) bool { return tmp == EmptyString }, using...)
	if len(using) > 0 {
		s.using = using
	}
	return s
}

func (s *sqlJoinOn) ToSQL() *SQL {
	// JOIN ON
	if s.on != nil && !s.on.Empty() {
		script := s.on.ToSQL()
		script.Prepare = ConcatString(SqlOn, SqlSpace, script.Prepare)
		return script
	}

	script := NewSQL(EmptyString)

	// JOIN USING
	if length := len(s.using); length > 0 {
		using := make([]string, 0, length)
		for _, column := range s.using {
			if column != EmptyString {
				using = append(using, column)
			}
		}
		if len(using) > 0 {
			using = s.way.Replaces(using)
			script.Prepare = ConcatString(SqlUsing, SqlSpace, SqlLeftSmallBracket, SqlSpace, strings.Join(using, SqlConcat), SqlSpace, SqlRightSmallBracket)
			return script
		}
	}

	return script
}

func newSQLJoinOn(way *Way) SQLJoinOn {
	return &sqlJoinOn{
		way: way,
		on:  way.F(),
	}
}

// SQLJoinAssoc Constructing conditions for join queries.
type SQLJoinAssoc func(leftAlias string, rightAlias string) SQLJoinOn

type sqlJoinSchema struct {
	rightTable SQLTable

	condition Maker

	joinType string
}

// SQLJoin Constructing multi-table join queries.
type SQLJoin interface {
	Maker

	GetMaster() SQLTable

	SetMaster(master SQLTable) SQLJoin

	NewTable(table string, alias string, args ...any) SQLTable

	NewSubquery(subquery Maker, alias string) SQLTable

	On(onList ...func(o SQLJoinOn, leftAlias string, rightAlias string)) SQLJoinAssoc

	Using(columns ...string) SQLJoinAssoc

	OnEqual(leftColumn string, rightColumn string) SQLJoinAssoc

	Join(joinTypeString string, leftTable SQLTable, rightTable SQLTable, on SQLJoinAssoc) SQLJoin

	InnerJoin(leftTable SQLTable, rightTable SQLTable, on SQLJoinAssoc) SQLJoin

	LeftJoin(leftTable SQLTable, rightTable SQLTable, on SQLJoinAssoc) SQLJoin

	RightJoin(leftTable SQLTable, rightTable SQLTable, on SQLJoinAssoc) SQLJoin

	// Queries Get query columns.
	Queries() SQLSelect

	// TableColumn Build *TableColumn based on SQLTable.
	TableColumn(table SQLTable) *TableColumn

	// TableSelect Add the queried column list based on the table's alias prefix.
	TableSelect(table SQLTable, columns ...string) []string

	// TableSelectAliases Add the queried column list based on the table's alias prefix, support setting the query column alias.
	TableSelectAliases(table SQLTable, aliases map[string]string, columns ...string) []string

	// SelectGroupsColumns Add the queried column list based on the table's alias prefix.
	SelectGroupsColumns(columns ...[]string) SQLJoin

	// SelectTableColumnAlias Batch set multiple columns of the specified table and set aliases for all columns.
	SelectTableColumnAlias(table SQLTable, columnAndColumnAlias ...string) SQLJoin
}

type sqlJoin struct {
	master SQLTable

	sqlSelect SQLSelect

	way *Way

	joins []*sqlJoinSchema
}

func NewSQLJoin(way *Way) SQLJoin {
	tmp := &sqlJoin{
		joins:     make([]*sqlJoinSchema, 0, 2),
		sqlSelect: NewSQLSelect(way),
		way:       way,
	}
	return tmp
}

func (s *sqlJoin) GetMaster() SQLTable {
	return s.master
}

func (s *sqlJoin) SetMaster(master SQLTable) SQLJoin {
	if master != nil && !master.Empty() {
		s.master = master
	}
	return s
}

func (s *sqlJoin) ToSQL() *SQL {
	script := s.Queries().ToSQL()
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(ConcatString(SqlSelect, SqlSpace))
	b.WriteString(script.Prepare)
	b.WriteString(ConcatString(SqlSpace, SqlFrom, SqlSpace))
	master := s.master.ToSQL()
	b.WriteString(master.Prepare)
	b.WriteString(SqlSpace)
	script.Args = append(script.Args, master.Args...)
	for index, tmp := range s.joins {
		if tmp == nil {
			continue
		}
		if index > 0 {
			b.WriteString(SqlSpace)
		}
		b.WriteString(tmp.joinType)
		right := tmp.rightTable.ToSQL()
		b.WriteString(SqlSpace)
		b.WriteString(right.Prepare)
		if right.Args != nil {
			script.Args = append(script.Args, right.Args...)
		}
		if tmp.condition != nil {
			if on := tmp.condition.ToSQL(); on != nil && !on.Empty() {
				b.WriteString(SqlSpace)
				b.WriteString(on.Prepare)
				if on.Args != nil {
					script.Args = append(script.Args, on.Args...)
				}
			}
		}
	}
	script.Prepare = b.String()
	return script
}

func (s *sqlJoin) NewTable(table string, alias string, args ...any) SQLTable {
	return NewSQLTable(table, args).Alias(alias)
}

func (s *sqlJoin) NewSubquery(subquery Maker, alias string) SQLTable {
	script := ParcelMaker(subquery).ToSQL()
	return s.NewTable(script.Prepare, alias, script.Args...)
}

// On For `... JOIN ON ...`
func (s *sqlJoin) On(onList ...func(o SQLJoinOn, leftAlias string, rightAlias string)) SQLJoinAssoc {
	return func(leftAlias string, rightAlias string) SQLJoinOn {
		return newSQLJoinOn(s.way).Filter(func(o Filter) {
			for _, tmp := range onList {
				if tmp != nil {
					newAssoc := newSQLJoinOn(s.way)
					tmp(newAssoc, leftAlias, rightAlias)
					newAssoc.Filter(func(f Filter) { o.Use(f) })
				}
			}
		})
	}
}

// Using For `... JOIN USING ...`
func (s *sqlJoin) Using(columns ...string) SQLJoinAssoc {
	return func(leftAlias string, rightAlias string) SQLJoinOn {
		return newSQLJoinOn(s.way).Using(columns...)
	}
}

// OnEqual For `... JOIN ON ... = ... [...]`
func (s *sqlJoin) OnEqual(leftColumn string, rightColumn string) SQLJoinAssoc {
	if leftColumn == EmptyString || rightColumn == EmptyString {
		return nil
	}
	return func(leftAlias string, rightAlias string) SQLJoinOn {
		return newSQLJoinOn(s.way).Filter(func(f Filter) {
			f.CompareEqual(SqlPrefix(leftAlias, leftColumn), SqlPrefix(rightAlias, rightColumn))
		})
	}
}

func (s *sqlJoin) Join(joinTypeString string, leftTable SQLTable, rightTable SQLTable, on SQLJoinAssoc) SQLJoin {
	if joinTypeString == EmptyString {
		joinTypeString = SqlJoinInner
	}
	if leftTable == nil || leftTable.Empty() {
		leftTable = s.master
	}
	if rightTable == nil || rightTable.Empty() {
		return s
	}
	join := &sqlJoinSchema{
		joinType:   joinTypeString,
		rightTable: rightTable,
	}
	if on != nil {
		join.condition = on(leftTable.GetAlias(), rightTable.GetAlias())
	}
	s.joins = append(s.joins, join)
	return s
}

func (s *sqlJoin) InnerJoin(leftTable SQLTable, rightTable SQLTable, on SQLJoinAssoc) SQLJoin {
	return s.Join(SqlJoinInner, leftTable, rightTable, on)
}

func (s *sqlJoin) LeftJoin(leftTable SQLTable, rightTable SQLTable, on SQLJoinAssoc) SQLJoin {
	return s.Join(SqlJoinLeft, leftTable, rightTable, on)
}

func (s *sqlJoin) RightJoin(leftTable SQLTable, rightTable SQLTable, on SQLJoinAssoc) SQLJoin {
	return s.Join(SqlJoinRight, leftTable, rightTable, on)
}

func (s *sqlJoin) Queries() SQLSelect {
	return s.sqlSelect
}

func (s *sqlJoin) TableColumn(table SQLTable) *TableColumn {
	result := s.way.T()
	if table == nil {
		return result
	}
	if alias := table.GetAlias(); alias != EmptyString {
		result.SetAlias(alias)
	}
	return result
}

func (s *sqlJoin) selectTableColumnOptionalColumnAlias(table SQLTable, aliases map[string]string, columns ...string) []string {
	change := s.TableColumn(table)
	result := make([]string, len(columns))
	for index, column := range columns {
		alias, ok := aliases[column]
		if !ok || alias == EmptyString {
			alias = columns[index]
		}
		result[index] = change.Column(column, alias)
	}
	return result
}

func (s *sqlJoin) TableSelect(table SQLTable, columns ...string) []string {
	return s.selectTableColumnOptionalColumnAlias(table, nil, columns...)
}

func (s *sqlJoin) TableSelectAliases(table SQLTable, aliases map[string]string, columns ...string) []string {
	return s.selectTableColumnOptionalColumnAlias(table, aliases, columns...)
}

func (s *sqlJoin) SelectGroupsColumns(columns ...[]string) SQLJoin {
	groups := make([]string, 0, 32)
	for _, values := range columns {
		groups = append(groups, values...)
	}
	s.sqlSelect.AddAll(groups...)
	return s
}

func (s *sqlJoin) SelectTableColumnAlias(table SQLTable, columnAndColumnAlias ...string) SQLJoin {
	length := len(columnAndColumnAlias)
	if length == 0 || length&1 == 1 {
		return s
	}
	tmp := s.TableColumn(table)
	for i := 0; i < length; i += 2 {
		if columnAndColumnAlias[i] != EmptyString && columnAndColumnAlias[i+1] != EmptyString {
			s.sqlSelect.Add(tmp.Column(columnAndColumnAlias[i], columnAndColumnAlias[i+1]))
		}
	}
	return s
}

// SQLGroupBy Constructing query groups.
type SQLGroupBy interface {
	Empty

	Maker

	Column(columns ...string) SQLGroupBy

	Group(prepare string, args ...any) SQLGroupBy

	GroupMaker(maker Maker) SQLGroupBy

	Having(having func(having Filter)) SQLGroupBy
}

type sqlGroupBy struct {
	having Filter

	groupColumnsMap map[string]int

	way *Way

	group string

	groupArgs []any

	groupColumns []string
}

func (s *sqlGroupBy) Empty() bool {
	return len(s.groupColumns) == 0 && s.group == EmptyString
}

func (s *sqlGroupBy) ToSQL() *SQL {
	script := NewSQL(EmptyString)
	groupBy := EmptyString
	if s.group != EmptyString {
		groupBy = s.group
		script.Args = append(script.Args, s.groupArgs...)
	}
	if group := strings.Join(s.way.Replaces(s.groupColumns), SqlConcat); group != EmptyString {
		if groupBy == EmptyString {
			groupBy = group
		} else {
			groupBy = ConcatString(groupBy, SqlConcat, group)
		}
	}

	if groupBy == EmptyString {
		return NewSQL(EmptyString)
	}

	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(ConcatString(SqlGroupBy, SqlSpace, groupBy))

	if !s.having.Empty() {
		b.WriteString(ConcatString(SqlSpace, SqlHaving, SqlSpace))
		having := ParcelFilter(s.having).ToSQL()
		b.WriteString(having.Prepare)
		script.Args = append(script.Args, having.Args...)
	}
	script.Prepare = b.String()
	return script
}

func (s *sqlGroupBy) Group(prepare string, args ...any) SQLGroupBy {
	s.group, s.groupArgs = prepare, args
	return s
}

func (s *sqlGroupBy) GroupMaker(maker Maker) SQLGroupBy {
	if maker == nil {
		return s
	}
	script := maker.ToSQL()
	if !script.Empty() {
		return s.Group(script.Prepare, script.Args...)
	}
	return s
}

func (s *sqlGroupBy) Column(columns ...string) SQLGroupBy {
	for _, column := range columns {
		if column == EmptyString {
			continue
		}
		if _, ok := s.groupColumnsMap[column]; ok {
			continue
		}
		s.groupColumnsMap[column] = len(s.groupColumns)
		s.groupColumns = append(s.groupColumns, column)
	}
	return s
}

func (s *sqlGroupBy) Having(having func(having Filter)) SQLGroupBy {
	if having != nil {
		having(s.having)
	}
	return s
}

func NewSQLGroupBy(way *Way) SQLGroupBy {
	return &sqlGroupBy{
		groupColumns:    make([]string, 0, 2),
		groupColumnsMap: make(map[string]int, 2),
		having:          way.F(),
		way:             way,
	}
}

// SQLOrderBy Constructing query orders.
type SQLOrderBy interface {
	Empty

	Maker

	Use(columns ...string) SQLOrderBy

	Asc(columns ...string) SQLOrderBy

	Desc(columns ...string) SQLOrderBy
}

type sqlOrderBy struct {
	allow map[string]*struct{}

	orderMap map[string]int

	way *Way

	orderBy []string
}

func (s *sqlOrderBy) Empty() bool {
	return len(s.orderBy) == 0
}

func (s *sqlOrderBy) ToSQL() *SQL {
	script := NewSQL(EmptyString)
	if s.Empty() {
		return script
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(ConcatString(SqlOrderBy, SqlSpace))
	b.WriteString(strings.Join(s.orderBy, SqlConcat))
	script.Prepare = b.String()
	return script
}

func (s *sqlOrderBy) Use(columns ...string) SQLOrderBy {
	allow := make(map[string]*struct{}, len(columns))
	for _, column := range columns {
		if column != EmptyString {
			allow[column] = nil
		}
	}
	if len(allow) > 0 {
		s.allow = MergeAssoc(s.allow, allow)
	}
	return s
}

func (s *sqlOrderBy) add(category string, columns ...string) SQLOrderBy {
	if category == EmptyString {
		return s
	}
	allow := s.allow != nil
	index := len(s.orderBy)
	for _, column := range columns {
		if column == EmptyString {
			continue
		}
		if allow {
			if _, ok := s.allow[column]; !ok {
				continue
			}
		}
		if _, ok := s.orderMap[column]; ok {
			continue
		}
		s.orderMap[column] = index
		index++
		order := s.way.Replace(column)
		order = ConcatString(order, SqlSpace, category)
		s.orderBy = append(s.orderBy, order)
	}
	return s
}

func (s *sqlOrderBy) Asc(columns ...string) SQLOrderBy {
	return s.add(SqlAsc, columns...)
}

func (s *sqlOrderBy) Desc(columns ...string) SQLOrderBy {
	return s.add(SqlDesc, columns...)
}

func NewSQLOrderBy(way *Way) SQLOrderBy {
	return &sqlOrderBy{
		orderBy:  make([]string, 0, 2),
		orderMap: make(map[string]int, 2),
		way:      way,
	}
}

// SQLLimit Constructing query limits.
type SQLLimit interface {
	Empty

	Maker

	Limit(limit int64) SQLLimit

	Offset(offset int64) SQLLimit

	Page(page int64, limit ...int64) SQLLimit
}

type sqlLimit struct {
	limit *int64

	offset *int64
}

func (s *sqlLimit) Empty() bool {
	return s.limit == nil
}

func (s *sqlLimit) ToSQL() *SQL {
	script := NewSQL(EmptyString)
	if s.Empty() {
		return script
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(SqlLimit)
	b.WriteString(SqlSpace)
	limit := strconv.FormatInt(*s.limit, 10)
	b.WriteString(limit)
	if s.offset != nil && *s.offset >= 0 {
		b.WriteString(SqlSpace)
		b.WriteString(SqlOffset)
		b.WriteString(SqlSpace)
		offset := strconv.FormatInt(*s.offset, 10)
		b.WriteString(offset)
	}
	script.Prepare = b.String()
	return script
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

func NewSQLLimit() SQLLimit {
	return &sqlLimit{}
}

// SQLOrderByLimit Construct the ORDER BY and LIMIT clauses or append the ORDER BY and LIMIT clauses to the end of the SQL statement.
type SQLOrderByLimit interface {
	Maker

	// OrderBy Custom ORDER BY
	OrderBy(fc func(o SQLOrderBy)) SQLOrderByLimit

	// Asc ORDER BY column ASC
	Asc(column string) SQLOrderByLimit

	// Desc ORDER BY column DSC
	Desc(column string) SQLOrderByLimit

	// Order Custom ORDER BY parsed-string
	Order(order string, replaces ...map[string]string) SQLOrderByLimit

	// Limit LIMIT n
	Limit(limit int64) SQLOrderByLimit

	// Offset OFFSET n
	Offset(offset int64) SQLOrderByLimit

	// Limiter LIMIT + OFFSET
	Limiter(limiter Limiter) SQLOrderByLimit

	// Page LIMIT + OFFSET
	Page(page int64, limit ...int64) SQLOrderByLimit
}

type sqlOrderByLimit struct {
	maker Maker

	order SQLOrderBy

	orderRegexp *regexp.Regexp

	limit SQLLimit
}

func (s *sqlOrderByLimit) ToSQL() *SQL {
	b := getStringBuilder()
	defer putStringBuilder(b)
	script := NewSQL(EmptyString)
	if maker := s.maker; maker != nil {
		if tmp := maker.ToSQL(); tmp != nil && !tmp.Empty() {
			b.WriteString(tmp.Prepare)
			if tmp.Args != nil {
				script.Args = append(script.Args, tmp.Args...)
			}
		}
	}
	if !s.order.Empty() {
		order := s.order.ToSQL()
		if !order.Empty() {
			b.WriteString(SqlSpace)
			b.WriteString(order.Prepare)
			script.Args = append(script.Args, order.Args...)
		}
	}
	if !s.limit.Empty() {
		limit := s.limit.ToSQL()
		if !limit.Empty() {
			b.WriteString(SqlSpace)
			b.WriteString(limit.Prepare)
			script.Args = append(script.Args, limit.Args...)
		}
	}
	script.Prepare = b.String()
	return script
}

// OrderBy SQL ORDER BY.
func (s *sqlOrderByLimit) OrderBy(fc func(o SQLOrderBy)) SQLOrderByLimit {
	if fc != nil {
		fc(s.order)
	}
	return s
}

// Asc set order by column ASC.
func (s *sqlOrderByLimit) Asc(column string) SQLOrderByLimit {
	s.order.Asc(column)
	return s
}

// Desc set order by column DESC.
func (s *sqlOrderByLimit) Desc(column string) SQLOrderByLimit {
	s.order.Desc(column)
	return s
}

// Order set the column sorting list in batches through regular expressions according to the request parameter value.
func (s *sqlOrderByLimit) Order(order string, replaces ...map[string]string) SQLOrderByLimit {
	columns := make(map[string]string, 8)
	mapping := make([]string, 0, 8)
	for _, tmp := range replaces {
		for column, value := range tmp {
			if column != EmptyString {
				columns[column] = value
			}
			if value != EmptyString {
				mapping = append(mapping, value)
			}
		}
	}
	if len(mapping) > 0 {
		s.order.Use(mapping...)
	}
	orders := strings.Split(order, ",")
	for _, v := range orders {
		if len(v) > 32 {
			continue
		}
		match := s.orderRegexp.FindAllStringSubmatch(strings.TrimSpace(v), -1)
		length := len(match)
		if length != 1 {
			continue
		}
		matched := match[0]
		length = len(matched) // the length should be 4.
		if length < 4 || matched[3] == EmptyString {
			continue
		}
		column := matched[1]
		if value, ok := columns[column]; ok {
			column = value
		}
		if matched[3][0] == 97 {
			s.Asc(column)
			continue
		}
		if matched[3][0] == 100 {
			s.Desc(column)
			continue
		}
	}
	return s
}

// Limit set limit.
func (s *sqlOrderByLimit) Limit(limit int64) SQLOrderByLimit {
	s.limit.Limit(limit)
	return s
}

// Offset set offset.
func (s *sqlOrderByLimit) Offset(offset int64) SQLOrderByLimit {
	s.limit.Offset(offset)
	return s
}

// Limiter set limit and offset at the same time.
func (s *sqlOrderByLimit) Limiter(limiter Limiter) SQLOrderByLimit {
	if limiter == nil {
		return s
	}
	return s.Limit(limiter.GetLimit()).Offset(limiter.GetOffset())
}

// Page Setting LIMIT and OFFSET by page number.
func (s *sqlOrderByLimit) Page(page int64, limit ...int64) SQLOrderByLimit {
	s.limit.Page(page, limit...)
	return s
}

func NewSQLOrderByLimit(way *Way, maker Maker) SQLOrderByLimit {
	// orderRegexp `column_name_first:a,column_name_second:d` => `column_name_first ASC, column_name_second DESC`.
	orderRegexp := regexp.MustCompile(`^([a-zA-Z][a-zA-Z0-9_]*([.][a-zA-Z][a-zA-Z0-9_]*)*):([ad])$`)
	return &sqlOrderByLimit{
		maker:       maker,
		order:       NewSQLOrderBy(way),
		orderRegexp: orderRegexp,
		limit:       NewSQLLimit(),
	}
}

// SQLUpsertColumn Constructing insert columns.
type SQLUpsertColumn interface {
	Empty

	Maker

	Add(columns ...string) SQLUpsertColumn

	Del(columns ...string) SQLUpsertColumn

	DelUseIndex(indexes ...int) SQLUpsertColumn

	ColumnIndex(column string) int

	ColumnExists(column string) bool

	Len() int

	SetColumns(columns []string) SQLUpsertColumn

	GetColumns() []string

	GetColumnsMap() map[string]*struct{}
}

type sqlUpsertColumn struct {
	columnsMap map[string]int

	way *Way

	columns []string
}

func NewSQLUpsertColumn(way *Way) SQLUpsertColumn {
	return &sqlUpsertColumn{
		columns:    make([]string, 0, 16),
		columnsMap: make(map[string]int, 16),
		way:        way,
	}
}

func (s *sqlUpsertColumn) Empty() bool {
	return len(s.columns) == 0
}

func (s *sqlUpsertColumn) ToSQL() *SQL {
	if s.Empty() {
		return NewSQL(EmptyString)
	}
	return NewSQL(ParcelPrepare(strings.Join(s.way.Replaces(s.columns), SqlConcat)))
}

func (s *sqlUpsertColumn) Add(columns ...string) SQLUpsertColumn {
	num := len(s.columns)
	for _, column := range columns {
		if column == EmptyString {
			continue
		}
		if _, ok := s.columnsMap[column]; ok {
			continue
		}
		s.columns = append(s.columns, column)
		s.columnsMap[column] = num
		num++
	}
	return s
}

func (s *sqlUpsertColumn) Del(columns ...string) SQLUpsertColumn {
	if columns == nil {
		s.columns = make([]string, 0, 16)
		s.columnsMap = make(map[string]int, 16)
		return s
	}
	deletedIndex := make(map[int]*struct{}, len(columns))
	for _, column := range columns {
		if column == EmptyString {
			continue
		}
		index, ok := s.columnsMap[column]
		if !ok {
			continue
		}
		deletedIndex[index] = &struct{}{}
	}
	length := len(s.columns)
	columns = make([]string, 0, length)
	columnsMap := make(map[string]int, length)
	num := 0
	for index, column := range s.columns {
		if _, ok := deletedIndex[index]; !ok {
			columns = append(columns, column)
			columnsMap[column] = num
			num++
		}
	}
	s.columns, s.columnsMap = columns, columnsMap
	return s
}

func (s *sqlUpsertColumn) DelUseIndex(indexes ...int) SQLUpsertColumn {
	length := len(s.columns)
	minIndex, maxIndex := 0, length
	if maxIndex == minIndex {
		return s
	}
	count := len(indexes)
	deletedIndex := make(map[int]*struct{}, count)
	for _, index := range indexes {
		if index >= minIndex && index < maxIndex {
			deletedIndex[index] = &struct{}{}
		}
	}
	if len(deletedIndex) == 0 {
		return s
	}
	columns := make([]string, 0, length)
	for index, column := range s.columns {
		if _, ok := deletedIndex[index]; ok {
			columns = append(columns, column)
		}
	}
	return s.Del(columns...)
}

func (s *sqlUpsertColumn) ColumnIndex(column string) int {
	index, ok := s.columnsMap[column]
	if !ok {
		return -1
	}
	return index
}

func (s *sqlUpsertColumn) ColumnExists(column string) bool {
	return s.ColumnIndex(column) >= 0
}

func (s *sqlUpsertColumn) SetColumns(columns []string) SQLUpsertColumn {
	return s.Del().Add(columns...)
}

func (s *sqlUpsertColumn) GetColumns() []string {
	return s.columns[:]
}

func (s *sqlUpsertColumn) GetColumnsMap() map[string]*struct{} {
	result := make(map[string]*struct{}, len(s.columns))
	for _, column := range s.GetColumns() {
		result[column] = &struct{}{}
	}
	return result
}

func (s *sqlUpsertColumn) Len() int {
	return len(s.columns)
}

// SQLInsertValue Constructing insert values.
type SQLInsertValue interface {
	Empty

	Maker

	SetSubquery(subquery Maker) SQLInsertValue

	SetValues(values ...[]any) SQLInsertValue

	Set(index int, value any) SQLInsertValue

	Del(indexes ...int) SQLInsertValue

	LenValues() int

	GetValues() [][]any
}

type sqlInsertValue struct {
	subquery Maker

	values [][]any
}

func NewSQLInsertValue() SQLInsertValue {
	return &sqlInsertValue{}
}

func (s *sqlInsertValue) Empty() bool {
	return s.subquery == nil && (len(s.values) == 0 || len(s.values[0]) == 0)
}

func (s *sqlInsertValue) ToSQL() *SQL {
	script := NewSQL(EmptyString)
	if s.Empty() {
		return script
	}
	if s.subquery != nil {
		return s.subquery.ToSQL()
	}
	count := len(s.values)
	if count == 0 {
		return script
	}
	length := len(s.values[0])
	if length == 0 {
		return script
	}
	line := make([]string, length)
	script.Args = make([]any, 0, count*length)
	for i := range length {
		line[i] = SqlPlaceholder
	}
	value := ParcelPrepare(strings.Join(line, SqlConcat))
	rows := make([]string, count)
	for i := range count {
		script.Args = append(script.Args, s.values[i]...)
		rows[i] = value
	}
	script.Prepare = strings.Join(rows, SqlConcat)
	return script
}

func (s *sqlInsertValue) SetSubquery(subquery Maker) SQLInsertValue {
	if subquery != nil {
		if script := subquery.ToSQL(); script.Empty() {
			return s
		}
	}
	s.subquery = subquery
	return s
}

func (s *sqlInsertValue) SetValues(values ...[]any) SQLInsertValue {
	s.values = values
	return s
}

func (s *sqlInsertValue) Set(index int, value any) SQLInsertValue {
	if index < 0 {
		return s
	}
	if s.values == nil {
		s.values = make([][]any, 1)
	}
	for num, tmp := range s.values {
		length := len(tmp)
		if index > length {
			continue
		}
		if index == length {
			s.values[num] = append(s.values[num], value)
		} else {
			s.values[num][index] = value
		}
	}
	return s
}

func (s *sqlInsertValue) Del(indexes ...int) SQLInsertValue {
	if s.values == nil {
		return s
	}
	length := len(indexes)
	if length == 0 {
		return s
	}
	deletedIndex := make(map[int]*struct{}, length)
	for _, index := range indexes {
		if index < 0 {
			continue
		}
		deletedIndex[index] = &struct{}{}
	}
	length = len(deletedIndex)
	if length == 0 {
		return s
	}
	values := make([][]any, len(s.values))
	for index, value := range s.values {
		values[index] = make([]any, 0, len(value))
		for num, tmp := range value {
			if _, ok := deletedIndex[num]; !ok {
				values[index] = append(values[index], tmp)
			}
		}
	}
	s.values = values
	return s
}

func (s *sqlInsertValue) LenValues() int {
	return len(s.values)
}

func (s *sqlInsertValue) GetValues() [][]any {
	return s.values
}

// SQLUpdateSet Constructing update sets.
type SQLUpdateSet interface {
	Empty

	Maker

	Update(update string, args ...any) SQLUpdateSet

	Set(column string, value any) SQLUpdateSet

	Decr(column string, decr any) SQLUpdateSet

	Incr(column string, incr any) SQLUpdateSet

	SetMap(columnValue map[string]any) SQLUpdateSet

	SetSlice(columns []string, values []any) SQLUpdateSet

	Len() int

	GetUpdate() (updates []string, args [][]any)

	UpdateIndex(prepare string) int

	UpdateExists(prepare string) bool
}

type sqlUpdateSet struct {
	updateMap map[string]int

	way *Way

	updateExpr []string

	updateArgs [][]any
}

func NewSQLUpdateSet(way *Way) SQLUpdateSet {
	return &sqlUpdateSet{
		updateExpr: make([]string, 0, 8),
		updateArgs: make([][]any, 0, 8),
		updateMap:  make(map[string]int, 8),
		way:        way,
	}
}

func (s *sqlUpdateSet) Empty() bool {
	return len(s.updateExpr) == 0
}

func (s *sqlUpdateSet) ToSQL() *SQL {
	script := NewSQL(EmptyString)
	if s.Empty() {
		return script
	}
	script.Prepare = strings.Join(s.updateExpr, SqlConcat)
	for _, tmp := range s.updateArgs {
		script.Args = append(script.Args, tmp...)
	}
	return script
}

func (s *sqlUpdateSet) beautifyUpdate(update string) string {
	update = strings.TrimSpace(update)
	for strings.Contains(update, "  ") {
		update = strings.ReplaceAll(update, "  ", SqlSpace)
	}
	return update
}

func (s *sqlUpdateSet) Update(update string, args ...any) SQLUpdateSet {
	if update == EmptyString {
		return s
	}
	update = s.beautifyUpdate(update)
	if update == EmptyString {
		return s
	}
	index, ok := s.updateMap[update]
	if ok {
		s.updateExpr[index], s.updateArgs[index] = update, args
		return s
	}
	s.updateMap[update] = len(s.updateExpr)
	s.updateExpr = append(s.updateExpr, update)
	s.updateArgs = append(s.updateArgs, args)
	return s
}

func (s *sqlUpdateSet) Set(column string, value any) SQLUpdateSet {
	column = s.way.Replace(column)
	return s.Update(fmt.Sprintf("%s = %s", column, SqlPlaceholder), value)
}

func (s *sqlUpdateSet) Decr(column string, decrement any) SQLUpdateSet {
	column = s.way.Replace(column)
	return s.Update(fmt.Sprintf("%s = %s - %s", column, column, SqlPlaceholder), decrement)
}

func (s *sqlUpdateSet) Incr(column string, increment any) SQLUpdateSet {
	s.way.Replace(column)
	return s.Update(fmt.Sprintf("%s = %s + %s", column, column, SqlPlaceholder), increment)
}

func (s *sqlUpdateSet) SetMap(columnValue map[string]any) SQLUpdateSet {
	for column, value := range columnValue {
		s.Set(column, value)
	}
	return s
}

func (s *sqlUpdateSet) SetSlice(columns []string, values []any) SQLUpdateSet {
	for index, column := range columns {
		s.Set(column, values[index])
	}
	return s
}

func (s *sqlUpdateSet) Len() int {
	return len(s.updateExpr)
}

func (s *sqlUpdateSet) GetUpdate() ([]string, [][]any) {
	return s.updateExpr, s.updateArgs
}

func (s *sqlUpdateSet) UpdateIndex(update string) int {
	update = s.beautifyUpdate(update)
	index, ok := s.updateMap[update]
	if !ok {
		return -1
	}
	return index
}

func (s *sqlUpdateSet) UpdateExists(update string) bool {
	return s.UpdateIndex(update) >= 0
}

/* CASE [xxx] WHEN xxx THEN XXX [WHEN yyy THEN YYY] [ELSE xxx] END [AS xxx] */

func sqlCaseValueFirst(value any) (prepare string) {
	if value == nil {
		prepare = SqlNull
		return
	}
	switch val := value.(type) {
	case bool:
		prepare = fmt.Sprintf("%t", val)
	case float32:
		prepare = strconv.FormatFloat(float64(val), 'g', -1, 64)
	case float64:
		prepare = strconv.FormatFloat(val, 'g', -1, 64)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		prepare = fmt.Sprintf("%d", val)
	case string:
		prepare = val
	default:
		panic(fmt.Errorf("hey: unsupported type `%T`", val))
	}
	return
}

func sqlCaseValuesToMaker(values ...any) Maker {
	length := len(values)
	if length == 0 {
		return nil
	}
	if tmp, ok := values[0].(Maker); ok {
		return tmp
	}
	if length == 1 {
		return NewSQL(sqlCaseValueFirst(values[0]))
	}
	first, ok := values[0].(string)
	if !ok {
		return nil
	}
	return NewSQL(sqlCaseValueFirst(first), values[1:]...)
}

func sqlCaseString(value string) string {
	return fmt.Sprintf("'%s'", value)
}

func sqlCaseParseMaker(maker Maker) (string, []any) {
	if maker == nil {
		return SqlNull, nil
	}
	tmp := maker.ToSQL()
	if tmp == nil {
		return SqlNull, nil
	}
	if es := sqlCaseString(EmptyString); tmp.Prepare == EmptyString || tmp.Prepare == es {
		tmp.Prepare, tmp.Args = es, nil
	}
	return tmp.Prepare, tmp.Args
}

// SQLWhenThen Store multiple pairs of WHEN xxx THEN xxx.
type SQLWhenThen interface {
	Maker

	V(value string) string

	When(values ...any) SQLWhenThen

	Then(values ...any) SQLWhenThen
}

type sqlWhenThen struct {
	when []Maker

	then []Maker
}

func (s *sqlWhenThen) V(value string) string {
	return sqlCaseString(value)
}

func (s *sqlWhenThen) ToSQL() *SQL {
	b := getStringBuilder()
	defer putStringBuilder(b)
	length1, length2 := len(s.when), len(s.then)
	script := NewSQL(EmptyString)
	if length1 != length2 || length1 == 0 {
		return script
	}
	for i := 0; i < length1; i++ {
		if i > 0 {
			b.WriteString(SqlSpace)
		}
		b.WriteString(SqlWhen)
		b.WriteString(SqlSpace)
		prepare, args := sqlCaseParseMaker(s.when[i])
		b.WriteString(prepare)
		script.Args = append(script.Args, args...)
		b.WriteString(SqlSpace)
		b.WriteString(SqlThen)
		b.WriteString(SqlSpace)
		prepare, args = sqlCaseParseMaker(s.then[i])
		b.WriteString(prepare)
		script.Args = append(script.Args, args...)
	}
	script.Prepare = b.String()
	return script
}

func (s *sqlWhenThen) When(values ...any) SQLWhenThen {
	s.when = append(s.when, sqlCaseValuesToMaker(values...))
	return s
}

func (s *sqlWhenThen) Then(values ...any) SQLWhenThen {
	s.then = append(s.then, sqlCaseValuesToMaker(values...))
	return s
}

// SQLCase Implementing SQL CASE.
type SQLCase interface {
	Maker

	V(value string) string

	Alias(alias string) SQLCase

	Case(values ...any) SQLCase

	When(fc func(w SQLWhenThen)) SQLCase

	Else(values ...any) SQLCase
}

type sqlCase struct {
	sqlCase Maker // CASE value , value is optional

	sqlWhen SQLWhenThen // WHEN xxx THEN xxx [WHEN xxx THEN xxx] ...

	sqlElse Maker // ELSE value , value is optional

	way *Way

	alias string // alias name for CASE , value is optional
}

func (s *sqlCase) V(value string) string {
	return sqlCaseString(value)
}

func (s *sqlCase) ToSQL() *SQL {
	script := NewSQL(EmptyString)
	if s.sqlWhen == nil {
		return script
	}
	whenThen := s.sqlWhen.ToSQL()
	if whenThen.Empty() {
		return script
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(SqlCase)
	if tmp := s.sqlCase; tmp != nil {
		b.WriteString(SqlSpace)
		express, params := sqlCaseParseMaker(tmp)
		b.WriteString(express)
		script.Args = append(script.Args, params...)
	}
	b.WriteString(SqlSpace)
	b.WriteString(whenThen.Prepare)
	script.Args = append(script.Args, whenThen.Args...)
	if tmp := s.sqlElse; tmp != nil {
		b.WriteString(SqlSpace)
		b.WriteString(SqlElse)
		b.WriteString(SqlSpace)
		express, params := sqlCaseParseMaker(tmp)
		b.WriteString(express)
		script.Args = append(script.Args, params...)
	}
	b.WriteString(SqlSpace)
	b.WriteString(SqlEnd)
	if alias := s.alias; alias != EmptyString {
		b.WriteString(SqlSpace)
		b.WriteString(SqlAs)
		b.WriteString(SqlSpace)
		if s.way != nil {
			alias = s.way.Replace(alias)
		}
		b.WriteString(alias)
	}
	script.Prepare = b.String()
	return script
}

func (s *sqlCase) Alias(alias string) SQLCase {
	s.alias = alias
	return s
}

func (s *sqlCase) Case(values ...any) SQLCase {
	if tmp := sqlCaseValuesToMaker(values...); tmp != nil {
		s.sqlCase = tmp
	}
	return s
}

func (s *sqlCase) When(fc func(w SQLWhenThen)) SQLCase {
	if fc == nil {
		return s
	}
	if s.sqlWhen == nil {
		s.sqlWhen = &sqlWhenThen{}
	}
	fc(s.sqlWhen)
	return s
}

func (s *sqlCase) Else(values ...any) SQLCase {
	if tmp := sqlCaseValuesToMaker(values...); tmp != nil {
		s.sqlElse = tmp
	}
	return s
}

func NewSQLCase(way *Way) SQLCase {
	return &sqlCase{
		way: way,
	}
}

// SQLInsertOnConflictUpdateSet Implement the following SQL statement:
// INSERT INTO ... ON CONFLICT ( column_a[, column_b, column_c...] ) DO UPDATE SET column1 = EXCLUDED.column1, column2 = EXCLUDED.column2, column3 = EXCLUDED.column3, column4 = 'fixed value' ...
type SQLInsertOnConflictUpdateSet interface {
	SQLUpdateSet

	// Excluded Construct the update expression column1 = EXCLUDED.column1, column2 = EXCLUDED.column2, column3 = EXCLUDED.column3 ...
	// This is how the `new` data is accessed that causes the conflict.
	Excluded(columns ...string) SQLInsertOnConflictUpdateSet
}

type sqlInsertOnConflictUpdateSet struct {
	SQLUpdateSet

	way *Way
}

func (s *sqlInsertOnConflictUpdateSet) Excluded(columns ...string) SQLInsertOnConflictUpdateSet {
	for _, column := range columns {
		tmp := s.way.Replace(column)
		s.Update(ConcatString(tmp, SqlSpace, SqlEqual, SqlSpace, SqlExcluded, SqlPoint, tmp))
	}
	return s
}

func NewSQLInsertOnConflictUpdateSet(way *Way) SQLInsertOnConflictUpdateSet {
	tmp := &sqlInsertOnConflictUpdateSet{
		way: way,
	}
	tmp.SQLUpdateSet = NewSQLUpdateSet(way)
	return tmp
}

// SQLInsertOnConflict Implement the following SQL statement:
// INSERT INTO ... ON CONFLICT (column_a[, column_b, column_c...]) DO NOTHING /* If a conflict occurs, the insert operation is ignored. */
// INSERT INTO ... ON CONFLICT (column_a[, column_b, column_c...]) DO UPDATE SET column1 = EXCLUDED.column1, column2 = EXCLUDED.column2, column3 = EXCLUDED.column3, column4 = 'fixed value' ... /* If a conflict occurs, the existing row is updated with the new value */
type SQLInsertOnConflict interface {
	// OnConflict The column causing the conflict, such as a unique key or primary key, which can be a single column or multiple columns.
	OnConflict(onConflicts ...string) SQLInsertOnConflict

	// Do The SQL statement that needs to be executed when a data conflict occurs. By default, nothing is done.
	Do(prepare string, args ...any) SQLInsertOnConflict

	// DoUpdateSet SQL update statements executed when data conflicts occur.
	DoUpdateSet(fc func(u SQLInsertOnConflictUpdateSet)) SQLInsertOnConflict

	// ToSQL The SQL statement and its parameter list that are finally executed.
	ToSQL() *SQL

	// SQLInsertOnConflict Executes the SQL statement constructed by the current object.
	SQLInsertOnConflict() (int64, error)
}

type sqlInsertOnConflict struct {
	ctx context.Context

	onConflictsDoUpdateSet SQLInsertOnConflictUpdateSet

	way *Way

	insertPrepare string

	onConflictsDoPrepare string

	insertPrepareArgs []any

	onConflicts []string

	onConflictsDoPrepareArgs []any
}

func (s *sqlInsertOnConflict) Context(ctx context.Context) SQLInsertOnConflict {
	s.ctx = ctx
	return s
}

func (s *sqlInsertOnConflict) OnConflict(onConflicts ...string) SQLInsertOnConflict {
	s.onConflicts = onConflicts
	return s
}

func (s *sqlInsertOnConflict) Do(prepare string, args ...any) SQLInsertOnConflict {
	s.onConflictsDoPrepare, s.onConflictsDoPrepareArgs = strings.TrimSpace(prepare), args
	return s
}

func (s *sqlInsertOnConflict) DoUpdateSet(fc func(u SQLInsertOnConflictUpdateSet)) SQLInsertOnConflict {
	tmp := s.onConflictsDoUpdateSet
	if tmp == nil {
		s.onConflictsDoUpdateSet = NewSQLInsertOnConflictUpdateSet(s.way)
		tmp = s.onConflictsDoUpdateSet
	}
	fc(tmp)
	return s
}

func (s *sqlInsertOnConflict) ToSQL() *SQL {
	script := NewSQL(EmptyString)
	if s.insertPrepare == EmptyString || len(s.onConflicts) == 0 {
		return script
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(s.insertPrepare)
	script.Args = append(script.Args, s.insertPrepareArgs...)
	b.WriteString(ConcatString(SqlSpace, SqlOn, SqlSpace, SqlConflict, SqlSpace))
	b.WriteString(ParcelPrepare(strings.Join(s.way.Replaces(s.onConflicts), SqlConcat)))
	b.WriteString(SqlSpace)
	b.WriteString(SqlDo)
	b.WriteString(SqlSpace)
	doPrepare, doPrepareArgs := SqlNothing, make([]any, 0)
	if s.onConflictsDoPrepare != EmptyString {
		doPrepare, doPrepareArgs = s.onConflictsDoPrepare, s.onConflictsDoPrepareArgs
	} else {
		if s.onConflictsDoUpdateSet != nil && s.onConflictsDoUpdateSet.Len() > 0 {
			update := s.onConflictsDoUpdateSet.ToSQL()
			bus := getStringBuilder()
			defer putStringBuilder(bus)
			b.WriteString(ConcatString(SqlUpdate, SqlSpace, SqlSet, SqlSpace))
			bus.WriteString(update.Prepare)
			doPrepare = bus.String()
			doPrepareArgs = update.Args
		}
	}
	b.WriteString(doPrepare)
	script.Args = append(script.Args, doPrepareArgs...)
	script.Prepare = b.String()
	return script
}

func (s *sqlInsertOnConflict) SQLInsertOnConflict() (int64, error) {
	ctx := s.ctx
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.way.cfg.TransactionMaxDuration)
		defer cancel()
	}
	script := s.ToSQL()
	return s.way.ExecContext(ctx, script.Prepare, script.Args...)
}

func NewSQLInsertOnConflict(way *Way, insertPrepare string, insertArgs ...any) SQLInsertOnConflict {
	return &sqlInsertOnConflict{
		way:               way,
		insertPrepare:     insertPrepare,
		insertPrepareArgs: insertArgs,
	}
}

/**
 * sql identifier.
 **/

type TableColumn struct {
	way   *Way
	alias string
}

// Alias Get the alias name value.
func (s *TableColumn) Alias() string {
	return s.alias
}

// aliasesName Adjust alias name.
func (s *TableColumn) aliasName(alias ...string) string {
	return s.way.Replace(LastNotEmptyString(alias))
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
	if s.alias == EmptyString {
		return s.way.Replaces(columns)
	}
	alias := s.way.Replace(s.alias)
	result := make([]string, len(columns))
	for index, column := range columns {
		result[index] = SqlPrefix(alias, s.way.Replace(column))
	}
	return result
}

// columnAlias Set an alias for the column.
// "column_name + alias_name" -> "column_name"
// "column_name + alias_name" -> "column_name AS alias_name"
func (s *TableColumn) columnAlias(column string, alias string) string {
	return SqlAlias(column, alias)
}

// Column Add table name prefix to single column name, allowing column alias to be set.
func (s *TableColumn) Column(column string, aliases ...string) string {
	return s.columnAlias(s.ColumnAll(column)[0], s.aliasName(aliases...))
}

// Sum SUM(column[, alias])
func (s *TableColumn) Sum(column string, aliases ...string) string {
	return s.columnAlias(fmt.Sprintf("SUM(%s)", s.Column(column)), s.aliasName(aliases...))
}

// Max MAX(column[, alias])
func (s *TableColumn) Max(column string, aliases ...string) string {
	return s.columnAlias(fmt.Sprintf("MAX(%s)", s.Column(column)), s.aliasName(aliases...))
}

// Min MIN(column[, alias])
func (s *TableColumn) Min(column string, aliases ...string) string {
	return s.columnAlias(fmt.Sprintf("MIN(%s)", s.Column(column)), s.aliasName(aliases...))
}

// Avg AVG(column[, alias])
func (s *TableColumn) Avg(column string, aliases ...string) string {
	return s.columnAlias(fmt.Sprintf("AVG(%s)", s.Column(column)), s.aliasName(aliases...))
}

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
	if length == 1 && counts[0] != EmptyString {
		// only set alias name
		return s.columnAlias(count, s.way.Replace(counts[0]))
	}
	// set COUNT function parameters and alias name
	countAlias := s.way.Replace(DefaultAliasNameCount)
	column := false
	for i := range length {
		if counts[i] == EmptyString {
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
func (s *TableColumn) aggregate(column string, defaultValue string, aliases ...string) string {
	return s.columnAlias(NullDefaultValue(s.Column(column), defaultValue), s.aliasName(aliases...))
}

// SUM COALESCE(SUM(column) ,0)[ AS column_alias_name]
func (s *TableColumn) SUM(column string, aliases ...string) string {
	return s.aggregate(s.Sum(column), "0", aliases...)
}

// MAX COALESCE(MAX(column) ,0)[ AS column_alias_name]
func (s *TableColumn) MAX(column string, aliases ...string) string {
	return s.aggregate(s.Max(column), "0", aliases...)
}

// MIN COALESCE(MIN(column) ,0)[ AS column_alias_name]
func (s *TableColumn) MIN(column string, aliases ...string) string {
	return s.aggregate(s.Min(column), "0", aliases...)
}

// AVG COALESCE(AVG(column) ,0)[ AS column_alias_name]
func (s *TableColumn) AVG(column string, aliases ...string) string {
	return s.aggregate(s.Avg(column), "0", aliases...)
}

func NewTableColumn(way *Way, aliases ...string) *TableColumn {
	tmp := &TableColumn{
		way: way,
	}
	tmp.alias = LastNotEmptyString(aliases)
	return tmp
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

	// Prepare Add SQL statement fragment.
	Prepare(prepare string, args ...any) SQLWindowFuncFrame

	// UnboundedPreceding Start of partition.
	UnboundedPreceding() SQLWindowFuncFrame

	// NPreceding First n rows.
	NPreceding(n int) SQLWindowFuncFrame

	// CurrentRow Current row.
	CurrentRow() SQLWindowFuncFrame

	// NFollowing After n rows.
	NFollowing(n int) SQLWindowFuncFrame

	// UnboundedFollowing End of partition.
	UnboundedFollowing() SQLWindowFuncFrame

	// Maker Custom SQL statement.
	Maker(maker Maker) SQLWindowFuncFrame
}

// sqlWindowFuncFrame Implementing the SQLWindowFuncFrame interface.
type sqlWindowFuncFrame struct {
	// frame Value is RANGE or ROWS.
	frame string

	// maker Custom SQL statement.
	maker Maker

	// prepare Frame values.
	prepare []string

	// valuesArgs
	args []any
}

func (s *sqlWindowFuncFrame) ToSQL() *SQL {
	if s.frame == EmptyString {
		return NewSQL(EmptyString)
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(s.frame)
	b.WriteString(SqlSpace)

	if maker := s.maker; maker != nil {
		if script := maker.ToSQL(); script != nil && !script.Empty() {
			b.WriteString(script.Prepare)
			script.Prepare = b.String()
			return script
		}
	}

	length := len(s.prepare)
	if length == 0 {
		return NewSQL(EmptyString)
	}
	if length == 1 {
		b.WriteString(s.prepare[0])
		return NewSQL(b.String(), s.args...)
	}
	b.WriteString(SqlBetween)
	b.WriteString(SqlSpace)
	b.WriteString(s.prepare[0])
	b.WriteString(SqlSpace)
	b.WriteString(SqlAnd)
	b.WriteString(SqlSpace)
	b.WriteString(s.prepare[1])
	return NewSQL(b.String(), s.args...)
}

func (s *sqlWindowFuncFrame) Prepare(prepare string, args ...any) SQLWindowFuncFrame {
	prepare = strings.TrimSpace(prepare)
	if prepare == EmptyString {
		return s
	}
	s.prepare = append(s.prepare, prepare)
	if args != nil {
		s.args = append(s.args, args...)
	}
	return s
}

func (s *sqlWindowFuncFrame) UnboundedPreceding() SQLWindowFuncFrame {
	return s.Prepare("UNBOUNDED PRECEDING")
}

func (s *sqlWindowFuncFrame) NPreceding(n int) SQLWindowFuncFrame {
	return s.Prepare(fmt.Sprintf("%d PRECEDING", n))
}

func (s *sqlWindowFuncFrame) CurrentRow() SQLWindowFuncFrame {
	return s.Prepare("CURRENT ROW")
}

func (s *sqlWindowFuncFrame) NFollowing(n int) SQLWindowFuncFrame {
	return s.Prepare(fmt.Sprintf("%d FOLLOWING", n))
}

func (s *sqlWindowFuncFrame) UnboundedFollowing() SQLWindowFuncFrame {
	return s.Prepare("UNBOUNDED FOLLOWING")
}

func (s *sqlWindowFuncFrame) Maker(maker Maker) SQLWindowFuncFrame {
	s.maker = maker
	return s
}

func NewSQLWindowFuncFrame(frame string) SQLWindowFuncFrame {
	return &sqlWindowFuncFrame{
		frame: frame,
	}
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

// Window Using custom function. for example: CUME_DIST(), PERCENT_RANK(), PERCENTILE_CONT(), PERCENTILE_DISC()...
func (s *WindowFunc) Window(window string, args ...any) *WindowFunc {
	s.window = NewSQL(window, args...)
	return s
}

// RowNumber ROW_NUMBER() Assign a unique serial number to each row, in the order specified, starting with 1.
func (s *WindowFunc) RowNumber() *WindowFunc {
	return s.Window("ROW_NUMBER()")
}

// Rank RANK() Assign a rank to each row, if there are duplicate values, the rank is skipped.
func (s *WindowFunc) Rank() *WindowFunc {
	return s.Window("RANK()")
}

// DenseRank DENSE_RANK() Similar to RANK(), but does not skip rankings.
func (s *WindowFunc) DenseRank() *WindowFunc {
	return s.Window("DENSE_RANK()")
}

// NTile N-TILE Divide the rows in the window into n buckets and assign a bucket number to each row.
func (s *WindowFunc) NTile(buckets int64) *WindowFunc {
	return s.Window(fmt.Sprintf("NTILE(%d)", buckets))
}

// Sum SUM() Returns the sum of all rows in the window.
func (s *WindowFunc) Sum(column string) *WindowFunc {
	return s.Window(s.way.T().Sum(column))
}

// Max MAX() Returns the maximum value within the window.
func (s *WindowFunc) Max(column string) *WindowFunc {
	return s.Window(s.way.T().Max(column))
}

// Min MIN() Returns the minimum value within the window.
func (s *WindowFunc) Min(column string) *WindowFunc {
	return s.Window(s.way.T().Min(column))
}

// Avg AVG() Returns the average of all rows in the window.
func (s *WindowFunc) Avg(column string) *WindowFunc {
	return s.Window(s.way.T().Avg(column))
}

// Count COUNT() Returns the number of rows in the window.
func (s *WindowFunc) Count(columns ...string) *WindowFunc {
	column := SqlStar
	for i := len(columns) - 1; i >= 0; i-- {
		if columns[i] != EmptyString {
			column = columns[i]
			break
		}
	}
	return s.Window(fmt.Sprintf("COUNT(%s)", column))
}

// Lag LAG() Returns the value of the row before the current row.
func (s *WindowFunc) Lag(column string, offset int64, defaultValue any) *WindowFunc {
	return s.Window(fmt.Sprintf("LAG(%s, %d, %s)", s.way.Replace(column), offset, argValueToString(defaultValue)))
}

// Lead LEAD() Returns the value of a row after the current row.
func (s *WindowFunc) Lead(column string, offset int64, defaultValue any) *WindowFunc {
	return s.Window(fmt.Sprintf("LEAD(%s, %d, %s)", s.way.Replace(column), offset, argValueToString(defaultValue)))
}

// NthValue NTH_VALUE() The Nth value can be returned according to the specified order. This is very useful when you need to get data at a specific position.
func (s *WindowFunc) NthValue(column string, lineNumber int64) *WindowFunc {
	return s.Window(fmt.Sprintf("NTH_VALUE(%s, %d)", s.way.Replace(column), lineNumber))
}

// FirstValue FIRST_VALUE() Returns the value of the first row in the window.
func (s *WindowFunc) FirstValue(column string) *WindowFunc {
	return s.Window(fmt.Sprintf("FIRST_VALUE(%s)", s.way.Replace(column)))
}

// LastValue LAST_VALUE() Returns the value of the last row in the window.
func (s *WindowFunc) LastValue(column string) *WindowFunc {
	return s.Window(fmt.Sprintf("LAST_VALUE(%s)", s.way.Replace(column)))
}

// Partition The OVER clause defines window partitions so that the window function is calculated independently in each partition.
func (s *WindowFunc) Partition(column ...string) *WindowFunc {
	s.partition = append(s.partition, s.way.Replaces(column)...)
	return s
}

// Asc Define the sorting within the partition so that the window function is calculated in order.
func (s *WindowFunc) Asc(column string) *WindowFunc {
	s.order = append(s.order, fmt.Sprintf("%s %s", s.way.Replace(column), SqlAsc))
	return s
}

// Desc Define the sorting within the partition so that the window function is calculated in descending order.
func (s *WindowFunc) Desc(column string) *WindowFunc {
	s.order = append(s.order, fmt.Sprintf("%s %s", s.way.Replace(column), SqlDesc))
	return s
}

// Range Define the window based on the physical row number, accurately control the number of rows (such as the first 2 rows and the last 3 rows).
func (s *WindowFunc) Range(fc func(frame SQLWindowFuncFrame)) *WindowFunc {
	if fc != nil {
		frame := NewSQLWindowFuncFrame(SqlRange)
		fc(frame)
		if tmp := frame.ToSQL(); tmp != nil && !tmp.Empty() {
			s.frameRange, s.frameRows = tmp, nil
		}
	}
	return s
}

// Rows Defines a window based on a range of values, including all rows with the same ORDER BY column value; suitable for handling scenarios with equal values (such as time ranges).
func (s *WindowFunc) Rows(fc func(frame SQLWindowFuncFrame)) *WindowFunc {
	if fc != nil {
		frame := NewSQLWindowFuncFrame(SqlRows)
		fc(frame)
		if tmp := frame.ToSQL(); tmp != nil && !tmp.Empty() {
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
	result, window := NewSQL(EmptyString), s.window
	if window == nil || window.Empty() {
		return result
	}

	b := getStringBuilder()
	defer putStringBuilder(b)

	b.WriteString(window.Prepare)
	result.Args = append(result.Args, window.Args...)
	b.WriteString(ConcatString(SqlSpace, SqlOver, SqlSpace, SqlLeftSmallBracket))

	num := 0
	if len(s.partition) > 0 {
		num++
		b.WriteString(ConcatString(SqlSpace, SqlPartitionBy, SqlSpace))
		b.WriteString(strings.Join(s.partition, SqlConcat))
	}
	if len(s.order) > 0 {
		num++
		b.WriteString(ConcatString(SqlSpace, SqlOrderBy, SqlSpace))
		b.WriteString(strings.Join(s.order, SqlConcat))
	}
	if frame := s.frameRange; frame != nil {
		if tmp := frame.ToSQL(); tmp != nil && !tmp.Empty() {
			num++
			b.WriteString(ConcatString(SqlSpace, tmp.Prepare))
			result.Args = append(result.Args, tmp.Args...)
		}
	}
	if frame := s.frameRows; frame != nil {
		if tmp := frame.ToSQL(); tmp != nil && !tmp.Empty() {
			num++
			b.WriteString(ConcatString(SqlSpace, tmp.Prepare))
			result.Args = append(result.Args, tmp.Args...)
		}
	}

	if num > 0 {
		b.WriteString(SqlSpace)
	}
	b.WriteString(SqlRightSmallBracket)
	if s.alias != EmptyString {
		b.WriteString(ConcatString(SqlSpace, SqlAs, SqlSpace))
		b.WriteString(s.way.Replace(s.alias))
	}
	result.Prepare = b.String()
	return result
}

func NewWindowFunc(way *Way, aliases ...string) *WindowFunc {
	return &WindowFunc{
		way:   way,
		alias: LastNotEmptyString(aliases),
	}
}
