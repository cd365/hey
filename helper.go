// Quickly build INSERT, DELETE, UPDATE, SELECT statements.
// Also allows you to call them to get the corresponding results.

package hey

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
)

const (
	Id = "id"
)

const (
	State0   = 0
	State1   = 1
	StateNo  = "NO"
	StateYes = "YES"
	StateOn  = "ON"
	StateOff = "OFF"
)

var (
	ErrNoAffectedRows = errors.New("hey: there are no affected rows")
)

var (
	// builder sql builder pool
	builder *sync.Pool

	// insertByStructPool insert with struct{}, *struct{}, []struct, []*struct{}, *[]struct{}, *[]*struct{}
	insertByStructPool *sync.Pool
)

// init initialize
func init() {
	builder = &sync.Pool{}
	builder.New = func() interface{} {
		return &strings.Builder{}
	}

	insertByStructPool = &sync.Pool{}
	insertByStructPool.New = func() interface{} {
		return &insertByStruct{}
	}
}

// getBuilder get sql builder from pool
func getBuilder() *strings.Builder {
	return builder.Get().(*strings.Builder)
}

// putBuilder put sql builder in the pool
func putBuilder(b *strings.Builder) {
	b.Reset()
	builder.Put(b)
}

// getInsertByStruct get *insertByStruct from pool
func getInsertByStruct() *insertByStruct {
	tmp := insertByStructPool.Get().(*insertByStruct)
	tmp.allow = make(map[string]*struct{})
	tmp.except = make(map[string]*struct{})
	tmp.used = make(map[string]*struct{})
	return tmp
}

// putInsertByStruct put *insertByStruct in the pool
func putInsertByStruct(b *insertByStruct) {
	b.tag = EmptyString
	b.allow = nil
	b.except = nil
	b.used = nil
	b.structReflectType = nil
	insertByStructPool.Put(b)
}

// MustAffectedRows at least one row is affected
func MustAffectedRows(affectedRows int64, err error) error {
	if err != nil {
		return err
	}
	if affectedRows <= 0 {
		return ErrNoAffectedRows
	}
	return nil
}

// LastNotEmptyString get last not empty string, return empty string if it does not exist
func LastNotEmptyString(sss []string) string {
	for i := len(sss) - 1; i >= 0; i-- {
		if sss[i] != EmptyString {
			return sss[i]
		}
	}
	return EmptyString
}

// RemoveDuplicate remove duplicate element
func RemoveDuplicate(dynamic ...interface{}) (result []interface{}) {
	mp, ok, length := make(map[interface{}]*struct{}), false, len(dynamic)
	result = make([]interface{}, 0, length)
	for i := 0; i < length; i++ {
		if _, ok = mp[dynamic[i]]; ok {
			continue
		}
		mp[dynamic[i]] = &struct{}{}
		result = append(result, dynamic[i])
	}
	return
}

// RemoveDuplicates remove duplicate element
func RemoveDuplicates[T comparable](
	dynamic ...T,
) (result []T) {
	mp, ok, length := make(map[T]*struct{}), false, len(dynamic)
	result = make([]T, 0, length)
	for i := 0; i < length; i++ {
		if _, ok = mp[dynamic[i]]; ok {
			continue
		}
		mp[dynamic[i]] = &struct{}{}
		result = append(result, dynamic[i])
	}
	return
}

// bindStruct bind the receiving object with the query result.
type bindStruct struct {
	// store root struct properties
	direct map[string]int

	// store non-root struct properties, such as: anonymous attribute structure and named attribute structure
	indirect map[string][]int

	// all used struct types, including the root struct
	structType map[reflect.Type]struct{}
}

func bindStructInit() *bindStruct {
	return &bindStruct{
		direct:     make(map[string]int),
		indirect:   make(map[string][]int),
		structType: make(map[reflect.Type]struct{}),
	}
}

// binding Match the binding according to the structure "db" tag and the query column name.
// Please ensure that the type of refStructType must be `reflect.Struct`.
func (s *bindStruct) binding(refStructType reflect.Type, depth []int, tag string) {
	if _, ok := s.structType[refStructType]; ok {
		// prevent structure loop nesting
		return
	}
	s.structType[refStructType] = struct{}{}
	length := refStructType.NumField()
	for i := 0; i < length; i++ {
		attribute := refStructType.Field(i)
		if !attribute.IsExported() {
			continue
		}
		// anonymous structure, or named structure
		if attribute.Type.Kind() == reflect.Struct ||
			(attribute.Type.Kind() == reflect.Ptr && attribute.Type.Elem().Kind() == reflect.Struct) {
			at := attribute.Type
			kind := at.Kind()
			if kind == reflect.Ptr {
				at = at.Elem()
				kind = at.Kind()
			}
			if kind == reflect.Struct {
				dst := depth[:]
				dst = append(dst, i)
				// recursive call
				s.binding(at, dst, tag)
			}
			continue
		}
		field := attribute.Tag.Get(tag)
		if field == EmptyString || field == "-" {
			continue
		}
		// root structure attribute
		if depth == nil {
			s.direct[field] = i
			continue
		}
		// others structure attribute, nested anonymous structure or named structure
		if _, ok := s.indirect[field]; !ok {
			dst := depth[:]
			dst = append(dst, i)
			s.indirect[field] = dst
		}
	}
}

// prepare The preparatory work before executing rows.Scan
// find the pointer of the corresponding field from the reflection value of the receiving object, and bind it.
// When nesting structures, it is recommended to use structure value nesting to prevent null pointers that may appear
// when the root structure accesses the properties of substructures, resulting in panic.
func (s *bindStruct) prepare(columns []string, rowsScan []interface{}, indirect reflect.Value, length int) error {
	for i := 0; i < length; i++ {
		index, ok := s.direct[columns[i]]
		if ok {
			// top structure
			field := indirect.Field(index)
			if !field.CanAddr() || !field.CanSet() {
				return fmt.Errorf("hey: column `%s` cann't set value", columns[i])
			}
			if field.Kind() == reflect.Ptr && field.IsNil() {
				indirect.Field(index).Set(reflect.New(field.Type()).Elem())
				rowsScan[i] = indirect.Field(index).Addr().Interface()
				continue
			}
			rowsScan[i] = field.Addr().Interface()
			continue
		}
		// parsing multi-layer structures
		line, ok := s.indirect[columns[i]]
		if !ok {
			// unable to find mapping property for current field Use *[]byte instead to receive
			rowsScan[i] = new([]byte)
			continue
		}
		count := len(line)
		if count < 2 {
			return fmt.Errorf("hey: unable to determine field `%s` mapping", columns[i])
		}
		cursor := make([]reflect.Value, count)
		cursor[0] = indirect
		for j := 0; j < count; j++ {
			parent := cursor[j]
			if j+1 < count {
				// middle layer structures
				latest := parent.Field(line[j])
				if latest.Type().Kind() == reflect.Ptr {
					if latest.IsNil() {
						parent.Field(line[j]).Set(reflect.New(latest.Type().Elem()))
						latest = parent.Field(line[j])
					}
					latest = latest.Elem()
				}
				cursor[j+1] = latest
				continue
			}
			// j + 1 == count
			// innermost structure
			field := parent.Field(line[j])
			if !field.CanAddr() || !field.CanSet() {
				return fmt.Errorf("hey: column `%s` cann't set value, multi-level", columns[i])
			}
			if field.Kind() == reflect.Ptr && field.IsNil() {
				parent.Field(line[j]).Set(reflect.New(field.Type()).Elem())
				rowsScan[i] = parent.Field(line[j]).Addr().Interface()
				continue
			}
			rowsScan[i] = field.Addr().Interface()
		}
	}
	return nil
}

// ScanSliceStruct Scan the query result set into the receiving object
// the receiving object type is *[]AnyStruct or *[]*AnyStruct.
func ScanSliceStruct(rows *sql.Rows, result interface{}, tag string) error {
	typeOf, valueOf := reflect.TypeOf(result), reflect.ValueOf(result)
	typeOfKind := typeOf.Kind()
	if typeOfKind != reflect.Ptr || typeOf.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("hey: the receiving parameter needs to be a slice pointer, yours is `%s`", typeOf.String())
	}
	var elemType reflect.Type
	setValues := valueOf.Elem()
	elemTypeIsPtr := false
	elem := typeOf.Elem().Elem()
	elemKind := elem.Kind()
	if elemKind == reflect.Struct {
		// *[]AnyStruct
		elemType = elem
	}
	if elemKind == reflect.Ptr {
		if elem.Elem().Kind() == reflect.Struct {
			// *[]*AnyStruct
			elemType = elem.Elem()
			elemTypeIsPtr = true
		}
	}
	if elemType == nil {
		return fmt.Errorf(
			"hey: slice elements need to be structures or pointers to structures, yours is `%s`",
			elem.String(),
		)
	}
	b := bindStructInit()
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	// elemType => struct{}
	b.binding(elemType, nil, tag)
	length := len(columns)
	rowsScan := make([]interface{}, length)
	for rows.Next() {
		object := reflect.New(elemType)
		indirect := reflect.Indirect(object)
		if err = b.prepare(columns, rowsScan, indirect, length); err != nil {
			return err
		}
		if err = rows.Scan(rowsScan...); err != nil {
			return err
		}
		if elemTypeIsPtr {
			setValues = reflect.Append(setValues, object)
			continue
		}
		setValues = reflect.Append(setValues, object.Elem())
	}
	valueOf.Elem().Set(setValues)
	return nil
}

func BasicTypeValue(value interface{}) interface{} {
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
	// struct tag name value used as table.column name
	tag string

	// only allowed fields Hash table
	allow map[string]*struct{}

	// ignored fields Hash table
	except map[string]*struct{}

	// already existing fields Hash table
	used map[string]*struct{}

	// struct reflect type, make sure it is the same structure type
	structReflectType reflect.Type
}

// setExcept filter the list of unwanted fields, prioritize method calls structFieldsValues() and structValues()
func (s *insertByStruct) setExcept(except []string) {
	for _, field := range except {
		s.except[field] = &struct{}{}
	}
}

// setAllow only allowed fields, prioritize method calls structFieldsValues() and structValues()
func (s *insertByStruct) setAllow(allow []string) {
	for _, field := range allow {
		s.allow[field] = &struct{}{}
	}
}

func panicBatchInsertByStruct() {
	panic("hey: slice element types are inconsistent")
}

// structFieldsValues checkout fields, values
func (s *insertByStruct) structFieldsValues(structReflectValue reflect.Value, allowed bool) (fields []string, values []interface{}) {
	reflectType := structReflectValue.Type()
	if s.structReflectType == nil {
		s.structReflectType = reflectType
	}
	if s.structReflectType != reflectType {
		panicBatchInsertByStruct()
	}
	length := reflectType.NumField()
	for i := 0; i < length; i++ {
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
		values = append(values, BasicTypeValue(valueIndexField.Interface()))
	}
	return
}

// structValues checkout values
func (s *insertByStruct) structValues(structReflectValue reflect.Value, allowed bool) (values []interface{}) {
	reflectType := structReflectValue.Type()
	if s.structReflectType != nil && s.structReflectType != reflectType {
		panicBatchInsertByStruct()
	}
	length := reflectType.NumField()
	for i := 0; i < length; i++ {
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

		values = append(values, BasicTypeValue(valueIndexField.Interface()))
	}
	return
}

// Insert object should be one of struct{}, *struct{}, []struct, []*struct{}, *[]struct{}, *[]*struct{}
func (s *insertByStruct) Insert(object interface{}, tag string, except []string, allow []string) (fields []string, values [][]interface{}) {
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
		values = make([][]interface{}, 1)
		fields, values[0] = s.structFieldsValues(reflectValue, allowed)
	}

	if kind == reflect.Slice {
		sliceLength := reflectValue.Len()
		values = make([][]interface{}, sliceLength)
		for i := 0; i < sliceLength; i++ {
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

// StructInsert object should be one of struct{}, *struct{}, []struct, []*struct{}, *[]struct{}, *[]*struct{}
// get fields and values based on struct tag.
func StructInsert(object interface{}, tag string, except []string, allow []string) (fields []string, values [][]interface{}) {
	b := getInsertByStruct()
	defer putInsertByStruct(b)
	fields, values = b.Insert(object, tag, except, allow)
	return
}

// StructModify object should be one of struct{}, *struct{} get the fields and values that need to be modified
func StructModify(object interface{}, tag string, except ...string) (fields []string, values []interface{}) {
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
	excepted := make(map[string]struct{})
	for _, field := range except {
		excepted[field] = struct{}{}
	}

	length := ofType.NumField()

	exists := make(map[string]struct{}, length)
	fields = make([]string, 0, length)
	values = make([]interface{}, 0, length)

	last := 0
	fieldsIndex := make(map[string]int)

	add := func(field string, value interface{}) {
		if _, ok := exists[field]; ok {
			values[fieldsIndex[field]] = value
			return
		}
		exists[field] = struct{}{}
		fields = append(fields, field)
		values = append(values, value)
		fieldsIndex[field] = last
		last++
	}

	for i := 0; i < length; i++ {
		field := ofType.Field(i)

		column := field.Tag.Get(tag)
		if column == EmptyString || column == "-" {
			continue
		}
		if _, ok := excepted[column]; ok {
			continue
		}

		fieldType := field.Type
		fieldKind := fieldType.Kind()
		pointerDepth := 0
		for fieldKind == reflect.Pointer {
			pointerDepth++
			fieldType = fieldType.Elem()
			fieldKind = fieldType.Kind()
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

// StructObtain object should be one of struct{}, *struct{} for get all fields and values
func StructObtain(object interface{}, tag string, except ...string) (fields []string, values []interface{}) {
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
	excepted := make(map[string]struct{})
	for _, field := range except {
		excepted[field] = struct{}{}
	}

	length := ofType.NumField()

	exists := make(map[string]struct{}, length)
	fields = make([]string, 0, length)
	values = make([]interface{}, 0, length)

	last := 0
	fieldsIndex := make(map[string]int)

	add := func(field string, value interface{}) {
		if _, ok := exists[field]; ok {
			values[fieldsIndex[field]] = value
			return
		}
		exists[field] = struct{}{}
		fields = append(fields, field)
		values = append(values, value)
		fieldsIndex[field] = last
		last++
	}

	for i := 0; i < length; i++ {
		field := ofType.Field(i)

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

// StructUpdate compare origin and latest for update
func StructUpdate(origin interface{}, latest interface{}, tag string, except ...string) (fields []string, values []interface{}) {
	if origin == nil || latest == nil || tag == EmptyString {
		return
	}

	originFields, originValues := StructObtain(origin, tag, except...)
	latestFields, latestValues := StructModify(latest, tag, except...)

	storage := make(map[string]interface{}, len(originFields))
	for k, v := range originFields {
		storage[v] = originValues[k]
	}

	exists := make(map[string]struct{})
	fields = make([]string, 0)
	values = make([]interface{}, 0)

	last := 0
	fieldsIndex := make(map[string]int)

	add := func(field string, value interface{}) {
		if _, ok := exists[field]; ok {
			values[fieldsIndex[field]] = value
			return
		}
		exists[field] = struct{}{}
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
		bvo, bvl := BasicTypeValue(storage[v]), BasicTypeValue(latestValues[k])
		if reflect.DeepEqual(bvo, bvl) {
			continue
		}
		add(v, bvl)
	}

	return
}

// RemoveSliceMemberByIndex delete slice member by index
func RemoveSliceMemberByIndex[T interface{}](indexList []int, elementList []T) []T {
	count := len(indexList)
	if count == 0 {
		return elementList
	}
	length := len(elementList)
	mp := make(map[int]*struct{}, count)
	for i := 0; i < count; i++ {
		mp[indexList[i]] = &struct{}{}
	}
	ok := false
	result := make([]T, 0, length)
	for i := 0; i < length; i++ {
		if _, ok = mp[i]; !ok {
			result = append(result, elementList[i])
		}
	}
	return result
}

// ConcatString concatenate string
func ConcatString(sss ...string) string {
	b := getBuilder()
	defer putBuilder(b)
	length := len(sss)
	for i := 0; i < length; i++ {
		b.WriteString(sss[i])
	}
	return b.String()
}

// SqlAlias sql alias name
func SqlAlias(name string, alias string) string {
	if alias == EmptyString {
		return name
	}
	return fmt.Sprintf("%s %s %s", name, SqlAs, alias)
}

// SqlPrefix sql prefix name
func SqlPrefix(prefix string, name string) string {
	if prefix == EmptyString {
		return name
	}
	return fmt.Sprintf("%s%s%s", prefix, SqlPoint, name)
}

// EvenSlice2Map even slice to map
func EvenSlice2Map[K comparable](elems ...K) map[K]K {
	length := len(elems)
	if length&1 == 1 {
		return make(map[K]K)
	}
	result := make(map[K]K, length/2)
	for i := 0; i < length; i += 2 {
		elems[i] = elems[i+1]
	}
	return result
}

// Slice2MapNewKey make map by slice, create key
func Slice2MapNewKey[K comparable](elems []K, createKey func(v K) K) map[K]K {
	length := len(elems)
	result := make(map[K]K, length)
	for i := 0; i < length; i++ {
		result[createKey(elems[i])] = elems[i]
	}
	return result
}

// Slice2MapNewVal make map by slice, create value
func Slice2MapNewVal[K comparable, V interface{}](elems []K, createValue func(v K) V) map[K]V {
	length := len(elems)
	result := make(map[K]V, length)
	for i := 0; i < length; i++ {
		result[elems[i]] = createValue(elems[i])
	}
	return result
}

// SliceMatchMap use the `key` value of each element in `elems` to match in the map, and call `handle` if the match is successful
func SliceMatchMap[K comparable, X interface{}, Y interface{}](kx map[K]X, handle func(x X, y Y), key func(y Y) K, elems []Y) {
	for i := len(elems) - 1; i >= 0; i-- {
		if tmp, ok := kx[key(elems[i])]; ok {
			handle(tmp, elems[i])
		}
	}
}

// SliceIter slice iteration
func SliceIter[V interface{}](iter func(v V) V, elems []V) []V {
	for i := len(elems) - 1; i >= 0; i-- {
		elems[i] = iter(elems[i])
	}
	return elems
}

// MergeMap merge multiple maps
func MergeMap[K comparable, V interface{}](elems ...map[K]V) map[K]V {
	length := len(elems)
	result := make(map[K]V)
	for i := 0; i < length; i++ {
		if i == 0 {
			result = elems[i]
			continue
		}
		for k, v := range elems[i] {
			result[k] = v
		}
	}
	return result
}

// MergeSlice merge multiple slices
func MergeSlice[V interface{}](elems ...[]V) []V {
	length := len(elems)
	result := make([]V, 0)
	for i := 0; i < length; i++ {
		if i == 0 {
			result = elems[i]
			continue
		}
		result = append(result, elems[i]...)
	}
	return result
}

// sqlReturning build RETURNING statement, the contents of the RETURNING clause are the same as the output list of the SELECT command.
func sqlReturning(cmd Commander, returning ...string) (prepare string, args []interface{}) {
	prepare, args = cmd.SQL()
	b := getBuilder()
	defer putBuilder(b)
	b.WriteString(prepare)
	b.WriteString(" RETURNING ")
	tmp := strings.Join(returning, ", ")
	if tmp == EmptyString {
		tmp = "*"
	}
	b.WriteString(tmp)
	prepare = b.String()
	return
}

// schema used to store basic information such as context.Context, *Way, SQL comment, table name.
type schema struct {
	ctx     context.Context
	way     *Way
	comment string
	table   string
}

// newSchema new schema with *Way
func newSchema(way *Way) *schema {
	return &schema{
		ctx: context.Background(),
		way: way,
	}
}

// comment make SQL statement builder, SqlPlaceholder "?" should not appear in comments
// defer putBuilder(builder) should be called immediately after calling the current method
func comment(schema *schema) (b *strings.Builder) {
	b = getBuilder()
	schema.comment = strings.TrimSpace(schema.comment)
	schema.comment = strings.ReplaceAll(schema.comment, SqlPlaceholder, EmptyString)
	if schema.comment == EmptyString {
		return
	}
	b.WriteString("/* ")
	b.WriteString(schema.comment)
	b.WriteString(" */")
	return
}

// Del for DELETE
type Del struct {
	schema *schema
	where  Filter
}

// NewDel for DELETE
func NewDel(way *Way) *Del {
	return &Del{
		schema: newSchema(way),
	}
}

// Comment set comment
func (s *Del) Comment(comment string) *Del {
	s.schema.comment = comment
	return s
}

// Context set context
func (s *Del) Context(ctx context.Context) *Del {
	s.schema.ctx = ctx
	return s
}

// Table set table name
func (s *Del) Table(table string) *Del {
	s.schema.table = table
	return s
}

// Where set where
func (s *Del) Where(where Filter) *Del {
	s.where = where
	return s
}

// SQL build SQL statement
func (s *Del) SQL() (prepare string, args []interface{}) {
	if s.schema.table == EmptyString {
		return
	}
	buf := comment(s.schema)
	defer putBuilder(buf)
	buf.WriteString("DELETE FROM ")
	buf.WriteString(s.schema.table)

	config := s.schema.way.config
	if config != nil && config.DeleteMustUseWhere && (s.where == nil || s.where.IsEmpty()) {
		prepare, args = EmptyString, nil
		return
	}

	if s.where != nil && !s.where.IsEmpty() {
		where, whereArgs := s.where.SQL()
		buf.WriteString(" WHERE ")
		buf.WriteString(where)
		args = whereArgs
	}

	prepare = buf.String()
	return
}

// Del execute the built SQL statement
func (s *Del) Del() (int64, error) {
	prepare, args := s.SQL()
	return s.schema.way.ExecContext(s.schema.ctx, prepare, args...)
}

// Way get current *Way
func (s *Del) Way() *Way {
	return s.schema.way
}

// F make new Filter
func (s *Del) F(filter ...Filter) Filter {
	return F().Filter(filter...)
}

// Add for INSERT
type Add struct {
	schema *schema

	/* insert one or more rows */
	exceptMap map[string]struct{}
	except    []string

	fieldsIndex int
	fieldsMap   map[string]int
	fields      []string
	values      [][]interface{}

	// subQuery INSERT VALUES is query statement
	subQuery *SubQuery

	// onConflict, on conflict do something
	onConflict     *string
	onConflictArgs []interface{}

	useResult func(result sql.Result) error
}

// NewAdd for INSERT
func NewAdd(way *Way) *Add {
	add := &Add{
		schema:    newSchema(way),
		exceptMap: make(map[string]struct{}),
		fieldsMap: make(map[string]int),
		fields:    make([]string, 0),
		values:    make([][]interface{}, 1),
	}
	return add
}

// Comment set comment
func (s *Add) Comment(comment string) *Add {
	s.schema.comment = comment
	return s
}

// Context set context
func (s *Add) Context(ctx context.Context) *Add {
	s.schema.ctx = ctx
	return s
}

// Table set table name
func (s *Add) Table(table string) *Add {
	s.schema.table = table
	return s
}

// Fields set fields, the current method resets the map field list and field index sequence
func (s *Add) Fields(fields []string) *Add {
	length := len(fields)
	// reset value of fieldsIndex, fieldsMap
	s.fieldsIndex, s.fieldsMap = 0, make(map[string]int, length*2)
	for i := 0; i < length; i++ {
		if _, ok := s.fieldsMap[fields[i]]; !ok {
			s.fieldsMap[fields[i]] = s.fieldsIndex
			s.fieldsIndex++
		}
	}
	s.fields = fields
	return s
}

// Values set values
func (s *Add) Values(values [][]interface{}) *Add {
	s.values = values
	return s
}

// FieldsValues set fields and values
func (s *Add) FieldsValues(fields []string, values [][]interface{}) *Add {
	return s.Fields(fields).Values(values)
}

// Except exclude some columns from insert one or more rows
func (s *Add) Except(except ...string) *Add {
	for _, field := range except {
		if field == EmptyString {
			continue
		}
		if _, ok := s.exceptMap[field]; ok {
			continue
		}
		s.exceptMap[field] = struct{}{}
		s.except = append(s.except, field)
	}
	return s
}

// FieldValue append field-value for insert one or more rows
func (s *Add) FieldValue(field string, value interface{}) *Add {
	if _, ok := s.exceptMap[field]; ok {
		return s
	}
	if index, ok := s.fieldsMap[field]; ok {
		for i := range s.values {
			s.values[i][index] = value
		}
		return s
	}
	s.fieldsMap[field] = s.fieldsIndex
	s.fieldsIndex++
	s.fields = append(s.fields, field)
	for i := range s.values {
		s.values[i] = append(s.values[i], value)
	}
	return s
}

// DefaultFieldValue append default field-value for insert one or more rows
func (s *Add) DefaultFieldValue(field string, value interface{}) *Add {
	if _, ok := s.exceptMap[field]; ok {
		return s
	}
	if _, ok := s.fieldsMap[field]; ok {
		return s
	}
	s.fieldsMap[field] = s.fieldsIndex
	s.fieldsIndex++
	s.fields = append(s.fields, field)
	for i := range s.values {
		s.values[i] = append(s.values[i], value)
	}
	return s
}

// Create value of create should be one of struct{}, *struct{}, map[string]interface{},
// []struct, []*struct{}, *[]struct{}, *[]*struct{}
func (s *Add) Create(create interface{}) *Add {
	if fieldValue, ok := create.(map[string]interface{}); ok {
		allow := make(map[string]*struct{})
		for _, field := range s.fields {
			allow[field] = &struct{}{}
		}
		allowed := len(allow) > 0
		for field, value := range fieldValue {
			if allowed {
				if _, ok := allow[field]; !ok {
					continue
				}
			}
			s.FieldValue(field, value)
		}
		return s
	}

	return s.FieldsValues(StructInsert(create, s.schema.way.tag, s.except, s.fields))
}

// ValuesSubQuery values is a query SQL statement
func (s *Add) ValuesSubQuery(prepare string, args ...interface{}) *Add {
	if prepare == EmptyString {
		return s
	}
	s.subQuery = NewSubQuery(prepare, args...)
	return s
}

// ValuesSubQueryGet values is a query SQL statement
func (s *Add) ValuesSubQueryGet(get *Get) *Add {
	if get == nil {
		return s
	}
	prepare, args := get.SQL()
	return s.ValuesSubQuery(prepare, args...)
}

// OnConflict on conflict do something
func (s *Add) OnConflict(prepare string, args []interface{}) *Add {
	s.onConflict = &prepare
	s.onConflictArgs = args
	return s
}

// UseResult provide sql.Result object
func (s *Add) UseResult(fc func(result sql.Result) error) *Add {
	if fc != nil {
		s.useResult = fc
	}
	return s
}

// SQL build SQL statement
func (s *Add) SQL() (prepare string, args []interface{}) {
	if s.schema.table == EmptyString {
		return
	}

	buf := comment(s.schema)
	defer putBuilder(buf)

	if s.subQuery != nil {
		buf.WriteString("INSERT INTO ")
		buf.WriteString(s.schema.table)
		if len(s.fields) > 0 {
			buf.WriteString(" ( ")
			buf.WriteString(strings.Join(s.fields, ", "))
			buf.WriteString(" )")
		}
		buf.WriteString(SqlSpace)
		subPrepare, subArgs := s.subQuery.SQL()
		buf.WriteString(subPrepare)
		prepare = buf.String()
		args = subArgs
		return
	}

	count := len(s.fields)
	if count == 0 {
		return
	}

	tmpList := make([]string, count)
	for i := 0; i < count; i++ {
		tmpList[i] = SqlPlaceholder
	}
	value := fmt.Sprintf("( %s )", strings.Join(tmpList, ", "))

	length := len(s.values)
	values := make([]string, length)
	for i := 0; i < length; i++ {
		args = append(args, s.values[i]...)
		values[i] = value
	}

	buf.WriteString("INSERT INTO ")
	buf.WriteString(s.schema.table)
	buf.WriteString(" ( ")
	buf.WriteString(strings.Join(s.fields, ", "))
	buf.WriteString(" ) VALUES ")
	buf.WriteString(strings.Join(values, ", "))
	if s.onConflict != nil && *s.onConflict != EmptyString {
		buf.WriteString(SqlSpace)
		buf.WriteString(*s.onConflict)
		args = append(args, s.onConflictArgs...)
	}
	prepare = buf.String()
	return
}

// Add execute the built SQL statement
func (s *Add) Add() (int64, error) {
	prepare, args := s.SQL()
	if s.useResult == nil {
		return s.schema.way.ExecContext(s.schema.ctx, prepare, args...)
	}
	result, err := s.schema.way.ExecuteContext(s.schema.ctx, prepare, args...)
	if err != nil {
		return 0, err
	}
	if err = s.useResult(result); err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// Returning insert a row and return the data
func (s *Add) Returning(fc func(row *sql.Row) error, returning ...string) error {
	prepare, args := sqlReturning(s, returning...)
	return s.schema.way.QueryRowContext(s.schema.ctx, fc, prepare, args...)
}

// Way get current *Way
func (s *Add) Way() *Way {
	return s.schema.way
}

// modify set the field to be updated
type modify struct {
	expr string
	args []interface{}
}

// Mod for UPDATE
type Mod struct {
	schema *schema

	// update main updated fields
	update map[string]*modify

	// updateSlice, the fields to be updated are updated sequentially
	updateSlice []string

	// modify secondary updated fields, the effective condition is len(update) > 0
	secondaryUpdate map[string]*modify

	// secondaryUpdateSlice, the fields to be updated are updated sequentially
	secondaryUpdateSlice []string

	// except excepted fields
	except      map[string]struct{}
	exceptSlice []string

	where Filter
}

// NewMod for UPDATE
func NewMod(way *Way) *Mod {
	return &Mod{
		schema:          newSchema(way),
		update:          make(map[string]*modify),
		secondaryUpdate: make(map[string]*modify),
		except:          make(map[string]struct{}),
	}
}

// Comment set comment
func (s *Mod) Comment(comment string) *Mod {
	s.schema.comment = comment
	return s
}

// Context set context
func (s *Mod) Context(ctx context.Context) *Mod {
	s.schema.ctx = ctx
	return s
}

// Table set table name
func (s *Mod) Table(table string) *Mod {
	s.schema.table = table
	return s
}

// Except exclude some fields from update
func (s *Mod) Except(except ...string) *Mod {
	length := len(except)
	for i := 0; i < length; i++ {
		if except[i] == EmptyString {
			continue
		}
		if _, ok := s.except[except[i]]; ok {
			continue
		}
		s.except[except[i]] = struct{}{}
		s.exceptSlice = append(s.exceptSlice, except[i])
	}
	return s
}

// expr build update field expressions and field values
func (s *Mod) expr(field string, expr string, args ...interface{}) *Mod {
	if _, ok := s.except[field]; ok {
		return s
	}
	tmp := &modify{
		expr: expr,
		args: args,
	}
	if _, ok := s.update[field]; ok {
		s.update[field] = tmp
		return s
	}
	s.updateSlice = append(s.updateSlice, field)
	s.update[field] = tmp
	return s
}

// Expr update field using custom expr
func (s *Mod) Expr(field string, expr string, args ...interface{}) *Mod {
	field, expr = strings.TrimSpace(field), strings.TrimSpace(expr)
	return s.expr(field, expr, args)
}

// Set field = value
func (s *Mod) Set(field string, value interface{}) *Mod {
	return s.expr(field, fmt.Sprintf("%s = %s", field, SqlPlaceholder), value)
}

// Incr SET field = field + value
func (s *Mod) Incr(field string, value interface{}) *Mod {
	return s.expr(field, fmt.Sprintf("%s = %s + %s", field, field, SqlPlaceholder), value)
}

// Decr SET field = field - value
func (s *Mod) Decr(field string, value interface{}) *Mod {
	return s.expr(field, fmt.Sprintf("%s = %s - %s", field, field, SqlPlaceholder), value)
}

// FieldsValues SET field = value by slice, require len(fields) == len(values)
func (s *Mod) FieldsValues(fields []string, values []interface{}) *Mod {
	len1, len2 := len(fields), len(values)
	if len1 != len2 {
		return s
	}
	for i := 0; i < len1; i++ {
		s.Set(fields[i], values[i])
	}
	return s
}

// Modify value of modify should be one of struct{}, *struct{}, map[string]interface{}
func (s *Mod) Modify(modify interface{}) *Mod {
	if fieldValue, ok := modify.(map[string]interface{}); ok {
		for field, value := range fieldValue {
			s.Set(field, value)
		}
		return s
	}
	return s.FieldsValues(StructModify(modify, s.schema.way.tag, s.exceptSlice...))
}

// Update for compare origin and latest to automatically calculate need to update fields
func (s *Mod) Update(originObject interface{}, latestObject interface{}) *Mod {
	return s.FieldsValues(StructUpdate(originObject, latestObject, s.schema.way.tag, s.exceptSlice...))
}

// defaultExpr append the update field collection when there is at least one item in the update field collection, for example, set the update timestamp
func (s *Mod) defaultExpr(field string, expr string, args ...interface{}) *Mod {
	if _, ok := s.except[field]; ok {
		return s
	}
	if _, ok := s.update[field]; ok {
		return s
	}
	tmp := &modify{
		expr: expr,
		args: args,
	}
	if _, ok := s.secondaryUpdate[field]; ok {
		s.secondaryUpdate[field] = tmp
		return s
	}
	s.secondaryUpdateSlice = append(s.secondaryUpdateSlice, field)
	s.secondaryUpdate[field] = tmp
	return s
}

// DefaultExpr update field using custom expression
func (s *Mod) DefaultExpr(field string, expr string, args ...interface{}) *Mod {
	field, expr = strings.TrimSpace(field), strings.TrimSpace(expr)
	return s.defaultExpr(field, expr, args)
}

// DefaultSet SET field = value
func (s *Mod) DefaultSet(field string, value interface{}) *Mod {
	return s.defaultExpr(field, fmt.Sprintf("%s = %s", field, SqlPlaceholder), value)
}

// DefaultIncr SET field = field + value
func (s *Mod) DefaultIncr(field string, value interface{}) *Mod {
	return s.defaultExpr(field, fmt.Sprintf("%s = %s + %s", field, field, SqlPlaceholder), value)
}

// DefaultDecr SET field = field - value
func (s *Mod) DefaultDecr(field string, value interface{}) *Mod {
	return s.defaultExpr(field, fmt.Sprintf("%s = %s - %s", field, field, SqlPlaceholder), value)
}

// Where set where
func (s *Mod) Where(where Filter) *Mod {
	s.where = where
	return s
}

// SetSQL prepare args of set
func (s *Mod) SetSQL() (prepare string, args []interface{}) {
	length := len(s.update)
	if length == 0 {
		return
	}
	mod := make(map[string]struct{})
	fields := make([]string, 0, length)
	for _, field := range s.updateSlice {
		if _, ok := s.except[field]; ok {
			continue
		}
		mod[field] = struct{}{}
		fields = append(fields, field)
	}
	for _, field := range s.secondaryUpdateSlice {
		if _, ok := s.except[field]; ok {
			continue
		}
		if _, ok := mod[field]; ok {
			continue
		}
		fields = append(fields, field)
	}
	length = len(fields)
	field := make([]string, length)
	value := make([]interface{}, 0, length)
	ok := false
	for k, v := range fields {
		if _, ok = s.update[v]; ok {
			field[k] = s.update[v].expr
			value = append(value, s.update[v].args...)
			continue
		}
		field[k] = s.secondaryUpdate[v].expr
		value = append(value, s.secondaryUpdate[v].args...)
	}
	buf := getBuilder()
	defer putBuilder(buf)
	buf.WriteString(strings.Join(field, ", "))
	prepare = buf.String()
	args = value
	return
}

// SQL build SQL statement
func (s *Mod) SQL() (prepare string, args []interface{}) {
	if s.schema.table == EmptyString {
		return
	}
	prepare, args = s.SetSQL()
	if prepare == EmptyString {
		return
	}
	buf := comment(s.schema)
	defer putBuilder(buf)
	buf.WriteString("UPDATE ")
	buf.WriteString(s.schema.table)
	buf.WriteString(" SET ")
	buf.WriteString(prepare)

	config := s.schema.way.config
	if config != nil && config.DeleteMustUseWhere && (s.where == nil || s.where.IsEmpty()) {
		prepare, args = EmptyString, nil
		return
	}

	if s.where != nil && !s.where.IsEmpty() {
		where, whereArgs := s.where.SQL()
		buf.WriteString(" WHERE ")
		buf.WriteString(where)
		args = append(args, whereArgs...)
	}

	prepare = buf.String()
	return
}

// Mod execute the built SQL statement
func (s *Mod) Mod() (int64, error) {
	prepare, args := s.SQL()
	if prepare == EmptyString {
		return 0, nil
	}
	return s.schema.way.ExecContext(s.schema.ctx, prepare, args...)
}

// Way get current *Way
func (s *Mod) Way() *Way {
	return s.schema.way
}

// F make new Filter
func (s *Mod) F(filter ...Filter) Filter {
	return F().Filter(filter...)
}

type WithQuery struct {
	alias   string
	prepare string
	args    []interface{}
}

func newWithQuery(alias string, prepare string, args ...interface{}) *WithQuery {
	return &WithQuery{
		alias:   alias,
		prepare: prepare,
		args:    args,
	}
}

func (s *WithQuery) SQL() (prepare string, args []interface{}) {
	b := getBuilder()
	defer putBuilder(b)
	b.WriteString(s.alias)
	b.WriteString(" AS ( ")
	b.WriteString(s.prepare)
	b.WriteString(" )")
	prepare = b.String()
	args = s.args
	return
}

type SubQuery struct {
	prepare string
	args    []interface{}
}

func NewSubQuery(prepare string, args ...interface{}) *SubQuery {
	return &SubQuery{
		prepare: prepare,
		args:    args,
	}
}

func (s *SubQuery) SQL() (prepare string, args []interface{}) {
	prepare, args = s.prepare, s.args
	return
}

// GetJoin join SQL statement
type GetJoin struct {
	joinType string    // join type
	table    string    // table name
	subQuery *SubQuery // table is sub query
	alias    *string   // query table alias name
	on       string    // conditions for join query; ON a.order_id = b.order_id  <=> USING ( order_id )
	using    []string  // conditions for join query; USING ( order_id, ... )
	column   []string  // the list of fields to be queried in the current table
}

func newJoin(joinType string, table ...string) *GetJoin {
	join := &GetJoin{
		joinType: joinType,
	}
	join.Table(LastNotEmptyString(table))
	return join
}

func InnerJoin(table ...string) *GetJoin {
	return newJoin(SqlJoinInner, table...)
}

func LeftJoin(table ...string) *GetJoin {
	return newJoin(SqlJoinLeft, table...)
}

func RightJoin(table ...string) *GetJoin {
	return newJoin(SqlJoinRight, table...)
}

func FullJoin(table ...string) *GetJoin {
	return newJoin(SqlJoinFull, table...)
}

// Table set table name
func (s *GetJoin) Table(table string) *GetJoin {
	s.table = table
	return s
}

// SubQuery table is a query SQL statement
func (s *GetJoin) SubQuery(prepare string, args ...interface{}) *GetJoin {
	s.subQuery = NewSubQuery(prepare, args...)
	return s
}

// SubQueryGet table is a query SQL statement
func (s *GetJoin) SubQueryGet(get *Get, alias ...string) *GetJoin {
	if get == nil {
		return s
	}
	prepare, args := get.SQL()
	s.subQuery = NewSubQuery(prepare, args...)
	if str := LastNotEmptyString(alias); str != EmptyString {
		s.Alias(str)
	}
	return s
}

// Alias for table alias name, don't forget to call the current method when the table is a SQL statement
func (s *GetJoin) Alias(alias string) *GetJoin {
	s.alias = &alias
	return s
}

// On join query condition
func (s *GetJoin) On(on string) *GetJoin {
	s.on = on
	return s
}

// Using join query condition
func (s *GetJoin) Using(fields ...string) *GetJoin {
	s.using = fields
	return s
}

// OnEqual join query condition, support multiple fields
func (s *GetJoin) OnEqual(fields ...string) *GetJoin {
	length := len(fields)
	if length&1 == 1 {
		return s
	}
	tmp := make([]string, 0, length)
	for i := 0; i < length; i += 2 {
		tmp = append(tmp, fmt.Sprintf("%s = %s", fields[i], fields[i+1]))
	}
	return s.On(strings.Join(tmp, " AND "))
}

// Column Set the list of fields to be queried in the current table
func (s *GetJoin) Column(column ...string) *GetJoin {
	s.column = column
	return s
}

// GetColumn Get the list of fields to be queried in the current table
func (s *GetJoin) GetColumn() []string {
	return s.column
}

// SQL build SQL statement
func (s *GetJoin) SQL() (prepare string, args []interface{}) {
	if s.table == EmptyString && (s.subQuery == nil || s.alias == nil || *s.alias == EmptyString) {
		return
	}
	usingLength := len(s.using)
	if s.on == EmptyString && usingLength == 0 {
		return
	}

	buf := getBuilder()
	defer putBuilder(buf)

	buf.WriteString(s.joinType)

	if s.subQuery != nil {
		buf.WriteString(" ( ")
		subPrepare, subArgs := s.subQuery.SQL()
		buf.WriteString(subPrepare)
		buf.WriteString(" ) AS ")
		buf.WriteString(*s.alias)
		args = append(args, subArgs...)
	} else {
		buf.WriteString(SqlSpace)
		buf.WriteString(s.table)
		if s.alias != nil && *s.alias != EmptyString {
			buf.WriteString(" AS ")
			buf.WriteString(*s.alias)
		}
	}

	if usingLength > 0 {
		buf.WriteString(" USING ( ")
		buf.WriteString(strings.Join(s.using, ", "))
		buf.WriteString(" )")
	} else {
		buf.WriteString(" ON ")
		buf.WriteString(s.on)
	}

	prepare = buf.String()
	return
}

// union SQL statement union
type union struct {
	unionType string
	prepare   string
	args      []interface{}
}

// Limiter limit and offset
type Limiter interface {
	GetLimit() int64
	GetOffset() int64
}

// Ident sql identifier
type Ident struct {
	prefix string
}

// V returns expressions in different formats based on the length of the parameter
// length=0: prefix value
// length=1: prefix.name
// length>1: prefix.name AS alias_name (the alias value must not be empty, otherwise it will not be used)
func (s *Ident) V(sss ...string) string {
	length := len(sss)
	if length == 0 {
		return s.prefix
	}
	name := sss[0]
	if s.prefix != EmptyString {
		name = fmt.Sprintf("%s%s%s", s.prefix, SqlPoint, name)
	}
	if length == 1 {
		return name
	}
	return SqlAlias(name, LastNotEmptyString(sss[1:]))
}

func (s *Ident) groupFunc(nameFunc string, field string, alias ...string) string {
	return SqlAlias(fmt.Sprintf("%s(%s)", nameFunc, field), LastNotEmptyString(alias))
}

// Avg AVG([prefix.]xxx)[ AS xxx|custom]
func (s *Ident) Avg(field string, alias ...string) string {
	return s.groupFunc(SqlAvg, s.V(field), alias...)
}

// Max MAX([prefix.]xxx)[ AS xxx|custom]
func (s *Ident) Max(field string, alias ...string) string {
	return s.groupFunc(SqlMax, s.V(field), alias...)
}

// Min MIN([prefix.]xxx)[ AS xxx|custom]
func (s *Ident) Min(field string, alias ...string) string {
	return s.groupFunc(SqlMin, s.V(field), alias...)
}

// Sum SUM([prefix.]xxx)[ AS xxx|custom]
func (s *Ident) Sum(field string, alias ...string) string {
	return s.groupFunc(SqlSum, s.V(field), alias...)
}

// COUNT COUNT([prefix.]xxx) AS count|custom
func (s *Ident) COUNT(field string, alias ...string) string {
	fieldAlias := LastNotEmptyString(alias)
	if fieldAlias == EmptyString {
		fieldAlias = "count"
	}
	return s.groupFunc(SqlCount, field, fieldAlias)
}

// AVG AVG([prefix.]xxx) AS xxx
func (s *Ident) AVG(field string) string {
	return s.Avg(field, field)
}

// MAX MAX([prefix.]xxx) AS xxx
func (s *Ident) MAX(field string) string {
	return s.Max(field, field)
}

// MIN MIN([prefix.]xxx) AS xxx
func (s *Ident) MIN(field string) string {
	return s.Min(field, field)
}

// SUM SUM([prefix.]xxx) AS xxx
func (s *Ident) SUM(field string) string {
	return s.Sum(field, field)
}

// Field Batch set field prefix
func (s *Ident) Field(field ...string) []string {
	prefix := s.prefix
	if prefix == EmptyString {
		return field[:]
	}
	length := len(field)
	result := make([]string, 0, length)
	for i := 0; i < length; i++ {
		if field[i] == EmptyString || strings.Contains(field[i], SqlPoint) {
			result = append(result, field[i])
			continue
		}
		result = append(result, fmt.Sprintf("%s%s%s", s.prefix, SqlPoint, field[i]))
	}
	return result
}

// Get for SELECT
type Get struct {
	// query table
	schema *schema

	// with of query
	with []*WithQuery

	// query field list
	column []string

	// the query table is a sub query
	subQuery *SubQuery

	// set an alias for the queried table
	alias *string

	// join query
	join []*GetJoin

	// WHERE condition to filter data
	where Filter

	// group query result
	group []string

	// use HAVING to filter data after grouping
	having Filter

	// union query
	union []*union

	// order query result
	order []string

	// ordered columns map list
	orderMap map[string]string

	// limit the number of query result
	limit *int64

	// query result offset
	offset *int64
}

// NewGet for SELECT
func NewGet(way *Way) *Get {
	return &Get{
		schema:   newSchema(way),
		orderMap: map[string]string{},
	}
}

// Comment set comment
func (s *Get) Comment(comment string) *Get {
	s.schema.comment = comment
	return s
}

// Context set context
func (s *Get) Context(ctx context.Context) *Get {
	s.schema.ctx = ctx
	return s
}

// With for with query
func (s *Get) With(alias string, prepare string, args ...interface{}) *Get {
	if alias == EmptyString || prepare == EmptyString {
		return s
	}
	s.with = append(s.with, newWithQuery(alias, prepare, args...))
	return s
}

// WithGet for with query using *Get
func (s *Get) WithGet(alias string, get *Get) *Get {
	prepare, args := get.SQL()
	return s.With(alias, prepare, args...)
}

// Table set table name
func (s *Get) Table(table string, alias ...string) *Get {
	s.schema.table = table
	if str := LastNotEmptyString(alias); str != EmptyString {
		s.Alias(str)
	}
	return s
}

// SubQuery table is a query SQL statement
func (s *Get) SubQuery(prepare string, args ...interface{}) *Get {
	s.subQuery = NewSubQuery(prepare, args...)
	return s
}

// SubQueryGet table is a query SQL statement
func (s *Get) SubQueryGet(get *Get, alias ...string) *Get {
	if get == nil {
		return s
	}
	prepare, args := get.SQL()
	s.subQuery = NewSubQuery(prepare, args...)
	if str := LastNotEmptyString(alias); str != EmptyString {
		s.Alias(str)
	}
	return s
}

// Alias for table alias name, don't forget to call the current method when the table is a SQL statement
func (s *Get) Alias(alias string) *Get {
	s.alias = &alias
	return s
}

// Joins for join one or more tables
func (s *Get) Joins(joins ...*GetJoin) *Get {
	length := len(joins)
	for i := 0; i < length; i++ {
		if joins[i] == nil {
			continue
		}
		s.join = append(s.join, joins[i])
	}
	return s
}

// typeJoin join with join-type
func (s *Get) typeJoin(joinType string, fs []func(j *GetJoin)) *Get {
	length := len(fs)
	joins := make([]*GetJoin, 0, length)
	for i := 0; i < length; i++ {
		if fs[i] == nil {
			continue
		}
		tmp := &GetJoin{
			joinType: joinType,
		}
		fs[i](tmp)
		joins = append(joins, tmp)
	}
	return s.Joins(joins...)
}

// InnerJoin for inner join
func (s *Get) InnerJoin(fs ...func(j *GetJoin)) *Get {
	return s.typeJoin(SqlJoinInner, fs)
}

// LeftJoin for left join
func (s *Get) LeftJoin(fs ...func(j *GetJoin)) *Get {
	return s.typeJoin(SqlJoinLeft, fs)
}

// RightJoin for right join
func (s *Get) RightJoin(fs ...func(j *GetJoin)) *Get {
	return s.typeJoin(SqlJoinRight, fs)
}

// FullJoin for full join
func (s *Get) FullJoin(fs ...func(j *GetJoin)) *Get {
	return s.typeJoin(SqlJoinFull, fs)
}

// Where set where
func (s *Get) Where(where Filter) *Get {
	s.where = where
	return s
}

// Group set group columns
func (s *Get) Group(group ...string) *Get {
	s.group = append(s.group, group...)
	return s
}

// Having set filter of group result
func (s *Get) Having(having Filter) *Get {
	s.having = having
	return s
}

// Union union query
func (s *Get) Union(prepare string, args ...interface{}) *Get {
	if prepare == EmptyString {
		return s
	}
	s.union = append(s.union, &union{
		unionType: SqlUnion,
		prepare:   prepare,
		args:      args,
	})
	return s
}

// UnionGet union query
func (s *Get) UnionGet(get ...*Get) *Get {
	for _, g := range get {
		if g == nil {
			continue
		}
		prepare, args := g.SQL()
		s.Union(prepare, args...)
	}
	return s
}

// UnionAll union all query
func (s *Get) UnionAll(prepare string, args ...interface{}) *Get {
	if prepare == EmptyString {
		return s
	}
	s.union = append(s.union, &union{
		unionType: SqlUnionAll,
		prepare:   prepare,
		args:      args,
	})
	return s
}

// UnionAllGet union all query
func (s *Get) UnionAllGet(get ...*Get) *Get {
	for _, g := range get {
		if g == nil {
			continue
		}
		prepare, args := g.SQL()
		s.UnionAll(prepare, args...)
	}
	return s
}

// Column set the columns list of query
func (s *Get) Column(column ...string) *Get {
	s.column = column
	return s
}

// AppendColumn append the columns list of query
func (s *Get) AppendColumn(column ...string) *Get {
	s.column = append(s.column, column...)
	return s
}

// orderBy set order by column
func (s *Get) orderBy(column string, order string) *Get {
	if column == EmptyString || (order != SqlAsc && order != SqlDesc) {
		return s
	}
	if _, ok := s.orderMap[column]; ok {
		return s
	}
	s.order = append(s.order, fmt.Sprintf("%s %s", column, order))
	s.orderMap[column] = order
	return s
}

// Asc set order by column ASC
func (s *Get) Asc(column string) *Get {
	return s.orderBy(column, SqlAsc)
}

// Desc set order by column Desc
func (s *Get) Desc(column string) *Get {
	return s.orderBy(column, SqlDesc)
}

var (
	// orderRegexp `column_name_first:a,column_name_second:d` => `column_name_first ASC, column_name_second DESC`
	orderRegexp = regexp.MustCompile(`^([a-zA-Z][a-zA-Z0-9_]*([.][a-zA-Z][a-zA-Z0-9_]*)*):([ad])$`)
)

// Order set the column sorting list in batches through regular expressions according to the request parameter value
func (s *Get) Order(order string, orderMap ...map[string]string) *Get {
	fieldMap := make(map[string]string)
	for _, m := range orderMap {
		for k, v := range m {
			fieldMap[k] = v
		}
	}

	orders := strings.Split(order, ",")
	for _, v := range orders {
		if len(v) > 32 {
			continue
		}
		match := orderRegexp.FindAllStringSubmatch(strings.TrimSpace(v), -1)
		length := len(match)
		if length != 1 {
			continue
		}
		matched := match[0]
		length = len(matched) // the length should be 4
		if length < 4 || matched[3] == "" {
			continue
		}
		field := matched[1]
		if val, ok := fieldMap[field]; ok && val != "" {
			field = val
		}
		if matched[3][0] == 97 {
			s.Asc(field)
			continue
		}
		if matched[3][0] == 100 {
			s.Desc(field)
			continue
		}
	}
	return s
}

// Limit set limit
func (s *Get) Limit(limit int64) *Get {
	if limit <= 0 {
		// cancel the set value
		s.limit = nil
		return s
	}
	s.limit = &limit
	return s
}

// Offset set offset
func (s *Get) Offset(offset int64) *Get {
	if offset <= 0 {
		// cancel the set value
		s.offset = nil
		return s
	}
	s.offset = &offset
	return s
}

// Limiter set limit and offset at the same time
func (s *Get) Limiter(limiter Limiter) *Get {
	if limiter == nil {
		return s
	}
	return s.Limit(limiter.GetLimit()).Offset(limiter.GetOffset())
}

// sqlTable build query base table SQL statement
func (s *Get) sqlTable() (prepare string, args []interface{}) {
	if s.schema.table == EmptyString && s.subQuery == nil {
		return
	}
	buf := comment(s.schema)
	defer putBuilder(buf)

	withLength := len(s.with)
	if withLength > 0 {
		buf.WriteString("WITH ")
		for i := 0; i < withLength; i++ {
			if i > 0 {
				buf.WriteString(", ")
			}
			pre, arg := s.with[i].SQL()
			buf.WriteString(pre)
			args = append(args, arg...)
		}
		buf.WriteString(SqlSpace)
	}

	buf.WriteString("SELECT ")

	if s.join != nil {
		length := len(s.join)
		if length > 0 {
			prefix := s.schema.table
			if s.alias != nil {
				prefix = *s.alias
			}
			s.column = s.schema.way.Ident(prefix).Field(s.column...)
		}
		for i := 0; i < length; i++ {
			if s.join[i].column != nil {
				prefix := s.join[i].table
				if s.join[i].alias != nil {
					prefix = *s.join[i].alias
				}
				s.AppendColumn(s.schema.way.Ident(prefix).Field(s.join[i].column...)...)
			}
		}
	}

	if s.column == nil {
		buf.WriteString(SqlStar)
	} else {
		buf.WriteString(strings.Join(s.column, ", "))
	}

	buf.WriteString(" FROM ")

	if s.subQuery == nil {
		buf.WriteString(s.schema.table)
	} else {
		buf.WriteString("( ")
		subPrepare, subArgs := s.subQuery.SQL()
		buf.WriteString(subPrepare)
		buf.WriteString(" )")
		args = append(args, subArgs...)
	}

	if s.alias != nil && *s.alias != EmptyString {
		buf.WriteString(" AS ")
		buf.WriteString(*s.alias)
	}

	if s.join != nil {
		length := len(s.join)
		for i := 0; i < length; i++ {
			if s.join[i] == nil {
				continue
			}
			joinPrepare, joinArgs := s.join[i].SQL()
			if joinPrepare != EmptyString {
				buf.WriteString(SqlSpace)
				buf.WriteString(joinPrepare)
				args = append(args, joinArgs...)
			}
		}
	}

	if s.where != nil && !s.where.IsEmpty() {
		where, whereArgs := s.where.SQL()
		buf.WriteString(" WHERE ")
		buf.WriteString(where)
		args = append(args, whereArgs...)
	}

	if s.group != nil {
		if len(s.group) > 0 {
			buf.WriteString(" GROUP BY ")
			buf.WriteString(strings.Join(s.group, ", "))
			if s.having != nil {
				having, havingArgs := s.having.SQL()
				if having != EmptyString {
					buf.WriteString(" HAVING ")
					buf.WriteString(having)
					args = append(args, havingArgs...)
				}
			}
		}
	}

	if s.union != nil {
		for _, u := range s.union {
			buf.WriteString(SqlSpace)
			buf.WriteString(u.unionType)
			buf.WriteString(" ( ")
			buf.WriteString(u.prepare)
			buf.WriteString(" )")
			args = append(args, u.args...)
		}
	}

	prepare = strings.TrimSpace(buf.String())
	return
}

// SQLCount build SQL statement for count
func (s *Get) SQLCount(columns ...string) (prepare string, args []interface{}) {
	if columns == nil {
		columns = []string{
			SqlAlias("COUNT(*)", "count"),
		}
	}

	if s.group != nil || s.union != nil {
		prepare, args = s.sqlTable()
		prepare, args = NewGet(s.schema.way).
			SubQuery(prepare, args...).
			Alias("count_table_rows").
			SQLCount(columns...)
		return
	}

	column := s.column // store selected columns
	s.column = columns // set count column
	prepare, args = s.sqlTable()
	s.column = column // restore origin selected columns

	return
}

// SQL build SQL statement
func (s *Get) SQL() (prepare string, args []interface{}) {
	prepare, args = s.sqlTable()
	if prepare == EmptyString {
		return
	}

	buf := getBuilder()
	defer putBuilder(buf)
	buf.WriteString(prepare)

	if len(s.order) > 0 {
		buf.WriteString(" ORDER BY ")
		buf.WriteString(strings.Join(s.order, ", "))
	}

	if s.limit != nil {
		buf.WriteString(fmt.Sprintf(" LIMIT %d", *s.limit))
		if s.offset != nil {
			buf.WriteString(fmt.Sprintf(" OFFSET %d", *s.offset))
		}
	}

	prepare = strings.TrimSpace(buf.String())
	return
}

// Count execute the built SQL statement and scan query result for count
func (s *Get) Count(column ...string) (count int64, err error) {
	prepare, args := s.SQLCount(column...)
	err = s.schema.way.QueryContext(s.schema.ctx, func(rows *sql.Rows) (err error) {
		if rows.Next() {
			err = rows.Scan(&count)
		}
		return
	}, prepare, args...)
	return
}

// Query execute the built SQL statement and scan query result
func (s *Get) Query(query func(rows *sql.Rows) (err error)) error {
	prepare, args := s.SQL()
	return s.schema.way.QueryContext(s.schema.ctx, query, prepare, args...)
}

// Get execute the built SQL statement and scan query result
func (s *Get) Get(result interface{}) error {
	prepare, args := s.SQL()
	return s.schema.way.TakeAllContext(s.schema.ctx, result, prepare, args...)
}

// Exists Determine whether the query result exists.
func (s *Get) Exists() (exists bool, err error) {
	err = s.Limit(1).Query(func(rows *sql.Rows) error {
		exists = rows.Next()
		return nil
	})
	return
}

// ScanAll execute the built SQL statement and scan all from the query results.
func (s *Get) ScanAll(fc func(rows *sql.Rows) error) error {
	return s.Query(func(rows *sql.Rows) error {
		return s.schema.way.ScanAll(rows, fc)
	})
}

// ScanOne execute the built SQL statement and scan at most once from the query results.
func (s *Get) ScanOne(dest ...interface{}) error {
	return s.Query(func(rows *sql.Rows) error {
		return s.schema.way.ScanOne(rows, dest...)
	})
}

// ViewMap execute the built SQL statement and scan all from the query results.
func (s *Get) ViewMap() (result []map[string]interface{}, err error) {
	err = s.Query(func(rows *sql.Rows) (err error) {
		result, err = ScanViewMap(rows)
		return
	})
	return
}

// Way get current *Way
func (s *Get) Way() *Way {
	return s.schema.way
}

// F make new Filter
func (s *Get) F(filter ...Filter) Filter {
	return F().Filter(filter...)
}

// CountQuery execute the built SQL statement and scan query result, count + query
func (s *Get) CountQuery(query func(rows *sql.Rows) (err error), countColumn ...string) (int64, error) {
	count, err := s.Count(countColumn...)
	if err != nil {
		return 0, err
	}
	if count <= 0 {
		return 0, nil
	}
	return count, s.Query(query)
}

// CountGet execute the built SQL statement and scan query result, count + get
func (s *Get) CountGet(result interface{}, countColumn ...string) (int64, error) {
	count, err := s.Count(countColumn...)
	if err != nil {
		return 0, err
	}
	if count <= 0 {
		return 0, nil
	}
	return count, s.Get(result)
}

// WindowFunc SQL window function.
type WindowFunc struct {
	// withFunc The window function used.
	withFunc string

	// partition Setting up window partitions.
	partition []string

	// order Sorting data within a group.
	order []string

	// windowFrame Window frame clause. `ROWS` or `RANGE`
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
	return s.WithFunc(fmt.Sprintf("LAG(%s, %d, %s)", column, offset, ArgString(defaultValue)))
}

// Lead LEAD() Returns the value of a row after the current row.
func (s *WindowFunc) Lead(column string, offset int64, defaultValue any) *WindowFunc {
	return s.WithFunc(fmt.Sprintf("LEAD(%s, %d, %s)", column, offset, ArgString(defaultValue)))
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
	b := getBuilder()
	defer putBuilder(b)
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

func NewWindowFunc(alias ...string) *WindowFunc {
	return &WindowFunc{
		alias: LastNotEmptyString(alias),
	}
}
