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

// Common data values for table.column.
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

var (
	// ErrNoAffectedRows No rows are affected when execute SQL.
	ErrNoAffectedRows = errors.New("database: there are no affected rows")
)

var (
	// insertByStructPool insert with struct{}, *struct{}, []struct, []*struct{}, *[]struct{}, *[]*struct{}.
	insertByStructPool = &sync.Pool{
		New: func() interface{} {
			return &insertByStruct{}
		},
	}

	// casePool case pool.
	casePool = &sync.Pool{
		New: func() interface{} {
			return newCase()
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

// GetCase get *Case from pool.
func GetCase() *Case {
	return casePool.Get().(*Case)
}

// PutCase put *Case in the pool.
func PutCase(i *Case) {
	i.alias = EmptyString
	i.when = nil
	i.then = nil
	i.others = nil
	casePool.Put(i)
}

// MustAffectedRows at least one row is affected.
func MustAffectedRows(affectedRows int64, err error) error {
	if err != nil {
		return err
	}
	if affectedRows <= 0 {
		return ErrNoAffectedRows
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

// RemoveDuplicate remove duplicate element.
func RemoveDuplicate(dynamic ...interface{}) (result []interface{}) {
	length := len(dynamic)
	mp := make(map[interface{}]*struct{}, length)
	ok := false
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

// RemoveDuplicates remove duplicate element.
func RemoveDuplicates[T comparable](dynamic ...T) (result []T) {
	length := len(dynamic)
	mp := make(map[T]*struct{}, length)
	ok := false
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
	// store root struct properties.
	direct map[string]int

	// store non-root struct properties, such as: anonymous attribute structure and named attribute structure.
	indirect map[string][]int

	// all used struct types, including the root struct.
	structType map[reflect.Type]*struct{}
}

func bindStructInit() *bindStruct {
	return &bindStruct{
		direct:     make(map[string]int, 32),
		indirect:   make(map[string][]int, 32),
		structType: make(map[reflect.Type]*struct{}, 8),
	}
}

// binding Match the binding according to the structure "db" tag and the query column name.
// Please ensure that the type of refStructType must be `reflect.Struct`.
func (s *bindStruct) binding(refStructType reflect.Type, depth []int, tag string) {
	if _, ok := s.structType[refStructType]; ok {
		// prevent structure loop nesting.
		return
	}
	s.structType[refStructType] = &struct{}{}
	length := refStructType.NumField()
	for i := 0; i < length; i++ {
		attribute := refStructType.Field(i)
		if !attribute.IsExported() {
			continue
		}
		// anonymous structure, or named structure.
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
				// recursive call.
				s.binding(at, dst, tag)
			}
			continue
		}
		field := attribute.Tag.Get(tag)
		if field == EmptyString || field == "-" {
			continue
		}
		// root structure attribute.
		if depth == nil {
			s.direct[field] = i
			continue
		}
		// others structure attribute, nested anonymous structure or named structure.
		if _, ok := s.indirect[field]; !ok {
			dst := depth[:]
			dst = append(dst, i)
			s.indirect[field] = dst
		}
	}
}

// prepare The preparatory work before executing rows.Scan.
// find the pointer of the corresponding field from the reflection value of the receiving object, and bind it.
// When nesting structures, it is recommended to use structure value nesting to prevent null pointers that may appear when the root structure accesses the properties of substructures, resulting in panic.
func (s *bindStruct) prepare(columns []string, rowsScan []interface{}, indirect reflect.Value, length int) error {
	for i := 0; i < length; i++ {
		index, ok := s.direct[columns[i]]
		if ok {
			// top structure.
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
		// parsing multi-layer structures.
		line, ok := s.indirect[columns[i]]
		if !ok {
			// unable to find mapping property for current field Use *[]byte instead to receive.
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
				// middle layer structures.
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
			// innermost structure.
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

// ScanSliceStruct Scan the query result set into the receiving object the receiving object type is *[]AnyStruct or *[]*AnyStruct.
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
	// struct tag name value used as table.column name.
	tag string

	// only allowed fields Hash table.
	allow map[string]*struct{}

	// ignored fields Hash table.
	except map[string]*struct{}

	// already existing fields Hash table.
	used map[string]*struct{}

	// struct reflect type, make sure it is the same structure type.
	structReflectType reflect.Type
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
func (s *insertByStruct) structFieldsValues(structReflectValue reflect.Value, allowed bool) (fields []string, values []interface{}) {
	reflectType := structReflectValue.Type()
	if s.structReflectType == nil {
		s.structReflectType = reflectType
	}
	if s.structReflectType != reflectType {
		panicSliceElementTypesAreInconsistent()
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

// structValues checkout values.
func (s *insertByStruct) structValues(structReflectValue reflect.Value, allowed bool) (values []interface{}) {
	reflectType := structReflectValue.Type()
	if s.structReflectType != nil && s.structReflectType != reflectType {
		panicSliceElementTypesAreInconsistent()
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

// Insert object should be one of struct{}, *struct{}, []struct, []*struct{}, *[]struct{}, *[]*struct{}.
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

// StructInsert object should be one of struct{}, *struct{}, []struct, []*struct{}, *[]struct{}, *[]*struct{}.
// get fields and values based on struct tag.
func StructInsert(object interface{}, tag string, except []string, allow []string) (fields []string, values [][]interface{}) {
	b := getInsertByStruct()
	defer putInsertByStruct(b)
	fields, values = b.Insert(object, tag, except, allow)
	return
}

// StructModify object should be one of struct{}, *struct{} get the fields and values that need to be modified.
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
	excepted := make(map[string]*struct{}, 32)
	for _, field := range except {
		excepted[field] = &struct{}{}
	}

	length := ofType.NumField()

	exists := make(map[string]*struct{}, length)
	fields = make([]string, 0, length)
	values = make([]interface{}, 0, length)

	last := 0
	fieldsIndex := make(map[string]int, 32)

	add := func(field string, value interface{}) {
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

// StructObtain object should be one of struct{}, *struct{} for get all fields and values.
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
	excepted := make(map[string]*struct{}, 32)
	for _, field := range except {
		excepted[field] = &struct{}{}
	}

	length := ofType.NumField()

	exists := make(map[string]*struct{}, length)
	fields = make([]string, 0, length)
	values = make([]interface{}, 0, length)

	last := 0
	fieldsIndex := make(map[string]int, 32)

	add := func(field string, value interface{}) {
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

// StructUpdate compare origin and latest for update.
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

	exists := make(map[string]*struct{}, 32)
	fields = make([]string, 0)
	values = make([]interface{}, 0)

	last := 0
	fieldsIndex := make(map[string]int, 32)

	add := func(field string, value interface{}) {
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
		bvo, bvl := BasicTypeValue(storage[v]), BasicTypeValue(latestValues[k])
		if reflect.DeepEqual(bvo, bvl) {
			continue
		}
		add(v, bvl)
	}

	return
}

// RemoveSliceMemberByIndex delete slice member by index.
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

// ConcatString concatenate string.
func ConcatString(sss ...string) string {
	b := getStringBuilder()
	defer putStringBuilder(b)
	length := len(sss)
	for i := 0; i < length; i++ {
		b.WriteString(sss[i])
	}
	return b.String()
}

// SqlAlias sql alias name.
func SqlAlias(name string, alias string) string {
	if alias == EmptyString {
		return name
	}
	return fmt.Sprintf("%s %s %s", name, SqlAs, alias)
}

// SqlPrefix sql prefix name.
func SqlPrefix(prefix string, name string) string {
	if prefix == EmptyString || strings.HasPrefix(name, fmt.Sprintf("%s%s", prefix, SqlPoint)) {
		return name
	}
	return fmt.Sprintf("%s%s%s", prefix, SqlPoint, name)
}

// EvenSlice2Map even slice to map.
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

// Slice2MapNewKey make map by slice, create key.
func Slice2MapNewKey[K comparable](elems []K, createKey func(v K) K) map[K]K {
	length := len(elems)
	result := make(map[K]K, length)
	for i := 0; i < length; i++ {
		result[createKey(elems[i])] = elems[i]
	}
	return result
}

// Slice2MapNewVal make map by slice, create value.
func Slice2MapNewVal[K comparable, V interface{}](elems []K, createValue func(v K) V) map[K]V {
	length := len(elems)
	result := make(map[K]V, length)
	for i := 0; i < length; i++ {
		result[elems[i]] = createValue(elems[i])
	}
	return result
}

// SliceMatchMap use the `key` value of each element in `elems` to match in the map, and call `handle` if the match is successful.
func SliceMatchMap[K comparable, X interface{}, Y interface{}](kx map[K]X, handle func(x X, y Y), key func(y Y) K, elems []Y) {
	for i := len(elems) - 1; i >= 0; i-- {
		if tmp, ok := kx[key(elems[i])]; ok {
			handle(tmp, elems[i])
		}
	}
}

// SliceIter slice iteration.
func SliceIter[V interface{}](iter func(v V) V, elems []V) []V {
	for i := len(elems) - 1; i >= 0; i-- {
		elems[i] = iter(elems[i])
	}
	return elems
}

// MergeMap merge multiple maps.
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

// MergeSlice merge multiple slices.
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

// Case Implementing SQL CASE.
type Case struct {
	alias  string
	when   []*PrepareArgs
	then   []*PrepareArgs
	others *PrepareArgs
}

func newCase() *Case {
	return &Case{}
}

// If CASE WHEN condition THEN expressions.
func (s *Case) If(when func(f Filter), then string, thenArgs ...interface{}) *Case {
	if when == nil || then == EmptyString {
		return s
	}
	w := GetFilter()
	defer PutFilter(w)
	when(w)
	if w.IsEmpty() {
		return s
	}
	prepare, args := w.SQL()
	s.when = append(s.when, &PrepareArgs{
		Prepare: prepare,
		Args:    args,
	})
	s.then = append(s.then, &PrepareArgs{
		Prepare: then,
		Args:    thenArgs,
	})
	return s
}

// Else Expressions of else.
func (s *Case) Else(elseExpr string, elseArgs ...interface{}) *Case {
	if elseExpr == EmptyString {
		return s
	}
	s.others = &PrepareArgs{
		Prepare: elseExpr,
		Args:    elseArgs,
	}
	return s
}

// Alias AS alias .
func (s *Case) Alias(alias string) *Case {
	s.alias = alias
	return s
}

// SQL Make SQL expr: CASE WHEN condition1 THEN result1 WHEN condition2 THEN result2 ... ELSE else_result END [AS alias_name] .
func (s *Case) SQL() (prepare string, args []interface{}) {
	lenWhen, lenThen := len(s.when), len(s.then)
	if lenWhen == 0 || lenWhen != lenThen || s.others == nil {
		return
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString("CASE")
	for i := 0; i < lenWhen; i++ {
		b.WriteString(" WHEN ")
		b.WriteString(s.when[i].Prepare)
		args = append(args, s.when[i].Args...)
		b.WriteString(" THEN ")
		b.WriteString(s.then[i].Prepare)
		args = append(args, s.then[i].Args...)
	}
	b.WriteString(SqlSpace)
	b.WriteString("ELSE ")
	b.WriteString(s.others.Prepare)
	args = append(args, s.others.Args...)
	b.WriteString(" END")
	if s.alias != EmptyString {
		b.WriteString(SqlSpace)
		b.WriteString("AS ")
		b.WriteString(s.alias)
	}
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

// newSchema new schema with *Way.
func newSchema(way *Way) *schema {
	return &schema{
		ctx: context.Background(),
		way: way,
	}
}

// comment make SQL statement builder, SqlPlaceholder "?" should not appear in comments.
// defer putStringBuilder(builder) should be called immediately after calling the current method.
func comment(schema *schema) (b *strings.Builder) {
	b = getStringBuilder()
	schema.comment = strings.TrimSpace(schema.comment)
	schema.comment = strings.ReplaceAll(schema.comment, SqlPlaceholder, EmptyString)
	if schema.comment == EmptyString {
		return
	}
	b.WriteString("/*")
	b.WriteString(schema.comment)
	b.WriteString("*/")
	return
}

// Del for DELETE.
type Del struct {
	schema *schema
	where  Filter
}

// NewDel for DELETE.
func NewDel(way *Way) *Del {
	return &Del{
		schema: newSchema(way),
	}
}

// Comment set comment.
func (s *Del) Comment(comment string) *Del {
	s.schema.comment = comment
	return s
}

// Context set context.
func (s *Del) Context(ctx context.Context) *Del {
	s.schema.ctx = ctx
	return s
}

// Table set table name.
func (s *Del) Table(table string) *Del {
	s.schema.table = table
	return s
}

// Where set where.
func (s *Del) Where(where Filter) *Del {
	s.where = where
	return s
}

// SQL build SQL statement.
func (s *Del) SQL() (prepare string, args []interface{}) {
	if s.schema.table == EmptyString {
		return
	}
	buf := comment(s.schema)
	defer putStringBuilder(buf)
	buf.WriteString("DELETE FROM ")
	buf.WriteString(s.schema.table)

	cfg := s.schema.way.cfg
	if cfg.DeleteMustUseWhere && (s.where == nil || s.where.IsEmpty()) {
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

// Del execute the built SQL statement.
func (s *Del) Del() (int64, error) {
	prepare, args := s.SQL()
	return s.schema.way.ExecContext(s.schema.ctx, prepare, args...)
}

// Way get current *Way.
func (s *Del) Way() *Way {
	return s.schema.way
}

// F make new Filter.
func (s *Del) F(fs ...Filter) Filter {
	return F().New(fs...)
}

// Add for INSERT.
type Add struct {
	schema *schema

	/* insert one or more rows */
	// Fields that are not allowed to be inserted.
	exceptMap map[string]*struct{}
	except    []string

	// Permit the following fields can be inserted.
	permitMap map[string]*struct{}
	permit    []string

	// Key-value pairs have been set.
	fieldsLastIndex  int
	fieldsFieldIndex map[string]int
	fields           []string
	values           [][]interface{}

	// Key-value default pairs have been set.
	defaultFields       []string
	defaultFieldsValues map[string]interface{}

	// subQuery INSERT VALUES is query statement.
	subQuery *SubQuery
}

// NewAdd for INSERT.
func NewAdd(way *Way) *Add {
	add := &Add{
		schema:              newSchema(way),
		exceptMap:           make(map[string]*struct{}, 32),
		permitMap:           make(map[string]*struct{}, 32),
		fieldsFieldIndex:    make(map[string]int, 32),
		values:              make([][]interface{}, 1),
		defaultFieldsValues: make(map[string]interface{}, 8),
	}
	return add
}

// Comment set comment.
func (s *Add) Comment(comment string) *Add {
	s.schema.comment = comment
	return s
}

// Context set context.
func (s *Add) Context(ctx context.Context) *Add {
	s.schema.ctx = ctx
	return s
}

// Table set table name.
func (s *Add) Table(table string) *Add {
	s.schema.table = table
	return s
}

// Except exclude some columns from insert one or more rows.
func (s *Add) Except(except ...string) *Add {
	remove := make(map[string]*struct{}, len(except))
	for _, field := range except {
		if field == EmptyString {
			continue
		}
		if _, ok := s.exceptMap[field]; ok {
			continue
		}
		s.exceptMap[field] = &struct{}{}
		s.except = append(s.except, field)

		if _, ok := s.permitMap[field]; ok {
			remove[field] = &struct{}{}
		}
	}

	if length := len(remove); length > 0 {
		latest := make([]string, 0, len(s.permit))
		for _, field := range s.permit {
			if _, ok := remove[field]; ok {
				delete(s.permitMap, field)
				continue
			}
			latest = append(latest, field)
		}
		s.permit = latest
	}

	return s
}

// Permit Set a list of fields that can only be inserted.
func (s *Add) Permit(permit ...string) *Add {
	remove := make(map[string]*struct{}, len(permit))
	for _, field := range permit {
		if field == EmptyString {
			continue
		}
		if _, ok := s.permitMap[field]; ok {
			continue
		}
		s.permitMap[field] = &struct{}{}
		s.permit = append(s.permit, field)

		if _, ok := s.exceptMap[field]; ok {
			remove[field] = &struct{}{}
		}
	}

	if length := len(remove); length > 0 {
		latest := make([]string, 0, len(s.except))
		for _, field := range s.except {
			if _, ok := remove[field]; ok {
				delete(s.exceptMap, field)
				continue
			}
			latest = append(latest, field)
		}
		s.except = latest
	}
	return s
}

// FieldsValues set fields and values.
func (s *Add) FieldsValues(fields []string, values [][]interface{}) *Add {
	length1, length2 := len(fields), len(values)
	if length1 == 0 || length2 == 0 {
		return s
	}
	for _, v := range values {
		if length1 != len(v) {
			return s
		}
	}

	if s.except != nil {
		// Delete fields and values that are not allowed to be inserted.
		indexes := make(map[int]*struct{}, length1)
		for i := 0; i < length1; i++ {
			if _, ok := s.exceptMap[fields[i]]; ok {
				indexes[i] = &struct{}{}
			}
		}
		if length3 := len(indexes); length3 > 0 {
			length4 := length1 - length3
			fields1 := make([]string, 0, length4)
			values1 := make([][]interface{}, length2)
			for k := range values {
				values1[k] = make([]interface{}, 0, length4)
			}
			for i := range fields {
				if _, ok := indexes[i]; ok {
					continue
				}
				fields1 = append(fields1, fields[i])
				for k := range values {
					values1[k] = append(values1[k], values[k][i])
				}
			}
			fields, values = fields1, values1
			length1, length2 = len(fields), len(values)
			if length1 == 0 || length2 == 0 {
				return s
			}
		}
	}

	if s.permit != nil {
		// Delete fields and values that are not allowed to be inserted.
		indexes := make(map[int]*struct{}, length1)
		for i := 0; i < length1; i++ {
			if _, ok := s.permitMap[fields[i]]; ok {
				indexes[i] = &struct{}{}
			}
		}
		if length3 := len(indexes); length3 > 0 {
			length4 := length1 - length3
			fields1 := make([]string, 0, length4)
			values1 := make([][]interface{}, length2)
			for k := range values {
				values1[k] = make([]interface{}, 0, length4)
			}
			for i := range fields {
				if _, ok := indexes[i]; !ok {
					continue
				}
				fields1 = append(fields1, fields[i])
				for k := range values {
					values1[k] = append(values1[k], values[k][i])
				}
			}
			fields, values = fields1, values1
			length1, length2 = len(fields), len(values)
			if length1 == 0 || length2 == 0 {
				return s
			}
		}
	}

	s.fieldsLastIndex = 0
	s.fieldsFieldIndex = make(map[string]int, length1*2)
	for i := 0; i < length1; i++ {
		if _, ok := s.fieldsFieldIndex[fields[i]]; !ok {
			s.fieldsFieldIndex[fields[i]] = s.fieldsLastIndex
			s.fieldsLastIndex++
		}
	}

	s.fields, s.values = fields, values
	return s
}

// FieldValue append field-value for insert one or more rows.
func (s *Add) FieldValue(field string, value interface{}) *Add {
	if s.except != nil {
		if _, ok := s.exceptMap[field]; ok {
			return s
		}
	}
	if s.permit != nil {
		if _, ok := s.permitMap[field]; !ok {
			return s
		}
	}

	// Replacement value already exists.
	if index, ok := s.fieldsFieldIndex[field]; ok {
		for i := range s.values {
			s.values[i][index] = value
		}
		return s
	}

	// It does not exist, add a key-value pair.
	s.fieldsFieldIndex[field] = s.fieldsLastIndex
	s.fieldsLastIndex++
	s.fields = append(s.fields, field)
	for i := range s.values {
		s.values[i] = append(s.values[i], value)
	}

	return s
}

// Default Add field = value .
func (s *Add) Default(fc func(o *Add)) *Add {
	if fc == nil {
		return s
	}

	// copy current object.
	v := *s
	add := &v

	// reset value for .fieldsLastIndex .fieldsFieldIndex .fields .values .
	add.fieldsLastIndex = 0
	add.fieldsFieldIndex = make(map[string]int, 32)
	add.fields = nil
	add.values = make([][]interface{}, 1)

	fc(add)

	num := len(add.fields)
	if num > 0 && len(add.values) == 1 {
		if num == len(add.values[0]) {
			// Batch add default key-value.
			s.defaultFields = append(s.defaultFields, add.fields...)
			for index, field := range add.fields {
				s.defaultFieldsValues[field] = add.values[0][index]
			}
		}
	}

	return s
}

// Create value of create should be one of struct{}, *struct{}, map[string]interface{}, []struct, []*struct{}, *[]struct{}, *[]*struct{}.
func (s *Add) Create(create interface{}) *Add {
	if fieldValue, ok := create.(map[string]interface{}); ok {
		for field, value := range fieldValue {
			s.FieldValue(field, value)
		}
		return s
	}

	return s.FieldsValues(StructInsert(create, s.schema.way.cfg.ScanTag, s.except, s.permit))
}

// ValuesSubQuery values is a query SQL statement.
func (s *Add) ValuesSubQuery(prepare string, args []interface{}) *Add {
	s.subQuery = NewSubQuery(prepare, args)
	return s
}

// ValuesSubQueryGet values is a query SQL statement.
func (s *Add) ValuesSubQueryGet(get *Get, fields ...string) *Add {
	if get == nil {
		return s
	}
	if length := len(fields); length > 0 {
		if s.except != nil {
			for _, c := range fields {
				if _, ok := s.exceptMap[c]; ok {
					return s
				}
			}
		}
		if s.permit != nil {
			for _, c := range fields {
				if _, ok := s.permitMap[c]; !ok {
					return s
				}
			}
		}
		s.fields = fields
	}
	return s.ValuesSubQuery(get.SQL())
}

// SQL build SQL statement.
func (s *Add) SQL() (prepare string, args []interface{}) {
	if s.schema.table == EmptyString {
		return
	}

	buf := comment(s.schema)
	defer putStringBuilder(buf)

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
		args = subArgs
		prepare = buf.String()
		return
	}

	count := len(s.fields)
	if count == 0 {
		return
	}

	addDefault := false
	for _, field := range s.defaultFields {
		if _, ok := s.fieldsFieldIndex[field]; ok {
			continue
		}
		value, ok := s.defaultFieldsValues[field]
		if !ok {
			continue
		}
		s.FieldValue(field, value)
		if !addDefault {
			addDefault = true
		}
	}
	if addDefault {
		count = len(s.fields)
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
	prepare = buf.String()
	return
}

// Add execute the built SQL statement.
func (s *Add) Add() (int64, error) {
	prepare, args := s.SQL()
	if prepare == EmptyString {
		return 0, nil
	}
	return s.schema.way.ExecContext(s.schema.ctx, prepare, args...)
}

// ReturningId execute the built SQL statement, returning auto-increment field value.
func (s *Add) ReturningId(helpers ...Helper) (id int64, err error) {
	helper := s.schema.way.cfg.Helper
	for i := len(helpers) - 1; i >= 0; i-- {
		if helpers[i] != nil {
			helper = helpers[i]
			break
		}
	}
	prepare, args := s.SQL()
	return s.schema.way.InsertReturningId(s.schema.ctx, helper, prepare, args)
}

// Way get current *Way.
func (s *Add) Way() *Way {
	return s.schema.way
}

// modify set the field to be updated.
type modify struct {
	expr string
	args []interface{}
}

// Mod for UPDATE.
type Mod struct {
	schema *schema

	// update updated fields.
	update map[string]*modify

	// updateSlice the fields to be updated are updated sequentially.
	updateSlice []string

	// update2 [default] updated fields, the effective condition is len(update) > 0.
	update2 map[string]*modify

	// update2Slice [default] the fields to be updated are updated sequentially.
	update2Slice []string

	// except excepted fields.
	except      map[string]*struct{}
	exceptSlice []string

	// permit permitted fields.
	permit      map[string]*struct{}
	permitSlice []string

	where Filter
}

// NewMod for UPDATE.
func NewMod(way *Way) *Mod {
	return &Mod{
		schema:  newSchema(way),
		update:  make(map[string]*modify, 8),
		update2: make(map[string]*modify, 8),
		except:  make(map[string]*struct{}, 32),
		permit:  make(map[string]*struct{}, 32),
	}
}

// Comment set comment.
func (s *Mod) Comment(comment string) *Mod {
	s.schema.comment = comment
	return s
}

// Context set context.
func (s *Mod) Context(ctx context.Context) *Mod {
	s.schema.ctx = ctx
	return s
}

// Table set table name.
func (s *Mod) Table(table string) *Mod {
	s.schema.table = table
	return s
}

// Except exclude some fields from update.
func (s *Mod) Except(except ...string) *Mod {
	length := len(except)
	remove := make(map[string]*struct{}, length)
	for i := 0; i < length; i++ {
		if except[i] == EmptyString {
			continue
		}
		if _, ok := s.except[except[i]]; ok {
			continue
		}
		s.except[except[i]] = &struct{}{}
		s.exceptSlice = append(s.exceptSlice, except[i])

		if _, ok := s.permit[except[i]]; ok {
			remove[except[i]] = &struct{}{}
		}
	}

	if count := len(remove); count > 0 {
		latest := make([]string, 0, len(s.permitSlice))
		for _, field := range s.permitSlice {
			if _, ok := remove[field]; ok {
				delete(s.permit, field)
				continue
			}
			latest = append(latest, field)
		}
		s.permitSlice = latest
	}
	return s
}

// Permit Sets a list of fields that can only be updated.
func (s *Mod) Permit(permit ...string) *Mod {
	length := len(permit)
	remove := make(map[string]*struct{}, length)
	for i := 0; i < length; i++ {
		if permit[i] == EmptyString {
			continue
		}
		if _, ok := s.permit[permit[i]]; ok {
			continue
		}
		s.permit[permit[i]] = &struct{}{}
		s.permitSlice = append(s.permitSlice, permit[i])

		if _, ok := s.except[permit[i]]; ok {
			remove[permit[i]] = &struct{}{}
		}
	}

	if count := len(remove); count > 0 {
		latest := make([]string, 0, len(s.exceptSlice))
		for _, field := range s.exceptSlice {
			if _, ok := remove[field]; ok {
				delete(s.except, field)
				continue
			}
			latest = append(latest, field)
		}
		s.exceptSlice = latest
	}
	return s
}

// fieldExprArgs SET field = expr .
func (s *Mod) fieldExprArgs(field string, expr string, args ...interface{}) *Mod {
	if field == EmptyString || expr == EmptyString {
		return s
	}

	if s.exceptSlice != nil {
		if _, ok := s.except[field]; ok {
			return s
		}
	}

	if s.permitSlice != nil {
		if _, ok := s.permit[field]; !ok {
			return s
		}
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

// Default SET field = expr .
func (s *Mod) Default(fc func(o *Mod)) *Mod {
	if fc == nil {
		return s
	}

	// copy current object.
	v := *s
	mod := &v

	// reset value for .update .updateSlice .
	mod.update = make(map[string]*modify, 8)
	mod.updateSlice = nil
	fc(mod)

	if mod.updateSlice != nil {
		// Batch add default key-value.
		s.update2Slice = append(s.update2Slice, mod.updateSlice...)
		for field, update := range mod.update {
			s.update2[field] = update
		}
	}

	return s
}

// Expr update field using custom expr.
func (s *Mod) Expr(field string, expr string, args ...interface{}) *Mod {
	field, expr = strings.TrimSpace(field), strings.TrimSpace(expr)
	return s.fieldExprArgs(field, expr, args...)
}

// Set field = value.
func (s *Mod) Set(field string, value interface{}) *Mod {
	return s.fieldExprArgs(field, fmt.Sprintf("%s = %s", field, SqlPlaceholder), value)
}

// Incr SET field = field + value.
func (s *Mod) Incr(field string, value interface{}) *Mod {
	return s.fieldExprArgs(field, fmt.Sprintf("%s = %s + %s", field, field, SqlPlaceholder), value)
}

// Decr SET field = field - value.
func (s *Mod) Decr(field string, value interface{}) *Mod {
	return s.fieldExprArgs(field, fmt.Sprintf("%s = %s - %s", field, field, SqlPlaceholder), value)
}

// SetCase SET salary = CASE WHEN department_id = 1 THEN salary * 1.1 WHEN department_id = 2 THEN salary * 1.05 ELSE salary.
func (s *Mod) SetCase(field string, value func(c *Case)) *Mod {
	if field == EmptyString || value == nil {
		return s
	}

	c := GetCase()
	defer PutCase(c)

	value(c)
	expr, args := c.SQL()
	if expr == EmptyString {
		return s
	}

	return s.fieldExprArgs(field, fmt.Sprintf("%s = %s", field, expr), args...)
}

// FieldsValues SET field = value by slice, require len(fields) == len(values).
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

// Modify value of modify should be one of struct{}, *struct{}, map[string]interface{}.
func (s *Mod) Modify(modify interface{}) *Mod {
	if fieldValue, ok := modify.(map[string]interface{}); ok {
		for field, value := range fieldValue {
			s.Set(field, value)
		}
		return s
	}
	return s.FieldsValues(StructModify(modify, s.schema.way.cfg.ScanTag, s.exceptSlice...))
}

// Update for compare origin and latest to automatically calculate need to update fields.
func (s *Mod) Update(originObject interface{}, latestObject interface{}) *Mod {
	return s.FieldsValues(StructUpdate(originObject, latestObject, s.schema.way.cfg.ScanTag, s.exceptSlice...))
}

// Where set where.
func (s *Mod) Where(where Filter) *Mod {
	s.where = where
	return s
}

// SetSQL prepare args of set.
func (s *Mod) SetSQL() (prepare string, args []interface{}) {
	length := len(s.update)
	if length == 0 {
		return
	}
	exists := make(map[string]*struct{}, 32)
	fields := make([]string, 0, length)
	for _, field := range s.updateSlice {
		exists[field] = &struct{}{}
		fields = append(fields, field)
	}
	for _, field := range s.update2Slice {
		if _, ok := exists[field]; !ok {
			fields = append(fields, field)
		}
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
		field[k] = s.update2[v].expr
		value = append(value, s.update2[v].args...)
	}
	buf := getStringBuilder()
	defer putStringBuilder(buf)
	buf.WriteString(strings.Join(field, ", "))
	prepare = buf.String()
	args = value
	return
}

// SQL build SQL statement.
func (s *Mod) SQL() (prepare string, args []interface{}) {
	if s.schema.table == EmptyString {
		return
	}
	prepare, args = s.SetSQL()
	if prepare == EmptyString {
		return
	}
	buf := comment(s.schema)
	defer putStringBuilder(buf)
	buf.WriteString("UPDATE ")
	buf.WriteString(s.schema.table)
	buf.WriteString(" SET ")
	buf.WriteString(prepare)

	cfg := s.schema.way.cfg
	if cfg.DeleteMustUseWhere && (s.where == nil || s.where.IsEmpty()) {
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

// Mod execute the built SQL statement.
func (s *Mod) Mod() (int64, error) {
	prepare, args := s.SQL()
	if prepare == EmptyString {
		return 0, nil
	}
	return s.schema.way.ExecContext(s.schema.ctx, prepare, args...)
}

// Way get current *Way.
func (s *Mod) Way() *Way {
	return s.schema.way
}

// F make new Filter.
func (s *Mod) F(fs ...Filter) Filter {
	return F().New(fs...)
}

// GetWith CTE: Common Table Expression.
type GetWith struct {
	recursive bool
	alias     string
	prepare   string
	args      []interface{}
}

func NewWith() *GetWith {
	return &GetWith{}
}

func (s *GetWith) Recursive(recursive bool) *GetWith {
	s.recursive = recursive
	return s
}

func (s *GetWith) With(alias string, prepare string, args []interface{}) *GetWith {
	if alias == EmptyString || prepare == EmptyString {
		return s
	}
	s.alias, s.prepare, s.args = alias, prepare, args
	return s
}

func (s *GetWith) WithGet(alias string, get *Get) *GetWith {
	if alias == EmptyString || get == nil {
		return s
	}
	prepare, args := get.SQL()
	if prepare == EmptyString {
		return s
	}
	return s.With(alias, prepare, args)
}

func (s *GetWith) SQL() (prepare string, args []interface{}) {
	b := getStringBuilder()
	defer putStringBuilder(b)
	if s.recursive {
		b.WriteString("RECURSIVE")
		b.WriteString(SqlSpace)
	}
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

func NewSubQuery(prepare string, args []interface{}) *SubQuery {
	return &SubQuery{
		prepare: prepare,
		args:    args,
	}
}

func (s *SubQuery) SQL() (prepare string, args []interface{}) {
	prepare, args = s.prepare, s.args
	return
}

// GetJoin join SQL statement.
type GetJoin struct {
	joinType string    // join type.
	table    string    // table name.
	subQuery *SubQuery // table is sub query.
	alias    *string   // query table alias name.
	on       string    // conditions for join query; ON a.order_id = b.order_id  <=> USING ( order_id ).
	using    []string  // conditions for join query; USING ( order_id, ... ).
}

// Table set table name.
func (s *GetJoin) Table(table string) *GetJoin {
	s.table = table
	return s
}

// SubQuery table is a query SQL statement.
func (s *GetJoin) SubQuery(prepare string, args []interface{}) *GetJoin {
	s.subQuery = NewSubQuery(prepare, args)
	return s
}

// SubQueryGet table is a query SQL statement.
func (s *GetJoin) SubQueryGet(get *Get, alias ...string) *GetJoin {
	if get == nil {
		return s
	}
	s.subQuery = NewSubQuery(get.SQL())
	if str := LastNotEmptyString(alias); str != EmptyString {
		s.Alias(str)
	}
	return s
}

// Alias for table alias name, don't forget to call the current method when the table is a SQL statement.
func (s *GetJoin) Alias(alias string) *GetJoin {
	s.alias = &alias
	return s
}

// On join query condition.
func (s *GetJoin) On(on string) *GetJoin {
	s.on = on
	return s
}

// Using join query condition.
func (s *GetJoin) Using(fields ...string) *GetJoin {
	s.using = fields
	return s
}

// OnEqual join query condition, support multiple fields.
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

// SQL build SQL statement.
func (s *GetJoin) SQL() (prepare string, args []interface{}) {
	if s.table == EmptyString && (s.subQuery == nil || s.alias == nil || *s.alias == EmptyString) {
		return
	}

	buf := getStringBuilder()
	defer putStringBuilder(buf)

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

	if len(s.using) > 0 {
		buf.WriteString(" USING ( ")
		buf.WriteString(strings.Join(s.using, ", "))
		buf.WriteString(" )")
	} else {
		if s.on != EmptyString {
			buf.WriteString(" ON ")
			buf.WriteString(s.on)
		}
	}

	prepare = buf.String()
	return
}

// Limiter limit and offset.
type Limiter interface {
	GetLimit() int64
	GetOffset() int64
}

// Get for SELECT.
type Get struct {
	// query table.
	schema *schema

	// with of query.
	with []*GetWith

	// distinct remove duplicate records: one field value or a combination of multiple fields.
	distinct bool

	// query field list.
	column []string

	// query field list.
	columnCase []string

	// query field list args.
	columnCaseArgs []interface{}

	// the query table is a sub query.
	subQuery *SubQuery

	// set an alias for the queried table.
	alias *string

	// join query.
	join []*GetJoin

	// WHERE condition to filter data.
	where Filter

	// group query result.
	group []string

	// use HAVING to filter data after grouping.
	having Filter

	// unions query with union.
	unions []*Get

	// unionsType query with union type.
	unionsType string

	// order query result.
	order []string

	// ordered columns map list.
	orderMap map[string]string

	// limit the number of query result.
	limit *int64

	// query result offset.
	offset *int64

	// native SQL statement.
	rawPrepare *string

	// args of rawPrepare.
	rawArgs []interface{}
}

// NewGet for SELECT.
func NewGet(way *Way) *Get {
	return &Get{
		schema:   newSchema(way),
		orderMap: map[string]string{},
	}
}

// Comment set comment.
func (s *Get) Comment(comment string) *Get {
	s.schema.comment = comment
	return s
}

// Context set context.
func (s *Get) Context(ctx context.Context) *Get {
	s.schema.ctx = ctx
	return s
}

// Raw Directly set the native SQL statement and the corresponding parameter list.
func (s *Get) Raw(prepare string, args []interface{}) *Get {
	if prepare != EmptyString {
		s.rawPrepare, s.rawArgs = &prepare, args
	}
	return s
}

// With for with query.
func (s *Get) With(with ...*GetWith) *Get {
	s.with = append(s.with, with...)
	return s
}

// Table set table name.
func (s *Get) Table(table string, alias ...string) *Get {
	s.schema.table = table
	if str := LastNotEmptyString(alias); str != EmptyString {
		s.Alias(str)
	}
	return s
}

// SubQuery table is a query SQL statement.
func (s *Get) SubQuery(prepare string, args []interface{}) *Get {
	s.subQuery = NewSubQuery(prepare, args)
	return s
}

// SubQueryGet table is a query SQL statement.
func (s *Get) SubQueryGet(get *Get, alias ...string) *Get {
	if get == nil {
		return s
	}
	s.subQuery = NewSubQuery(get.SQL())
	if name := LastNotEmptyString(alias); name != EmptyString {
		s.Alias(name)
	}
	return s
}

// Alias for table alias name, don't forget to call the current method when the table is a SQL statement.
func (s *Get) Alias(alias string) *Get {
	s.alias = &alias
	return s
}

// joins for join one or more tables.
func (s *Get) joins(joins ...*GetJoin) *Get {
	length := len(joins)
	for i := 0; i < length; i++ {
		if joins[i] == nil {
			continue
		}
		s.join = append(s.join, joins[i])
	}
	return s
}

// typeJoin join with join-type.
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
	return s.joins(joins...)
}

// InnerJoin for inner join.
func (s *Get) InnerJoin(fs ...func(j *GetJoin)) *Get {
	return s.typeJoin(SqlJoinInner, fs)
}

// LeftJoin for left join.
func (s *Get) LeftJoin(fs ...func(j *GetJoin)) *Get {
	return s.typeJoin(SqlJoinLeft, fs)
}

// RightJoin for right join.
func (s *Get) RightJoin(fs ...func(j *GetJoin)) *Get {
	return s.typeJoin(SqlJoinRight, fs)
}

// FullJoin for full join.
func (s *Get) FullJoin(fs ...func(j *GetJoin)) *Get {
	return s.typeJoin(SqlJoinFull, fs)
}

// CrossJoin for cross join.
func (s *Get) CrossJoin(fs ...func(j *GetJoin)) *Get {
	return s.typeJoin(SqlJoinCross, fs)
}

// Where set where.
func (s *Get) Where(where Filter) *Get {
	s.where = where
	return s
}

// Group set group columns.
func (s *Get) Group(group ...string) *Get {
	s.group = append(s.group, group...)
	return s
}

// Having set filter of group result.
func (s *Get) Having(having Filter) *Get {
	s.having = having
	return s
}

// Union for union(After calling the current method, only WITH, ORDER BY, LIMIT, and OFFSET are valid for the current query attributes.).
func (s *Get) Union(unions ...*Get) *Get {
	s.unions = unions
	s.unionsType = SqlUnion
	return s
}

// UnionAll for union all(After calling the current method, only WITH, ORDER BY, LIMIT, and OFFSET are valid for the current query attributes.).
func (s *Get) UnionAll(unions ...*Get) *Get {
	s.unions = unions
	s.unionsType = SqlUnionAll
	return s
}

// Column set the columns list of query.
func (s *Get) Column(column ...string) *Get {
	s.column = column
	return s
}

// AddColumn append the columns list of query.
func (s *Get) AddColumn(column ...string) *Get {
	s.column = append(s.column, column...)
	return s
}

// AddColumnCase append the columns list of query.
func (s *Get) AddColumnCase(caseList ...func(c *Case)) *Get {
	fc := func(fc func(c *Case)) (string, []interface{}) {
		if fc == nil {
			return EmptyString, nil
		}
		c := GetCase()
		defer PutCase(c)
		fc(c)
		return c.SQL()
	}

	for _, c := range caseList {
		k, v := fc(c)
		if k == EmptyString {
			continue
		}

		s.columnCase = append(s.columnCase, k)
		if v != nil {
			s.columnCaseArgs = append(s.columnCaseArgs, v...)
		}
	}
	return s
}

// Distinct Remove duplicate records: one field value or a combination of multiple fields.
func (s *Get) Distinct(distinct bool) *Get {
	s.distinct = distinct
	return s
}

// orderBy set order by column.
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

// Asc set order by column ASC.
func (s *Get) Asc(column string) *Get {
	return s.orderBy(column, SqlAsc)
}

// Desc set order by column Desc.
func (s *Get) Desc(column string) *Get {
	return s.orderBy(column, SqlDesc)
}

var (
	// orderRegexp `column_name_first:a,column_name_second:d` => `column_name_first ASC, column_name_second DESC`.
	orderRegexp = regexp.MustCompile(`^([a-zA-Z][a-zA-Z0-9_]*([.][a-zA-Z][a-zA-Z0-9_]*)*):([ad])$`)
)

// Order set the column sorting list in batches through regular expressions according to the request parameter value.
func (s *Get) Order(order string, orderMap ...map[string]string) *Get {
	fieldMap := make(map[string]string, 8)
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
		length = len(matched) // the length should be 4.
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

// Limit set limit.
func (s *Get) Limit(limit int64) *Get {
	if limit <= 0 {
		// cancel the set value.
		s.limit = nil
		return s
	}
	s.limit = &limit
	return s
}

// Offset set offset.
func (s *Get) Offset(offset int64) *Get {
	if offset <= 0 {
		// cancel the set value.
		s.offset = nil
		return s
	}
	s.offset = &offset
	return s
}

// Limiter set limit and offset at the same time.
func (s *Get) Limiter(limiter Limiter) *Get {
	if limiter == nil {
		return s
	}
	return s.Limit(limiter.GetLimit()).Offset(limiter.GetOffset())
}

// BuildWith Build SQL WITH.
// [WITH a AS ( xxx )[, b AS ( xxx ) ]].
func BuildWith(withs []*GetWith) (prepare string, args []interface{}) {
	b := getStringBuilder()
	defer putStringBuilder(b)

	b.WriteString("WITH ")

	num := 0

	for _, with := range withs {
		prepareThis, argsThis := with.SQL()
		if prepareThis == EmptyString {
			continue
		}
		if num > 0 {
			b.WriteString(", ")
		}
		num++
		b.WriteString(prepareThis)
		args = append(args, argsThis...)
	}

	if num == 0 {
		args = nil
		return
	}

	b.WriteString(SqlSpace)
	prepare = b.String()

	return
}

// BuildUnion Combines the results of two or more SELECT queries into a single result set.
// [WITH xxx]
// /*query1*/( [WITH xxx] SELECT xxx FROM xxx [INNER JOIN xxx ON xxx] [WHERE xxx] [GROUP BY xxx [HAVING xxx]] [ORDER BY xxx] [LIMIT xxx [OFFSET xxx]])
// UNION [ALL]
// /*query2*/( [WITH xxx] SELECT xxx FROM xxx [INNER JOIN xxx ON xxx] [WHERE xxx] [GROUP BY xxx [HAVING xxx]] [ORDER BY xxx] [LIMIT xxx [OFFSET xxx]])
// [ORDER BY xxx] [LIMIT xxx [OFFSET xxx]]
func BuildUnion(withs []*GetWith, unionType string, gets []*Get) (prepare string, args []interface{}) {
	b := getStringBuilder()
	defer putStringBuilder(b)

	if unionType == EmptyString {
		unionType = SqlUnionAll
	}

	num := 0

	for _, get := range gets {
		if get == nil {
			continue
		}

		prepareThis, argsThis := get.SQL()
		if prepareThis == EmptyString {
			continue
		}

		if num == 0 {
			if withs != nil {
				prepareWith, argsWith := BuildWith(withs)
				b.WriteString(prepareWith)
				args = append(args, argsWith...)
			}
		} else {
			b.WriteString(SqlSpace)
			b.WriteString(unionType)
			b.WriteString(SqlSpace)
		}

		num++

		b.WriteString("( ")
		b.WriteString(prepareThis)
		b.WriteString(" )")

		args = append(args, argsThis...)
	}

	if num > 1 {
		prepare = strings.TrimSpace(b.String())
	} else {
		args = nil
	}

	return
}

// BuildTable Build query table (without ORDER BY, LIMIT, OFFSET).
// [WITH xxx] SELECT xxx FROM xxx [INNER JOIN xxx ON xxx] [WHERE xxx] [GROUP BY xxx [HAVING xxx]]
func BuildTable(s *Get) (prepare string, args []interface{}) {
	if s.rawPrepare != nil {
		prepare, args = *s.rawPrepare, s.rawArgs
		return
	}

	if s.unions != nil {
		prepare, args = BuildUnion(s.with, s.unionsType, s.unions)
		return
	}

	if s.schema.table == EmptyString && s.subQuery == nil {
		return
	}

	buf := comment(s.schema)
	defer putStringBuilder(buf)

	if s.with != nil {
		prepareWith, argsWith := BuildWith(s.with)
		buf.WriteString(prepareWith)
		args = append(args, argsWith...)
	}

	buf.WriteString("SELECT ")

	if s.distinct {
		buf.WriteString(SqlDistinct)
		buf.WriteString(SqlSpace)
	}

	if s.column == nil && s.columnCase == nil {
		buf.WriteString(SqlStar)
	} else {
		column := s.column
		if s.columnCase != nil {
			column = append(column, s.columnCase...)
			if s.columnCaseArgs != nil {
				args = append(args, s.columnCaseArgs...)
			}
		}
		buf.WriteString(strings.Join(column, ", "))
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

	prepare = strings.TrimSpace(buf.String())

	return
}

// BuildOrderByLimitOffset Build query table of ORDER BY, LIMIT, OFFSET.
// [ORDER BY xxx] [LIMIT xxx [OFFSET xxx]]
func BuildOrderByLimitOffset(s *Get) (prepare string) {
	buf := getStringBuilder()
	defer putStringBuilder(buf)

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

	return buf.String()
}

// BuildGet Build a complete query.
// [WITH xxx] SELECT xxx FROM xxx [INNER JOIN xxx ON xxx] [WHERE xxx] [GROUP BY xxx [HAVING xxx]] [ORDER BY xxx] [LIMIT xxx [OFFSET xxx]]
func BuildGet(s *Get) (prepare string, args []interface{}) {
	prepare, args = BuildTable(s)
	if prepare == EmptyString {
		return
	}

	buf := getStringBuilder()
	defer putStringBuilder(buf)

	buf.WriteString(prepare)

	orderByLimitOffset := BuildOrderByLimitOffset(s)
	if orderByLimitOffset != EmptyString {
		buf.WriteString(orderByLimitOffset)
	}

	prepare = strings.TrimSpace(buf.String())

	return
}

// BuildCount Build count query.
// SELECT COUNT(*) AS count FROM ( [WITH xxx] SELECT xxx FROM xxx [INNER JOIN xxx ON xxx] [WHERE xxx] [GROUP BY xxx [HAVING xxx]] ) AS a
// SELECT COUNT(*) AS count FROM ( query1 UNION [ALL] query2 [UNION [ALL] ...] ) AS a
func BuildCount(s *Get, countColumns ...string) (prepare string, args []interface{}) {
	if countColumns == nil {
		countColumns = []string{
			SqlAlias("COUNT(*)", DefaultAliasNameCount),
		}
	}

	prepare, args = BuildTable(s)
	if prepare == EmptyString {
		return EmptyString, nil
	}

	return NewGet(s.schema.way).
		Column(countColumns...).
		SubQuery(prepare, args).
		Alias(AliasA).
		SQL()
}

// SQL build SQL statement.
func (s *Get) SQL() (prepare string, args []interface{}) {
	return BuildGet(s)
}

// SQLCount build SQL statement for count.
func (s *Get) SQLCount(columns ...string) (string, []interface{}) {
	return BuildCount(s, columns...)
}

// GetCount execute the built SQL statement and scan query result for count.
func GetCount(get *Get, countColumns ...string) (count int64, err error) {
	prepare, args := BuildCount(get, countColumns...)
	err = get.schema.way.QueryContext(get.schema.ctx, func(rows *sql.Rows) (err error) {
		if rows.Next() {
			err = rows.Scan(&count)
		}
		return
	}, prepare, args...)
	return
}

// GetQuery execute the built SQL statement and scan query result.
func GetQuery(get *Get, query func(rows *sql.Rows) (err error)) error {
	prepare, args := BuildGet(get)
	return get.schema.way.QueryContext(get.schema.ctx, query, prepare, args...)
}

// GetGet execute the built SQL statement and scan query result.
func GetGet(get *Get, result interface{}) error {
	prepare, args := BuildGet(get)
	return get.schema.way.TakeAllContext(get.schema.ctx, result, prepare, args...)
}

// GetExists Determine whether the query result exists.
func GetExists(get *Get) (exists bool, err error) {
	get.Limit(1)
	err = GetQuery(get, func(rows *sql.Rows) error {
		exists = rows.Next()
		return nil
	})
	return
}

// GetScanAll execute the built SQL statement and scan all from the query results.
func GetScanAll(get *Get, fc func(rows *sql.Rows) error) error {
	return GetQuery(get, func(rows *sql.Rows) error {
		return get.schema.way.ScanAll(rows, fc)
	})
}

// GetScanOne execute the built SQL statement and scan at most once from the query results.
func GetScanOne(get *Get, dest ...interface{}) error {
	return GetQuery(get, func(rows *sql.Rows) error {
		return get.schema.way.ScanOne(rows, dest...)
	})
}

// GetViewMap execute the built SQL statement and scan all from the query results.
func GetViewMap(get *Get) (result []map[string]interface{}, err error) {
	err = GetQuery(get, func(rows *sql.Rows) (err error) {
		result, err = ScanViewMap(rows)
		return
	})
	return
}

// GetCountQuery execute the built SQL statement and scan query result, count + query.
func GetCountQuery(get *Get, query func(rows *sql.Rows) (err error), countColumn ...string) (int64, error) {
	count, err := GetCount(get, countColumn...)
	if err != nil {
		return 0, err
	}
	if count <= 0 {
		return 0, nil
	}
	return count, GetQuery(get, query)
}

// GetCountGet execute the built SQL statement and scan query result, count + get.
func GetCountGet(get *Get, result interface{}, countColumn ...string) (int64, error) {
	count, err := GetCount(get, countColumn...)
	if err != nil {
		return 0, err
	}
	if count <= 0 {
		return 0, nil
	}
	return count, GetGet(get, result)
}

// Count execute the built SQL statement and scan query result for count.
func (s *Get) Count(column ...string) (int64, error) {
	return GetCount(s, column...)
}

// Query execute the built SQL statement and scan query result.
func (s *Get) Query(query func(rows *sql.Rows) (err error)) error {
	return GetQuery(s, query)
}

// Get execute the built SQL statement and scan query result.
func (s *Get) Get(result interface{}) error {
	return GetGet(s, result)
}

// Exists Determine whether the query result exists.
func (s *Get) Exists() (bool, error) {
	return GetExists(s)
}

// ScanAll execute the built SQL statement and scan all from the query results.
func (s *Get) ScanAll(fc func(rows *sql.Rows) error) error {
	return GetScanAll(s, fc)
}

// ScanOne execute the built SQL statement and scan at most once from the query results.
func (s *Get) ScanOne(dest ...interface{}) error {
	return GetScanOne(s, dest...)
}

// ViewMap execute the built SQL statement and scan all from the query results.
func (s *Get) ViewMap() ([]map[string]interface{}, error) {
	return GetViewMap(s)
}

// CountQuery execute the built SQL statement and scan query result, count + query.
func (s *Get) CountQuery(query func(rows *sql.Rows) (err error), countColumn ...string) (int64, error) {
	return GetCountQuery(s, query, countColumn...)
}

// CountGet execute the built SQL statement and scan query result, count + get.
func (s *Get) CountGet(result interface{}, countColumn ...string) (int64, error) {
	return GetCountGet(s, result, countColumn...)
}

// F make new Filter.
func (s *Get) F(fs ...Filter) Filter {
	return F().New(fs...)
}

// Way get current *Way.
func (s *Get) Way() *Way {
	return s.schema.way
}

// getLink Use keywords to connect multiple queries.
func getLink(keyword string, gets ...*Get) (prepare string, args []interface{}) {
	if keyword == EmptyString {
		return
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	added := false
	for _, v := range gets {
		if v == nil {
			continue
		}
		prepareThis, argsThis := BuildGet(v)
		if prepareThis == EmptyString {
			continue
		}
		if added {
			b.WriteString(EmptyString)
			b.WriteString(keyword)
			b.WriteString(EmptyString)
		} else {
			added = true
		}
		b.WriteString(SqlLeftSmallBracket)
		b.WriteString(SqlSpace)
		b.WriteString(prepareThis)
		b.WriteString(SqlSpace)
		b.WriteString(SqlRightSmallBracket)
		args = append(args, argsThis...)
	}
	prepare = b.String()
	return
}

// ExpectGet (query1) EXCEPT (query2) EXCEPT (query3)...
func ExpectGet(gets ...*Get) (prepare string, args []interface{}) {
	return getLink(SqlExpect, gets...)
}

// IntersectGet (query1) INTERSECT (query2) INTERSECT (query3)...
func IntersectGet(gets ...*Get) (prepare string, args []interface{}) {
	return getLink(SqlIntersect, gets...)
}
