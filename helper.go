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

// schema used to store basic information such as context.Context, *Way, SQL comment, table name.
type schema struct {
	ctx     context.Context
	way     *Way
	comment string
	table   TableCmder
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
func (s *Del) Table(table string, args ...interface{}) *Del {
	s.schema.table = NewTableCmder(table, args)
	return s
}

// TableCmder Set table script.
func (s *Del) TableCmder(cmder TableCmder) *Del {
	if cmder == nil || IsEmptyCmder(cmder) {
		return s
	}
	s.schema.table = cmder
	return s
}

// Where set where.
func (s *Del) Where(where func(where Filter)) *Del {
	if where == nil {
		return s
	}
	if s.where == nil {
		s.where = s.schema.way.F()
	}
	where(s.where)
	return s
}

// Cmd build SQL statement.
func (s *Del) Cmd() (prepare string, args []interface{}) {
	if s.schema.table == nil || s.schema.table.IsEmpty() {
		return
	}

	b := comment(s.schema)
	defer putStringBuilder(b)
	b.WriteString("DELETE ")
	b.WriteString("FROM ")

	table, param := s.schema.table.Cmd()
	b.WriteString(s.schema.way.NameReplace(table))
	if param != nil {
		args = append(args, param...)
	}

	cfg := s.schema.way.cfg
	if cfg.DeleteMustUseWhere && (s.where == nil || s.where.IsEmpty()) {
		prepare, args = EmptyString, nil
		return
	}

	if s.where != nil && !s.where.IsEmpty() {
		where, whereArgs := s.where.Cmd()
		b.WriteString(" WHERE ")
		b.WriteString(where)
		if whereArgs != nil {
			args = append(args, whereArgs...)
		}
	}

	prepare = b.String()
	return
}

// Del execute the built SQL statement.
func (s *Del) Del() (int64, error) {
	prepare, args := s.Cmd()
	return s.schema.way.ExecContext(s.schema.ctx, prepare, args...)
}

// Way get current *Way.
func (s *Del) Way() *Way {
	return s.schema.way
}

// Add for INSERT.
type Add struct {
	schema        *schema
	except        InsertField
	permit        InsertField
	fields        InsertField
	values        InsertValue
	fieldsDefault InsertField
	valuesDefault InsertValue
	fromCmder     Cmder
}

// NewAdd for INSERT.
func NewAdd(way *Way) *Add {
	add := &Add{
		schema: newSchema(way),
		fields: way.InsertFieldCmd(),
		values: NewInsertValue(),
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
	s.schema.table = NewTableCmder(table, nil)
	return s
}

// ExceptPermit Set a list of fields that are not allowed to be inserted and a list of fields that are only allowed to be inserted.
func (s *Add) ExceptPermit(custom func(except InsertField, permit InsertField)) *Add {
	if custom != nil {
		if s.except == nil {
			s.except = s.schema.way.InsertFieldCmd()
		}
		if s.permit == nil {
			s.permit = s.schema.way.InsertFieldCmd()
		}
		custom(s.except, s.permit)
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
	s.fields.SetFields(fields)
	s.values.SetValues(values...)
	if s.permit != nil {
		columns := s.permit.GetFieldsMap()
		deletes := make([]string, 0, 32)
		for _, field := range s.fields.GetFields() {
			if _, ok := columns[field]; !ok {
				deletes = append(deletes, field)
			}
		}
		s.fields.Del(deletes...)
	}
	if s.except != nil {
		columns := s.except.GetFieldsMap()
		deletes := make([]string, 0, 32)
		for _, field := range s.fields.GetFields() {
			if _, ok := columns[field]; ok {
				deletes = append(deletes, field)
			}
		}
		s.fields.Del(deletes...)
	}
	return s
}

// FieldValue append field-value for insert one or more rows.
func (s *Add) FieldValue(field string, value interface{}) *Add {
	if s.permit != nil {
		if !s.permit.FieldExists(field) {
			return s
		}
	}
	if s.except != nil {
		if s.except.FieldExists(field) {
			return s
		}
	}
	s.fields.Add(field)
	s.values.Set(s.fields.FieldIndex(field), value)
	return s
}

// Default Add field = value .
func (s *Add) Default(add func(add *Add)) *Add {
	if add == nil {
		return s
	}
	v := *s
	tmp := &v
	tmp.fields, tmp.values = s.schema.way.InsertFieldCmd(), NewInsertValue()
	add(tmp)
	if !tmp.fields.IsEmpty() && !tmp.values.IsEmpty() {
		if s.fieldsDefault == nil {
			s.fieldsDefault = s.schema.way.InsertFieldCmd()
		}
		if tmp.valuesDefault == nil {
			s.valuesDefault = NewInsertValue()
		}
		fields, values := tmp.fields.GetFields(), tmp.values.GetValues()
		for index, field := range fields {
			s.fieldsDefault.Add(field)
			s.valuesDefault.Set(s.fieldsDefault.FieldIndex(field), values[0][index])
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
	return s.FieldsValues(StructInsert(create, s.schema.way.cfg.ScanTag, s.except.GetFields(), s.permit.GetFields()))
}

// CmderValues values is a query SQL statement.
func (s *Add) CmderValues(cmdValues Cmder, fields []string) *Add {
	if cmdValues == nil || IsEmptyCmder(cmdValues) {
		return s
	}
	s.fields = s.schema.way.InsertFieldCmd().SetFields(fields)
	s.fromCmder = cmdValues
	return s
}

// Cmd build SQL statement.
func (s *Add) Cmd() (prepare string, args []interface{}) {
	if s.schema.table == nil || s.schema.table.IsEmpty() {
		return
	}
	b := comment(s.schema)
	defer putStringBuilder(b)
	if s.fromCmder != nil {
		b.WriteString("INSERT ")
		b.WriteString("INTO ")
		table, _ := s.schema.table.Cmd()
		b.WriteString(s.schema.way.NameReplace(table))
		if !IsEmptyCmder(s.fields) {
			b.WriteString(SqlSpace)
			fields, _ := s.fields.Cmd()
			b.WriteString(fields)
		}
		b.WriteString(SqlSpace)
		fromPrepare, fromArgs := s.fromCmder.Cmd()
		b.WriteString(fromPrepare)
		if fromArgs != nil {
			args = append(args, fromArgs...)
		}
		prepare = b.String()
		return
	}
	if IsEmptyCmder(s.fields) || IsEmptyCmder(s.values) {
		return
	}
	b.WriteString("INSERT ")
	b.WriteString("INTO ")
	table, _ := s.schema.table.Cmd()
	b.WriteString(s.schema.way.NameReplace(table))
	b.WriteString(SqlSpace)
	// add default field-value
	if s.fieldsDefault != nil {
		fields, values := s.fieldsDefault.GetFields(), s.valuesDefault.GetValues()
		if len(values) > 0 && len(fields) == len(values[0]) {
			for index, field := range fields {
				if s.fields.FieldExists(field) {
					continue
				}
				s.FieldValue(field, values[0][index])
			}
		}
	}
	fields, _ := s.fields.Cmd()
	b.WriteString(fields)
	b.WriteString(SqlSpace)
	values, param := s.values.Cmd()
	b.WriteString("VALUES ")
	b.WriteString(values)
	if param != nil {
		args = append(args, param...)
	}
	prepare = b.String()
	return
}

// Add execute the built SQL statement.
func (s *Add) Add() (int64, error) {
	prepare, args := s.Cmd()
	if prepare == EmptyString {
		return 0, nil
	}
	return s.schema.way.ExecContext(s.schema.ctx, prepare, args...)
}

// AddGetId execute the built SQL statement, returning auto-increment field value.
func (s *Add) AddGetId(
	adjust func(prepare string, args []interface{}) (string, []interface{}),
	custom func(ctx context.Context, stmt *Stmt, args []interface{}) (id int64, err error),
) (id int64, err error) {
	if custom == nil {
		return 0, nil
	}
	prepare, args := s.Cmd()
	if prepare == EmptyString {
		return 0, nil
	}
	if adjust != nil {
		prepare, args = adjust(prepare, args)
	}
	stmt, err := s.schema.way.PrepareContext(s.schema.ctx, prepare)
	if err != nil {
		return 0, err
	}
	defer func() { _ = stmt.Close() }()
	return custom(s.schema.ctx, stmt, args)
}

// Way get current *Way.
func (s *Add) Way() *Way {
	return s.schema.way
}

// Mod for UPDATE.
type Mod struct {
	schema        *schema
	except        InsertField
	permit        InsertField
	update        UpdateSet
	updateDefault UpdateSet
	where         Filter
}

// NewMod for UPDATE.
func NewMod(way *Way) *Mod {
	return &Mod{
		schema: newSchema(way),
		update: way.UpdateSet(),
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
func (s *Mod) Table(table string, args ...interface{}) *Mod {
	s.schema.table = NewTableCmder(table, args)
	return s
}

// ExceptPermit Set a list of fields that are not allowed to be updated and a list of fields that are only allowed to be updated.
func (s *Mod) ExceptPermit(custom func(except InsertField, permit InsertField)) *Mod {
	if custom != nil {
		if s.except == nil {
			s.except = s.schema.way.InsertFieldCmd()
		}
		if s.permit == nil {
			s.permit = s.schema.way.InsertFieldCmd()
		}
		custom(s.except, s.permit)
	}
	return s
}

// Default SET field = expr .
func (s *Mod) Default(custom func(mod *Mod)) *Mod {
	if custom == nil {
		return s
	}
	tmp := *s
	mod := &tmp
	mod.update = s.schema.way.UpdateSet()
	custom(mod)
	if !mod.update.IsEmpty() {
		if s.updateDefault == nil {
			s.updateDefault = s.schema.way.UpdateSet()
		}
		prepares, args := mod.update.GetUpdate()
		for index, prepare := range prepares {
			if s.update.UpdateExists(prepare) {
				continue
			}
			s.update.Update(prepare, args[index]...)
		}
	}
	return s
}

// Expr update field using custom expression.
func (s *Mod) Expr(expr string, args ...interface{}) *Mod {
	s.update.Update(expr, args...)
	return s
}

// Set field = value.
func (s *Mod) Set(field string, value interface{}) *Mod {
	if s.permit != nil {
		if !s.permit.FieldExists(field) {
			return s
		}
	}
	if s.except != nil {
		if s.except.FieldExists(field) {
			return s
		}
	}
	s.update.Set(field, value)
	return s
}

// Incr SET field = field + value.
func (s *Mod) Incr(field string, value interface{}) *Mod {
	if s.permit != nil {
		if !s.permit.FieldExists(field) {
			return s
		}
	}
	if s.except != nil {
		if s.except.FieldExists(field) {
			return s
		}
	}
	s.update.Incr(field, value)
	return s
}

// Decr SET field = field - value.
func (s *Mod) Decr(field string, value interface{}) *Mod {
	if s.permit != nil {
		if !s.permit.FieldExists(field) {
			return s
		}
	}
	if s.except != nil {
		if s.except.FieldExists(field) {
			return s
		}
	}
	s.update.Decr(field, value)
	return s
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

// Update Value of update should be one of struct{}, *struct{}, map[string]interface{}.
func (s *Mod) Update(update interface{}) *Mod {
	if fieldValue, ok := update.(map[string]interface{}); ok {
		for field, value := range fieldValue {
			s.Set(field, value)
		}
		return s
	}
	return s.FieldsValues(StructModify(update, s.schema.way.cfg.ScanTag))
}

// Contrast Compare old and new to automatically calculate need to update fields.
func (s *Mod) Contrast(old, new interface{}, except ...string) *Mod {
	return s.FieldsValues(StructUpdate(old, new, s.schema.way.cfg.ScanTag, except...))
}

// Where set where.
func (s *Mod) Where(where func(where Filter)) *Mod {
	if where == nil {
		return s
	}
	if s.where == nil {
		s.where = s.schema.way.F()
	}
	where(s.where)
	return s
}

// GetUpdateSet prepare args of SET.
func (s *Mod) GetUpdateSet() (prepare string, args []interface{}) {
	if s.update.IsEmpty() {
		return
	}
	return s.update.Cmd()
}

// Cmd build SQL statement.
func (s *Mod) Cmd() (prepare string, args []interface{}) {
	if s.schema.table == nil || s.schema.table.IsEmpty() {
		return
	}
	update, updateArgs := s.GetUpdateSet()
	if update == EmptyString {
		return
	}
	b := comment(s.schema)
	defer putStringBuilder(b)
	b.WriteString("UPDATE ")

	table, param := s.schema.table.Cmd()
	b.WriteString(s.schema.way.NameReplace(table))
	if param != nil {
		args = append(args, param...)
	}

	b.WriteString(" SET ")
	b.WriteString(update)
	if updateArgs != nil {
		args = append(args, updateArgs...)
	}

	cfg := s.schema.way.cfg
	if cfg.DeleteMustUseWhere && (s.where == nil || s.where.IsEmpty()) {
		prepare, args = EmptyString, nil
		return
	}

	if s.where != nil && !s.where.IsEmpty() {
		where, whereArgs := s.where.Cmd()
		b.WriteString(" WHERE ")
		b.WriteString(where)
		if whereArgs != nil {
			args = append(args, whereArgs...)
		}
	}

	prepare = b.String()
	return
}

// Mod execute the built SQL statement.
func (s *Mod) Mod() (int64, error) {
	prepare, args := s.Cmd()
	if prepare == EmptyString {
		return 0, nil
	}
	return s.schema.way.ExecContext(s.schema.ctx, prepare, args...)
}

// Way get current *Way.
func (s *Mod) Way() *Way {
	return s.schema.way
}

// Limiter limit and offset.
type Limiter interface {
	GetLimit() int64
	GetOffset() int64
}

// Get for SELECT.
type Get struct {
	schema *schema
	with   QueryWith
	fields QueryField
	join   QueryJoin
	where  Filter
	group  QueryGroup
	order  QueryOrder
	limit  QueryLimit
}

// NewGet for SELECT.
func NewGet(way *Way) *Get {
	return &Get{
		schema: newSchema(way),
		fields: way.QueryField(),
		where:  way.F(),
		group:  way.QueryGroup(),
		order:  way.QueryOrder(),
		limit:  NewQueryLimit(),
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

// With for with query.
func (s *Get) With(alias string, script Cmder) *Get {
	if alias == EmptyString || IsEmptyCmder(script) {
		return s
	}
	if s.with == nil {
		s.with = NewQueryWith()
	}
	s.with.Add(alias, script)
	return s
}

// Table set table name.
func (s *Get) Table(table string) *Get {
	s.schema.table = NewTableCmder(table, nil)
	return s
}

// Alias for table alias name, don't forget to call the current method when the table is a SQL statement.
func (s *Get) Alias(alias string) *Get {
	s.schema.table.Alias(alias)
	return s
}

// Subquery table is a subquery.
func (s *Get) Subquery(subquery Cmder, alias string) *Get {
	if IsEmptyCmder(subquery) {
		return s
	}
	if alias == EmptyString {
		if tab, ok := subquery.(*Get); ok && tab != nil {
			alias = tab.schema.table.GetAlias()
			if alias != EmptyString {
				tab.Alias(EmptyString)
				defer tab.Alias(alias)
			}
		}
		if alias == EmptyString {
			if tab, ok := subquery.(TableCmder); ok && tab != nil {
				alias = tab.GetAlias()
				if alias != EmptyString {
					tab.Alias(EmptyString)
					defer tab.Alias(alias)
				}
			}
		}
	}
	if alias == EmptyString {
		return s
	}
	prepare, args := subquery.Cmd()
	s.schema.table = NewTableCmder(ConcatString("( ", prepare, " )"), args).Alias(alias)
	return s
}

// Join for `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN` ...
func (s *Get) Join(custom func(join QueryJoin)) *Get {
	if custom == nil {
		return s
	}
	if s.join != nil {
		custom(s.join)
		return s
	}
	master := s.schema.table
	if IsEmptyCmder(master) {
		return s
	}
	alias := master.GetAlias()
	prepare, args := master.Alias(EmptyString).Cmd()
	if alias == EmptyString {
		alias = AliasA // set master default alias name.
	} else {
		master.Alias(alias) // restore master default alias name.
	}
	s.join = s.schema.way.QueryJoin().SetMaster(s.schema.way.QueryJoinTable(prepare, alias, args...))
	custom(s.join)
	return s
}

// Where set where.
func (s *Get) Where(where func(where Filter)) *Get {
	if where == nil {
		return s
	}
	if s.where == nil {
		s.where = s.schema.way.F()
	}
	where(s.where)
	return s
}

// Group set group columns.
func (s *Get) Group(group ...string) *Get {
	s.group.Group(group...)
	return s
}

// Having set filter of group result.
func (s *Get) Having(having func(having Filter)) *Get {
	s.group.Having(having)
	return s
}

// Fields set the fields list of query.
func (s *Get) Fields(custom func(fields QueryField)) *Get {
	if custom == nil {
		return s
	}
	tmp := s.schema.way.QueryField()
	custom(tmp)
	if tmp.Len() > 0 {
		s.fields = tmp
	}
	return s
}

// Asc set order by column ASC.
func (s *Get) Asc(column string) *Get {
	s.order.Asc(column)
	return s
}

// Desc set order by column Desc.
func (s *Get) Desc(column string) *Get {
	s.order.Desc(column)
	return s
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
			if k == EmptyString || v == EmptyString {
				continue
			}
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
	s.limit.Limit(limit)
	return s
}

// Offset set offset.
func (s *Get) Offset(offset int64) *Get {
	s.limit.Offset(offset)
	return s
}

// Limiter set limit and offset at the same time.
func (s *Get) Limiter(limiter Limiter) *Get {
	if limiter == nil {
		return s
	}
	return s.Limit(limiter.GetLimit()).Offset(limiter.GetOffset())
}

// CmderGetTable Build query table (without ORDER BY, LIMIT, OFFSET).
// [WITH xxx] SELECT xxx FROM xxx [INNER JOIN xxx ON xxx] [WHERE xxx] [GROUP BY xxx [HAVING xxx]]
func CmderGetTable(s *Get) (prepare string, args []interface{}) {
	if s.schema.table == nil || s.schema.table.IsEmpty() {
		return
	}
	b := comment(s.schema)
	defer putStringBuilder(b)
	if s.with != nil {
		prepareWith, argsWith := s.with.Cmd()
		if prepareWith != EmptyString {
			b.WriteString(prepareWith)
			b.WriteString(SqlSpace)
			if argsWith != nil {
				args = append(args, argsWith...)
			}
		}
	}
	if s.join != nil {
		joinPrepare, joinArgs := s.join.Cmd()
		if joinPrepare != EmptyString {
			b.WriteString(joinPrepare)
			if joinArgs != nil {
				args = append(args, joinArgs...)
			}
		}
	} else {
		b.WriteString("SELECT ")
		fields, fieldsArgs := s.fields.Cmd()
		b.WriteString(fields)
		if fieldsArgs != nil {
			args = append(args, fieldsArgs...)
		}
		b.WriteString(" FROM ")
		table, param := s.schema.table.Cmd()
		b.WriteString(s.schema.way.NameReplace(table))
		if param != nil {
			args = append(args, param...)
		}
	}
	if s.where != nil && !s.where.IsEmpty() {
		where, whereArgs := s.where.Cmd()
		b.WriteString(" WHERE ")
		b.WriteString(where)
		args = append(args, whereArgs...)
	}
	if s.group != nil && !s.group.IsEmpty() {
		b.WriteString(SqlSpace)
		group, groupArgs := s.group.Cmd()
		b.WriteString(group)
		if groupArgs != nil {
			args = append(args, groupArgs...)
		}
	}
	prepare = strings.TrimSpace(b.String())
	return
}

// CmderGetOrderLimitOffset Build query table of ORDER BY, LIMIT, OFFSET.
// [ORDER BY xxx] [LIMIT xxx [OFFSET xxx]]
func CmderGetOrderLimitOffset(s *Get) (prepare string, args []interface{}) {
	b := getStringBuilder()
	defer putStringBuilder(b)
	if !s.order.IsEmpty() {
		order, orderArgs := s.order.Cmd()
		if order != EmptyString {
			b.WriteString(SqlSpace)
			b.WriteString(order)
			if orderArgs != nil {
				args = append(args, orderArgs...)
			}
		}
	}
	if !s.limit.IsEmpty() {
		limit, limitArgs := s.limit.Cmd()
		if limit != EmptyString {
			b.WriteString(SqlSpace)
			b.WriteString(limit)
			if limitArgs != nil {
				args = append(args, limitArgs...)
			}
		}
	}
	prepare = b.String()
	return
}

// CmderGetCmd Build a complete query.
// [WITH xxx] SELECT xxx FROM xxx [INNER JOIN xxx ON xxx] [WHERE xxx] [GROUP BY xxx [HAVING xxx]] [ORDER BY xxx] [LIMIT xxx [OFFSET xxx]]
func CmderGetCmd(s *Get) (prepare string, args []interface{}) {
	prepare, args = CmderGetTable(s)
	if prepare == EmptyString {
		return
	}
	if s.group != nil && !s.group.IsEmpty() {
		return
	}
	b := getStringBuilder()
	defer putStringBuilder(b)
	b.WriteString(prepare)
	if tmp, param := CmderGetOrderLimitOffset(s); tmp != EmptyString {
		b.WriteString(tmp)
		if param != nil {
			args = append(args, param...)
		}
	}
	prepare = strings.TrimSpace(b.String())
	return
}

// CmderGetCount Build count query.
// SELECT COUNT(*) AS count FROM ( [WITH xxx] SELECT xxx FROM xxx [INNER JOIN xxx ON xxx] [WHERE xxx] [GROUP BY xxx [HAVING xxx]] ) AS a
// SELECT COUNT(*) AS count FROM ( query1 UNION [ALL] query2 [UNION [ALL] ...] ) AS a
func CmderGetCount(s *Get, countColumns ...string) (prepare string, args []interface{}) {
	if countColumns == nil {
		countColumns = []string{
			SqlAlias("COUNT(*)", s.schema.way.NameReplace(DefaultAliasNameCount)),
		}
	}
	if IsEmptyCmder(s) {
		return
	}
	return NewGet(s.schema.way).
		Fields(func(fields QueryField) { fields.AddFields(countColumns...) }).
		Subquery(s, AliasA).
		Cmd()
}

// Cmd build SQL statement.
func (s *Get) Cmd() (prepare string, args []interface{}) {
	return CmderGetCmd(s)
}

// CountCmd build SQL statement for count.
func (s *Get) CountCmd(columns ...string) (string, []interface{}) {
	return CmderGetCount(s, columns...)
}

// GetCount execute the built SQL statement and scan query result for count.
func GetCount(get *Get, countColumns ...string) (count int64, err error) {
	prepare, args := CmderGetCount(get, countColumns...)
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
	prepare, args := CmderGetCmd(get)
	return get.schema.way.QueryContext(get.schema.ctx, query, prepare, args...)
}

// GetGet execute the built SQL statement and scan query result.
func GetGet(get *Get, result interface{}) error {
	prepare, args := CmderGetCmd(get)
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

// Way get current *Way.
func (s *Get) Way() *Way {
	return s.schema.way
}
