// Quickly build INSERT, DELETE, UPDATE, SELECT statements.
// Also allows you to call them to get the corresponding results.

package hey

import (
	"context"
	"database/sql"
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
	// struct tag name value used as table.column name.
	tag string

	// only allowed fields Hash table.
	allow map[string]*struct{}

	// ignored fields Hash table.
	except map[string]*struct{}

	// already existing fields Hash table.
	used map[string]*struct{}

	// struct reflects type, make sure it is the same structure type.
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

// schema used to store basic information such as context.Context, *Way, SQL comment, table name.
type schema struct {
	ctx     context.Context
	way     *Way
	comment string
	table   SQLTable
}

// newSchema new schema with *Way.
func newSchema(way *Way) *schema {
	return &schema{
		ctx: context.Background(),
		way: way,
	}
}

// comment make SQL statement builder, SqlPlaceholder `?` should not appear in comments.
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

// GetContext get context.
func (s *Del) GetContext() context.Context {
	return s.schema.ctx
}

// Table set table name.
func (s *Del) Table(table string, args ...any) *Del {
	s.schema.table = NewSQLTable(s.schema.way.Replace(table), args)
	return s
}

// SQLTable Set table script.
func (s *Del) SQLTable(sqlTable SQLTable) *Del {
	if sqlTable == nil || IsEmptyCmder(sqlTable) {
		return s
	}
	s.schema.table = sqlTable
	return s
}

// Where set where.
func (s *Del) Where(where func(f Filter)) *Del {
	if where == nil {
		return s
	}
	if s.where == nil {
		s.where = s.schema.way.F()
	}
	where(s.where)
	return s
}

// Filters Using multiple WHERE conditions.
func (s *Del) Filters(filters ...Filter) *Del {
	return s.Where(func(f Filter) {
		f.Use(filters...)
	})
}

// Cmd build SQL statement.
func (s *Del) Cmd() (prepare string, args []any) {
	if s.schema.table == nil || s.schema.table.IsEmpty() {
		return
	}

	b := comment(s.schema)
	defer putStringBuilder(b)
	b.WriteString(ConcatString(SqlDelete, SqlSpace, SqlFrom, SqlSpace))

	table, param := s.schema.table.Cmd()
	b.WriteString(s.schema.way.Replace(table))
	if param != nil {
		args = append(args, param...)
	}

	cfg := s.schema.way.cfg
	if cfg.DeleteMustUseWhere && (s.where == nil || s.where.IsEmpty()) {
		prepare, args = EmptyString, nil
		return
	}

	if s.where != nil && !s.where.IsEmpty() {
		where, whereArgs := ParcelFilter(s.where).Cmd()
		b.WriteString(ConcatString(SqlSpace, SqlWhere, SqlSpace))
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

// GetWay get current *Way.
func (s *Del) GetWay() *Way {
	return s.schema.way
}

// SetWay use the specified *Way object.
func (s *Del) SetWay(way *Way) *Del {
	if way != nil {
		s.schema.way = way
	}
	return s
}

// Add for INSERT.
type Add struct {
	schema         *schema
	except         SQLUpsertColumn
	permit         SQLUpsertColumn
	columns        SQLUpsertColumn
	values         SQLInsertValue
	columnsDefault SQLUpsertColumn
	valuesDefault  SQLInsertValue
	fromCmder      Cmder
}

// NewAdd for INSERT.
func NewAdd(way *Way) *Add {
	add := &Add{
		schema:  newSchema(way),
		columns: NewSQLUpsertColumn(way),
		values:  NewSQLInsertValue(),
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

// GetContext get context.
func (s *Add) GetContext() context.Context {
	return s.schema.ctx
}

// Table set table name.
func (s *Add) Table(table string) *Add {
	s.schema.table = NewSQLTable(s.schema.way.Replace(table), nil)
	return s
}

// ExceptPermit Set a list of columns that are not allowed to be inserted and a list of columns that are only allowed to be inserted.
func (s *Add) ExceptPermit(custom func(except SQLUpsertColumn, permit SQLUpsertColumn)) *Add {
	if custom == nil {
		return s
	}
	except, permit := s.except, s.permit
	if except == nil {
		except = NewSQLUpsertColumn(s.schema.way)
	}
	if permit == nil {
		permit = NewSQLUpsertColumn(s.schema.way)
	}
	custom(except, permit)
	s.except, s.permit = except, permit
	if s.except.IsEmpty() {
		s.except = nil
	}
	if s.permit.IsEmpty() {
		s.permit = nil
	}
	return s
}

// ColumnsValues set columns and values.
func (s *Add) ColumnsValues(columns []string, values [][]any) *Add {
	length1, length2 := len(columns), len(values)
	if length1 == 0 || length2 == 0 {
		return s
	}
	for _, v := range values {
		if length1 != len(v) {
			return s
		}
	}
	s.columns.SetColumns(columns)
	s.values.SetValues(values...)
	if s.permit != nil {
		columns1 := s.permit.GetColumnsMap()
		deletes := make([]string, 0, 32)
		for _, column := range s.columns.GetColumns() {
			if _, ok := columns1[column]; !ok {
				deletes = append(deletes, column)
			}
		}
		for _, column := range deletes {
			s.values.Del(s.columns.ColumnIndex(column))
			s.columns.Del(column)
		}
	}
	if s.except != nil {
		columns1 := s.except.GetColumnsMap()
		deletes := make([]string, 0, 32)
		for _, column := range s.columns.GetColumns() {
			if _, ok := columns1[column]; ok {
				deletes = append(deletes, column)
			}
		}
		for _, column := range deletes {
			s.values.Del(s.columns.ColumnIndex(column))
			s.columns.Del(column)
		}
	}
	return s
}

// ColumnValue append column-value for insert one or more rows.
func (s *Add) ColumnValue(column string, value any) *Add {
	if s.permit != nil {
		if !s.permit.ColumnExists(column) {
			return s
		}
	}
	if s.except != nil {
		if s.except.ColumnExists(column) {
			return s
		}
	}
	s.columns.Add(column)
	s.values.Set(s.columns.ColumnIndex(column), value)
	return s
}

// Default Add column = value .
func (s *Add) Default(add func(add *Add)) *Add {
	if add == nil {
		return s
	}
	v := *s
	tmp := &v
	tmp.columns, tmp.values = NewSQLUpsertColumn(s.schema.way), NewSQLInsertValue()
	add(tmp)
	if !tmp.columns.IsEmpty() && !tmp.values.IsEmpty() {
		if s.columnsDefault == nil {
			s.columnsDefault = NewSQLUpsertColumn(s.schema.way)
		}
		if tmp.valuesDefault == nil {
			s.valuesDefault = NewSQLInsertValue()
		}
		columns, values := tmp.columns.GetColumns(), tmp.values.GetValues()
		for index, column := range columns {
			s.columnsDefault.Add(column)
			s.valuesDefault.Set(s.columnsDefault.ColumnIndex(column), values[0][index])
		}
	}
	return s
}

// Create value of creation should be one of struct{}, *struct{}, map[string]any, []struct, []*struct{}, *[]struct{}, *[]*struct{}.
func (s *Add) Create(create any) *Add {
	if columnValue, ok := create.(map[string]any); ok {
		for column, value := range columnValue {
			s.ColumnValue(column, value)
		}
		return s
	}
	var except, permit []string
	if s.except != nil {
		except = s.except.GetColumns()
	}
	if s.permit != nil {
		permit = s.permit.GetColumns()
	}
	return s.ColumnsValues(StructInsert(create, s.schema.way.cfg.ScanTag, except, permit))
}

// CmderValues values is a query SQL statement.
func (s *Add) CmderValues(cmdValues Cmder, columns []string) *Add {
	if cmdValues == nil || IsEmptyCmder(cmdValues) {
		return s
	}
	s.columns = NewSQLUpsertColumn(s.schema.way).SetColumns(columns)
	s.fromCmder = cmdValues
	return s
}

// GetColumns list of columns to insert.
func (s *Add) GetColumns() []string {
	return s.columns.GetColumns()
}

// GetValues list of values to insert.
func (s *Add) GetValues() [][]any {
	return s.values.GetValues()
}

// Cmd build SQL statement.
func (s *Add) Cmd() (prepare string, args []any) {
	if s.schema.table == nil || s.schema.table.IsEmpty() {
		return
	}
	b := comment(s.schema)
	defer putStringBuilder(b)
	if s.fromCmder != nil {
		b.WriteString(ConcatString(SqlInsert, SqlSpace, SqlInto, SqlSpace))
		table, _ := s.schema.table.Cmd()
		b.WriteString(s.schema.way.Replace(table))
		if !IsEmptyCmder(s.columns) {
			b.WriteString(SqlSpace)
			columns, _ := s.columns.Cmd()
			b.WriteString(columns)
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
	if IsEmptyCmder(s.columns) || IsEmptyCmder(s.values) {
		return
	}
	b.WriteString(ConcatString(SqlInsert, SqlSpace, SqlInto, SqlSpace))
	table, _ := s.schema.table.Cmd()
	b.WriteString(s.schema.way.Replace(table))
	b.WriteString(SqlSpace)
	// add default column-value
	if s.columnsDefault != nil {
		columns, values := s.columnsDefault.GetColumns(), s.valuesDefault.GetValues()
		if len(values) > 0 && len(columns) == len(values[0]) {
			for index, column := range columns {
				if s.columns.ColumnExists(column) {
					continue
				}
				s.ColumnValue(column, values[0][index])
			}
		}
	}
	columns, _ := s.columns.Cmd()
	b.WriteString(columns)
	b.WriteString(SqlSpace)
	values, param := s.values.Cmd()
	b.WriteString(ConcatString(SqlValues, SqlSpace))
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

// AddOne execute the built SQL statement, return the sequence value of the data.
func (s *Add) AddOne(custom func(add AddOneReturnSequenceValue)) (int64, error) {
	add := s.schema.way.NewAddOne(s.Cmd()).Context(s.schema.ctx)
	custom(add)
	return add.AddOne()
}

// GetWay get current *Way.
func (s *Add) GetWay() *Way {
	return s.schema.way
}

// SetWay use the specified *Way object.
func (s *Add) SetWay(way *Way) *Add {
	if way != nil {
		s.schema.way = way
	}
	return s
}

// Mod for UPDATE.
type Mod struct {
	schema        *schema
	except        SQLUpsertColumn
	permit        SQLUpsertColumn
	update        SQLUpdateSet
	updateDefault SQLUpdateSet
	where         Filter
}

// NewMod for UPDATE.
func NewMod(way *Way) *Mod {
	return &Mod{
		schema: newSchema(way),
		update: NewSQLUpdateSet(way),
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

// GetContext get context.
func (s *Mod) GetContext() context.Context {
	return s.schema.ctx
}

// Table set table name.
func (s *Mod) Table(table string, args ...any) *Mod {
	s.schema.table = NewSQLTable(s.schema.way.Replace(table), args)
	return s
}

// ExceptPermit Set a list of columns that are not allowed to be updated and a list of columns that are only allowed to be updated.
func (s *Mod) ExceptPermit(custom func(except SQLUpsertColumn, permit SQLUpsertColumn)) *Mod {
	if custom == nil {
		return s
	}
	except, permit := s.except, s.permit
	if except == nil {
		except = NewSQLUpsertColumn(s.schema.way)
	}
	if permit == nil {
		permit = NewSQLUpsertColumn(s.schema.way)
	}
	custom(except, permit)
	s.except, s.permit = except, permit
	if s.except.IsEmpty() {
		s.except = nil
	}
	if s.permit.IsEmpty() {
		s.permit = nil
	}
	return s
}

// Default SET column = expr .
func (s *Mod) Default(custom func(mod *Mod)) *Mod {
	if custom == nil {
		return s
	}
	tmp := *s
	mod := &tmp
	mod.update = NewSQLUpdateSet(s.schema.way)
	custom(mod)
	if !mod.update.IsEmpty() {
		if s.updateDefault == nil {
			s.updateDefault = NewSQLUpdateSet(s.schema.way)
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

// Expr update column using custom expression.
func (s *Mod) Expr(expr string, args ...any) *Mod {
	s.update.Update(expr, args...)
	return s
}

// Set column = value.
func (s *Mod) Set(column string, value any) *Mod {
	if s.permit != nil {
		if !s.permit.ColumnExists(column) {
			return s
		}
	}
	if s.except != nil {
		if s.except.ColumnExists(column) {
			return s
		}
	}
	s.update.Set(column, value)
	return s
}

// Incr SET column = column + value.
func (s *Mod) Incr(column string, value any) *Mod {
	if s.permit != nil {
		if !s.permit.ColumnExists(column) {
			return s
		}
	}
	if s.except != nil {
		if s.except.ColumnExists(column) {
			return s
		}
	}
	s.update.Incr(column, value)
	return s
}

// Decr SET column = column - value.
func (s *Mod) Decr(column string, value any) *Mod {
	if s.permit != nil {
		if !s.permit.ColumnExists(column) {
			return s
		}
	}
	if s.except != nil {
		if s.except.ColumnExists(column) {
			return s
		}
	}
	s.update.Decr(column, value)
	return s
}

// ColumnsValues SET column = value by slice, require len(columns) == len(values).
func (s *Mod) ColumnsValues(columns []string, values []any) *Mod {
	len1, len2 := len(columns), len(values)
	if len1 != len2 {
		return s
	}
	for i := range len1 {
		s.Set(columns[i], values[i])
	}
	return s
}

// Update Value of update should be one of anyStruct, *anyStruct, map[string]any.
func (s *Mod) Update(update any) *Mod {
	if columnValue, ok := update.(map[string]any); ok {
		for column, value := range columnValue {
			s.Set(column, value)
		}
		return s
	}
	return s.ColumnsValues(StructModify(update, s.schema.way.cfg.ScanTag))
}

// Compare For compare old and new to automatically calculate the need to update columns.
func (s *Mod) Compare(old, new any, except ...string) *Mod {
	return s.ColumnsValues(StructUpdate(old, new, s.schema.way.cfg.ScanTag, except...))
}

// Where set where.
func (s *Mod) Where(where func(f Filter)) *Mod {
	if where == nil {
		return s
	}
	if s.where == nil {
		s.where = s.schema.way.F()
	}
	where(s.where)
	return s
}

// Filters Using multiple WHERE conditions.
func (s *Mod) Filters(filters ...Filter) *Mod {
	return s.Where(func(f Filter) {
		f.Use(filters...)
	})
}

// GetUpdateSet prepare args of SET.
func (s *Mod) GetUpdateSet() (prepare string, args []any) {
	if s.update.IsEmpty() {
		return
	}
	return s.update.Cmd()
}

// Cmd build SQL statement.
func (s *Mod) Cmd() (prepare string, args []any) {
	if s.schema.table == nil || s.schema.table.IsEmpty() {
		return
	}
	update, updateArgs := s.GetUpdateSet()
	if update == EmptyString {
		return
	}
	b := comment(s.schema)
	defer putStringBuilder(b)
	b.WriteString(ConcatString(SqlUpdate, SqlSpace))

	table, param := s.schema.table.Cmd()
	b.WriteString(s.schema.way.Replace(table))
	if param != nil {
		args = append(args, param...)
	}

	b.WriteString(ConcatString(SqlSpace, SqlSet, SqlSpace))
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
		where, whereArgs := ParcelFilter(s.where).Cmd()
		b.WriteString(ConcatString(SqlSpace, SqlWhere, SqlSpace))
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

// GetWay get current *Way.
func (s *Mod) GetWay() *Way {
	return s.schema.way
}

// SetWay use the specified *Way object.
func (s *Mod) SetWay(way *Way) *Mod {
	if way != nil {
		s.schema.way = way
	}
	return s
}

// Limiter limit and offset.
type Limiter interface {
	GetLimit() int64
	GetOffset() int64
}

// Get for SELECT.
type Get struct {
	schema  *schema
	with    SQLWith
	columns SQLSelect
	join    SQLJoin
	where   Filter
	group   SQLGroupBy
	order   SQLOrderBy
	limit   SQLLimit
}

// NewGet for SELECT.
func NewGet(way *Way) *Get {
	return &Get{
		schema:  newSchema(way),
		columns: NewSQLSelect(way),
		where:   way.F(),
		group:   NewSQLGroupBy(way),
		order:   NewSQLOrderBy(way),
		limit:   NewSQLLimit(),
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

// GetContext get context.
func (s *Get) GetContext() context.Context {
	return s.schema.ctx
}

// With for with query, COMMON TABLE EXPRESSION.
func (s *Get) With(fc func(w SQLWith)) *Get {
	if fc == nil {
		return s
	}
	if s.with == nil {
		s.with = NewSQLWith()
	}
	fc(s.with)
	return s
}

// Table set table name.
func (s *Get) Table(table string) *Get {
	s.schema.table = NewSQLTable(s.schema.way.Replace(table), nil)
	return s
}

// Alias for table alias name, remember to call the current method when the table is a SQL statement.
func (s *Get) Alias(alias string) *Get {
	s.schema.table.Alias(s.schema.way.Replace(alias))
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
			if tab, ok := subquery.(SQLTable); ok && tab != nil {
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
	prepare, args := ParcelCmder(subquery).Cmd()
	s.schema.table = NewSQLTable(prepare, args).Alias(alias)
	return s
}

// Join for `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN` ...
func (s *Get) Join(custom func(join SQLJoin)) *Get {
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
		alias = AliasA // set primary default alias name.
	} else {
		master.Alias(alias) // restore master default alias name.
	}
	way := s.schema.way
	s.join = NewSQLJoin(way).SetMaster(NewSQLTable(prepare, args).Alias(alias))
	custom(s.join)
	return s
}

// Where set where.
func (s *Get) Where(where func(f Filter)) *Get {
	if where == nil {
		return s
	}
	if s.where == nil {
		s.where = s.schema.way.F()
	}
	where(s.where)
	return s
}

// Filters Using multiple WHERE conditions.
func (s *Get) Filters(filters ...Filter) *Get {
	return s.Where(func(f Filter) {
		f.Use(filters...)
	})
}

// GroupBy set group columns.
func (s *Get) GroupBy(group ...string) *Get {
	s.group.Group(group...)
	return s
}

// Having set filter of group results.
func (s *Get) Having(having func(f Filter)) *Get {
	s.group.Having(having)
	return s
}

// GetSelect Get select object.
func (s *Get) GetSelect() SQLSelect {
	if s.join != nil {
		return s.join.Queries()
	}
	return s.columns
}

// SetSelect Set select object.
func (s *Get) SetSelect(sqlSelect SQLSelect) *Get {
	if sqlSelect == nil {
		return s
	}
	if s.join != nil {
		s.join.Queries().Set(sqlSelect.Get())
		return s
	}
	s.columns.Set(sqlSelect.Get())
	return s
}

// Select Set the column list for select.
func (s *Get) Select(columns ...string) *Get {
	if length := len(columns); length == 0 {
		return s
	}
	if tmp := NewSQLSelect(s.schema.way).AddAll(columns...); tmp.Len() > 0 {
		s.SetSelect(tmp)
	}
	return s
}

// OrderBy SQL ORDER BY.
func (s *Get) OrderBy(fc func(o SQLOrderBy)) *Get {
	if fc != nil {
		fc(s.order)
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

// OrderString set the column sorting list in batches through regular expressions according to the request parameter value.
func (s *Get) OrderString(order string, replaces ...map[string]string) *Get {
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
		match := orderRegexp.FindAllStringSubmatch(strings.TrimSpace(v), -1)
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
func CmderGetTable(s *Get) (prepare string, args []any) {
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
		b.WriteString(ConcatString(SqlSelect, SqlSpace))
		columns, columnsArgs := s.columns.Cmd()
		b.WriteString(columns)
		if columnsArgs != nil {
			args = append(args, columnsArgs...)
		}
		b.WriteString(ConcatString(SqlSpace, SqlFrom, SqlSpace))
		table, param := s.schema.table.Cmd()
		b.WriteString(s.schema.way.Replace(table))
		if param != nil {
			args = append(args, param...)
		}
	}
	if s.where != nil && !s.where.IsEmpty() {
		where, whereArgs := ParcelFilter(s.where).Cmd()
		b.WriteString(ConcatString(SqlSpace, SqlWhere, SqlSpace))
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
func CmderGetOrderLimitOffset(s *Get) (prepare string, args []any) {
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
func CmderGetCmd(s *Get) (prepare string, args []any) {
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
func CmderGetCount(s *Get, countColumns ...string) (prepare string, args []any) {
	if IsEmptyCmder(s) {
		return
	}

	if countColumns == nil {
		countColumns = []string{
			SqlAlias("COUNT(*)", s.schema.way.Replace(DefaultAliasNameCount)),
		}
	}

	selects := s.GetSelect()

	prepare, args = s.Select(countColumns...).Cmd()

	s.SetSelect(selects)
	return
}

// Cmd build SQL statement.
func (s *Get) Cmd() (prepare string, args []any) {
	return CmderGetCmd(s)
}

// CountCmd build SQL statement for count.
func (s *Get) CountCmd(columns ...string) (string, []any) {
	return CmderGetCount(s, columns...)
}

// GetCount execute the built SQL statement and scan query results for count.
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

// GetQuery execute the built SQL statement and scan query results.
func GetQuery(get *Get, query func(rows *sql.Rows) (err error)) error {
	prepare, args := CmderGetCmd(get)
	return get.schema.way.QueryContext(get.schema.ctx, query, prepare, args...)
}

// GetGet execute the built SQL statement and scan query result.
func GetGet(get *Get, result any) error {
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
func GetScanOne(get *Get, dest ...any) error {
	return GetQuery(get, func(rows *sql.Rows) error {
		return get.schema.way.ScanOne(rows, dest...)
	})
}

// GetViewMap execute the built SQL statement and scan all from the query results.
func GetViewMap(get *Get) (result []map[string]any, err error) {
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

// GetCountGet execute the built SQL statement and scan query results, count + get.
func GetCountGet(get *Get, result any, countColumn ...string) (int64, error) {
	count, err := GetCount(get, countColumn...)
	if err != nil {
		return 0, err
	}
	if count <= 0 {
		return 0, nil
	}
	return count, GetGet(get, result)
}

// Count execute the built SQL statement and scan query results for count.
func (s *Get) Count(column ...string) (int64, error) {
	return GetCount(s, column...)
}

// Query executes the built SQL statement and scan query results.
func (s *Get) Query(query func(rows *sql.Rows) (err error)) error {
	return GetQuery(s, query)
}

// Get execute the built SQL statement and scan query results.
func (s *Get) Get(result any) error {
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
func (s *Get) ScanOne(dest ...any) error {
	return GetScanOne(s, dest...)
}

// ViewMap execute the built SQL statement and scan all from the query results.
func (s *Get) ViewMap() ([]map[string]any, error) {
	return GetViewMap(s)
}

// CountQuery execute the built SQL statement and scan query results, count + query.
func (s *Get) CountQuery(query func(rows *sql.Rows) (err error), countColumn ...string) (int64, error) {
	return GetCountQuery(s, query, countColumn...)
}

// CountGet execute the built SQL statement and scan query result, count and get.
func (s *Get) CountGet(result any, countColumn ...string) (int64, error) {
	return GetCountGet(s, result, countColumn...)
}

// GetWay get current *Way.
func (s *Get) GetWay() *Way {
	return s.schema.way
}

// SetWay use the specified *Way object.
func (s *Get) SetWay(way *Way) *Get {
	if way != nil {
		s.schema.way = way
	}
	return s
}
