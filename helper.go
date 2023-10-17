package hey

import (
	"database/sql"
	"fmt"
	"reflect"
)

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

// bindStruct bind the receiving object with the query result.
type bindStruct struct {
	// store root struct properties
	direct map[string]int

	// store non-root struct properties, such as: anonymous attribute structure and named attribute structure
	indirect map[string][]int

	// all used struct types, including the root struct
	structType map[string]struct{}
}

func bindStructInit() *bindStruct {
	return &bindStruct{
		direct:     make(map[string]int),
		indirect:   make(map[string][]int),
		structType: make(map[string]struct{}),
	}
}

// binding Match the binding according to the structure "db" tag and the query column name.
// Please ensure that the type of refStructType must be `reflect.Struct`.
func (s *bindStruct) binding(refStructType reflect.Type, depth []int, tag string) {
	refStructTypeString := refStructType.String()
	if _, ok := s.structType[refStructTypeString]; ok {
		// prevent structure loop nesting
		return
	}
	s.structType[refStructTypeString] = struct{}{}
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
		if field == "" || field == "-" {
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
func (s *bindStruct) prepare(columns []string, rowsScanList []interface{}, indirect reflect.Value) error {
	length := len(columns)
	for i := 0; i < length; i++ {
		index, ok := s.direct[columns[i]]
		if ok {
			// top structure
			field := indirect.Field(index)
			if !(field.CanAddr() && field.CanSet()) {
				return fmt.Errorf("column `%s` cann't set value", columns[i])
			}
			if field.Kind() == reflect.Ptr && field.IsNil() {
				indirect.Field(index).Set(reflect.New(field.Type()).Elem())
				rowsScanList[i] = indirect.Field(index).Addr().Interface()
				continue
			}
			rowsScanList[i] = field.Addr().Interface()
			continue
		}
		// parsing multi-layer structures
		line, ok := s.indirect[columns[i]]
		if !ok {
			// unable to find mapping property for current field Use *[]byte instead to receive
			rowsScanList[i] = new([]byte)
			continue
		}
		count := len(line)
		if count < 2 {
			return fmt.Errorf("unable to determine field `%s` mapping", columns[i])
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
			if !(field.CanAddr() && field.CanSet()) {
				return fmt.Errorf("column `%s` cann't set value, multi-level", columns[i])
			}
			if field.Kind() == reflect.Ptr && field.IsNil() {
				parent.Field(line[j]).Set(reflect.New(field.Type()).Elem())
				rowsScanList[i] = parent.Field(line[j]).Addr().Interface()
				continue
			}
			rowsScanList[i] = field.Addr().Interface()
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
		return fmt.Errorf("the receiving parameter needs to be a slice pointer, yours is `%s`", typeOf.String())
	}
	var elemType reflect.Type
	setValues := valueOf.Elem()
	elemTypeIsPtr := false
	elem := typeOf.Elem().Elem()
	switch elem.Kind() {
	// *[]AnyStruct
	case reflect.Struct:
		elemType = elem
	case reflect.Ptr:
		// *[]*AnyStruct
		if elem.Elem().Kind() == reflect.Struct {
			elemType = elem.Elem()
			elemTypeIsPtr = true
		}
	}
	if elemType == nil {
		return fmt.Errorf(
			"slice elements need to be structures or pointers to structures, yours is `%s`",
			elem.String(),
		)
	}
	b := bindStructInit()
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	b.binding(elemType, nil, tag)
	length := len(columns)
	for rows.Next() {
		object := reflect.New(elemType)
		indirect := reflect.Indirect(object)
		rowsScanList := make([]interface{}, length)
		if err = b.prepare(columns, rowsScanList, indirect); err != nil {
			return err
		}
		if err = rows.Scan(rowsScanList...); err != nil {
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

// StructInsert create one by AnyStruct or *AnyStruct
// Obtain a list of all fields to be inserted and corresponding values through the tag attribute of the structure,
// and support the exclusion of fixed fields.
func StructInsert(insert interface{}, tag string, except ...string) (create map[string]interface{}) {
	if insert == nil || tag == "" {
		return
	}
	valueOf := reflect.ValueOf(insert)
	kind := valueOf.Kind()
	for ; kind == reflect.Ptr; kind = valueOf.Kind() {
		valueOf = valueOf.Elem()
	}
	if kind != reflect.Struct {
		return
	}
	length := len(except)
	ignore := make(map[string]struct{}, length)
	for i := 0; i < length; i++ {
		ignore[except[i]] = struct{}{}
	}
	create = make(map[string]interface{})
	typeOf := valueOf.Type()
	length = typeOf.NumField()
	add := func(column string, value interface{}) {
		if column == "" || column == "-" || value == nil {
			return
		}
		if _, ok := ignore[column]; ok {
			return
		}
		create[column] = value
	}
	parse := func(column string, value reflect.Value) {
		valueKind := value.Kind()
		for valueKind == reflect.Ptr {
			if value.IsNil() {
				return
			}
			value = value.Elem()
			valueKind = value.Kind()
		}
		if valueKind == reflect.Struct {
			for c, v := range StructInsert(value.Interface(), tag, except...) {
				add(c, v)
			}
			return
		}
		add(column, value.Interface())
	}
	for i := 0; i < length; i++ {
		field := typeOf.Field(i)
		if !field.IsExported() {
			continue
		}
		parse(field.Tag.Get(tag), valueOf.Field(i))
	}
	return
}

// StructUpdate compare origin and latest for update
func StructUpdate(origin interface{}, latest interface{}, tag string, except ...string) (modify map[string]interface{}) {
	if origin == nil || latest == nil || tag == "" {
		return
	}
	originTypeOf, latestTypeOf := reflect.TypeOf(origin), reflect.TypeOf(latest)
	originValueOf, latestValueOf := reflect.ValueOf(origin), reflect.ValueOf(latest)
	originKind, latestKind := originTypeOf.Kind(), latestTypeOf.Kind()
	for ; originKind == reflect.Ptr; originKind = originTypeOf.Kind() {
		originTypeOf = originTypeOf.Elem()
		originValueOf = originValueOf.Elem()
	}
	for ; latestKind == reflect.Ptr; latestKind = latestTypeOf.Kind() {
		latestTypeOf = latestTypeOf.Elem()
		latestValueOf = latestValueOf.Elem()
	}
	if originKind != reflect.Struct || latestKind != reflect.Struct {
		return
	}
	excepted := make(map[string]struct{})
	for _, field := range except {
		excepted[field] = struct{}{}
	}
	modify = make(map[string]interface{})
	latestMapValue := make(map[string]reflect.Value)
	latestMapIndex := make(map[string]int)
	length := latestValueOf.Type().NumField()
	for i := 0; i < length; i++ {
		name := latestTypeOf.Field(i).Name
		latestMapIndex[name] = i
		latestMapValue[name] = latestValueOf.Field(i)
	}
	length = originTypeOf.NumField()
	for i := 0; i < length; i++ {
		originField := originTypeOf.Field(i)
		column := originField.Tag.Get(tag)
		if column == "" || column == "-" {
			continue
		}
		if _, ok := excepted[column]; ok {
			continue
		}
		originFieldName := originField.Name
		latestFieldValue, ok := latestMapValue[originFieldName]
		if !ok {
			continue
		}
		latestFieldType := latestTypeOf.Field(latestMapIndex[originFieldName]).Type
		latestFieldTypeKind := latestFieldType.Kind()
		originFieldType := originTypeOf.Field(i).Type
		if latestFieldTypeKind != reflect.Ptr {
			if latestFieldTypeKind == originFieldType.Kind() {
				originValue, latestValue := originValueOf.Field(i).Interface(), latestFieldValue.Interface()
				if !reflect.DeepEqual(originValue, latestValue) {
					modify[column] = latestValue
				}
			}
			continue
		}
		if latestFieldValue.IsNil() {
			continue
		}
		latestFieldType = latestFieldType.Elem()
		if latestFieldType.String() != originFieldType.String() {
			continue
		}
		originValue, latestValue := originValueOf.Field(i).Interface(), latestFieldValue.Elem().Interface()
		if !reflect.DeepEqual(originValue, latestValue) {
			modify[column] = latestValue
		}
	}
	return
}

// StructAssign struct assign, by attribute name, latest attribute value => target attribute value
func StructAssign(target interface{}, latest interface{}) {
	t0, v0 := reflect.TypeOf(target), reflect.ValueOf(target)
	t1, v1 := reflect.TypeOf(latest), reflect.ValueOf(latest)
	t0k, t1k := t0.Kind(), t1.Kind()
	if t0k != reflect.Ptr {
		return
	}
	for ; t0k == reflect.Ptr; t0k = t0.Kind() {
		t0 = t0.Elem()
		v0 = v0.Elem()
	}
	for ; t1k == reflect.Ptr; t1k = t1.Kind() {
		t1 = t1.Elem()
		v1 = v1.Elem()
	}
	if t0k != reflect.Struct || t1k != reflect.Struct {
		return
	}
	mi1 := make(map[string]int)
	mv1 := make(map[string]reflect.Value)
	length := v1.Type().NumField()
	for i := 0; i < length; i++ {
		name := t1.Field(i).Name
		mi1[name] = i
		mv1[name] = v1.Field(i)
	}
	length = t0.NumField()
	for i := 0; i < length; i++ {
		value0 := v0.Field(i)
		if !value0.CanAddr() || !value0.CanSet() {
			continue
		}
		field0 := t0.Field(i)
		value1, ok := mv1[field0.Name]
		if !ok {
			continue
		}
		field1 := t1.Field(mi1[field0.Name])
		field0type := field0.Type
		field1type := field1.Type
		if field0type.String() != field1type.String() {
			continue
		}
		if !reflect.DeepEqual(value0.Interface(), value1.Interface()) {
			if field0type.Kind() == reflect.Ptr && value0.IsNil() {
				value0 = reflect.Indirect(reflect.New(field0type))
			}
			value0.Set(value1)
		}
	}
}

// RowsNext traversing and processing query results
func RowsNext(rows *sql.Rows, fc func() error) (err error) {
	for rows.Next() {
		if err = fc(); err != nil {
			return
		}
	}
	return
}

// RowsNextIndex traverse with slice index and process query results
func RowsNextIndex(rows *sql.Rows, fc func(i int) error) (err error) {
	i := 0
	for rows.Next() {
		if err = fc(i); err != nil {
			return
		}
	}
	return
}

// RowsNextOneRow scan one line of query results
func RowsNextOneRow(rows *sql.Rows, dest ...interface{}) error {
	if rows.Next() {
		return rows.Scan(dest...)
	}
	return nil
}

// ConcatString concatenate string
func ConcatString(sss ...string) string {
	b := getSqlBuilder()
	defer putSqlBuilder(b)
	length := len(sss)
	for i := 0; i < length; i++ {
		b.WriteString(sss[i])
	}
	return b.String()
}
