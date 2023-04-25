package hey

import (
	"database/sql"
	"reflect"
)

// RemoveDuplicate remove duplicate element
func RemoveDuplicate(dynamic ...interface{}) (result []interface{}) {
	has := make(map[interface{}]*struct{})
	ok := false
	for _, v := range dynamic {
		if _, ok = has[v]; ok {
			continue
		}
		has[v] = &struct{}{}
		result = append(result, v)
	}
	return
}

func StructAttributeIndex(rt reflect.Type) (attributeIndex map[string]int) {
	attributeIndex = make(map[string]int)
	kind := rt.Kind()
	if rt == nil || kind != reflect.Struct {
		return
	}
	for index := 0; index < rt.NumField(); index++ {
		attributeIndex[rt.Field(index).Name] = index
	}
	return
}

func StructTagIndex(rt reflect.Type, tag string) (tagIndex map[string]int) {
	tagIndex = make(map[string]int)
	kind := rt.Kind()
	if rt == nil || kind != reflect.Struct {
		return
	}
	value := ""
	for index := 0; index < rt.NumField(); index++ {
		value = rt.Field(index).Tag.Get(tag)
		if value == "" || value == "-" {
			continue
		}
		tagIndex[value] = index
	}
	return
}

func ScanByte(rows *sql.Rows) ([]map[string][]byte, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	var lineValues [][]byte
	var scanList []interface{}
	var lineResult map[string][]byte
	columnLength := len(columnTypes)
	result := make([]map[string][]byte, 0)
	for rows.Next() {
		lineValues = make([][]byte, columnLength)
		scanList = make([]interface{}, columnLength)
		for i := range lineValues {
			scanList[i] = &lineValues[i]
		}
		if err = rows.Scan(scanList...); err != nil {
			return nil, err
		}
		lineResult = make(map[string][]byte, columnLength)
		for index, value := range lineValues {
			lineResult[columnTypes[index].Name()] = value
		}
		result = append(result, lineResult)
	}
	return result, nil
}

func ScanAny(rows *sql.Rows) ([]map[string]interface{}, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	var lineValues []interface{}
	var scanList []interface{}
	var lineResult map[string]interface{}
	columnLength := len(columnTypes)
	result := make([]map[string]interface{}, 0)
	for rows.Next() {
		lineValues = make([]interface{}, columnLength)
		scanList = make([]interface{}, columnLength)
		for i := range lineValues {
			scanList[i] = &lineValues[i]
		}
		if err = rows.Scan(scanList...); err != nil {
			return nil, err
		}
		lineResult = make(map[string]interface{}, columnLength)
		for index, value := range lineValues {
			lineResult[columnTypes[index].Name()] = value
		}
		result = append(result, lineResult)
	}
	return result, nil
}

func scanSliceStructColumn(rowsScanList []interface{}, rowsScanIndex int, rowsScanColumnName string, receiverTagIndex map[string]int, receiver reflect.Value) {
	reflectZeroValue := true
	field := reflect.Value{}
	if index, ok := receiverTagIndex[rowsScanColumnName]; ok {
		field = receiver.Field(index)
		reflectZeroValue = false
	}
	if reflectZeroValue || !field.CanSet() {
		empty := make([]byte, 0)
		bytesTypePtrValue := reflect.New(reflect.TypeOf(empty))
		bytesTypePtrValue.Elem().Set(reflect.ValueOf(empty))
		rowsScanList[rowsScanIndex] = bytesTypePtrValue.Interface()
		return
	}
	rowsScanList[rowsScanIndex] = field.Addr().Interface()
}

func scanSliceStructColumns(rowsColumns []string, rowsScanList []interface{}, receiverTagIndex map[string]int, receiver reflect.Value) {
	for rowsScanIndex, rowsScanColumnName := range rowsColumns {
		scanSliceStructColumn(rowsScanList, rowsScanIndex, rowsScanColumnName, receiverTagIndex, receiver)
	}
}

// ScanSliceStruct scan queries result into slice *[]struct or *[]*struct
func ScanSliceStruct(rows *sql.Rows, result interface{}, structTag string) (err error) {
	typeOf, valueOf := reflect.TypeOf(result), reflect.ValueOf(result)
	typeOfKind := typeOf.Kind()
	if typeOfKind != reflect.Ptr || typeOf.Elem().Kind() != reflect.Slice {
		return
	}
	// *[]any
	rowsColumns, err := rows.Columns()
	if err != nil {
		return
	}
	if structTag == "" {
		structTag = "db"
	}
	setValues := valueOf.Elem()
	var elemType reflect.Type
	elemTypeIsPtr := false
	elem := typeOf.Elem().Elem()
	switch elem.Kind() {
	case reflect.Struct:
		// *[]anyStruct
		elemType = elem
	case reflect.Ptr:
		if elem.Elem().Kind() == reflect.Struct {
			// *[]*anyStruct
			elemType = elem.Elem()
			elemTypeIsPtr = true
		}
	}
	if elemType == nil {
		return
	}
	receiverTagIndex := StructTagIndex(elemType, structTag)
	length := len(rowsColumns)
	for rows.Next() {
		object := reflect.New(elemType)
		receiver := reflect.Indirect(object)
		rowsScanList := make([]interface{}, length)
		scanSliceStructColumns(rowsColumns, rowsScanList, receiverTagIndex, receiver)
		if err = rows.Scan(rowsScanList...); err != nil {
			return
		}
		if elemTypeIsPtr {
			setValues = reflect.Append(setValues, object)
		} else {
			setValues = reflect.Append(setValues, object.Elem())
		}
	}
	valueOf.Elem().Set(setValues)
	return
}

// StructAssign struct attribute assignment
// origin.Age = latest.Age, origin.Name = latest.Name
// both the origin and latest parameters must be struct pointers
func StructAssign(origin interface{}, latest interface{}) {
	originValue, latestValue := reflect.ValueOf(origin), reflect.ValueOf(latest)
	originType, latestType := originValue.Type(), latestValue.Type()
	if originType.Kind() != reflect.Pointer || originType.Elem().Kind() != reflect.Struct {
		return
	}
	if latestType.Kind() != reflect.Pointer || latestType.Elem().Kind() != reflect.Struct {
		return
	}
	originValue, latestValue = originValue.Elem(), latestValue.Elem()
	originMap, latestMap := StructAttributeIndex(originType.Elem()), StructAttributeIndex(latestType.Elem())
	for attribute, index := range latestMap {
		i, ok := originMap[attribute]
		if !ok {
			continue
		}
		originField := originValue.Field(i)
		if !originField.CanSet() {
			continue
		}
		latestField := latestValue.Field(index)
		if originField.Type() != latestField.Type() {
			continue
		}
		originField.Set(latestField)
	}
}

// StructModify struct attribute assignment
// origin.Age = latest.Age, origin.Name = latest.Name OR origin.Age = *latest.Age, origin.Name = *latest.Name
// both the origin and latest parameters must be struct pointers
func StructModify(origin interface{}, latest interface{}) {
	originValue, latestValue := reflect.ValueOf(origin), reflect.ValueOf(latest)
	originType, latestType := originValue.Type(), latestValue.Type()
	if originType.Kind() != reflect.Pointer || originType.Elem().Kind() != reflect.Struct {
		return
	}
	if latestType.Kind() != reflect.Pointer || latestType.Elem().Kind() != reflect.Struct {
		return
	}
	originValue, latestValue = originValue.Elem(), latestValue.Elem()
	originMap, latestMap := StructAttributeIndex(originType.Elem()), StructAttributeIndex(latestType.Elem())
	for attribute, index := range latestMap {
		i, ok := originMap[attribute]
		if !ok {
			continue
		}
		originField := originValue.Field(i)
		if !originField.CanSet() {
			continue
		}
		latestField := latestValue.Field(index)
		originFieldType, latestFieldType := originField.Type(), latestField.Type()
		if originFieldType == latestFieldType {
			originField.Set(latestField)
			continue
		}
		if latestFieldType.Kind() != reflect.Pointer {
			continue
		}
		if latestFieldType.Elem() != originFieldType {
			continue
		}
		if latestField.IsNil() {
			continue
		}
		originField.Set(latestField.Elem())
	}
}
