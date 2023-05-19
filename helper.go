package hey

import (
	"database/sql"
	"fmt"
	"reflect"
)

// RemoveDuplicate remove duplicate element
func RemoveDuplicate(dynamic ...interface{}) (result []interface{}) {
	mp := make(map[interface{}]*struct{})
	ok := false
	length := len(dynamic)
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

func StructAttributeIndex(rt reflect.Type) map[string]int {
	if rt == nil {
		return nil
	}
	if rt.Kind() != reflect.Struct {
		return nil
	}
	attributeMapIndex := make(map[string]int)
	numField := rt.NumField()
	for index := 0; index < numField; index++ {
		attributeMapIndex[rt.Field(index).Name] = index
	}
	return attributeMapIndex
}

func StructTagIndex(rt reflect.Type, tag string) map[string]int {
	if rt == nil {
		return nil
	}
	if rt.Kind() != reflect.Struct {
		return nil
	}
	tagMapIndex := make(map[string]int)
	numField := rt.NumField()
	for index := 0; index < numField; index++ {
		value := rt.Field(index).Tag.Get(tag)
		if value == "" || value == "-" {
			continue
		}
		tagMapIndex[value] = index
	}
	return tagMapIndex
}

func scanSliceStructColumn(rowsScanList []interface{}, rowsScanIndex int, rowsScanColumnName string, receiverMapIndex map[string]int, receiver reflect.Value) (err error) {
	index, ok := receiverMapIndex[rowsScanColumnName]
	if ok {
		field := receiver.Field(index)
		if field.CanSet() {
			rowsScanList[rowsScanIndex] = field.Addr().Interface()
			return
		}
		err = fmt.Errorf("field `%s` mapping object property is not available", rowsScanColumnName)
		return
	}
	err = fmt.Errorf("the field `%s` has no mapping object property", rowsScanColumnName)
	return
}

func scanSliceStructColumns(rowsColumns []string, rowsScanList []interface{}, receiverMapIndex map[string]int, receiver reflect.Value) (err error) {
	length := len(rowsColumns)
	for i := 0; i < length; i++ {
		if err = scanSliceStructColumn(rowsScanList, i, rowsColumns[i], receiverMapIndex, receiver); err != nil {
			return
		}
	}
	return
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
	setValues := valueOf.Elem()
	var elemType reflect.Type
	elemTypeIsPtr := false
	elem := typeOf.Elem().Elem()
	switch elem.Kind() {
	case reflect.Struct:
		// *[]AnyStruct
		elemType = elem
	case reflect.Ptr:
		if elem.Elem().Kind() == reflect.Struct {
			// *[]*AnyStruct
			elemType = elem.Elem()
			elemTypeIsPtr = true
		}
	}
	if elemType == nil {
		return
	}
	if structTag == "" {
		structTag = scannerDefaultStructTag
	}
	var receiverMapIndex map[string]int
	receiverMapIndex = StructTagIndex(elemType, structTag)
	if len(receiverMapIndex) == 0 {
		receiverMapIndex = StructAttributeIndex(elemType)
	}
	length := len(rowsColumns)
	for rows.Next() {
		object := reflect.New(elemType)
		receiver := reflect.Indirect(object)
		rowsScanList := make([]interface{}, length)
		if err = scanSliceStructColumns(rowsColumns, rowsScanList, receiverMapIndex, receiver); err != nil {
			return
		}
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

// StructAssign struct attribute assignment, both the origin and latest parameters must be struct pointers
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

// StructModify struct attribute is not nil assignment, both the origin and latest parameters must be struct pointers
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
