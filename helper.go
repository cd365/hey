package hey

import (
	"database/sql"
	"fmt"
	"reflect"
)

const (
	// ScanTagName scan query results into a struct or slice specifying the struct(using struct `ScanTagName` tag)
	ScanTagName = "db"
)

// assoc lists struct `ScanTagName` tag in struct through reflect
func assoc(reflectType reflect.Type) map[string]int {
	result := make(map[string]int)
	if reflectType == nil || reflectType.Kind() != reflect.Struct {
		return result
	}
	for i := 0; i < reflectType.NumField(); i++ {
		value := reflectType.Field(i).Tag.Get(ScanTagName)
		if value == "-" || value == "" {
			continue
		}
		result[value] = i
	}
	return result
}

// RowsAssoc query result reflect to query object one of *struct, *[]struct, *[]*struct
func RowsAssoc(rows *sql.Rows, result interface{}) (n int64, err error) {
	if result == nil {
		err = fmt.Errorf("query object is nil")
		return
	}
	t0 := reflect.TypeOf(result)
	if t0.Kind() != reflect.Ptr {
		err = fmt.Errorf("scan object is not a pointer: %s", t0.Name())
		return
	}
	var columns []string
	columns, err = rows.Columns()
	if err != nil {
		return
	}
	kind1 := t0.Elem().Kind()
	// reflect zero value
	zero := reflect.Value{}
	// scan to *AnyStruct
	if kind1 == reflect.Struct {
		if !rows.Next() {
			return
		}
		n++
		rlt := reflect.New(t0.Elem())
		rlv := reflect.Indirect(rlt)
		tags := assoc(t0.Elem())
		length := len(columns)
		fields := make([]interface{}, length)
		for key, val := range columns {
			field := reflect.Value{}
			if index, ok := tags[val]; ok {
				field = rlv.Field(index)
			}
			if field == zero || !field.CanSet() {
				// the corresponding property cannot be found in the structure, or the property in the structure cannot be exported, use the default []byte to receive the value of the database (the value will not be scanned into the structure in the end)
				bytesTypePtrValue := reflect.New(reflect.TypeOf([]byte{}))
				bytesTypePtrValue.Elem().Set(reflect.ValueOf([]byte{}))
				fields[key] = bytesTypePtrValue.Interface()
				continue
			}
			fields[key] = field.Addr().Interface()
		}
		if err = rows.Scan(fields...); err != nil {
			return
		}
		reflect.ValueOf(result).Elem().Set(rlt.Elem())
		return
	}
	// scan to *[]AnyStruct or *[]*AnyStruct
	if kind1 == reflect.Slice {
		v0 := reflect.ValueOf(result)
		lists := v0.Elem()
		// scan to *[]AnyStruct
		if t0.Elem().Elem().Kind() == reflect.Struct {
			tags := assoc(t0.Elem().Elem())
			for rows.Next() {
				n++
				rlt := reflect.New(t0.Elem().Elem())
				rlv := reflect.Indirect(rlt)
				length := len(columns)
				fields := make([]interface{}, length)
				for key, val := range columns {
					field := reflect.Value{}
					if index, ok := tags[val]; ok {
						field = rlv.Field(index)
					}
					if field == zero || !field.CanSet() {
						bytesTypePtrValue := reflect.New(reflect.TypeOf([]byte{}))
						bytesTypePtrValue.Elem().Set(reflect.ValueOf([]byte{}))
						fields[key] = bytesTypePtrValue.Interface()
						continue
					}
					fields[key] = field.Addr().Interface()
				}
				if err = rows.Scan(fields...); err != nil {
					return
				}
				lists = reflect.Append(lists, rlt.Elem())
			}
			reflect.ValueOf(result).Elem().Set(lists)
			return
		}
		// scan to *[]*AnyStruct
		if t0.Elem().Elem().Kind() == reflect.Ptr && t0.Elem().Elem().Elem().Kind() == reflect.Struct {
			tags := assoc(t0.Elem().Elem().Elem())
			for rows.Next() {
				n++
				rlt := reflect.New(t0.Elem().Elem().Elem())
				rlv := reflect.Indirect(rlt)
				length := len(columns)
				fields := make([]interface{}, length)
				for key, val := range columns {
					field := reflect.Value{}
					if index, ok := tags[val]; ok {
						field = rlv.Field(index)
					}
					if field == zero || !field.CanSet() {
						bytesTypePtrValue := reflect.New(reflect.TypeOf([]byte{}))
						bytesTypePtrValue.Elem().Set(reflect.ValueOf([]byte{}))
						fields[key] = bytesTypePtrValue.Interface()
						continue
					}
					fields[key] = field.Addr().Interface()
				}
				if err = rows.Scan(fields...); err != nil {
					return
				}
				lists = reflect.Append(lists, rlt)
			}
			reflect.ValueOf(result).Elem().Set(lists)
			return
		}
	}
	err = fmt.Errorf("scan object reflect type unsupported, data type: %#v", result)
	return
}

// RowsBytes record does not exist returns []map[string][]byte, nil
func RowsBytes(rows *sql.Rows) ([]map[string][]byte, error) {
	var columnTypes []*sql.ColumnType
	var err error
	columnTypes, err = rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	var values [][]byte
	var scan []interface{}
	var tmp map[string][]byte
	cols := len(columnTypes)
	queries := make([]map[string][]byte, 0)
	for rows.Next() {
		values = make([][]byte, cols)
		scan = make([]interface{}, cols)
		for i := range values {
			scan[i] = &values[i]
		}
		if err = rows.Scan(scan...); err != nil {
			return nil, err
		}
		tmp = make(map[string][]byte)
		for index, value := range values {
			tmp[columnTypes[index].Name()] = value
		}
		queries = append(queries, tmp)
	}
	return queries, nil
}

// RowsString record does not exist returns []map[string]*string, nil
func RowsString(rows *sql.Rows) ([]map[string]*string, error) {
	var columnTypes []*sql.ColumnType
	var err error
	columnTypes, err = rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	var values [][]byte
	var scan []interface{}
	var tmp map[string]*string
	cols := len(columnTypes)
	queries := make([]map[string]*string, 0)
	for rows.Next() {
		values = make([][]byte, cols)
		scan = make([]interface{}, cols)
		for i := range values {
			scan[i] = &values[i]
		}
		if err = rows.Scan(scan...); err != nil {
			return nil, err
		}
		tmp = make(map[string]*string)
		for index, value := range values {
			if value == nil {
				tmp[columnTypes[index].Name()] = nil
			} else {
				val := string(value)
				tmp[columnTypes[index].Name()] = &val
			}
		}
		queries = append(queries, tmp)
	}
	return queries, nil
}

// RowsAny record does not exist returns []map[string]interface{}, nil
func RowsAny(rows *sql.Rows) ([]map[string]interface{}, error) {
	var columnTypes []*sql.ColumnType
	var err error
	columnTypes, err = rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	var values []interface{}
	var scan []interface{}
	var tmp map[string]interface{}
	cols := len(columnTypes)
	queries := make([]map[string]interface{}, 0)
	for rows.Next() {
		values = make([]interface{}, cols)
		scan = make([]interface{}, cols)
		for i := range values {
			scan[i] = &values[i]
		}
		if err = rows.Scan(scan...); err != nil {
			return nil, err
		}
		tmp = make(map[string]interface{})
		for index, value := range values {
			tmp[columnTypes[index].Name()] = value
		}
		queries = append(queries, tmp)
	}
	return queries, nil
}

// RemoveDuplicate remove duplicate
func RemoveDuplicate(items ...interface{}) (result []interface{}) {
	has := make(map[interface{}]*struct{})
	ok := false
	for _, item := range items {
		if _, ok = has[item]; ok {
			continue
		}
		has[item] = &struct{}{}
		result = append(result, item)
	}
	return
}

// StructAssign any two structure fields with the same type can be assigned
func StructAssign(a interface{}, b interface{}) {
	at, av := reflect.TypeOf(a), reflect.ValueOf(a)
	bt, bv := reflect.TypeOf(b), reflect.ValueOf(b)
	if at.Kind() != reflect.Ptr {
		panic("a must be a struct pointer")
	} else {
		at, av = at.Elem(), av.Elem()
	}
	if bt.Kind() == reflect.Ptr {
		bt, bv = bt.Elem(), bv.Elem()
	}
	length := bv.NumField()
	if length == 0 {
		return
	}
	var afv, bfv reflect.Value
	for i := 0; i < length; i++ {
		afv = av.FieldByName(bt.Field(i).Name)
		if !afv.IsValid() {
			continue
		}
		bfv = bv.Field(i)
		if afv.Kind() != bfv.Kind() {
			continue
		}
		afv.Set(bfv)
	}
	return
}

// StructInsert use struct `ScanTagName` tag to INSERT, `a` is a pointer to any type of structure or any structure , ignore is a list of ignored fields
func StructInsert(a interface{}, ignore ...string) (field []string, value []interface{}) {
	at, av := reflect.TypeOf(a), reflect.ValueOf(a)
	if at.Kind() == reflect.Ptr {
		at, av = at.Elem(), av.Elem()
	}
	length := av.NumField()
	column, ok := "", false
	remove := make(map[string]*struct{})
	for _, key := range ignore {
		remove[key] = &struct{}{}
	}
	for i := 0; i < length; i++ {
		column = at.Field(i).Tag.Get(ScanTagName)
		if column == "" || column == "-" {
			continue
		}
		if _, ok = remove[column]; ok {
			continue
		}
		field = append(field, column)
		value = append(value, av.Field(i).Interface())
	}
	return
}

// StructUpdate use struct tag to UPDATE
// comparison structure a and b, ignore is a list of ignored fields
// find the updated attribute value in b
// the field name is subject to the `ScanTagName` label value of a
func StructUpdate(a interface{}, b interface{}, ignore ...string) (field []string, value []interface{}) {
	at, av := reflect.TypeOf(a), reflect.ValueOf(a)
	bt, bv := reflect.TypeOf(b), reflect.ValueOf(b)
	if at.Kind() != reflect.Ptr {
		panic("a must be a struct pointer")
	} else {
		at, av = at.Elem(), av.Elem()
	}
	if bt.Kind() == reflect.Ptr {
		bt, bv = bt.Elem(), bv.Elem()
	}
	length := bv.NumField()
	if length == 0 {
		return
	}
	var afv, bfv reflect.Value
	var afk, bfk reflect.Kind
	var afi, bfi interface{}
	var asf reflect.StructField
	var name string
	var ok bool
	remove := make(map[string]*struct{})
	for _, key := range ignore {
		remove[key] = &struct{}{}
	}
	for i := 0; i < length; i++ {
		asf, ok = at.FieldByName(bt.Field(i).Name)
		if !ok {
			continue
		}
		afv = av.FieldByName(bt.Field(i).Name)
		if !afv.IsValid() {
			continue
		}
		bfv = bv.Field(i)
		afk, bfk = afv.Kind(), bfv.Kind()
		afi, bfi = afv.Interface(), bfv.Interface()
		if afk != bfk {
			if bfk != reflect.Ptr {
				continue
			}
			bfv = bfv.Elem()
			bfk = bfv.Kind()
			if afk != bfk {
				continue
			}
			bfi = bfv.Interface()
			if bfi == nil {
				continue
			}
		}
		if afi == bfi {
			continue
		}
		name = asf.Tag.Get(ScanTagName)
		if name == "" || name == "-" {
			continue
		}
		if _, ok = remove[name]; ok {
			continue
		}
		field = append(field, name)
		value = append(value, bfi)
	}
	return
}

// StructFuncByName build a function to get the value of a struct property based on its name; (*AnyStruct, attributeName) => func(*AnyStruct, string) | (AnyStruct, attributeName) => func(AnyStruct, string)
func StructFuncByName(sss interface{}, name string) (result func(sss interface{}) interface{}) {
	if sss == nil || name == "" {
		return
	}
	ptr := false
	rtp := reflect.TypeOf(sss)
	kind := rtp.Kind()
	if kind == reflect.Ptr {
		ptr = true
		rtp = rtp.Elem()
		kind = rtp.Kind()
	}
	if kind != reflect.Struct {
		return
	}
	length := rtp.NumField()
	for i := 0; i < length; i++ {
		if rtp.Field(i).Name != name {
			continue
		}
		result = func(sss interface{}) interface{} {
			if ptr {
				return reflect.ValueOf(sss).Elem().Field(i).Interface()
			}
			return reflect.ValueOf(sss).Field(i).Interface()
		}
		break
	}
	return
}

// StructFuncByQueryField construct a function to obtain the corresponding attribute value according to the `ScanTagName` tag value of the structure; (*AnyStruct, tagValue) => func(*AnyStruct, string) | (AnyStruct, tagValue) => func(AnyStruct, string)
func StructFuncByQueryField(sss interface{}, field string) (result func(sss interface{}) interface{}) {
	if sss == nil || field == "" {
		return
	}
	ptr := false
	rtp := reflect.TypeOf(sss)
	kind := rtp.Kind()
	if kind == reflect.Ptr {
		ptr = true
		rtp = rtp.Elem()
		kind = rtp.Kind()
	}
	if kind != reflect.Struct {
		return
	}
	length := rtp.NumField()
	val := ""
	for i := 0; i < length; i++ {
		val = rtp.Field(i).Tag.Get(ScanTagName)
		if val == "" || val == "-" || val != field {
			continue
		}
		result = func(sss interface{}) interface{} {
			if ptr {
				return reflect.ValueOf(sss).Elem().Field(i).Interface()
			}
			return reflect.ValueOf(sss).Field(i).Interface()
		}
		break
	}
	return
}

// SliceStructAttributeValueByName get the attributes in the slice object as a new slice; []*AnyStruct => []interface{}, []AnyStruct => []interface{}
func SliceStructAttributeValueByName(sss interface{}, name string) (result []interface{}) {
	result = make([]interface{}, 0)
	if sss == nil || name == "" {
		return
	}
	ptr := false
	rtp := reflect.TypeOf(sss)
	kind := rtp.Kind()
	if kind == reflect.Ptr {
		ptr = true
		rtp = rtp.Elem()
		kind = rtp.Kind()
	}
	if kind != reflect.Slice {
		return
	}
	rvl := reflect.ValueOf(sss)
	if ptr {
		rvl = rvl.Elem()
	}
	var call func(sss interface{}) interface{}
	var item interface{}
	length := rvl.Len()
	for i := 0; i < length; i++ {
		item = rvl.Index(i).Interface()
		if call == nil {
			call = StructFuncByName(item, name)
		}
		if call != nil {
			result = append(result, call(item))
		}
	}
	return
}

// SliceStructAttributeValueByQueryField get the attributes in the slice object as a new slice using struct `ScanTagName` tag value; []*AnyStruct => []interface{}, []AnyStruct => []interface{}
func SliceStructAttributeValueByQueryField(sss interface{}, field string) (result []interface{}) {
	result = make([]interface{}, 0)
	if sss == nil || field == "" {
		return
	}
	ptr := false
	rtp := reflect.TypeOf(sss)
	kind := rtp.Kind()
	if kind == reflect.Ptr {
		ptr = true
		rtp = rtp.Elem()
		kind = rtp.Kind()
	}
	if kind != reflect.Slice {
		return
	}
	rvl := reflect.ValueOf(sss)
	if ptr {
		rvl = rvl.Elem()
	}
	var call func(sss interface{}) interface{}
	var item interface{}
	length := rvl.Len()
	for i := 0; i < length; i++ {
		item = rvl.Index(i).Interface()
		if call == nil {
			call = StructFuncByQueryField(item, field)
		}
		if call != nil {
			result = append(result, call(item))
		}
	}
	return
}

// SliceToMapByName slice to map uses the slice attribute name as the key name of the map; []*AnyStruct => map[interface{}]*AnyStruct, []AnyStruct => map[interface{}]AnyStruct
func SliceToMapByName(sss interface{}, name string, result interface{}) {
	if sss == nil || name == "" || result == nil {
		return
	}
	ptr := false
	rtp := reflect.TypeOf(sss)
	kind := rtp.Kind()
	if kind == reflect.Ptr {
		ptr = true
		rtp = rtp.Elem()
		kind = rtp.Kind()
	}
	if kind != reflect.Slice {
		return
	}
	ptr1 := false
	rtp1 := reflect.TypeOf(result)
	kind1 := rtp1.Kind()
	if kind1 == reflect.Ptr {
		ptr1 = true
		rtp1 = rtp1.Elem()
		kind1 = rtp1.Kind()
	}
	if kind1 != reflect.Map {
		return
	}
	if rtp.Elem().Kind() != rtp1.Elem().Kind() {
		return
	}
	if rtp.Elem().Kind() == reflect.Ptr {
		// package.Type
		if rtp.Elem().Elem().String() != rtp1.Elem().Elem().String() {
			return
		}
	} else {
		// package.Type
		if rtp.Elem().String() != rtp1.Elem().String() {
			return
		}
	}
	rvl := reflect.ValueOf(sss)
	if ptr {
		rvl = rvl.Elem()
	}
	var call func(sss interface{}) interface{}
	var item interface{}
	length := rvl.Len()
	keyValue := make(map[interface{}]interface{}, length)
	for i := 0; i < length; i++ {
		item = rvl.Index(i).Interface()
		if call == nil {
			call = StructFuncByName(item, name)
		}
		if call != nil {
			keyValue[call(item)] = item
		}
	}
	var rvl1 reflect.Value
	if ptr1 {
		rvl1 = reflect.ValueOf(result).Elem()
	} else {
		rvl1 = reflect.ValueOf(result)
	}
	for key, val := range keyValue {
		rvl1.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(val))
	}
	return
}

// SliceToMapByQueryField slice to map uses the `ScanTagName` tag value of the slice attribute as the key name of the map; []*AnyStruct => map[interface{}]*AnyStruct, []AnyStruct => map[interface{}]AnyStruct
func SliceToMapByQueryField(sss interface{}, field string, result interface{}) {
	if sss == nil || field == "" || field == "-" || result == nil {
		return
	}
	ptr := false
	rtp := reflect.TypeOf(sss)
	kind := rtp.Kind()
	if kind == reflect.Ptr {
		ptr = true
		rtp = rtp.Elem()
		kind = rtp.Kind()
	}
	if kind != reflect.Slice {
		return
	}
	ptr1 := false
	rtp1 := reflect.TypeOf(result)
	kind1 := rtp1.Kind()
	if kind1 == reflect.Ptr {
		ptr1 = true
		rtp1 = rtp1.Elem()
		kind1 = rtp1.Kind()
	}
	if kind1 != reflect.Map {
		return
	}
	if rtp.Elem().Kind() != rtp1.Elem().Kind() {
		return
	}
	if rtp.Elem().Kind() == reflect.Ptr {
		// package.Type
		if rtp.Elem().Elem().String() != rtp1.Elem().Elem().String() {
			return
		}
	} else {
		// package.Type
		if rtp.Elem().String() != rtp1.Elem().String() {
			return
		}
	}
	rvl := reflect.ValueOf(sss)
	if ptr {
		rvl = rvl.Elem()
	}
	var call func(sss interface{}) interface{}
	var item interface{}
	length := rvl.Len()
	keyValue := make(map[interface{}]interface{}, length)
	for i := 0; i < length; i++ {
		item = rvl.Index(i).Interface()
		if call == nil {
			call = StructFuncByQueryField(item, field)
		}
		if call != nil {
			keyValue[call(item)] = item
		}
	}
	var rvl1 reflect.Value
	if ptr1 {
		rvl1 = reflect.ValueOf(result).Elem()
	} else {
		rvl1 = reflect.ValueOf(result)
	}
	for key, val := range keyValue {
		rvl1.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(val))
	}
	return
}
