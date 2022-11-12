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
