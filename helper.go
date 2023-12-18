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

var (
	// sqlBuilder sql builder pool
	sqlBuilder *sync.Pool

	// insertByStructPool insert with struct{}, *struct{}, []struct, []*struct{}, *[]struct{}, *[]*struct{}
	insertByStructPool *sync.Pool
)

// init initialize
func init() {
	sqlBuilder = &sync.Pool{}
	sqlBuilder.New = func() interface{} {
		return &strings.Builder{}
	}

	insertByStructPool = &sync.Pool{}
	insertByStructPool.New = func() interface{} {
		return &insertByStruct{}
	}
}

// getSqlBuilder get sql builder from pool
func getSqlBuilder() *strings.Builder {
	return sqlBuilder.Get().(*strings.Builder)
}

// putSqlBuilder put sql builder in the pool
func putSqlBuilder(b *strings.Builder) {
	b.Reset()
	sqlBuilder.Put(b)
}

// getInsertByStruct get *insertByStruct from pool
func getInsertByStruct() *insertByStruct {
	return insertByStructPool.Get().(*insertByStruct)
}

// putInsertByStruct put *insertByStruct in the pool
func putInsertByStruct(b *insertByStruct) {
	b.tag = ""
	b.used = nil
	b.except = nil
	b.structReflectType = nil
	insertByStructPool.Put(b)
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
func (s *bindStruct) prepare(columns []string, rowsScan []interface{}, indirect reflect.Value, length int) error {
	for i := 0; i < length; i++ {
		index, ok := s.direct[columns[i]]
		if ok {
			// top structure
			field := indirect.Field(index)
			if !field.CanAddr() || !field.CanSet() {
				return fmt.Errorf("column `%s` cann't set value", columns[i])
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
			if !field.CanAddr() || !field.CanSet() {
				return fmt.Errorf("column `%s` cann't set value, multi-level", columns[i])
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
				return ""
			case reflect.Bool:
				return false
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
				reflect.Float32, reflect.Float64:
				return 0
			}
			return reflect.Indirect(reflect.New(t)).Interface()
		}
		t, v = t.Elem(), v.Elem()
		k = t.Kind()
	}
	return v.Interface()
}

type insertByStruct struct {
	tag               string              // struct tag name value used as table.column name
	used              map[string]struct{} // already existing field Hash table
	except            map[string]struct{} // ignored field Hash table
	structReflectType reflect.Type        // struct reflect type, make sure it is the same structure type
}

// setExcept filter the list of unwanted fields, prioritize method calls structFieldsValues() and structValues()
func (s *insertByStruct) setExcept(except []string) {
	length := len(except)
	s.except = make(map[string]struct{}, length)
	for i := 0; i < length; i++ {
		if _, ok := s.except[except[i]]; ok {
			continue
		}
		s.except[except[i]] = struct{}{}
	}
	// prevent the same field name from appearing twice in the field list
	s.used = make(map[string]struct{})
}

func panicBatchInsertByStruct() {
	panic("hey: slice element types are inconsistent")
}

// structFieldsValues checkout fields, values
func (s *insertByStruct) structFieldsValues(structReflectValue reflect.Value) (fields []string, values []interface{}) {
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
			tmpFields, tmpValues := s.structFieldsValues(valueIndexField)
			fields = append(fields, tmpFields...)
			values = append(values, tmpValues...)
			continue
		}

		valueIndexFieldTag := field.Tag.Get(s.tag)
		if valueIndexFieldTag == "" || valueIndexFieldTag == "-" {
			continue
		}
		if _, ok := s.except[valueIndexFieldTag]; ok {
			continue
		}
		if _, ok := s.used[valueIndexFieldTag]; ok {
			continue
		}
		s.used[valueIndexFieldTag] = struct{}{}

		fields = append(fields, valueIndexFieldTag)
		values = append(values, BasicTypeValue(valueIndexField.Interface()))
	}
	return
}

// structValues checkout values
func (s *insertByStruct) structValues(structReflectValue reflect.Value) (values []interface{}) {
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
			values = append(values, s.structValues(valueIndexField)...)
			continue
		}

		valueIndexFieldTag := field.Tag.Get(s.tag)
		if valueIndexFieldTag == "" || valueIndexFieldTag == "-" {
			continue
		}
		if _, ok := s.except[valueIndexFieldTag]; ok {
			continue
		}

		values = append(values, BasicTypeValue(valueIndexField.Interface()))
	}
	return
}

// Insert object should be one of struct{}, *struct{}, []struct, []*struct{}, *[]struct{}, *[]*struct{}
func (s *insertByStruct) Insert(object interface{}, tag string, except ...string) (fields []string, values [][]interface{}) {
	if object == nil || tag == "" {
		return
	}
	reflectValue := reflect.ValueOf(object)
	kind := reflectValue.Kind()
	for ; kind == reflect.Ptr; kind = reflectValue.Kind() {
		reflectValue = reflectValue.Elem()
	}
	s.tag = tag
	s.setExcept(except)
	switch kind {
	case reflect.Struct:
		values = make([][]interface{}, 1)
		fields, values[0] = s.structFieldsValues(reflectValue)
		return
	case reflect.Slice:
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
				fields, values[i] = s.structFieldsValues(indexValue)
			} else {
				values[i] = s.structValues(indexValue)
			}
		}
	}
	return
}

// StructInsert object should be one of struct{}, *struct{}, []struct, []*struct{}, *[]struct{}, *[]*struct{}
// get fields and values based on struct tag.
func StructInsert(object interface{}, tag string, except ...string) (fields []string, values [][]interface{}) {
	b := getInsertByStruct()
	defer putInsertByStruct(b)
	fields, values = b.Insert(object, tag, except...)
	return
}

// StructModify object should be one of struct{}, *struct{} get the fields and values that need to be modified
func StructModify(object interface{}, tag string, except ...string) (fields []string, values []interface{}) {
	if object == nil || tag == "" {
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
		if column == "" || column == "-" {
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
	if object == nil || tag == "" {
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
		if column == "" || column == "-" {
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
	if origin == nil || latest == nil || tag == "" {
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

// RowsNext traversing and processing query results
func RowsNext(rows *sql.Rows, fc func() error) (err error) {
	for rows.Next() {
		if err = fc(); err != nil {
			return
		}
	}
	return
}

// RowsNextRow scan one line of query results
func RowsNextRow(rows *sql.Rows, dest ...interface{}) error {
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

// SqlAlias sql alias name
func SqlAlias(name string, alias string) string {
	if alias == "" {
		return name
	}
	return fmt.Sprintf("%s %s %s", name, SqlAs, alias)
}

// SqlPrefix sql prefix name
func SqlPrefix(prefix string, name string) string {
	if prefix == "" {
		return name
	}
	return fmt.Sprintf("%s.%s", prefix, name)
}

const (
	SqlAs       = "AS"
	SqlAsc      = "ASC"
	SqlDesc     = "DESC"
	SqlUnion    = "UNION"
	SqlUnionAll = "UNION ALL"
)

const (
	SqlJoinInner = "INNER JOIN"
	SqlJoinLeft  = "LEFT JOIN"
	SqlJoinRight = "RIGHT JOIN"
	SqlJoinFull  = "FULL JOIN"
)

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

// comment make SQL statement builder, defer putSqlBuilder(builder) should be called immediately after calling the current method
func comment(schema *schema) (b *strings.Builder) {
	b = getSqlBuilder()
	schema.comment = strings.TrimSpace(schema.comment)
	if schema.comment == "" {
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

// AndWhere and where
func (s *Del) AndWhere(where ...Filter) *Del {
	if s.where == nil {
		s.where = s.schema.way.Filter()
	}
	s.where.Filter(where...)
	return s
}

// OrWhere or where
func (s *Del) OrWhere(where ...Filter) *Del {
	if s.where == nil {
		s.where = s.schema.way.Filter()
	}
	s.where.OrFilter(where...)
	return s
}

// WhereIn set where IN
func (s *Del) WhereIn(column string, values ...interface{}) *Del {
	return s.Where(NewFilter().In(column, values...))
}

// WhereEqual set where EQUAL
func (s *Del) WhereEqual(column string, values interface{}) *Del {
	return s.Where(NewFilter().Equal(column, values))
}

// SQL build SQL statement
func (s *Del) SQL() (prepare string, args []interface{}) {
	if s.schema.table == "" {
		return
	}
	buf := comment(s.schema)
	defer putSqlBuilder(buf)
	buf.WriteString("DELETE FROM ")
	buf.WriteString(s.schema.table)
	w := false
	if s.where != nil {
		where, whereArgs := s.where.SQL()
		if where != "" {
			w = true
			buf.WriteString(" WHERE ")
			buf.WriteString(where)
			args = whereArgs
		}
	}
	if s.schema.way.Config.DeleteMustUseWhere && !w {
		prepare, args = "", nil
		return
	}
	prepare = buf.String()
	return
}

// Del execute the built SQL statement
func (s *Del) Del() (int64, error) {
	prepare, args := s.SQL()
	return s.schema.way.ExecContext(s.schema.ctx, prepare, args...)
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

// Fields set fields
func (s *Add) Fields(fields []string) *Add {
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
		if field == "" {
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

// FieldValue add field-value for insert one or more rows
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

// DefaultFieldValue customize default field-value for insert one or more rows
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

// Create value of create should be oneof struct{}, *struct{}, map[string]interface{},
// []struct, []*struct{}, *[]struct{}, *[]*struct{}
func (s *Add) Create(create interface{}) *Add {
	if fieldValue, ok := create.(map[string]interface{}); ok {
		for field, value := range fieldValue {
			s.FieldValue(field, value)
		}
		return s
	}
	return s.FieldsValues(StructInsert(create, s.schema.way.Tag, s.except...))
}

// ValuesSubQuery values is a query SQL statement
func (s *Add) ValuesSubQuery(prepare string, args ...interface{}) *Add {
	if prepare == "" {
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

// SQL build SQL statement
func (s *Add) SQL() (prepare string, args []interface{}) {
	if s.schema.table == "" {
		return
	}

	buf := comment(s.schema)
	defer putSqlBuilder(buf)

	if s.subQuery != nil {
		buf.WriteString("INSERT INTO ")
		buf.WriteString(s.schema.table)
		if len(s.fields) > 0 {
			buf.WriteString(" ( ")
			buf.WriteString(strings.Join(s.fields, ", "))
			buf.WriteString(" )")
		}
		buf.WriteString(" ")
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
		tmpList[i] = Placeholder
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
	if s.onConflict != nil && *s.onConflict != "" {
		buf.WriteString(" ")
		buf.WriteString(*s.onConflict)
		args = append(args, s.onConflictArgs...)
	}
	prepare = buf.String()
	return
}

// Add execute the built SQL statement
func (s *Add) Add() (int64, error) {
	prepare, args := s.SQL()
	return s.schema.way.ExecContext(s.schema.ctx, prepare, args...)
}

// modify set the field to be updated
type modify struct {
	expression string
	args       []interface{}
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
		if except[i] == "" {
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

// expression build update field expressions and field values
func (s *Mod) expression(field string, expression string, args ...interface{}) *Mod {
	if _, ok := s.except[field]; ok {
		return s
	}

	tmp := &modify{
		expression: expression,
		args:       args,
	}

	if _, ok := s.update[field]; ok {
		s.update[field] = tmp
		return s
	}

	s.updateSlice = append(s.updateSlice, field)
	s.update[field] = tmp
	return s
}

// Expression update field using custom expression
func (s *Mod) Expression(field string, expression string, args ...interface{}) *Mod {
	field, expression = strings.TrimSpace(field), strings.TrimSpace(expression)
	return s.expression(field, expression, args)
}

// Set field = value
func (s *Mod) Set(field string, value interface{}) *Mod {
	return s.expression(field, fmt.Sprintf("%s = %s", field, Placeholder), value)
}

// Incr SET field = field + value
func (s *Mod) Incr(field string, value interface{}) *Mod {
	return s.expression(field, fmt.Sprintf("%s = %s + %s", field, field, Placeholder), value)
}

// Decr SET field = field - value
func (s *Mod) Decr(field string, value interface{}) *Mod {
	return s.expression(field, fmt.Sprintf("%s = %s - %s", field, field, Placeholder), value)
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

// Modify value of modify should be oneof struct{}, *struct{}, map[string]interface{}
func (s *Mod) Modify(modify interface{}) *Mod {
	if fieldValue, ok := modify.(map[string]interface{}); ok {
		for field, value := range fieldValue {
			s.Set(field, value)
		}
		return s
	}
	return s.FieldsValues(StructModify(modify, s.schema.way.Tag, s.exceptSlice...))
}

// Update for compare origin and latest to automatically calculate need to update fields
func (s *Mod) Update(originObject interface{}, latestObject interface{}) *Mod {
	return s.FieldsValues(StructUpdate(originObject, latestObject, s.schema.way.Tag, s.exceptSlice...))
}

// defaultExpression append the update field collection when there is at least one item in the update field collection, for example, set the update timestamp
func (s *Mod) defaultExpression(field string, expression string, args ...interface{}) *Mod {
	if _, ok := s.except[field]; ok {
		return s
	}

	if _, ok := s.update[field]; ok {
		return s
	}

	tmp := &modify{
		expression: expression,
		args:       args,
	}

	if _, ok := s.secondaryUpdate[field]; ok {
		s.secondaryUpdate[field] = tmp
		return s
	}

	s.secondaryUpdateSlice = append(s.secondaryUpdateSlice, field)
	s.secondaryUpdate[field] = tmp
	return s
}

// DefaultExpression update field using custom expression
func (s *Mod) DefaultExpression(field string, expression string, args ...interface{}) *Mod {
	field, expression = strings.TrimSpace(field), strings.TrimSpace(expression)
	return s.defaultExpression(field, expression, args)
}

// DefaultSet SET field = value
func (s *Mod) DefaultSet(field string, value interface{}) *Mod {
	return s.defaultExpression(field, fmt.Sprintf("%s = %s", field, Placeholder), value)
}

// DefaultIncr SET field = field + value
func (s *Mod) DefaultIncr(field string, value interface{}) *Mod {
	return s.defaultExpression(field, fmt.Sprintf("%s = %s + %s", field, field, Placeholder), value)
}

// DefaultDecr SET field = field - value
func (s *Mod) DefaultDecr(field string, value interface{}) *Mod {
	return s.defaultExpression(field, fmt.Sprintf("%s = %s - %s", field, field, Placeholder), value)
}

// Where set where
func (s *Mod) Where(where Filter) *Mod {
	s.where = where
	return s
}

// AndWhere and where
func (s *Mod) AndWhere(where ...Filter) *Mod {
	if s.where == nil {
		s.where = s.schema.way.Filter()
	}
	s.where.Filter(where...)
	return s
}

// OrWhere or where
func (s *Mod) OrWhere(where ...Filter) *Mod {
	if s.where == nil {
		s.where = s.schema.way.Filter()
	}
	s.where.OrFilter(where...)
	return s
}

// WhereIn set where IN
func (s *Mod) WhereIn(field string, values ...interface{}) *Mod {
	return s.Where(NewFilter().In(field, values...))
}

// WhereEqual set where EQUAL
func (s *Mod) WhereEqual(field string, value interface{}) *Mod {
	return s.Where(NewFilter().Equal(field, value))
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
			field[k] = s.update[v].expression
			value = append(value, s.update[v].args...)
			continue
		}
		field[k] = s.secondaryUpdate[v].expression
		value = append(value, s.secondaryUpdate[v].args...)
	}
	buf := comment(s.schema)
	defer putSqlBuilder(buf)
	buf.WriteString(strings.Join(field, ", "))
	prepare = buf.String()
	args = value
	return
}

// SQL build SQL statement
func (s *Mod) SQL() (prepare string, args []interface{}) {
	if s.schema.table == "" {
		return
	}
	prepare, args = s.SetSQL()
	if prepare == "" {
		return
	}
	buf := comment(s.schema)
	defer putSqlBuilder(buf)
	buf.WriteString("UPDATE ")
	buf.WriteString(s.schema.table)
	buf.WriteString(" SET ")
	buf.WriteString(prepare)
	w := false
	if s.where != nil {
		key, val := s.where.SQL()
		if key != "" {
			w = true
			buf.WriteString(" WHERE ")
			buf.WriteString(key)
			args = append(args, val...)
		}
	}
	if s.schema.way.Config.UpdateMustUseWhere && !w {
		prepare, args = "", nil
		return
	}
	prepare = buf.String()
	return
}

// Mod execute the built SQL statement
func (s *Mod) Mod() (int64, error) {
	prepare, args := s.SQL()
	return s.schema.way.ExecContext(s.schema.ctx, prepare, args...)
}

type WithQuery struct {
	alias   string
	prepare string
	args    []interface{}
}

func newWithQuery(
	alias string,
	prepare string,
	args ...interface{},
) *WithQuery {
	return &WithQuery{
		alias:   alias,
		prepare: prepare,
		args:    args,
	}
}

func (s *WithQuery) SQL() (prepare string, args []interface{}) {
	b := getSqlBuilder()
	defer putSqlBuilder(b)
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
}

// NewGetJoin new join
func NewGetJoin(joinType string, table ...string) *GetJoin {
	getJoin := &GetJoin{
		joinType: joinType,
	}
	for i := len(table) - 1; i >= 0; i-- {
		if table[i] != "" {
			getJoin.Table(table[i])
			break
		}
	}
	return getJoin
}

// Table set table name
func (s *GetJoin) Table(table string) *GetJoin {
	s.table = table
	return s
}

// TableSubQuery table is a query SQL statement
func (s *GetJoin) TableSubQuery(prepare string, args ...interface{}) *GetJoin {
	s.subQuery = NewSubQuery(prepare, args...)
	return s
}

// TableSubQueryGet table is a query SQL statement
func (s *GetJoin) TableSubQueryGet(get *Get, alias ...string) *GetJoin {
	if get == nil {
		return s
	}
	prepare, args := get.SQL()
	s.subQuery = NewSubQuery(prepare, args...)
	for i := len(alias) - 1; i >= 0; i-- {
		if alias[i] != "" {
			s.TableAlias(alias[i])
			break
		}
	}
	return s
}

// TableAlias table alias name, don't forget to call the current method when the table is a SQL statement
func (s *GetJoin) TableAlias(alias string) *GetJoin {
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
	if length&1 != 0 {
		return s
	}
	tmp := make([]string, 0, length)
	for i := 0; i < length; i += 2 {
		tmp = append(tmp, fmt.Sprintf("%s = %s", fields[i], fields[i+1]))
	}
	return s.On(strings.Join(tmp, " AND "))
}

// SQL build SQL statement
func (s *GetJoin) SQL() (prepare string, args []interface{}) {
	if s.table == "" && (s.subQuery == nil || s.alias == nil || *s.alias == "") {
		return
	}
	usingLength := len(s.using)
	if s.on == "" && usingLength == 0 {
		return
	}

	buf := getSqlBuilder()
	defer putSqlBuilder(buf)

	buf.WriteString(s.joinType)

	if s.subQuery != nil {
		buf.WriteString(" ( ")
		subPrepare, subArgs := s.subQuery.SQL()
		buf.WriteString(subPrepare)
		buf.WriteString(" ) AS ")
		buf.WriteString(*s.alias)
		args = append(args, subArgs...)
	} else {
		buf.WriteString(" ")
		buf.WriteString(s.table)
		if s.alias != nil && *s.alias != "" {
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

type Identifier struct {
	prefix string
}

func (s *Identifier) V(sss ...string) string {
	length := len(sss)
	if length == 0 {
		return s.prefix
	}
	name := sss[0]
	if name == "" {
		return name
	}
	if s.prefix != "" {
		name = fmt.Sprintf("%s.%s", s.prefix, name)
	}
	if length == 1 {
		return name
	}
	alias := sss[1:]
	for i := len(alias); i >= 0; i-- {
		if alias[i] != "" {
			return SqlAlias(name, alias[i])
		}
	}
	return name
}

// Get for SELECT
type Get struct {
	schema   *schema           // query table
	with     []*WithQuery      // with of query
	column   []string          // query field list
	subQuery *SubQuery         // the query table is a sub query
	alias    *string           // set an alias for the queried table
	join     []*GetJoin        // join query
	where    Filter            // WHERE condition to filter data
	group    []string          // group query result
	having   Filter            // use HAVING to filter data after grouping
	union    []*union          // union query
	order    []string          // order query result
	orderMap map[string]string // ordered columns map list
	limit    *int64            // limit the number of query result
	offset   *int64            // query result offset
}

// NewGet for SELECT
func NewGet(way *Way) *Get {
	return &Get{
		schema:   newSchema(way),
		orderMap: map[string]string{},
	}
}

// Clone create a new object based on all properties of the current object
func (s *Get) Clone() *Get {
	get := NewGet(s.schema.way)
	get.schema.ctx = s.schema.ctx
	get.schema.comment = s.schema.comment
	get.schema.table = s.schema.table
	if get.with != nil {
		get.with = s.with[:]
	}
	get.Column(s.column...)
	if s.subQuery != nil {
		get.subQuery = NewSubQuery(s.subQuery.prepare, s.subQuery.args...)
	}
	if s.alias != nil {
		get.TableAlias(*s.alias)
	}
	for _, v := range s.join {
		join := NewGetJoin(v.joinType)
		join.table = v.table
		if v.subQuery != nil {
			join.subQuery = NewSubQuery(v.subQuery.prepare, v.subQuery.args...)
		}
		if v.alias != nil {
			join.TableAlias(*v.alias)
		}
		join.on = v.on
		get.join = append(get.join, join)
	}
	if s.where != nil {
		get.where = s.where.Copy(s.where)
	}
	get.Group(s.group...)
	if s.having != nil {
		get.having = s.having.Copy(s.having)
	}
	for _, v := range s.union {
		get.union = append(get.union, &union{
			unionType: v.unionType,
			prepare:   v.prepare,
			args:      v.args,
		})
	}
	if s.order != nil {
		get.order = s.order[:]
	}
	for k, v := range s.orderMap {
		get.orderMap[k] = v
	}
	if s.limit != nil {
		get.Limit(*s.limit)
	}
	if s.offset != nil {
		get.Offset(*s.offset)
	}
	return get
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
	if alias == "" || prepare == "" {
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
	for i := len(alias) - 1; i >= 0; i-- {
		if alias[i] != "" {
			s.TableAlias(alias[i])
			break
		}
	}
	return s
}

// TableSubQuery table is a query SQL statement
func (s *Get) TableSubQuery(prepare string, args ...interface{}) *Get {
	s.subQuery = NewSubQuery(prepare, args...)
	return s
}

// TableSubQueryGet table is a query SQL statement
func (s *Get) TableSubQueryGet(get *Get, alias ...string) *Get {
	if get == nil {
		return s
	}
	prepare, args := get.SQL()
	s.subQuery = NewSubQuery(prepare, args...)
	for i := len(alias) - 1; i >= 0; i-- {
		if alias[i] != "" {
			s.TableAlias(alias[i])
			break
		}
	}
	return s
}

// TableAlias table alias name, don't forget to call the current method when the table is a SQL statement
func (s *Get) TableAlias(alias string) *Get {
	s.alias = &alias
	return s
}

// Join for join one or more tables
func (s *Get) Join(joins ...*GetJoin) *Get {
	length := len(joins)
	for i := 0; i < length; i++ {
		if joins[i] == nil {
			continue
		}
		s.join = append(s.join, joins[i])
	}
	return s
}

// InnerJoin for inner join
func (s *Get) InnerJoin(table ...string) *GetJoin {
	return NewGetJoin(SqlJoinInner, table...)
}

// LeftJoin for left join
func (s *Get) LeftJoin(table ...string) *GetJoin {
	return NewGetJoin(SqlJoinLeft, table...)
}

// RightJoin for right join
func (s *Get) RightJoin(table ...string) *GetJoin {
	return NewGetJoin(SqlJoinRight, table...)
}

// FullJoin for full join
func (s *Get) FullJoin(table ...string) *GetJoin {
	return NewGetJoin(SqlJoinFull, table...)
}

// Where set where
func (s *Get) Where(where Filter) *Get {
	s.where = where
	return s
}

// AndWhere and where
func (s *Get) AndWhere(where ...Filter) *Get {
	if s.where == nil {
		s.where = s.schema.way.Filter()
	}
	s.where.Filter(where...)
	return s
}

// OrWhere or where
func (s *Get) OrWhere(where ...Filter) *Get {
	if s.where == nil {
		s.where = s.schema.way.Filter()
	}
	s.where.OrFilter(where...)
	return s
}

// WhereIn set where IN
func (s *Get) WhereIn(column string, values ...interface{}) *Get {
	return s.Where(NewFilter().In(column, values...))
}

// WhereEqual set where EQUAL
func (s *Get) WhereEqual(column string, value interface{}) *Get {
	return s.Where(NewFilter().Equal(column, value))
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
	if prepare == "" {
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
	if prepare == "" {
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

// AddCol append the columns list of query
func (s *Get) AddCol(column ...string) *Get {
	s.column = append(s.column, column...)
	return s
}

// Column set the columns list of query
func (s *Get) Column(column ...string) *Get {
	s.column = column
	return s
}

// orderBy set order by column
func (s *Get) orderBy(column string, order string) *Get {
	if column == "" || (order != SqlAsc && order != SqlDesc) {
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
	// orderParamRegexp `column_name_first:a,column_name_second:d` => `column_name_first ASC, column_name_second DESC`
	orderParamRegexp = regexp.MustCompile(`^([a-zA-Z][a-zA-Z0-9_]*([.][a-zA-Z][a-zA-Z0-9_]*)*):([ad])$`)
)

// OrderParam set the column sorting list in batches through regular expressions according to the request parameter value
func (s *Get) OrderParam(param string) *Get {
	for _, v := range strings.Split(param, ",") {
		if len(v) > 32 {
			continue
		}
		match := orderParamRegexp.FindAllStringSubmatch(strings.TrimSpace(v), -1)
		length := len(match)
		if length != 1 {
			continue
		}
		matched := match[0]
		length = len(matched)
		if matched[length-1][0] == 97 {
			s.Asc(matched[1])
			continue
		}
		if matched[length-1][0] == 100 {
			s.Desc(matched[1])
		}
	}
	return s
}

// Limit set limit
func (s *Get) Limit(limit int64) *Get {
	if limit <= 0 {
		return s
	}
	s.limit = &limit
	return s
}

// Offset set offset
func (s *Get) Offset(offset int64) *Get {
	if offset < 0 {
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
	if s.schema.table == "" && s.subQuery == nil {
		return
	}
	buf := comment(s.schema)
	defer putSqlBuilder(buf)

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
		buf.WriteString(" ")
	}

	buf.WriteString("SELECT ")

	if s.column == nil {
		buf.WriteString("*")
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

	if s.alias != nil && *s.alias != "" {
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
			if joinPrepare != "" {
				buf.WriteString(" ")
				buf.WriteString(joinPrepare)
				args = append(args, joinArgs...)
			}
		}
	}

	if s.where != nil {
		where, whereArgs := s.where.SQL()
		if where != "" {
			buf.WriteString(" WHERE ")
			buf.WriteString(where)
			args = append(args, whereArgs...)
		}
	}

	if s.group != nil {
		if len(s.group) > 0 {
			buf.WriteString(" GROUP BY ")
			buf.WriteString(strings.Join(s.group, ", "))
			if s.having != nil {
				having, havingArgs := s.having.SQL()
				if having != "" {
					buf.WriteString(" HAVING ")
					buf.WriteString(having)
					args = append(args, havingArgs...)
				}
			}
		}
	}

	if s.union != nil {
		for _, u := range s.union {
			buf.WriteString(" ")
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
			TableSubQuery(prepare, args...).
			TableAlias("count_table_rows").
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
	if prepare == "" {
		return
	}

	buf := getSqlBuilder()
	defer putSqlBuilder(buf)
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
	return s.schema.way.ScanAllContext(s.schema.ctx, result, prepare, args...)
}

// RowsNextRow execute the built SQL statement and scan query result, only scan one row
func (s *Get) RowsNextRow(dest ...interface{}) error {
	return s.Query(func(rows *sql.Rows) error { return s.schema.way.RowsNextRow(rows, dest...) })
}

// CountQuery execute the built SQL statement and scan query result, count + query
func (s *Get) CountQuery(query func(rows *sql.Rows) (err error), countColumn ...string) (int64, error) {
	count, err := s.Count(countColumn...)
	if err != nil || count == 0 {
		return count, err
	}
	return count, s.Query(query)
}

// CountGet execute the built SQL statement and scan query result, count + get
func (s *Get) CountGet(result interface{}, countColumn ...string) (int64, error) {
	count, err := s.Count(countColumn...)
	if err != nil || count == 0 {
		return count, err
	}
	return count, s.Get(result)
}
