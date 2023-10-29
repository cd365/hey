package hey

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
)

var (
	// sqlBuilder sql builder pool
	sqlBuilder *sync.Pool
)

// init initialize
func init() {
	sqlBuilder = &sync.Pool{}
	sqlBuilder.New = func() interface{} {
		return &strings.Builder{}
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

// WhereEqual set where =
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
	filter := false
	if s.where != nil {
		where, whereArgs := s.where.SQL()
		if where != "" {
			filter = true
			buf.WriteString(" WHERE ")
			buf.WriteString(where)
			args = whereArgs
		}
	}
	if s.schema.way.Config.DeleteMustUseWhere && !filter {
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

	// create, the columns in the first map will be used as the column list for insert data
	create []map[string]interface{}

	// except  exclude some fields not in the add list
	except []string

	// column for INSERT VALUES is query statement
	column []string

	// subQuery INSERT VALUES is query statement
	subQuery *SubQuery
}

// NewAdd for INSERT
func NewAdd(way *Way) *Add {
	add := &Add{
		schema: newSchema(way),
		create: make([]map[string]interface{}, 1),
	}
	add.create[0] = make(map[string]interface{})
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

// Except exclude some columns from insert, only valid for Create and Map methods
func (s *Add) Except(except ...string) *Add {
	s.except = append(s.except, except...)
	return s
}

// getExcept except columns for map
func (s *Add) getExcept() (except map[string]struct{}) {
	except = make(map[string]struct{})
	for _, field := range s.except {
		except[field] = struct{}{}
	}
	return
}

// Set add column-value for insert one or more rows
func (s *Add) Set(column string, value interface{}) *Add {
	for i := range s.create {
		s.create[i][column] = value
	}
	return s
}

// Map add column-value for insert one or more rows, skip ignored fields list
func (s *Add) Map(add map[string]interface{}) *Add {
	length := len(add)
	if length == 0 {
		return s
	}
	except := s.getExcept()
	for column := range add {
		if _, ok := except[column]; ok {
			continue
		}
		s.Set(column, add[column])
	}
	return s
}

// DefaultSet add column-value for insert one or more rows, for set column default value
func (s *Add) DefaultSet(column string, value interface{}) *Add {
	for i := range s.create {
		if _, ok := s.create[i][column]; !ok {
			s.create[i][column] = value
		}
	}
	return s
}

// DefaultMap add column-value for insert one or more rows, for set column default value, skip ignored fields list
func (s *Add) DefaultMap(add map[string]interface{}) *Add {
	length := len(add)
	if length == 0 {
		return s
	}
	except := s.getExcept()
	for column := range add {
		if _, ok := except[column]; ok {
			continue
		}
		s.DefaultSet(column, add[column])
	}
	return s
}

// Create insert by struct, insert one or more rows
func (s *Add) Create(creates ...interface{}) *Add {
	length := len(creates)
	if length == 0 {
		return s
	}
	s.create = make([]map[string]interface{}, 0, length)
	for i := 0; i < length; i++ {
		s.create = append(s.create, StructInsert(creates[i], s.schema.way.Tag, s.except...))
	}
	return s
}

// Batch insert by []map, insert one or more rows
func (s *Add) Batch(batch ...map[string]interface{}) *Add {
	length := len(batch)
	if length == 0 {
		return s
	}
	except := s.getExcept()
	s.create = make([]map[string]interface{}, length)
	for index := range batch {
		s.create[index] = make(map[string]interface{})
		for column := range batch[index] {
			if _, ok := except[column]; ok {
				continue
			}
			s.create[index][column] = batch[index][column]
		}
	}
	return s
}

// Column set insert column for ValuesSubQuery
func (s *Add) Column(column ...string) *Add {
	s.column = column
	return s
}

// ValuesSubQuery values is a query SQL statement
func (s *Add) ValuesSubQuery(prepare string, args ...interface{}) *Add {
	s.subQuery = NewSubQuery(prepare, args...)
	return s
}

// ValuesSubQueryGet values is a query SQL statement
func (s *Add) ValuesSubQueryGet(get *Get) *Add {
	if get == nil {
		return s
	}
	prepare, args := get.SQL()
	s.subQuery = NewSubQuery(prepare, args...)
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
		if len(s.column) > 0 {
			buf.WriteString(" ( ")
			buf.WriteString(strings.Join(s.column, ", "))
			buf.WriteString(" )")
		}
		buf.WriteString(" ")
		qp, qa := s.subQuery.SQL()
		buf.WriteString(qp)
		prepare = buf.String()
		args = qa
		return
	}
	except := s.getExcept()
	rowsCount := len(s.create)
	columnCount := len(s.create[0])
	columns := make([]string, 0, columnCount)
	for field := range s.create[0] {
		if field == "" {
			continue
		}
		if _, ok := except[field]; ok {
			continue
		}
		columns = append(columns, field)
	}
	columnCount = len(columns)
	if columnCount == 0 {
		return
	}
	sort.Strings(columns)
	values := make([][]interface{}, rowsCount)
	for num, add := range s.create {
		values[num] = make([]interface{}, 0, columnCount)
		for _, field := range columns {
			if value, ok := add[field]; !ok {
				values[num] = append(values[num], nil)
			} else {
				values[num] = append(values[num], value)
			}
		}
	}
	place := make([]string, columnCount)
	for i := 0; i < columnCount; i++ {
		place[i] = Placeholder
	}
	buf.WriteString("INSERT INTO ")
	buf.WriteString(s.schema.table)
	buf.WriteString(" ( ")
	buf.WriteString(strings.Join(columns, ", "))
	buf.WriteString(" ) VALUES")
	placeValue := strings.Join(place, ", ")
	for i := 0; i < rowsCount; i++ {
		if i != 0 {
			buf.WriteString(",")
		}
		buf.WriteString(" ( ")
		buf.WriteString(placeValue)
		buf.WriteString(" )")
		args = append(args, values[i]...)
	}
	prepare = buf.String()
	return
}

// Add execute the built SQL statement
func (s *Add) Add() (int64, error) {
	prepare, args := s.SQL()
	return s.schema.way.ExecContext(s.schema.ctx, prepare, args...)
}

// modify set the column to be updated
type modify struct {
	expr string
	args []interface{}
}

// Mod for UPDATE
type Mod struct {
	schema *schema

	// update main updated columns
	update map[string]*modify

	// modify secondary updated columns, the effective condition is len(update) > 0
	secondaryUpdate map[string]*modify

	// except excepted columns
	except map[string]struct{}

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

// Except exclude some columns from update
func (s *Mod) Except(except ...string) *Mod {
	length := len(except)
	for i := 0; i < length; i++ {
		s.except[except[i]] = struct{}{}
	}
	return s
}

// expr build update column expressions and column values
func (s *Mod) expr(column string, expr string, args ...interface{}) *Mod {
	if _, ok := s.except[column]; ok {
		return s
	}
	s.update[column] = &modify{
		expr: expr,
		args: args,
	}
	return s
}

// Set SET column = value
func (s *Mod) Set(column string, value interface{}) *Mod {
	return s.expr(column, fmt.Sprintf("%s = %s", column, Placeholder), value)
}

// Incr SET column = column + value
func (s *Mod) Incr(column string, value interface{}) *Mod {
	return s.expr(column, fmt.Sprintf("%s = %s + %s", column, column, Placeholder), value)
}

// Decr SET column = column - value
func (s *Mod) Decr(column string, value interface{}) *Mod {
	return s.expr(column, fmt.Sprintf("%s = %s - %s", column, column, Placeholder), value)
}

// Map SET column = value by map
func (s *Mod) Map(columnValue map[string]interface{}) *Mod {
	for column, value := range columnValue {
		s.Set(column, value)
	}
	return s
}

// Slice SET column = value by slice, require len(column) = len(value)
func (s *Mod) Slice(column []string, value []interface{}) *Mod {
	len1, len2 := len(column), len(value)
	if len1 != len2 {
		return s
	}
	for i := 0; i < len1; i++ {
		s.Set(column[i], value[i])
	}
	return s
}

// Compare for compare origin and latest to automatically calculate the list of columns and corresponding values that need to be updated
func (s *Mod) Compare(origin interface{}, latest interface{}) *Mod {
	except := make([]string, 0, len(s.except))
	for v := range s.except {
		except = append(except, v)
	}
	return s.Map(StructUpdate(origin, latest, s.schema.way.Tag, except...))
}

// defaultExpr append the update field collection when there is at least one item in the update field collection, for example, set the update timestamp
func (s *Mod) defaultExpr(column string, expr string, args ...interface{}) *Mod {
	if _, ok := s.except[column]; ok {
		return s
	}
	if _, ok := s.update[column]; ok {
		return s
	}
	s.secondaryUpdate[column] = &modify{
		expr: expr,
		args: args,
	}
	return s
}

// DefaultSet SET column = value
func (s *Mod) DefaultSet(column string, value interface{}) *Mod {
	return s.defaultExpr(column, fmt.Sprintf("%s = %s", column, Placeholder), value)
}

// DefaultIncr SET column = column + value
func (s *Mod) DefaultIncr(column string, value interface{}) *Mod {
	return s.defaultExpr(column, fmt.Sprintf("%s = %s + %s", column, column, Placeholder), value)
}

// DefaultDecr SET column = column - value
func (s *Mod) DefaultDecr(column string, value interface{}) *Mod {
	return s.defaultExpr(column, fmt.Sprintf("%s = %s - %s", column, column, Placeholder), value)
}

// DefaultMap SET column = value by map
func (s *Mod) DefaultMap(columnValue map[string]interface{}) *Mod {
	for column, value := range columnValue {
		s.DefaultSet(column, value)
	}
	return s
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
func (s *Mod) WhereIn(column string, values ...interface{}) *Mod {
	return s.Where(NewFilter().In(column, values...))
}

// WhereEqual set where =
func (s *Mod) WhereEqual(column string, value interface{}) *Mod {
	return s.Where(NewFilter().Equal(column, value))
}

// SQL build SQL statement
func (s *Mod) SQL() (prepare string, args []interface{}) {
	if s.schema.table == "" {
		return
	}
	length := len(s.update)
	if length == 0 {
		return
	}
	mod := make(map[string]struct{})
	columns := make([]string, 0, length)
	for column := range s.update {
		if _, ok := s.except[column]; ok {
			continue
		}
		mod[column] = struct{}{}
		columns = append(columns, column)
	}
	for column := range s.secondaryUpdate {
		if _, ok := s.except[column]; ok {
			continue
		}
		if _, ok := mod[column]; ok {
			continue
		}
		columns = append(columns, column)
	}
	length = len(columns)
	sort.Strings(columns)
	field := make([]string, length)
	value := make([]interface{}, 0, length)
	ok := false
	for k, v := range columns {
		if _, ok = s.update[v]; ok {
			field[k] = s.update[v].expr
			value = append(value, s.update[v].args...)
			continue
		}
		field[k] = s.secondaryUpdate[v].expr
		value = append(value, s.secondaryUpdate[v].args...)
	}
	args = value
	buf := comment(s.schema)
	defer putSqlBuilder(buf)
	buf.WriteString("UPDATE ")
	buf.WriteString(s.schema.table)
	buf.WriteString(" SET ")
	buf.WriteString(strings.Join(field, ", "))
	filter := false
	if s.where != nil {
		key, val := s.where.SQL()
		if key != "" {
			filter = true
			buf.WriteString(" WHERE ")
			buf.WriteString(key)
			args = append(args, val...)
		}
	}
	if s.schema.way.Config.UpdateMustUseWhere && !filter {
		args = nil
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
	on       string    // conditions for join query
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

// TableAlias table alias name, don’t forget to call the current method when the table is a SQL statement
func (s *GetJoin) TableAlias(alias string) *GetJoin {
	s.alias = &alias
	return s
}

// On join query condition
func (s *GetJoin) On(on string) *GetJoin {
	s.on = on
	return s
}

// OnEqual join query condition
func (s *GetJoin) OnEqual(left string, right string) *GetJoin {
	return s.On(fmt.Sprintf("%s = %s", left, right))
}

// SQL build SQL statement
func (s *GetJoin) SQL() (prepare string, args []interface{}) {
	buf := getSqlBuilder()
	defer putSqlBuilder(buf)
	buf.WriteString(s.joinType)
	if s.subQuery != nil && s.alias != nil && *s.alias != "" {
		buf.WriteString(" ( ")
		subPrepare, subArgs := s.subQuery.SQL()
		buf.WriteString(subPrepare)
		buf.WriteString(" ) AS ")
		buf.WriteString(*s.alias)
		buf.WriteString(" ON ")
		buf.WriteString(s.on)
		prepare = buf.String()
		args = append(args, subArgs...)
		return
	}
	if s.table != "" {
		buf.WriteString(" ")
		buf.WriteString(s.table)
		if s.alias != nil && *s.alias != "" {
			buf.WriteString(" AS ")
			buf.WriteString(*s.alias)
		}
		buf.WriteString(" ON ")
		buf.WriteString(s.on)
		prepare = buf.String()
		return
	}
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

// TableField table helper
type TableField struct {
	table string // table name
	alias string // table alias name
}

// prefix get prefix value
func (s *TableField) prefix() string {
	if s.alias != "" {
		return s.alias
	}
	return s.table
}

// Alias table or column set alias name
func (s *TableField) Alias(name string, alias string) string {
	return SqlAlias(name, alias)
}

// Field table's field
func (s *TableField) Field(field string, alias ...string) string {
	field = SqlPrefix(s.prefix(), field)
	for i := len(alias) - 1; i >= 0; i-- {
		if alias[i] != "" {
			field = SqlAlias(field, alias[i])
			break
		}
	}
	return field
}

// Fields table's fields
func (s *TableField) Fields(fields ...string) []string {
	prefix := s.prefix()
	for i := len(fields) - 1; i >= 0; i-- {
		fields[i] = SqlPrefix(prefix, fields[i])
	}
	return fields
}

// SetTable set table name
func (s *TableField) SetTable(table string) *TableField {
	s.table = table
	return s
}

// SetAlias set table alias name
func (s *TableField) SetAlias(alias string) *TableField {
	s.alias = alias
	return s
}

// GetTable get table name
func (s *TableField) GetTable() string {
	return s.table
}

// GetAlias get table alias name
func (s *TableField) GetAlias() string {
	return s.alias
}

// newTableField new table helper
func newTableField(table ...string) *TableField {
	s := &TableField{}
	for i := len(table) - 1; i >= 0; i-- {
		if table[i] != "" {
			s.table = table[i]
			break
		}
	}
	return s
}

// Get for SELECT
type Get struct {
	schema   *schema           // query table
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

// TableAlias table alias name, don’t forget to call the current method when the table is a SQL statement
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

// WhereEqual set where =
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

// Query execute the built SQL statement and scan query result
func (s *Get) Query(query func(rows *sql.Rows) (err error)) error {
	prepare, args := s.SQL()
	return s.schema.way.QueryContext(s.schema.ctx, query, prepare, args...)
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

// Get execute the built SQL statement and scan query result
func (s *Get) Get(result interface{}) error {
	prepare, args := s.SQL()
	return s.schema.way.ScanAllContext(s.schema.ctx, result, prepare, args...)
}

// CountGet execute the built SQL statement and scan query result, count + get
func (s *Get) CountGet(result interface{}, countColumn ...string) (count int64, err error) {
	count, err = s.Count(countColumn...)
	if err != nil || count == 0 {
		return
	}
	err = s.Get(result)
	return
}

// CountQuery execute the built SQL statement and scan query result, count + query
func (s *Get) CountQuery(query func(rows *sql.Rows) (err error), countColumn ...string) (count int64, err error) {
	count, err = s.Count(countColumn...)
	if err != nil || count == 0 {
		return
	}
	err = s.Query(query)
	return
}
