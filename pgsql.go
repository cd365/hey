package hey

import (
	"crypto/md5"
	"fmt"
	"os"
	"reflect"
	"strings"
)

// PgsqlPrepareFix fix postgresql SQL statement
// hey uses `?` as the placeholder symbol of the SQL statement by default
// and may need to use different placeholder symbols for different databases
// use the current method to convert ?, ?, ? ... into $1, $2, $3 ... as placeholders for SQL statements
func PgsqlPrepareFix(str string) string {
	index := 0
	for strings.Contains(str, Placeholder) {
		index++
		str = strings.Replace(str, Placeholder, fmt.Sprintf("$%d", index), 1)
	}
	return str
}

// PgsqlCloneTableStruct clone table structure
func PgsqlCloneTableStruct(originTableName, latestTableName string) []string {
	return []string{fmt.Sprintf("CREATE TABLE %s ( LIKE %s INCLUDING ALL )", latestTableName, originTableName)}
}

type pgsqlInsertUpdate struct {

	// add insert object
	add *Add

	// onConflict list of conflicting fields
	onConflict []string

	// mod update object
	mod *Mod
}

func NewPgsqlInsertUpdater(way *Way) InsertUpdater {
	return &pgsqlInsertUpdate{
		add: NewAdd(way),
		mod: NewMod(way),
	}
}

func (s *pgsqlInsertUpdate) Add(add func(add *Add)) InsertUpdater {
	add(s.add)
	return s
}

func (s *pgsqlInsertUpdate) OnConflict(fields ...string) InsertUpdater {
	s.onConflict = fields
	return s
}

func (s *pgsqlInsertUpdate) Mod(mod func(mod *Mod)) InsertUpdater {
	mod(s.mod)
	return s
}

func (s *pgsqlInsertUpdate) SQL() (prepare string, args []interface{}) {
	prepareAdd, argsAdd := s.add.SQL()
	prepareMod, argsMod := s.mod.SetSQL()

	conflict := getSqlBuilder()
	defer putSqlBuilder(conflict)
	conflict.WriteString(" ON CONFLICT ( ")
	conflict.WriteString(strings.Join(s.onConflict, ", "))
	conflict.WriteString(" ) ")
	if prepareMod == "" {
		// ON CONFLICT( field1 ... ) DO NOTHING;
		conflict.WriteString("DO NOTHING")
	} else {
		// ON CONFLICT( field1, field2, field3 ) DO UPDATE SET key = value, field1 = ?, field2 = 123 ...;
		conflict.WriteString("DO UPDATE SET ")
		conflict.WriteString(prepareMod)
	}
	builder := getSqlBuilder()
	defer putSqlBuilder(builder)
	builder.WriteString(prepareAdd)
	builder.WriteString(conflict.String())
	prepare = builder.String()
	args = argsAdd
	args = append(args, argsMod...)
	return
}

/*
 * implement batch updates:
 * 1. UPDATE student SET name = tmp.name, age = tmp.age FROM ( VALUES ( 1, 'Alice', 18 ), ( 2, 'Frank', 20 ) ) AS tmp ( sid, name, age ) WHERE student.sid = tmp.sid
 * 2. batch insert the data into the temporary table and then update it in batches; create temporary table, insert data, update data, truncate temporary table, drop temporary table
 */

type ofPgsqlBatchUpdate struct {
	fieldsString string
	fields       []string
	values       []interface{}
}

type pgsqlBatchUpdate struct {
	way *Way

	// comment sql prepare comment
	comment string

	// table update table name
	table string

	// except fields list not to be updated
	except []string

	// extraFields attachment key-value pair
	extraFields []string
	extraValues []interface{}

	// matchFields WHERE condition filter fields list
	matchFields []string

	// mergePrepareArgs merge sql prepare and sql args
	mergePrepareArgs func(prepare []string, args [][]interface{}) []string

	// cloneTableStruct, clone table structure, returns the DDL of the cloned table
	cloneTableStruct func(originTableName, latestTableName string) []string

	// where filter data for batch update
	where Filter
}

func NewPgsqlBatchUpdater(way *Way) BatchUpdater {
	return &pgsqlBatchUpdate{
		way:              way,
		cloneTableStruct: PgsqlCloneTableStruct,
		mergePrepareArgs: func(prepare []string, args [][]interface{}) []string {
			for i := range prepare {
				if args[i] == nil || len(args) == 0 {
					continue
				}
				prepare[i] = MergePrepareArgs(prepare[i], args[i])
				args[i] = nil
			}
			return prepare
		},
		where: way.Filter(),
	}
}

// Comment with comment
func (s *pgsqlBatchUpdate) Comment(comment string) BatchUpdater {
	s.comment = comment
	return s
}

// Table of batch update, table name
func (s *pgsqlBatchUpdate) Table(table string) BatchUpdater {
	s.table = table
	return s
}

// Except of batch update, except fields list
func (s *pgsqlBatchUpdate) Except(except ...string) BatchUpdater {
	s.except = except
	return s
}

// Extra of batch update, extra update fields-values list
func (s *pgsqlBatchUpdate) Extra(fields []string, values []interface{}) BatchUpdater {
	fieldsLength, valuesLength := len(fields), len(values)
	if fieldsLength == 0 || fieldsLength != valuesLength {
		return s
	}
	fieldsMap := make(map[string]struct{})
	s.extraFields, s.extraValues = nil, nil
	for i := 0; i < fieldsLength; i++ {
		if _, ok := fieldsMap[fields[i]]; ok {
			continue
		}
		fieldsMap[fields[i]] = struct{}{}
		s.extraFields = append(s.extraFields, fields[i])
		s.extraValues = append(s.extraValues, values[i])
	}
	return s
}

// Match of batch update, match fields list(where condition filter)
func (s *pgsqlBatchUpdate) Match(fields ...string) BatchUpdater {
	fieldsMap := make(map[string]struct{})
	length := len(fields)
	s.matchFields = nil
	for i := 0; i < length; i++ {
		if _, ok := fieldsMap[fields[i]]; ok {
			continue
		}
		fieldsMap[fields[i]] = struct{}{}
		s.matchFields = append(s.matchFields, fields[i])
	}
	return s
}

// modify group sql using map[string][]*ofPgsqlBatchUpdate
func (s *pgsqlBatchUpdate) modify(updates interface{}) (filterFieldsMap map[string]struct{}, filterFieldsSlice []string, batch map[string][]*ofPgsqlBatchUpdate) {
	batch = make(map[string][]*ofPgsqlBatchUpdate)

	// filter fields list
	length := len(s.matchFields)
	filterFieldsMap = map[string]struct{}{}
	filterFieldsSlice = make([]string, 0, length)
	for i := 0; i < length; i++ {
		if s.matchFields[i] == "" {
			continue
		}
		if _, ok := filterFieldsMap[s.matchFields[i]]; ok {
			continue
		}
		filterFieldsMap[s.matchFields[i]] = struct{}{}
		filterFieldsSlice = append(filterFieldsSlice, s.matchFields[i])
	}
	if len(filterFieldsSlice) < 1 {
		return
	}

	// allow filter fields
	length = len(s.except)
	exclude := make([]string, 0, length)
	for i := 0; i < length; i++ {
		if _, ok := filterFieldsMap[s.except[i]]; !ok {
			exclude = append(exclude, s.except[i])
		}
	}

	typeOf, valueOf := reflect.TypeOf(updates), reflect.ValueOf(updates)
	kind := typeOf.Kind()
	for kind == reflect.Ptr {
		typeOf, valueOf = typeOf.Elem(), valueOf.Elem()
		kind = typeOf.Kind()
	}

	ffmLen := len(filterFieldsMap)

	addBatch := func(fields []string, values []interface{}) {
		fieldsLength, valuesLength := len(fields), len(values)
		if fieldsLength <= 1 || fieldsLength != valuesLength {
			return
		}
		if ffmLen == fieldsLength {
			same := 0
			for _, v := range fields {
				if _, ok := filterFieldsMap[v]; ok {
					same++
				}
			}
			if ffmLen == same {
				return
			}
		}
		// append custom field-value
		for index, field := range s.extraFields {
			fields = append(fields, field)
			values = append(values, s.extraValues[index])
		}
		update := &ofPgsqlBatchUpdate{
			fieldsString: fmt.Sprintf("( %s )", strings.Join(fields, ", ")),
			fields:       fields,
			values:       values,
		}
		batch[update.fieldsString] = append(batch[update.fieldsString], update)
	}

	// struct
	if kind == reflect.Struct {
		addBatch(StructModify(valueOf.Interface(), s.way.Tag, exclude...))
	}

	// []struct || []*struct
	if kind == reflect.Slice {
		count := valueOf.Len()
		for i := 0; i < count; i++ {
			addBatch(StructModify(valueOf.Index(i).Interface(), s.way.Tag, exclude...))
		}
	}

	return
}

// MergePrepareArgs merge prepared SQL statements and parameter lists
func (s *pgsqlBatchUpdate) MergePrepareArgs(fc func(prepare []string, args [][]interface{}) []string) BatchUpdater {
	s.mergePrepareArgs = fc
	return s
}

// CloneTableStruct setting clone table structure
func (s *pgsqlBatchUpdate) CloneTableStruct(fc func(originTableName, latestTableName string) []string) BatchUpdater {
	s.cloneTableStruct = fc
	return s
}

// update of batch update, construct sql statements
func (s *pgsqlBatchUpdate) updateOfSql(
	updates interface{},
) (prepareGroup []string, argsGroup [][]interface{}) {
	ffm, ffs, batch := s.modify(updates)
	serial := 1
	addUpdate := func(fieldsString string, list []*ofPgsqlBatchUpdate) (prepare string, args []interface{}) {
		b := getSqlBuilder()
		defer putSqlBuilder(b)

		if s.comment != "" {
			b.WriteString(fmt.Sprintf("/* %s@%d */", s.comment, serial))
		}

		b.WriteString("UPDATE ")
		b.WriteString(s.table)
		b.WriteString(" SET ")

		var valuesString string
		fieldsMap := make(map[string]struct{})
		for k, v := range list {
			// values
			args = append(args, v.values...)

			// fields
			if k == 0 {
				count := len(v.fields)
				setFields := make([]string, 0, count)
				setValues := make([]string, count)
				for p, q := range v.fields {
					fieldsMap[q] = struct{}{}
					if _, ok := ffm[q]; !ok {
						setFields = append(setFields, fmt.Sprintf("%s = tmp.%s", q, q))
					}
					setValues[p] = Placeholder
				}
				valuesString = fmt.Sprintf("( %s )", strings.Join(setValues, ", "))
				b.WriteString(strings.Join(setFields, ", "))
				b.WriteString(" FROM ( VALUES ")
				b.WriteString(valuesString)
				continue
			}

			// append valuesString
			b.WriteString(", ")
			b.WriteString(valuesString)
		}

		b.WriteString(" ) AS tmp ")

		// tmp table fields
		b.WriteString(fieldsString)

		// set where
		for _, v := range ffs {
			if _, ok := fieldsMap[v]; !ok {
				continue
			}
			s.where.And(fmt.Sprintf("%s.%s = tmp.%s", s.table, v, v))
		}
		wherePrepare, whereArgs := s.where.SQL()
		if wherePrepare != "" {
			b.WriteString(" WHERE ")
			b.WriteString(wherePrepare)
			args = append(args, whereArgs...)
		}

		prepare = b.String()
		return
	}
	for k, v := range batch {
		prepare, args := addUpdate(k, v)
		prepareGroup = append(prepareGroup, prepare)
		argsGroup = append(argsGroup, args)
		serial++
	}
	// WARN: beware of sql inject
	prepareGroup = s.mergePrepareArgs(prepareGroup, argsGroup)
	argsGroup = make([][]interface{}, len(prepareGroup))
	return
}

// SafeUpdates create template table, batch insert data into template table, update table data by template table, drop template table
func (s *pgsqlBatchUpdate) updateOfTable(updates interface{}) (prepareGroup []string, argsGroup [][]interface{}) {
	ffm, ffs, batch := s.modify(updates)
	now := s.way.Now()
	serial := 1
	addUpdate := func(fieldsString string, list []*ofPgsqlBatchUpdate) {
		if s.comment == "" {
			s.comment = fmt.Sprintf("multiple steps batch updates %s", s.table)
		}
		sqlComment := fmt.Sprintf("%s@%d", s.comment, serial)

		// create template table
		key := fmt.Sprintf("%d_%d", os.Getpid(), now.UnixNano())
		table := s.table
		if index := strings.LastIndex(table, "."); index >= 0 {
			// warn: if "." appears at the end of table, panic
			table = table[index+1:]
		}
		// set template table name
		tmpTable := fmt.Sprintf("%s_%s_%x", table, fmt.Sprintf("%x", md5.Sum([]byte(key)))[:6], serial-1)
		prepare := s.cloneTableStruct(table, tmpTable)
		createTable := func(i int) {
			createSql := getSqlBuilder()
			defer putSqlBuilder(createSql)
			createSql.WriteString(fmt.Sprintf("/* %s.1.%d */", sqlComment, i+1))
			createSql.WriteString(prepare[i])
			prepareGroup = append(prepareGroup, createSql.String())
			argsGroup = append(argsGroup, nil)
		}
		for i := range prepare {
			createTable(i)
		}

		dropSql := getSqlBuilder()
		defer putSqlBuilder(dropSql)
		dropSql.WriteString(fmt.Sprintf("/* %s.4 */", sqlComment))
		dropSql.WriteString(fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTable))
		dropPrepare := dropSql.String()

		// drop template table
		defer func() {
			prepareGroup = append(prepareGroup, dropPrepare)
			argsGroup = append(argsGroup, nil)
		}()

		insertSql := getSqlBuilder()
		defer putSqlBuilder(insertSql)

		insertSql.WriteString(fmt.Sprintf("/* %s@%d.2 */", s.comment, serial))

		insertSql.WriteString("INSERT INTO ")
		insertSql.WriteString(tmpTable)
		insertSql.WriteString(" ")
		insertSql.WriteString(fieldsString)
		insertSql.WriteString(" VALUES ")

		var addArgs []interface{}
		var modArgs []interface{}
		var valuesString string
		var setFields []string
		var setValues []string
		fieldsMap := make(map[string]struct{})
		for k, v := range list {
			// values
			addArgs = append(addArgs, v.values...)

			// fields
			if k == 0 {
				count := len(v.fields)
				setFields = make([]string, 0, count)
				setValues = make([]string, count)
				for p, q := range v.fields {
					fieldsMap[q] = struct{}{}
					if _, ok := ffm[q]; !ok {
						setFields = append(setFields, fmt.Sprintf("%s = tmp.%s", q, q))
					}
					setValues[p] = Placeholder
				}
				valuesString = fmt.Sprintf("( %s )", strings.Join(setValues, ", "))
				insertSql.WriteString(valuesString)
				continue
			}

			// append valuesString
			insertSql.WriteString(", ")
			insertSql.WriteString(valuesString)
		}
		updateSql := getSqlBuilder()
		defer putSqlBuilder(updateSql)

		updateSql.WriteString(fmt.Sprintf("/* %s@%d.3 */", s.comment, serial))

		updateSql.WriteString("UPDATE ")
		updateSql.WriteString(table)
		updateSql.WriteString(" SET ")
		updateSql.WriteString(strings.Join(setFields, ", "))
		updateSql.WriteString(" FROM ")
		updateSql.WriteString(tmpTable)
		updateSql.WriteString(" AS tmp")

		// set where
		for _, v := range ffs {
			if _, ok := fieldsMap[v]; !ok {
				continue
			}
			s.where.And(fmt.Sprintf("%s.%s = tmp.%s", table, v, v))
		}
		wherePrepare, whereArgs := s.where.SQL()
		if wherePrepare != "" {
			updateSql.WriteString(" WHERE ")
			updateSql.WriteString(wherePrepare)
			modArgs = append(modArgs, whereArgs...)
		}

		prepareGroup = append(prepareGroup, insertSql.String())
		argsGroup = append(argsGroup, addArgs)

		prepareGroup = append(prepareGroup, updateSql.String())
		argsGroup = append(argsGroup, modArgs)

		return
	}
	for k, v := range batch {
		addUpdate(k, v)
		serial++
	}
	return
}

// Update of batch update
func (s *pgsqlBatchUpdate) Update(update interface{}) ([]string, [][]interface{}) {
	if s.cloneTableStruct != nil {
		return s.updateOfTable(update)
	}
	return s.updateOfSql(update)
}

/*
 * implement batch deletes:
 * DELETE FROM student USING ( VALUES ( 6 ), ( 7 ), ( 8 ) ) AS tmp ( sid ) WHERE student.sid = tmp.sid;
 * DELETE FROM student USING ( VALUES ( 9, 'Alice Smith' ) ) AS tmp ( sid, name ) WHERE student.sid = tmp.sid AND student.name = tmp.name;
 */
