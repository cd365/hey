package hey

import (
	"fmt"
	"strconv"
	"strings"
	"unsafe"
)

type Pgsql struct{}

// Prepare fix pgsql SQL statement, ?, ?, ?... => $1, $2, $3...
func (s *Pgsql) Prepare(str string) string {
	p := *(*[]byte)(unsafe.Pointer(&str))
	q := getSqlBuilder()
	defer putSqlBuilder(q)
	var index int64 = 1
	length := len(p)
	for i := 0; i < length; i++ {
		if p[i] != '?' {
			q.WriteByte(p[i])
		} else {
			q.WriteByte('$')
			q.WriteString(strconv.FormatInt(index, 10))
			index++
		}
	}
	return q.String()
}

func (s *Pgsql) IfNull(field string, value interface{}) string {
	return fmt.Sprintf("COALESCE(%s,%s)", field, args2string(value))
}

// InsertOnConflict rely on the unique index of the table
func (s *Pgsql) InsertOnConflict(
	onConflictColumns []string, // unique index of column or columns
	updateColumns []string, // need update columns
) (prepare string, args []interface{}) {
	length := len(onConflictColumns)
	if length == 0 {
		return
	}
	length = len(updateColumns)
	buffer := getSqlBuilder()
	defer putSqlBuilder(buffer)
	buffer.WriteString("ON CONFLICT(")
	buffer.WriteString(strings.Join(onConflictColumns, ", "))
	buffer.WriteString(") ")
	length = len(updateColumns)
	if length == 0 {
		buffer.WriteString("DO NOTHING")
		prepare = buffer.String()
		return
	}
	buffer.WriteString("DO UPDATE SET ")
	assign := make([]string, length)
	for i := 0; i < length; i++ {
		assign[i] = fmt.Sprintf("%s = EXCLUDED.%s", updateColumns[i], updateColumns[i])
	}
	buffer.WriteString(strings.Join(assign, ", "))
	prepare = buffer.String()
	return
}

// CloneTableStruct clone table structure
func (s *Pgsql) CloneTableStruct(
	dst string, // new table name
	src string, // refer table name
	seq string, // table sequence column name
) (
	create []string, // create ddl
	drop []string, // drop ddl
) {
	create = append(
		create,
		fmt.Sprintf("DROP TABLE IF EXISTS %s;", dst),
		// INCLUDING CONSTRAINTS INCLUDING INDEXES INCLUDING COMMENTS
		fmt.Sprintf("CREATE TABLE %s ( LIKE %s INCLUDING ALL );", dst, src),
	)
	drop = append(
		drop,
		fmt.Sprintf("DROP TABLE IF EXISTS %s;", dst),
	)
	if seq != "" {
		nameSeq := fmt.Sprintf("%s_%s_seq", dst, seq)
		// SELECT setval('example_id_seq', 100000);
		// SELECT nextval('example_id_seq');
		create = append(
			create,
			fmt.Sprintf("DROP SEQUENCE IF EXISTS %s;", nameSeq),
			fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s START 1;", nameSeq),
			fmt.Sprintf("ALTER TABLE IF EXISTS %s ALTER COLUMN %s SET DEFAULT nextval('%s');", dst, seq, nameSeq),
		)
		drop = append(
			drop,
			fmt.Sprintf("DROP SEQUENCE IF EXISTS %s;", nameSeq),
		)
	}
	return
}
