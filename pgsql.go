package hey

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	Dollar = "$"
)

type Pgsql struct{}

var (
	DefaultPgsql = &Pgsql{}
)

// Prepare fix pgsql SQL statement, ?, ?, ?... => $1, $2, $3...
func (s *Pgsql) Prepare(str string) string {
	var index int64
	latest := getSqlBuilder()
	defer putSqlBuilder(latest)
	origin := []byte(str)
	length := len(origin)
	byte36 := Dollar[0]      // $
	byte63 := Placeholder[0] // ?
	for i := 0; i < length; i++ {
		if origin[i] == byte63 {
			index++
			latest.WriteByte(byte36)
			latest.WriteString(strconv.FormatInt(index, 10))
		} else {
			latest.WriteByte(origin[i])
		}
	}
	return latest.String()
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
