// Postgres.

package pgsql

import (
	"bytes"
	"fmt"
	"github.com/cd365/hey"
	"strconv"
	"strings"
	"sync"
)

var (
	builder *sync.Pool
)

func init() {
	builder = &sync.Pool{}
	builder.New = func() interface{} {
		return &strings.Builder{}
	}
}

func getBuilder() *strings.Builder {
	return builder.Get().(*strings.Builder)
}

func putBuilder(b *strings.Builder) {
	b.Reset()
	builder.Put(b)
}

// Prepare Fix pgsql SQL statement, ?, ?, ?... => $1, $2, $3...
func Prepare(str string) string {
	var index int64
	latest := getBuilder()
	defer putBuilder(latest)
	origin := []byte(str)
	length := len(origin)
	byte36 := hey.SqlDollar[0]      // $
	byte63 := hey.SqlPlaceholder[0] // ?
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

// PrepareString Merge executed SQL statements and parameters.
func PrepareString(prepare string, args []interface{}) string {
	length := len(args)
	if length == 0 {
		return prepare
	}
	bts := []byte(prepare)
	buf := getBuilder()
	defer putBuilder(buf)
	for i := 0; i < length; i++ {
		tmp := fmt.Sprintf("%s%d", hey.SqlDollar, i+1)
		n := bytes.Index(bts, []byte(tmp))
		if n < 0 {
			return prepare
		}
		if _, err := buf.Write(bts[:n]); err != nil {
			panic(err)
		}
		if _, err := buf.WriteString(hey.ArgString(args[i])); err != nil {
			panic(err)
		}
		n += len(tmp)
		bts = bts[n:]
	}
	if _, err := buf.Write(bts); err != nil {
		panic(err)
	}
	return buf.String()
}

// InsertOnConflict Rely on the unique index of the table.
func InsertOnConflict(
	onConflictColumns []string, // unique index of column or columns.
	updateColumns []string, // need update columns.
) (prepare string, args []interface{}) {
	length := len(onConflictColumns)
	if length == 0 {
		return
	}
	buffer := getBuilder()
	defer putBuilder(buffer)
	buffer.WriteString("ON CONFLICT (")
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

// CloneTableStruct Clone table structure.
func CloneTableStruct(
	dst string, // new table name.
	src string, // refer table name.
	seq string, // table sequence column name.
) (
	create []string, // create ddl.
	drop []string, // drop ddl.
) {
	create = append(
		create,
		fmt.Sprintf("DROP TABLE IF EXISTS %s;", dst),
		// INCLUDING CONSTRAINTS INCLUDING INDEXES INCLUDING COMMENTS.
		fmt.Sprintf("CREATE TABLE %s ( LIKE %s INCLUDING ALL );", dst, src),
	)
	drop = append(
		drop,
		fmt.Sprintf("DROP TABLE IF EXISTS %s;", dst),
	)

	if seq != hey.EmptyString {
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
