package hey

import (
	"fmt"
	"reflect"
	"strings"
)

// InsertUpdater insert on conflict do update
type InsertUpdater interface {
	// Add insert object
	Add(add func(add *Add)) InsertUpdater

	// OnConflict insert on conflict fields
	OnConflict(fields ...string) InsertUpdater

	// Mod update object
	Mod(mod func(mod *Mod)) InsertUpdater

	// SQL insert on conflict do update
	SQL() (prepare string, args []interface{})
}

// BatchUpdater batch update
type BatchUpdater interface {
	// Comment with comment
	Comment(comment string) BatchUpdater

	// Table batch update table name, empty is not allowed
	Table(table string) BatchUpdater

	// Except batch update except field list, like "id", "created_at" ...
	Except(except ...string) BatchUpdater

	// Extra batch update extra fields list, require len(fields) == len(values)
	Extra(fields []string, values []interface{}) BatchUpdater

	// Match batch update filter fields, like "order_id" ...
	Match(fields ...string) BatchUpdater

	// CloneTableStruct setting clone table structure, by CREATE TABLE, BATCH INSERT, SELECT DATA, DROP TABLE
	CloneTableStruct(fc func(originTableName, latestTableName string) []string) BatchUpdater

	// MergePrepareArgs merge prepared SQL statements and parameter lists, by SQL
	MergePrepareArgs(fc func(prepare []string, args [][]interface{}) []string) BatchUpdater

	// Update one of struct, *struct, []struct, []*struct, *[]struct, *[]*struct
	Update(update interface{}) (prepare []string, args [][]interface{})
}

// argsString sql args string value
func argsString(i interface{}) string {
	if i == nil {
		return "NULL"
	}
	switch i.(type) {
	case string:
		return fmt.Sprintf("'%s'", i)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", i)
	case float32, float64:
		return fmt.Sprintf("%f", i)
	case bool:
		return fmt.Sprintf("%t", i)
	}
	typeOf, valueOf := reflect.TypeOf(i), reflect.ValueOf(i)
	kind := typeOf.Kind()
	for kind == reflect.Ptr {
		if valueOf.IsNil() {
			return "NULL"
		}
		typeOf, valueOf = typeOf.Elem(), valueOf.Elem()
		kind = typeOf.Kind()
	}
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.String, reflect.Bool:
		return argsString(valueOf.Interface())
	}
	// not char, varchar, text, int, bigint, float, decimal, bool type
	return fmt.Sprintf("'%v'", valueOf.Interface())
}

// MergePrepareArgs merge prepared SQL statements and parameter lists
func MergePrepareArgs(prepare string, args []interface{}) string {
	length := len(args)
	for i := 0; i < length; i++ {
		prepare = strings.Replace(prepare, Placeholder, argsString(args[i]), 1)
	}
	return prepare
}
