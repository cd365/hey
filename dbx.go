package hey

import (
	"fmt"
	"reflect"
)

func ArgString(i interface{}) string {
	if i == nil {
		return SqlNull
	}
	t, v := reflect.TypeOf(i), reflect.ValueOf(i)
	k := t.Kind()
	for k == reflect.Ptr {
		if v.IsNil() {
			return SqlNull
		}
		t, v = t.Elem(), v.Elem()
		k = t.Kind()
	}
	// any base type to string
	tmp := v.Interface()
	switch tmp.(type) {
	case bool:
		return fmt.Sprintf("%t", tmp)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", tmp)
	case float32, float64:
		return fmt.Sprintf("%f", tmp)
	case string:
		return fmt.Sprintf("'%s'", tmp)
	default:
		return fmt.Sprintf("'%v'", tmp)
	}
}

func PrepareArgs(prepare string, args []interface{}) string {
	count := len(args)
	if count == 0 {
		return prepare
	}
	index := 0
	origin := []byte(prepare)
	latest := getSqlBuilder()
	defer putSqlBuilder(latest)
	length := len(origin)
	byte63 := SqlPlaceholder[0]
	for i := 0; i < length; i++ {
		if origin[i] == byte63 && index < count {
			latest.WriteString(ArgString(args[index]))
			index++
		} else {
			latest.WriteByte(origin[i])
		}
	}
	return latest.String()
}
