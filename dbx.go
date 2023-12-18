package hey

import (
	"fmt"
	"reflect"
	"strings"
)

const (
	null = "NULL"
)

func args2string(i interface{}) string {
	if i == nil {
		return null
	}
	t, v := reflect.TypeOf(i), reflect.ValueOf(i)
	k := t.Kind()
	for k == reflect.Ptr {
		if v.IsNil() {
			return null
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
	length := len(args)
	for i := 0; i < length; i++ {
		prepare = strings.Replace(prepare, Placeholder, args2string(args[i]), 1)
	}
	return prepare
}
