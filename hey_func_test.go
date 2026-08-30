package hey

import (
	"reflect"
	"testing"
)

func TestDiscardDuplicateAnyHashUnsafeComparableValue(t *testing.T) {
	type wrapper struct {
		Value any
	}

	value := wrapper{Value: []byte{1, 2, 3}}
	result := DiscardDuplicateAny(nil, value, value)

	// Values that cannot be hashed are retained because comparing them would panic.
	if !reflect.DeepEqual(result, []any{value, value}) {
		t.Fatalf("unexpected result: %#v", result)
	}
}
