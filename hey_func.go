// This is where some of the most commonly used functions are stored.

package hey

import (
	"maps"
	"strings"
	"unsafe"

	"github.com/cd365/hey/v6/cst"
)

// AnyAny Convert any type of slice to []any.
func AnyAny[T any](slice []T) []any {
	result := make([]any, len(slice))
	for i, v := range slice {
		result[i] = v
	}
	return result
}

// DiscardDuplicate Slice deduplication.
func DiscardDuplicate[T comparable](discard func(tmp T) bool, dynamic ...T) (result []T) {
	length := len(dynamic)
	mp := make(map[T]*struct{}, length)
	ok := false
	result = make([]T, 0, length)
	for i := range length {
		if _, ok = mp[dynamic[i]]; ok {
			continue
		}
		if discard != nil {
			if discard(dynamic[i]) {
				continue
			}
		}
		mp[dynamic[i]] = nil
		result = append(result, dynamic[i])
	}
	return result
}

// MergeArray Merge slices.
func MergeArray[V any](values ...[]V) []V {
	length := len(values)
	result := make([]V, 0)
	for i := range length {
		if i == 0 {
			result = values[i]
			continue
		}
		result = append(result, values[i]...)
	}
	return result
}

// MergeMap Merge maps.
func MergeMap[K comparable, V any](values ...map[K]V) map[K]V {
	length := len(values)
	result := make(map[K]V, 8)
	for i := range length {
		maps.Copy(result, values[i])
	}
	return result
}

// MapToArray Create a slice from a map.
func MapToArray[K comparable, V any, W any](values map[K]V, fc func(k K, v V) W) []W {
	if fc == nil {
		return nil
	}
	length := len(values)
	result := make([]W, 0, length)
	for index, value := range values {
		result = append(result, fc(index, value))
	}
	return result
}

// MapToMap Create a map based on another map.
func MapToMap[K comparable, V any, X comparable, Y any](values map[K]V, fc func(k K, v V) (X, Y)) map[X]Y {
	if fc == nil {
		return nil
	}
	result := make(map[X]Y, len(values))
	for key, value := range values {
		k, v := fc(key, value)
		result[k] = v
	}
	return result
}

// ArrayToArray Create a slice from another slice.
func ArrayToArray[V any, W any](values []V, fc func(k int, v V) W) []W {
	if fc == nil {
		return nil
	}
	result := make([]W, len(values))
	for index, value := range values {
		result[index] = fc(index, value)
	}
	return result
}

// ArrayToMap Create a map from a slice.
func ArrayToMap[V any, K comparable, W any](values []V, fc func(v V) (K, W)) map[K]W {
	if fc == nil {
		return nil
	}
	length := len(values)
	result := make(map[K]W, length)
	for i := range length {
		k, v := fc(values[i])
		result[k] = v
	}
	return result
}

// ArrayDiscard Delete some elements from a slice.
func ArrayDiscard[V any](values []V, discard func(k int, v V) bool) []V {
	if values == nil || discard == nil {
		return values
	}
	result := make([]V, 0, len(values))
	for index, value := range values {
		if !discard(index, value) {
			result = append(result, value)
		}
	}
	return result
}

// MapDiscard Delete some elements from the map.
func MapDiscard[K comparable, V any](values map[K]V, discard func(k K, v V) bool) map[K]V {
	if values == nil || discard == nil {
		return values
	}
	result := make(map[K]V, len(values))
	for index, value := range values {
		if !discard(index, value) {
			result[index] = value
		}
	}
	return result
}

// JoinString Concatenate multiple strings in sequence.
func JoinString(elems ...string) string {
	builder := poolGetStringBuilder()
	defer poolPutStringBuilder(builder)
	length := len(elems)
	for i := range length {
		builder.WriteString(elems[i])
	}
	return builder.String()
}

// LastNotEmptyString Get last not empty string, return empty string if it does not exist.
func LastNotEmptyString(sss []string) string {
	for i := len(sss) - 1; i >= 0; i-- {
		if sss[i] != cst.Empty {
			return sss[i]
		}
	}
	return cst.Empty
}

// InValues Build column IN ( values[0].attributeN, values[1].attributeN, values[2].attributeN ... )
func InValues[T any](values []T, fc func(tmp T) any) []any {
	if fc == nil {
		return nil
	}
	length := len(values)
	if length == 0 {
		return nil
	}
	num := 0
	result := make([]any, 0, length)
	for _, value := range values {
		elem := fc(value)
		if elem != nil {
			num++
			result = append(result, elem)
		}
	}
	return result[:num]
}

// InGroupValues Build ( column1, column2, column3 ... ) IN ( ( values[0].attribute1, values[0].attribute2, values[0].attribute3 ... ), ( values[1].attribute1, values[1].attribute2, values[1].attribute3 ... ) ... )
func InGroupValues[T any](values []T, fc func(tmp T) []any) [][]any {
	if fc == nil {
		return nil
	}
	length := len(values)
	if length == 0 {
		return nil
	}
	num := 0
	result := make([][]any, 0, length)
	for _, value := range values {
		elem := fc(value)
		if elem != nil {
			num++
			result = append(result, elem)
		}
	}
	return result[:num]
}

// ValuePascal Pascal case.
func ValuePascal(value string) string {
	if value == cst.Empty {
		return cst.Empty
	}
	length := len(value)
	result := make([]byte, 0, length)
	next2upper := true
	for i := range length {
		if value[i] == '_' {
			next2upper = true
			continue
		}
		if next2upper && value[i] >= 'a' && value[i] <= 'z' {
			result = append(result, value[i]-32)
		} else {
			result = append(result, value[i])
		}
		next2upper = false
	}
	return string(result[:])
}

// ValueCamel Camel case.
func ValueCamel(value string) string {
	if value == cst.Empty {
		return cst.Empty
	}
	value = ValuePascal(value)
	return JoinString(strings.ToLower(value[0:1]), value[1:])
}

// ValueUnderline Underline case.
func ValueUnderline(value string) string {
	if value == cst.Empty {
		return cst.Empty
	}
	length := len(value)
	result := make([]byte, 0, length)
	for i := range length {
		if value[i] >= 'A' && value[i] <= 'Z' {
			if i > 0 {
				result = append(result, '_')
			}
			result = append(result, value[i]+32)
		} else {
			result = append(result, value[i])
		}
	}
	return *(*string)(unsafe.Pointer(&result))
}
