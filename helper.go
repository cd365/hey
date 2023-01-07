package hey

// RemoveDuplicate remove duplicate element
func RemoveDuplicate(dynamic ...interface{}) (result []interface{}) {
	has := make(map[interface{}]*struct{})
	ok := false
	for _, v := range dynamic {
		if _, ok = has[v]; ok {
			continue
		}
		has[v] = &struct{}{}
		result = append(result, v)
	}
	return
}
