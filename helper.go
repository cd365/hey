package hey

import (
	"database/sql"
)

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

func ScanByte(rows *sql.Rows) ([]map[string][]byte, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	var lineValues [][]byte
	var scanList []interface{}
	var lineResult map[string][]byte
	columnLength := len(columnTypes)
	result := make([]map[string][]byte, 0)
	for rows.Next() {
		lineValues = make([][]byte, columnLength)
		scanList = make([]interface{}, columnLength)
		for i := range lineValues {
			scanList[i] = &lineValues[i]
		}
		if err = rows.Scan(scanList...); err != nil {
			return nil, err
		}
		lineResult = make(map[string][]byte, columnLength)
		for index, value := range lineValues {
			lineResult[columnTypes[index].Name()] = value
		}
		result = append(result, lineResult)
	}
	return result, nil
}

func ScanAny(rows *sql.Rows) ([]map[string]interface{}, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	var lineValues []interface{}
	var scanList []interface{}
	var lineResult map[string]interface{}
	columnLength := len(columnTypes)
	result := make([]map[string]interface{}, 0)
	for rows.Next() {
		lineValues = make([]interface{}, columnLength)
		scanList = make([]interface{}, columnLength)
		for i := range lineValues {
			scanList[i] = &lineValues[i]
		}
		if err = rows.Scan(scanList...); err != nil {
			return nil, err
		}
		lineResult = make(map[string]interface{}, columnLength)
		for index, value := range lineValues {
			lineResult[columnTypes[index].Name()] = value
		}
		result = append(result, lineResult)
	}
	return result, nil
}
