package main

import (
	"fmt"
	"strconv"
	"strings"
)

func convertRow(columnPointers []interface{}, tableInfo *TableInfo) map[string]interface{} {
	convertedRow := make(map[string]interface{})
	for i, fldInfo := range tableInfo.Fields {
		val := *columnPointers[i].(*interface{})
		switch data := val.(type) {
		case []byte:
			switch {
			case strings.Contains(fldInfo.Type, "char") || strings.Contains(fldInfo.Type, "text"):
				convertedRow[fldInfo.Field] = string(data)
				continue
			case strings.Contains(fldInfo.Type, "int"):
				strVal := string(data)
				intVal, _ := strconv.ParseInt(strVal, 10, 64)
				convertedRow[fldInfo.Field] = intVal
				continue
			case strings.Contains(fldInfo.Type, "double") || strings.Contains(fldInfo.Type, "decimal") || strings.Contains(fldInfo.Type, "float"):
				strVal := string(data)
				fltVal, _ := strconv.ParseFloat(strVal, 64)
				convertedRow[fldInfo.Field] = fltVal
				continue
			}
		case int64:
			convertedRow[fldInfo.Field] = data
		case nil:
			convertedRow[fldInfo.Field] = data
		}
	}

	return convertedRow
}

func (e *DbExplorer) getAllRowsFromTable(tableName string) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	rows, err := e.Db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	resultRows := make([]map[string]interface{}, 0)
	tableInfo := e.TablesInfo[tableName]
	colsCount := len(tableInfo.Fields)
	for rows.Next() {
		columns := make([]interface{}, colsCount)
		colPointers := make([]interface{}, colsCount)
		for i := range columns {
			colPointers[i] = &columns[i]
		}
		err := rows.Scan(colPointers...)
		if err != nil {
			return nil, err
		}
		convertedRow := convertRow(colPointers, tableInfo)
		resultRows = append(resultRows, convertedRow)
	}
	return resultRows, nil
}

func (e *DbExplorer) getRowsFromTableByLimit(tableName string, limit int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM %s LIMIT ?", tableName)
	tableInfo := e.TablesInfo[tableName]
	colsCount := len(tableInfo.Fields)
	rows, err := e.Db.Query(query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	results := make([]map[string]interface{}, 0)
	for rows.Next() {
		columns := make([]interface{}, colsCount)
		colPointers := make([]interface{}, colsCount)
		for i := range colPointers {
			colPointers[i] = &columns[i]
		}
		err := rows.Scan(colPointers...)
		if err != nil {
			return nil, err
		}
		convertedRow := convertRow(colPointers, tableInfo)
		results = append(results, convertedRow)
	}

	return results, nil
}

func (e *DbExplorer) getRowsFromTableByLimitAndOffset(tableName string, limit int64, offset int64) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE id > ? LIMIT ?", tableName)
	rows, err := e.Db.Query(query, offset, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tableInfo := e.TablesInfo[tableName]
	colsCount := len(tableInfo.Fields)
	results := make([]map[string]interface{}, 0)
	for rows.Next() {
		columns := make([]interface{}, colsCount)
		colPointers := make([]interface{}, colsCount)
		for i := range colPointers {
			colPointers[i] = &columns[i]
		}
		err := rows.Scan(colPointers...)
		if err != nil {
			return nil, err
		}
		convertedRow := convertRow(colPointers, tableInfo)
		results = append(results, convertedRow)
	}
	return results, nil
}

func (e *DbExplorer) getRowFromTableById(tableName string, id int64) (map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE id = ?", tableName)
	tableInfo := e.TablesInfo[tableName]
	colsCount := len(tableInfo.Fields)
	columns := make([]interface{}, colsCount)
	columnPointers := make([]interface{}, colsCount)
	for i := range columnPointers {
		columnPointers[i] = &columns[i]
	}
	err := e.Db.QueryRow(query, id).Scan(columnPointers...)
	if err != nil {
		return nil, err
	}
	result := convertRow(columnPointers, tableInfo)

	return result, nil
}

func (e *DbExplorer) addRowToTable(tableName string, record map[string]interface{}) error {

}
