package main

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
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
	tableInfo := e.TablesInfo[tableName]
	pkName := tableInfo.findPrimKeyName()
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s > ? LIMIT ?", tableName, *pkName)
	rows, err := e.Db.Query(query, offset, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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
	tableInfo := e.TablesInfo[tableName]
	colsCount := len(tableInfo.Fields)
	primKeyFieldName := tableInfo.findPrimKeyName()

	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", tableName, *primKeyFieldName)
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

func (e *DbExplorer) addRowToTable(tableName string, record map[string]interface{}) (*int64, error) {
	/*
		1. создаем пустую мапу на основе информации о полях таблицы
		2. идем по ключам созданной мапы, смотрим, есть ли в пришедшей мапе значения по ключам в созданной мапе
		3. если значение есть, то добавляем его (можно еще сделать проверку на nil)
		4. если значения нет, то проверяем есть ли в таблице значение по умолчанию для данного поля, если значения по умолчанию нет - то нужно дать значение по умолчанию для данного типа
	*/

	tableInfo := e.TablesInfo[tableName]
	rowForAdding := make(map[string]interface{})
	for _, fldInfo := range tableInfo.Fields {
		if fldInfo.Key == "PRI" {
			continue
		}
		switch {
		case strings.Contains(fldInfo.Type, "char") || strings.Contains(fldInfo.Type, "text"):
			if fldInfo.Null == "NO" {
				rowForAdding[fldInfo.Field] = ""
			} else {
				rowForAdding[fldInfo.Field] = nil
			}
			v, exists := record[fldInfo.Field]
			if exists {
				if val, ok := v.(string); ok {
					rowForAdding[fldInfo.Field] = val
				}
			}
		case strings.Contains(fldInfo.Type, "int"):
			if fldInfo.Null == "NO" {
				rowForAdding[fldInfo.Field] = 0
			} else {
				rowForAdding[fldInfo.Field] = nil
			}
			v, exists := record[fldInfo.Field]
			if exists {
				if val, ok := v.(int); ok {
					rowForAdding[fldInfo.Field] = val
				}
			}
		case strings.Contains(fldInfo.Type, "double") || strings.Contains(fldInfo.Type, "float") || strings.Contains(fldInfo.Type, "decimal"):
			if fldInfo.Null == "NO" {
				rowForAdding[fldInfo.Type] = 0.0
			} else {
				rowForAdding[fldInfo.Type] = nil
			}
			v, exists := record[fldInfo.Field]
			if exists {
				if val, ok := v.(float32); ok {
					rowForAdding[fldInfo.Field] = val
				}
				if val, ok := v.(float64); ok {
					rowForAdding[fldInfo.Field] = val
				}
			}
		}
	}

	columns := make([]string, 0)
	values := make([]interface{}, 0)
	placeholders := make([]string, 0)
	for k, v := range rowForAdding {
		columns = append(columns, k)
		values = append(values, v)
		placeholders = append(placeholders, "?")
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(columns, ", "), strings.Join(placeholders, ", "))
	result, err := e.Db.Exec(query, values...)
	if err != nil {

		return nil, err
	}
	lastId, _ := result.LastInsertId()

	return &lastId, nil
}

func (e *DbExplorer) updateRecordTable(tableName string, id int64, inRecord map[string]interface{}) *Response {

	tableInfo := e.TablesInfo[tableName]
	row, err := e.getRowFromTableById(tableName, id)
	if err != nil {
		resp := Response{}
		if errors.Is(err, sql.ErrNoRows) {
			resp.Err = err
			resp.StatusCode = http.StatusNotFound
		} else {
			resp.Err = err
			resp.StatusCode = http.StatusInternalServerError
		}
		return &resp
	}
	updRow := make(map[string]interface{})
	for fldName := range inRecord {
		_, exists := row[fldName]
		if exists {
			fldInfo := tableInfo.getFieldInfoByName(fldName)
			if fldInfo.Key == "PRI" {
				return &Response{
					Err:        fmt.Errorf("field %s have invalid type", fldName),
					StatusCode: http.StatusBadRequest,
				}
			}
			switch inRecord[fldName].(type) {
			case string:
				if strings.Contains(fldInfo.Type, "char") || strings.Contains(fldInfo.Type, "text") {
					updRow[fldName] = inRecord[fldName]
				} else {
					return &Response{
						Err:        fmt.Errorf("field %s have invalid type", fldName),
						StatusCode: http.StatusBadRequest,
					}
				}
			case int:
				if strings.Contains(fldInfo.Type, "int") {
					updRow[fldName] = inRecord[fldName]
				} else {
					return &Response{
						Err:        fmt.Errorf("field %s have invalid type", fldName),
						StatusCode: http.StatusBadRequest,
					}
				}
			case float32:
				if strings.Contains(fldInfo.Type, "double") || strings.Contains(fldInfo.Type, "decimal") || strings.Contains(fldInfo.Type, "float") {
					updRow[fldName] = inRecord[fldName]
				} else {
					return &Response{
						Err:        fmt.Errorf("field %s have invalid type", fldName),
						StatusCode: http.StatusBadRequest,
					}
				}
			case float64:
				if strings.Contains(fldInfo.Type, "double") || strings.Contains(fldInfo.Type, "decimal") || strings.Contains(fldInfo.Type, "float") {
					updRow[fldName] = inRecord[fldName]
				} else {
					return &Response{
						Err:        fmt.Errorf("field %s have invalid type", fldName),
						StatusCode: http.StatusBadRequest,
					}
				}
			case nil:
				if fldInfo.Null == "YES" {
					updRow[fldName] = nil
				} else {
					return &Response{
						Err:        fmt.Errorf("field %s have invalid type", fldName),
						StatusCode: http.StatusBadRequest,
					}
				}
			}
		}
	}
	columns := make([]string, 0)
	values := make([]interface{}, 0)
	for col, val := range updRow {
		columns = append(columns, fmt.Sprintf("%s = ?", col))
		values = append(values, val)
	}
	query := fmt.Sprintf("UPDATE %s SET %s", tableName, strings.Join(columns, ", "))
	_, err = e.Db.Exec(query, values...)
	if err != nil {
		return &Response{
			Err:        err,
			StatusCode: http.StatusInternalServerError,
		}
	}

	return &Response{
		Err:        nil,
		StatusCode: http.StatusOK,
	}
}

func (e *DbExplorer) deleteRecordById(tableName string, id int64) (*int64, error) {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = ?", tableName)
	res, err := e.Db.Exec(query, id)
	if err != nil {
		return nil, err
	}
	affected, _ := res.RowsAffected()
	return &affected, nil
}
