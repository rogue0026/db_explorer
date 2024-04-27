package main

import (
	"fmt"
)

func (e *DbExplorer) getAllRowsFromTable(tableName string) ([]map[string]interface{}, error) {
	sqlQuery := fmt.Sprintf("SELECT * FROM %s", tableName)
	rows, err := e.Db.Query(sqlQuery)
	if err != nil {
		e.Logger.Println(err)
		return nil, err
	}
	defer rows.Close()

	tableInfo := e.TablesInfo[tableName]
	colsCount := len(tableInfo.Fields)

	rowsFromTable := make([]map[string]interface{}, 0)
	rowForScanning := make([]interface{}, colsCount)
	for i := range rowForScanning {
		var v interface{}
		rowForScanning[i] = &v
	}
	for rows.Next() {
		err = rows.Scan(rowForScanning...)
		if err != nil {
			e.Logger.Println(err)
			return nil, err
		}
		result := convertToNormalValues(tableInfo, rowForScanning)
		rowsFromTable = append(rowsFromTable, result)
	}

	return rowsFromTable, nil
}

func (e *DbExplorer) getRowsFromTableByLimit(tableName string, limit int) ([]map[string]interface{}, error) {
	sqlQuery := fmt.Sprintf("SELECT * FROM %s LIMIT ?", tableName)
	rows, err := e.Db.Query(sqlQuery, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tabInfo := e.TablesInfo[tableName]
	colsCount := len(tabInfo.Fields)
	rowsFromTable := make([]map[string]interface{}, 0)
	rowForScanning := make([]interface{}, colsCount)
	for i := range rowForScanning {
		var v interface{}
		rowForScanning[i] = &v
	}
	for rows.Next() {
		err = rows.Scan(rowForScanning...)
		if err != nil {
			return nil, err
		}
		result := convertToNormalValues(tabInfo, rowForScanning)
		rowsFromTable = append(rowsFromTable, result)
	}

	return rowsFromTable, nil
}

func convertToNormalValues(tabInfo *TableInfo, row []interface{}) map[string]interface{} {
	for idx, elem := range row {
		value := *elem
	}
}
