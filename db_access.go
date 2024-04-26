package main

import (
	"fmt"
)

func (e *DbExplorer) getAllRowsFromTable(tableName string) error {
	sqlQuery := fmt.Sprintf("SELECT * FROM %s", tableName)
	rows, err := e.Db.Query(sqlQuery)
	if err != nil {
		e.Logger.Println(err)
		e.Logger.Println(err)
		return err
	}
	defer rows.Close()

	tableInfo := e.TablesInfo[tableName]
	colsCount := len(tableInfo.Fields)

	rowBuffer := make([]interface{}, colsCount)
	results := make([]map[string]interface{}, 0)
	for rows.Next() {
		for i := range rowBuffer {
			rowBuffer[i] = new([]byte)
		}
		err = rows.Scan(rowBuffer...)
		if err != nil {
			e.Logger.Println(err)
			return err
		}

	}

	return nil
}

func f(tabInfo *TableInfo, row []interface{}) (map[string]interface{}, error) {
	resultRow := make(map[string]interface{})
	for idx, fieldInfo := range tabInfo.Fields {
		if row[idx] == nil {
			resultRow[fieldInfo.Field] = nil
		} else {
		}
	}
}
