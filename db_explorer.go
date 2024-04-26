package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type FieldInfo struct {
	Field      string
	Type       string
	Collation  sql.NullString
	Null       string
	Key        string
	Default    sql.NullString
	Extra      string
	Privileges string
	Comment    string
}

type TableInfo struct {
	TableName string
	Fields    []*FieldInfo
}

func GetTableNames(db *sql.DB) ([]string, error) {
	sqlQuery := "SHOW TABLES"
	rows, err := db.Query(sqlQuery)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	tableNames := make([]string, 0)
	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		tableNames = append(tableNames, name)
	}

	return tableNames, nil
}

func ScanTables(db *sql.DB) (map[string]*TableInfo, error) {
	tableNames, err := GetTableNames(db)
	if err != nil {
		return nil, err
	}

	tablesInfo := make(map[string]*TableInfo)
	for _, name := range tableNames {
		tableInfoQuery := fmt.Sprintf("SHOW FULL COLUMNS FROM %s", name)
		rows, err := db.Query(tableInfoQuery)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		fieldsInfo := make([]*FieldInfo, 0)
		for rows.Next() {
			fldInfo := FieldInfo{}
			err = rows.Scan(
				&fldInfo.Field,
				&fldInfo.Type,
				&fldInfo.Collation,
				&fldInfo.Null,
				&fldInfo.Key,
				&fldInfo.Default,
				&fldInfo.Extra,
				&fldInfo.Privileges,
				&fldInfo.Comment,
			)
			if err != nil {
				return nil, err
			}
			fieldsInfo = append(fieldsInfo, &fldInfo)
		}
		tInfo := TableInfo{
			TableName: name,
			Fields:    fieldsInfo,
		}
		tablesInfo[name] = &tInfo
	}

	return tablesInfo, nil
}

type DbExplorer struct {
	Logger     *log.Logger
	Db         *sql.DB
	TablesInfo map[string]*TableInfo
}

func (e *DbExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if r.URL.Path == "/" {
			e.handlerAllTableNames(w, r)
			return
		}
		if strings.Count(r.URL.Path, "/") == 1 {
			slashPos := strings.Index(r.URL.Path, "/")
			tableName := r.URL.Path[slashPos+1:]
			e.handlerAllTableRecords(tableName)(w, r)
			return
		}
	}
}

func NewDbExplorer(db *sql.DB) (*DbExplorer, error) {
	tablesInfo, err := ScanTables(db)
	if err != nil {
		return nil, err
	}
	logger := log.New(os.Stdout, "", log.Lshortfile)
	explorer := DbExplorer{
		Db:         db,
		Logger:     logger,
		TablesInfo: tablesInfo,
	}

	return &explorer, nil
}

func (e *DbExplorer) handlerAllTableNames(w http.ResponseWriter, r *http.Request) {
	tables := make([]string, 0)
	for name := range e.TablesInfo {
		tables = append(tables, name)
	}
	resp := map[string]interface{}{"tables": tables}
	wrapped := map[string]interface{}{"response": resp}
	js, _ := json.MarshalIndent(&wrapped, "", "   ")
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

func (e *DbExplorer) handlerAllTableRecords(tableName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, tableExists := e.TablesInfo[tableName]
		if !tableExists {
			resp := map[string]string{"error": "unknown table"}
			js, _ := json.MarshalIndent(&resp, "", "   ")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			w.Write(js)
			return
		} else {

		}
	}
}