package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
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

func (ti *TableInfo) getFieldByName(name string) *FieldInfo {
	for i := range ti.Fields {
		if ti.Fields[i] == name
	}
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
		if strings.Count(r.URL.Path, "/") == 1 && !r.URL.Query().Has("limit") && !r.URL.Query().Has("offset") {
			slashPos := strings.Index(r.URL.Path, "/")
			tableName := r.URL.Path[slashPos+1:]
			e.handlerAllRecords(tableName)(w, r)
			return
		}
		if strings.Count(r.URL.Path, "/") == 1 && r.URL.Query().Has("limit") && !r.URL.Query().Has("offset") { // only limit
			slashPos := strings.Index(r.URL.Path, "/")
			tableName := r.URL.Path[slashPos+1:]
			e.handlerAllRecordsWithLimit(tableName)(w, r)
			return
		}
		if strings.Count(r.URL.Path, "/") == 1 && r.URL.Query().Has("limit") && r.URL.Query().Has("offset") {
			slashPos := strings.Index(r.URL.Path, "/")
			tableName := r.URL.Path[slashPos+1:]
			e.handlerAllRecordsWithLimitAndOffset(tableName)(w, r)
			return
		}
		if strings.Count(r.URL.Path, "/") == 2 {
			data := strings.Split(strings.TrimLeft(r.URL.Path, "/"), "/")
			tableName := data[0]
			id := data[1]
			e.handlerRecordById(tableName, id)(w, r)
			return
		}
	case http.MethodPut:
		tableName := strings.Trim(r.URL.Path, "/")
		e.handlerAddRecordToTable(tableName)(w, r)
		return
	case http.MethodPost:
		//data := strings.Split(strings.TrimLeft(r.URL.Path, "/"), "/")
		//tableName := data[0]
		//id := data[1]
		//e.handlerUpdateRecord(tableName, id)
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
	rows, err := e.Db.Query("SHOW TABLES")
	if err != nil {
		e.Logger.Println(err)
		sendJSONErrResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}
	tables := make([]string, 0)
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			e.Logger.Println(err)
			sendJSONErrResponse(w, err.Error(), http.StatusInternalServerError)
			return
		}
		tables = append(tables, name)
	}
	resp := map[string]interface{}{"tables": tables}
	wrapped := map[string]interface{}{"response": resp}
	js, _ := json.MarshalIndent(&wrapped, "", "   ")
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

func (e *DbExplorer) handlerAllRecords(tableName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, tableExists := e.TablesInfo[tableName]
		if !tableExists {
			sendJSONErrResponse(w, "unknown table", http.StatusNotFound)
		} else {
			rows, err := e.getAllRowsFromTable(tableName)
			if err != nil {
				e.Logger.Println(err)
				errResp := map[string]string{"error": err.Error()}
				js, _ := json.MarshalIndent(&errResp, "", "   ")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				w.Write(js)
				return
			}
			records := map[string]interface{}{"records": rows}
			response := map[string]interface{}{"response": records}
			js, _ := json.MarshalIndent(&response, "", "   ")
			w.Header().Set("Content-Type", "application/json")
			w.Write(js)
		}
	}
}

func (e *DbExplorer) handlerAllRecordsWithLimit(tableName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, tableExists := e.TablesInfo[tableName]
		if !tableExists {
			sendJSONErrResponse(w, "unknown table", http.StatusNotFound)
		} else {
			qLimit := r.URL.Query().Get("limit")
			if qLimit == "" {
				limit := 5
				rows, err := e.getRowsFromTableByLimit(tableName, limit)
				if err != nil {
					sendJSONErrResponse(w, err.Error(), http.StatusInternalServerError)
					return
				}
				records := map[string]interface{}{"records": rows}
				response := map[string]interface{}{"response": records}
				js, _ := json.MarshalIndent(&response, "", "   ")
				w.Header().Set("Content-Type", "application/json")
				w.Write(js)
				return
			} else {
				lim, err := strconv.Atoi(qLimit)
				if err != nil {
					sendJSONErrResponse(w, "bad limit param", http.StatusBadRequest)
					return
				}
				rows, err := e.getRowsFromTableByLimit(tableName, lim)
				if err != nil {
					sendJSONErrResponse(w, err.Error(), http.StatusInternalServerError)
					return
				}
				records := map[string]interface{}{"records": rows}
				response := map[string]interface{}{"response": records}
				js, _ := json.MarshalIndent(&response, "", "   ")
				w.Header().Set("Content-Type", "application/json")
				w.Write(js)
				return
			}
		}
	}
}

func (e *DbExplorer) handlerAllRecordsWithLimitAndOffset(tableName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, tableExists := e.TablesInfo[tableName]
		if !tableExists {
			sendJSONErrResponse(w, "unknown table", http.StatusNotFound)
		} else {
			qLimit := r.URL.Query().Get("limit")
			if qLimit == "" {
				qLimit = "5"
			}
			qOffset := r.URL.Query().Get("offset")
			if qOffset == "" {
				qOffset = "0"
			}
			lim, err := strconv.ParseInt(qLimit, 10, 64)
			if err != nil {
				sendJSONErrResponse(w, "bad limit parameter", http.StatusBadRequest)
				return
			}
			off, err := strconv.ParseInt(qOffset, 10, 64)
			if err != nil {
				sendJSONErrResponse(w, "bad offset parameter", http.StatusBadRequest)
				return
			}
			rows, err := e.getRowsFromTableByLimitAndOffset(tableName, lim, off)
			if err != nil {
				sendJSONErrResponse(w, err.Error(), http.StatusInternalServerError)
				return
			}
			records := map[string]interface{}{"records": rows}
			response := map[string]interface{}{"response": records}
			js, _ := json.MarshalIndent(&response, "", "   ")
			w.Header().Set("Content-Type", "application/json")
			w.Write(js)
			return
		}
	}
}

func (e *DbExplorer) handlerRecordById(tableName string, queryId string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, tableExists := e.TablesInfo[tableName]
		if !tableExists {
			sendJSONErrResponse(w, "unknown table", http.StatusNotFound)
		} else {
			id, err := strconv.ParseInt(queryId, 10, 64)
			if err != nil {
				sendJSONErrResponse(w, "bad id value", http.StatusBadRequest)
				return
			}
			row, err := e.getRowFromTableById(tableName, id)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					sendJSONErrResponse(w, "record not found", http.StatusNotFound)
					return
				} else {
					sendJSONErrResponse(w, err.Error(), http.StatusInternalServerError)
					return
				}
			}
			record := map[string]interface{}{"record": row}
			response := map[string]interface{}{"response": record}
			js, _ := json.MarshalIndent(&response, "", "   ")
			w.Header().Set("Content-Type", "application/json")
			w.Write(js)
			return
		}
	}
}

func (e *DbExplorer) handlerAddRecordToTable(tableName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, exists := e.TablesInfo[tableName]
		if !exists {
			sendJSONErrResponse(w, "unknown table", http.StatusNotFound)
			return
		} else {
			record := make(map[string]interface{})
			err := json.NewDecoder(r.Body).Decode(&record)
			if err != nil {
				e.Logger.Println(err)
				sendJSONErrResponse(w, err.Error(), http.StatusInternalServerError)
				return
			}
			lastInsertId, err := e.addRowToTable(tableName, record)
			if err != nil {
				//e.Logger.Println(err)
				sendJSONErrResponse(w, err.Error(), http.StatusInternalServerError)
				return
			}
			id := map[string]interface{}{"id": lastInsertId}
			response := map[string]interface{}{"response": id}
			js, _ := json.MarshalIndent(&response, "", "   ")
			w.Write(js)
			return
		}
	}
}

type Response struct {
	Err        error
	StatusCode int
}

func (e *DbExplorer) handlerUpdateRecord(tableName string, queryId string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, exists := e.TablesInfo[tableName]
		if !exists {
			sendJSONErrResponse(w, "unknown table", http.StatusNotFound)
			return
		}
		id, err := strconv.ParseInt(queryId, 10, 64)
		if err != nil {
			sendJSONErrResponse(w, err.Error(), http.StatusBadRequest)
			return
		}
		record := make(map[string]interface{})
		err = json.NewDecoder(r.Body).Decode(&record)
		if err != nil {
			e.Logger.Println(err)
			sendJSONErrResponse(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp := e.updateRecordTable(tableName, id, record)
		if resp.Err != nil {
			e.Logger.Println(err)
			sendJSONErrResponse(w, resp.Err.Error(), resp.StatusCode)
			return
		}
		upd := map[string]interface{}{"updated": 1}
		wrapped := map[string]interface{}{"response": upd}
		js, _ := json.MarshalIndent(&wrapped, "", "   ")
		w.WriteHeader(resp.StatusCode)
		w.Write(js)
	}
}
