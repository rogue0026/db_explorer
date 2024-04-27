package main

import (
	"encoding/json"
	"net/http"
)

func sendJSONErrResponse(w http.ResponseWriter, text string, statusCode int) {
	resp := map[string]string{"error": text}
	js, err := json.MarshalIndent(&resp, "", "   ")
	if err != nil {
		http.Error(w, "unknown internal server error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	w.Write(js)
}
