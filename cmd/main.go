package main

import (
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

func main() {
	err := initializeTransactionLog()
	if err != nil {
		log.Fatal(err)
	}
	r := mux.NewRouter()
	r.HandleFunc("/v1/{key}", keyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", keyValueGetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", keyValueDeleteHandler).Methods("DELETE")
	fmt.Println("Start Server on port 8080")
	log.Fatal(http.ListenAndServe(":8080", r))
}
