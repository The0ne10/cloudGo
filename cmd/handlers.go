package main

import (
	"errors"
	"github.com/gorilla/mux"
	"io"
	"net/http"
)

func Put(key, value string) error {
	store.Lock()
	store.m[key] = value
	store.Unlock()
	return nil
}

func Get(key string) (string, error) {
	store.RLock()
	value, ok := store.m[key]
	store.RUnlock()
	if !ok {
		return "", ErrorNoSuchKey
	}
	return value, nil
}

func Delete(key string) error {
	store.Lock()
	delete(store.m, key)
	store.Unlock()
	return nil
}

func keyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = Put(key, string(value)) // Сохранить значение как строку
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	logger.WritePut(key, string(value))

	w.WriteHeader(http.StatusCreated)
}

func keyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	value, err := Get(key)
	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(value)) // запись значение в ответ
}

func keyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	err := Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
