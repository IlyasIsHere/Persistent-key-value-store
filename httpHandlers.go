package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
)

// This is the request handler for the get URL.
func getHandler(lsmdb *lsmDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		if r.Method != "GET" {
			http.Error(w, "Invalid request type", http.StatusBadRequest)
			return
		}

		key := r.URL.Query().Get("key")

		if len(key) == 0 {
			http.Error(w, "Key must not be empty", http.StatusBadRequest)
			return
		}

		v, err := lsmdb.Get([]byte(key))

		if err != nil {
			if err == ErrKeyNotFound {
				fmt.Fprintf(w, "Key not found")
				return
			}
			fmt.Fprintf(w, "Some error happened.")
			return
		}

		// We reach here if the error is nil, which means the value was found
		fmt.Fprint(w, string(v))
	}

}

type KeyValue struct {
	Key   string
	Value string
}

func setHandler(lsmdb *lsmDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Invalid request type", http.StatusBadRequest)
			return
		}

		var entry KeyValue
		err := json.NewDecoder(r.Body).Decode(&entry)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if len(entry.Key) == 0 || len(entry.Value) == 0 {
			http.Error(w, "Key and value must not be empty", http.StatusBadRequest)
			return
		}

		if len(entry.Key) > math.MaxUint32 {
			http.Error(w, "Key length exceeds maximum allowed", http.StatusBadRequest)
			return
		}

		if len(entry.Value) > math.MaxUint32 {
			http.Error(w, "Value length exceeds maximum allowed", http.StatusBadRequest)
			return
		}

		if err := lsmdb.Set([]byte(entry.Key), []byte(entry.Value)); err != nil {
			// http.Error(w, "Some error happened.", http.StatusBadRequest)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fmt.Fprint(w, "OK")
	}
}

func delHandler(lsmdb *lsmDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Invalid request type")
			return
		}

		key := r.URL.Query().Get("key")

		if len(key) == 0 {
			http.Error(w, "Key must not be empty", http.StatusBadRequest)
			return
		}

		v, err := lsmdb.Del([]byte(key))

		if err != nil {
			if err == ErrKeyNotFound {
				fmt.Fprintf(w, "Key not found")
				return
			}
			fmt.Fprintf(w, "Some error happened.")
			return
		}

		fmt.Fprint(w, string(v))
	}
}

func handleRequests(lsmdb *lsmDB) {
	http.HandleFunc("/get", getHandler(lsmdb))
	http.HandleFunc("/set", setHandler(lsmdb))
	http.HandleFunc("/del", delHandler(lsmdb))
	log.Fatal(http.ListenAndServe(":8080", nil))
}
