package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/5aradise/distributed-system/datastore"
	"github.com/5aradise/distributed-system/httptools"
	"github.com/5aradise/distributed-system/signal"
	"log"
	"net/http"
	"strings"
)

var port = flag.Int("port", 8083, "db server port")

type DbGetResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type DbPostRequest struct {
	Value string `json:"value"`
}

var db *datastore.Db

func main() {
	flag.Parse()

	var err error
	db, err = datastore.Open(".")
	if err != nil {
		log.Fatalf("Failed to open datastore: %v", err)
	}
	defer db.Close()

	log.Printf("DB service started on port %d", *port)

	h := new(http.ServeMux)

	h.HandleFunc("/db/", dbHandler)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("OK"))
	})

	server := httptools.CreateServer(*port, h)

	go server.Start() // Запускаємо сервер в окремій горутині
	log.Printf("DB server is listening on port %d", *port)

	signal.WaitForTerminationSignal()
	log.Println("DB service shutting down...")
}

func dbHandler(rw http.ResponseWriter, r *http.Request) {
	trimmedPath := strings.TrimPrefix(r.URL.Path, "/db/")
	if trimmedPath == "" || strings.Contains(trimmedPath, "/") {
		http.Error(rw, "Invalid key in path. Expected /db/<key>", http.StatusBadRequest)
		return
	}
	key := trimmedPath

	switch r.Method {
	case http.MethodGet:
		handleGet(rw, key)
	case http.MethodPost:
		handlePost(rw, r, key)
	default:
		http.Error(rw, "Method Not Allowed", http.StatusMethodNotAllowed)
	}
}

func handleGet(rw http.ResponseWriter, key string) {
	value, err := db.Get(key)
	if err != nil {
		if err == datastore.ErrNotFound {
			rw.WriteHeader(http.StatusNotFound)
			return
		}
		log.Printf("Error getting value for key %s: %v", key, err)
		http.Error(rw, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(rw).Encode(DbGetResponse{Key: key, Value: value}); err != nil {
		log.Printf("Error encoding response for key %s: %v", key, err)
	}
}

func handlePost(rw http.ResponseWriter, r *http.Request, key string) {
	var req DbPostRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(rw, fmt.Sprintf("Invalid JSON body: %v", err), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if err := db.Put(key, req.Value); err != nil {
		log.Printf("Error putting value for key %s: %v", key, err)
		http.Error(rw, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	rw.WriteHeader(http.StatusOK)
}
