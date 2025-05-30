package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/5aradise/distributed-system/httptools"
	"github.com/5aradise/distributed-system/signal"
)

var port = flag.Int("port", 8080, "server port")

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"

const teamName = "faang"

const dbServiceAddress = "http://db:8083"

type DbPostRequest struct {
	Value string `json:"value"`
}

type DbGetResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	flag.Parse()

	go initializeDataInDB()

	h := new(http.ServeMux)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(rw, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}

		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		requestKey := r.URL.Query().Get("key")
		if requestKey == "" {
			http.Error(rw, "Missing key parameter", http.StatusBadRequest)
			return
		}

		dbURL := fmt.Sprintf("%s/db/%s", dbServiceAddress, requestKey)

		log.Printf("Querying DB service at URL: %s", dbURL)

		resp, err := http.DefaultClient.Get(dbURL)
		if err != nil {
			log.Printf("Error querying db service: %v", err)
			http.Error(rw, "Failed to query database", http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			rw.WriteHeader(http.StatusNotFound)
			return
		}

		if resp.StatusCode != http.StatusOK {
			log.Printf("DB service returned non-OK status: %d for key %s. URL: %s", resp.StatusCode, requestKey, dbURL)
			http.Error(rw, "Database service error", http.StatusInternalServerError)
			return
		}

		var dbResp DbGetResponse
		if err := json.NewDecoder(resp.Body).Decode(&dbResp); err != nil {
			log.Printf("Error decoding response from db service: %v", err)
			http.Error(rw, "Failed to decode database response", http.StatusInternalServerError)
			return
		}

		responsePayload := map[string]string{"data": dbResp.Value}
		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(rw).Encode(responsePayload); err != nil {
			log.Printf("Error encoding final response: %v", err)
		}
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)

	go server.Start()
	log.Printf("Server is listening on port %d", *port)
	signal.WaitForTerminationSignal()
}

func initializeDataInDB() {
	time.Sleep(5 * time.Second)

	currentDate := time.Now().Format("2006-01-02")
	dbKey := teamName

	payload := DbPostRequest{Value: currentDate}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error marshalling payload for db init: %v", err)
		return
	}

	dbURL := fmt.Sprintf("%s/db/%s", dbServiceAddress, dbKey)
	req, err := http.NewRequest(http.MethodPost, dbURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		log.Printf("Error creating request for db init: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("Error on initial POST to db service: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		log.Printf("DB service returned non-OK status for initial POST: %d. URL: %s", resp.StatusCode, dbURL)
		return
	}

	log.Printf("Successfully initialized data in DB: key='%s', value='%s'", dbKey, currentDate)
}
