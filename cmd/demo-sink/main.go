// Command demo-sink is a local stand-in for a monitored host and a webhook
// receiver: /ok returns 200, /bad returns 500, POST /hook records the body,
// GET /hook lists recorded bodies.
package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
)

func main() {
	var mu sync.Mutex
	var received []json.RawMessage
	mux := http.NewServeMux()
	mux.HandleFunc("/ok", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("/bad", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) })
	mux.HandleFunc("/hook", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodPost {
			b, _ := io.ReadAll(r.Body)
			received = append(received, json.RawMessage(b))
			log.Printf("webhook received: %s", b)
			w.WriteHeader(202)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if received == nil {
			w.Write([]byte("[]"))
			return
		}
		_ = json.NewEncoder(w).Encode(received)
	})
	port := os.Getenv("SINK_PORT")
	if port == "" {
		port = "9090"
	}
	log.Printf("demo-sink listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
