package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// RequestPayload defines the expected JSON body
type RequestPayload struct {
	N   int    `json:"n"`
	URL string `json:"url"`
}

// Global variables
var (
	mainNumber int64      // current multiplier (use int64 to avoid overflow)
	mu         sync.Mutex // protects mainNumber
)

// HTTP client with timeout to prevent hanging goroutines
var httpClient = &http.Client{
	Timeout: 10 * time.Second,
}

// Safety limits for Railway free tier
const (
	maxConcurrency = 50      // max simultaneous requests
	maxRequests    = 1000000 // max total requests per batch
)

// rateTest sends n concurrent GET requests to the given URL.
// It first updates the global mainNumber by multiplying it with n,
// then performs the load test with the updated total (capped).
func rateTest(n int, url string) {
	// Update mainNumber safely
	mu.Lock()
	mainNumber = mainNumber * int64(n)
	if mainNumber > maxRequests {
		mainNumber = maxRequests
	}
	totalRequests := int(mainNumber)
	mu.Unlock()

	fmt.Printf("Starting load test: %d requests to %s (concurrency limited to %d)\n",
		totalRequests, url, maxConcurrency)

	// Semaphore to limit concurrency
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for i := 1; i <= totalRequests; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sem <- struct{}{}        // acquire a slot
			defer func() { <-sem }() // release the slot

			resp, err := httpClient.Get(url)
			if err != nil {
				fmt.Printf("Request %d failed: %v\n", id, err)
				return
			}
			defer resp.Body.Close()
			_, _ = io.ReadAll(resp.Body)
			fmt.Printf("Request %d done (status: %s)\n", id, resp.Status)
		}(i)
	}

	wg.Wait()
	fmt.Printf("All %d requests completed.\n", totalRequests)

	// Launch the load test in a separate goroutine
	go rateTest(n, url)
}

func main() {
	// Initialize mainNumber to 1
	mainNumber = 1

	// Set up HTTP routes
	mux := http.NewServeMux()

	// Endpoint to trigger load test
	mux.HandleFunc("/reply", func(w http.ResponseWriter, r *http.Request) {
		// Read request body
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body", http.StatusBadRequest)
			return
		}

		// Parse JSON
		var payload RequestPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			http.Error(w, "Invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}

		// Validate input
		if payload.N <= 0 || payload.URL == "" {
			http.Error(w, "Missing or invalid 'n' or 'url'", http.StatusBadRequest)
			return
		}

		// Respond immediately (the actual number will be computed inside rateTest)
		fmt.Fprintf(w, "Load test queued. Multiplier: %d, URL: %s", payload.N, payload.URL)

		// Launch the load test in a separate goroutine
		go rateTest(payload.N, payload.URL)
	})

	// Endpoint to check current multiplier
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		current := mainNumber
		mu.Unlock()
		fmt.Fprintf(w, "Current mainNumber: %d", current)
	})
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // fallback for local testing
	}

	// Create HTTP server with timeouts
	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	// Start server in a goroutine
	go func() {
		fmt.Println("Server listening on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Server error: %v\n", err)
			os.Exit(1)
		}
	}()

	// Graceful shutdown on SIGINT or SIGTERM
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	fmt.Println("\nShutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		fmt.Printf("Graceful shutdown failed: %v\n", err)
	}
	fmt.Println("Server stopped.")
}

/*

Build command: go build -o app

Start command: ./app


{
  "build": {
    "builder": "NIXPACKS"
  }
}
web: ./app



*/
