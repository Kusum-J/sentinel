package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"sentinel/internal/config"
	"sentinel/internal/db"
	"sentinel/internal/handlers"
)

func main() {
	log.Println("Starting Sentinel API Server...")
	cfg := config.Load()

	// Connect to Mongo with fallback
	var database db.Database
	mongoClient, err := db.NewMongoClient(cfg)
	if err != nil {
		log.Printf("WARNING: Failed to connect to MongoDB: %v. Falling back to IN-MEMORY DATABASE.", err)
		database = db.NewInMemoryDB()
	} else {
		database = mongoClient
	}

	// Connect to Redis with fallback
	var queueCache db.QueueCache
	redisClient, err := db.NewRedisClient(cfg)
	if err != nil {
		log.Printf("WARNING: Failed to connect to Redis: %v. Falling back to IN-MEMORY QUEUE/CACHE.", err)
		queueCache = db.NewInMemoryQueueCache()
	} else {
		queueCache = redisClient
	}

	// Create and start API server
	server := handlers.NewServer(database, queueCache)
	mux := http.NewServeMux()
	mux.Handle("/", server)

	httpServer := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: mux,
	}

	go func() {
		log.Printf("API Server is running on port %s", cfg.Port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("API Server error: %v", err)
		}
	}()

	// Graceful shutdown setup
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("Shutting down API Server...")
	// Wait a moment for connections to finish
	time.Sleep(1 * time.Second)
	log.Println("API Server stopped.")
}
