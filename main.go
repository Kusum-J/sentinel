package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"sentinel/internal/config"
	"sentinel/internal/db"
	"sentinel/internal/handlers"
	"sentinel/internal/models"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type WorkerService struct {
	Mongo db.Database
	Redis db.QueueCache
	Cfg   *config.Config
}

func main() {
	log.Println("Starting Sentinel in Combined Single-Process Mode...")
	cfg := config.Load()

	// 1. Initialize MongoDB with Fallback
	var database db.Database
	mongoClient, err := db.NewMongoClient(cfg)
	if err != nil {
		log.Printf("WARNING: Failed to connect to MongoDB: %v. Falling back to IN-MEMORY DATABASE.", err)
		database = db.NewInMemoryDB()
	} else {
		database = mongoClient
	}

	// 2. Initialize Redis with Fallback
	var queueCache db.QueueCache
	redisClient, err := db.NewRedisClient(cfg)
	if err != nil {
		log.Printf("WARNING: Failed to connect to Redis: %v. Falling back to IN-MEMORY QUEUE/CACHE.", err)
		queueCache = db.NewInMemoryQueueCache()
	} else {
		queueCache = redisClient
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 3. Start Scheduler & Workers
	workerService := &WorkerService{
		Mongo: database,
		Redis: queueCache,
		Cfg:   cfg,
	}

	log.Println("Starting Background Scheduler...")
	go workerService.startScheduler(ctx)

	log.Printf("Starting Background Worker Pool (%d workers)...", cfg.WorkerCount)
	var wg sync.WaitGroup
	for i := 1; i <= cfg.WorkerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			workerService.startWorker(ctx, workerID)
		}(i)
	}

	// 4. Start API Server
	server := handlers.NewServer(database, queueCache)
	mux := http.NewServeMux()
	mux.Handle("/", server)

	httpServer := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: mux,
	}

	go func() {
		log.Printf("API Server is running on http://localhost:%s", cfg.Port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("API Server error: %v", err)
		}
	}()

	// 5. Handle Graceful Shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("Shutting down Sentinel...")
	cancel() // Stops workers and scheduler

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = httpServer.Shutdown(shutdownCtx)

	wg.Wait()
	log.Println("Sentinel stopped successfully.")
}

func (s *WorkerService) startScheduler(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.scheduleDueMonitors(ctx)
		}
	}
}

func (s *WorkerService) scheduleDueMonitors(ctx context.Context) {
	monitors, err := s.Mongo.GetMonitors(ctx)
	if err != nil {
		return
	}

	now := time.Now()
	for _, m := range monitors {
		due := false
		if m.LastQueuedAt == nil {
			due = true
		} else {
			due = now.Sub(*m.LastQueuedAt) >= time.Duration(m.IntervalSeconds)*time.Second
		}

		if due {
			acquired, err := s.Redis.AcquireInFlightLock(ctx, m.ID.Hex())
			if err != nil || !acquired {
				continue
			}

			err = s.Redis.EnqueueJob(ctx, m.ID.Hex())
			if err != nil {
				_ = s.Redis.ReleaseInFlightLock(ctx, m.ID.Hex())
				continue
			}

			_ = s.Mongo.UpdateLastQueued(ctx, m.ID.Hex())
			log.Printf("Scheduler: Queued monitor %s (%s)", m.ID.Hex(), m.URL)
		}
	}
}

func (s *WorkerService) startWorker(ctx context.Context, workerID int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			monitorID, err := s.Redis.DequeueJob(ctx)
			if err != nil || monitorID == "" {
				continue
			}

			s.processCheck(ctx, monitorID, workerID)
		}
	}
}

func (s *WorkerService) processCheck(ctx context.Context, monitorID string, workerID int) {
	defer func() {
		_ = s.Redis.ReleaseInFlightLock(ctx, monitorID)
	}()

	m, err := s.Mongo.GetMonitor(ctx, monitorID)
	if err != nil {
		return
	}

	var lastCheck *models.Check
	cachedLastCheck, err := s.Redis.GetLatestStatus(ctx, monitorID)
	if err == nil && cachedLastCheck != nil {
		lastCheck = cachedLastCheck
	} else {
		dbLastCheck, err := s.Mongo.GetLatestCheck(ctx, monitorID)
		if err == nil && dbLastCheck != nil {
			lastCheck = dbLastCheck
		}
	}

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", m.URL, nil)
	if err != nil {
		return
	}
	req.Header.Set("User-Agent", "Sentinel-Uptime-Monitor/1.0")

	startTime := time.Now()
	resp, reqErr := client.Do(req)
	duration := time.Since(startTime).Milliseconds()

	isUp := false
	statusCode := 0
	errorMsg := ""

	if reqErr != nil {
		isUp = false
		errorMsg = reqErr.Error()
		log.Printf("Worker %d: [DOWN] %s - Error: %v", workerID, m.URL, reqErr)
	} else {
		statusCode = resp.StatusCode
		resp.Body.Close()
		isUp = statusCode >= 200 && statusCode < 400
		if isUp {
			log.Printf("Worker %d: [UP] %s - Status: %d (%dms)", workerID, m.URL, statusCode, duration)
		} else {
			log.Printf("Worker %d: [DOWN] %s - Status: %d (%dms)", workerID, m.URL, statusCode, duration)
		}
	}

	check := models.Check{
		ID:             primitive.NewObjectID(),
		MonitorID:      db.StringToObjectID(monitorID),
		Timestamp:      startTime,
		StatusCode:     statusCode,
		ResponseTimeMS: duration,
		IsUp:           isUp,
		ErrorMsg:       errorMsg,
	}

	_ = s.Mongo.SaveCheck(ctx, check)
	_ = s.Redis.CacheLatestStatus(ctx, &check)

	statusChanged := false
	var changedTo string
	if lastCheck == nil {
		statusChanged = true
		if isUp {
			changedTo = "up"
		} else {
			changedTo = "down"
		}
	} else if lastCheck.IsUp != isUp {
		statusChanged = true
		if isUp {
			changedTo = "up"
		} else {
			changedTo = "down"
		}
	}

	if statusChanged {
		event := models.StatusEvent{
			ID:        primitive.NewObjectID(),
			MonitorID: db.StringToObjectID(monitorID),
			Timestamp: startTime,
			ChangedTo: changedTo,
		}
		_ = s.Mongo.SaveStatusEvent(ctx, event)
		log.Printf("Worker %d: STATUS CHANGE EVENT for %s -> transitioned to %s", workerID, m.URL, changedTo)
	}
}
