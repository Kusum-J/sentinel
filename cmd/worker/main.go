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

	"go.mongodb.org/mongo-driver/bson/primitive"
	"sentinel/internal/config"
	"sentinel/internal/db"
	"sentinel/internal/models"
)

type WorkerService struct {
	Mongo db.Database
	Redis db.QueueCache
	Cfg   *config.Config
}

func main() {
	log.Println("Starting Sentinel Scheduler + Worker Pool...")
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

	service := &WorkerService{
		Mongo: database,
		Redis: queueCache,
		Cfg:   cfg,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start Scheduler
	go service.startScheduler(ctx)

	// Start Worker Pool
	var wg sync.WaitGroup
	for i := 1; i <= cfg.WorkerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			service.startWorker(ctx, workerID)
		}(i)
	}

	// Graceful shutdown setup
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("Shutting down Scheduler + Workers...")
	cancel() // cancel context to stop scheduler and workers
	wg.Wait()
	log.Println("Scheduler + Workers stopped.")
}

func (s *WorkerService) startScheduler(ctx context.Context) {
	log.Println("Scheduler loop started.")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Scheduler loop stopping.")
			return
		case <-ticker.C:
			s.scheduleDueMonitors(ctx)
		}
	}
}

func (s *WorkerService) scheduleDueMonitors(ctx context.Context) {
	// Query all monitors
	monitors, err := s.Mongo.GetMonitors(ctx)
	if err != nil {
		log.Printf("Scheduler error fetching monitors: %v", err)
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
			// Attempt to acquire in-flight lock to prevent duplicates
			acquired, err := s.Redis.AcquireInFlightLock(ctx, m.ID.Hex())
			if err != nil {
				log.Printf("Scheduler error acquiring lock for %s: %v", m.ID.Hex(), err)
				continue
			}

			if !acquired {
				// Job is already in-flight or worker queue is backed up
				log.Printf("Scheduler: Check for monitor %s is already in-flight. Skipping queue.", m.ID.Hex())
				continue
			}

			// Push job to queue
			err = s.Redis.EnqueueJob(ctx, m.ID.Hex())
			if err != nil {
				log.Printf("Scheduler error queueing monitor %s: %v", m.ID.Hex(), err)
				// Release lock if enqueuing fails
				_ = s.Redis.ReleaseInFlightLock(ctx, m.ID.Hex())
				continue
			}

			// Update last_queued_at
			err = s.Mongo.UpdateLastQueued(ctx, m.ID.Hex())
			if err != nil {
				log.Printf("Scheduler error updating last_queued_at for %s: %v", m.ID.Hex(), err)
			}

			log.Printf("Scheduler: Queued monitor %s (%s)", m.ID.Hex(), m.URL)
		}
	}
}

func (s *WorkerService) startWorker(ctx context.Context, workerID int) {
	log.Printf("Worker %d started.", workerID)
	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %d stopping.", workerID)
			return
		default:
			// DequeueJob blocks up to 5 seconds
			monitorID, err := s.Redis.DequeueJob(ctx)
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("Worker %d queue error: %v", workerID, err)
				}
				continue
			}

			if monitorID == "" {
				// Timeout waiting for job, loop again
				continue
			}

			log.Printf("Worker %d: Processing job for monitor %s", workerID, monitorID)
			s.processCheck(ctx, monitorID, workerID)
		}
	}
}

func (s *WorkerService) processCheck(ctx context.Context, monitorID string, workerID int) {
	// Always release the in-flight lock when this check completes
	defer func() {
		err := s.Redis.ReleaseInFlightLock(ctx, monitorID)
		if err != nil {
			log.Printf("Worker %d error releasing lock for %s: %v", workerID, monitorID, err)
		}
	}()

	// Fetch monitor details
	m, err := s.Mongo.GetMonitor(ctx, monitorID)
	if err != nil {
		log.Printf("Worker %d: Monitor %s not found (may have been deleted): %v", workerID, monitorID, err)
		return
	}

	// Fetch previous check (cache first, then DB fallback)
	var lastCheck *models.Check
	cachedLastCheck, err := s.Redis.GetLatestStatus(ctx, monitorID)
	if err != nil {
		log.Printf("Worker %d cache query error: %v", workerID, err)
	}

	if cachedLastCheck != nil {
		lastCheck = cachedLastCheck
	} else {
		// fallback to Mongo
		dbLastCheck, err := s.Mongo.GetLatestCheck(ctx, monitorID)
		if err == nil && dbLastCheck != nil {
			lastCheck = dbLastCheck
		}
	}

	// Execute URL check
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", m.URL, nil)
	if err != nil {
		log.Printf("Worker %d: Error building request for %s: %v", workerID, m.URL, err)
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
		log.Printf("Worker %d: Check failed for %s (%s): %v", workerID, m.URL, monitorID, reqErr)
	} else {
		statusCode = resp.StatusCode
		resp.Body.Close()
		isUp = statusCode >= 200 && statusCode < 400
		log.Printf("Worker %d: Check success for %s (%s) - Code: %d, Time: %dms", workerID, m.URL, monitorID, statusCode, duration)
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

	// Write check to database
	err = s.Mongo.SaveCheck(ctx, check)
	if err != nil {
		log.Printf("Worker %d: Error saving check to database for %s: %v", workerID, monitorID, err)
	}

	// Cache check in Redis
	err = s.Redis.CacheLatestStatus(ctx, &check)
	if err != nil {
		log.Printf("Worker %d: Error caching latest check for %s: %v", workerID, monitorID, err)
	}

	// Detect status changes and log status events
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
		err = s.Mongo.SaveStatusEvent(ctx, event)
		if err != nil {
			log.Printf("Worker %d: Error saving status event for %s: %v", workerID, monitorID, err)
		}
		log.Printf("Worker %d: STATUS CHANGE EVENT for %s (%s) -> transitioned to %s", workerID, m.URL, monitorID, changedTo)
	}
}
