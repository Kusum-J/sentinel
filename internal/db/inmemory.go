package db

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"sentinel/internal/models"
)

// InMemoryDB implements the Database interface
type InMemoryDB struct {
	monitors map[string]models.Monitor
	checks   map[string][]models.Check
	events   map[string][]models.StatusEvent
	mu       sync.RWMutex
}

func NewInMemoryDB() *InMemoryDB {
	return &InMemoryDB{
		monitors: make(map[string]models.Monitor),
		checks:   make(map[string][]models.Check),
		events:   make(map[string][]models.StatusEvent),
	}
}

func (db *InMemoryDB) CreateMonitor(ctx context.Context, m models.Monitor) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.monitors[m.ID.Hex()] = m
	return nil
}

func (db *InMemoryDB) GetMonitors(ctx context.Context) ([]models.Monitor, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	list := make([]models.Monitor, 0, len(db.monitors))
	for _, m := range db.monitors {
		list = append(list, m)
	}
	return list, nil
}

func (db *InMemoryDB) GetMonitor(ctx context.Context, id string) (*models.Monitor, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	m, exists := db.monitors[id]
	if !exists {
		return nil, errors.New("not found")
	}
	return &m, nil
}

func (db *InMemoryDB) DeleteMonitor(ctx context.Context, id string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.monitors, id)
	delete(db.checks, id)
	delete(db.events, id)
	return nil
}

func (db *InMemoryDB) SaveCheck(ctx context.Context, c models.Check) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	key := c.MonitorID.Hex()
	db.checks[key] = append(db.checks[key], c)
	return nil
}

func (db *InMemoryDB) GetLatestCheck(ctx context.Context, monitorID string) (*models.Check, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	list, exists := db.checks[monitorID]
	if !exists || len(list) == 0 {
		return nil, nil
	}
	// Return the last check (most recent)
	return &list[len(list)-1], nil
}

func (db *InMemoryDB) SaveStatusEvent(ctx context.Context, e models.StatusEvent) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	key := e.MonitorID.Hex()
	db.events[key] = append(db.events[key], e)
	return nil
}

func (db *InMemoryDB) GetStatusEvents(ctx context.Context, monitorID string) ([]models.StatusEvent, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	list, exists := db.events[monitorID]
	if !exists {
		return []models.StatusEvent{}, nil
	}
	// Return in reverse chronological order
	reversed := make([]models.StatusEvent, len(list))
	for i, e := range list {
		reversed[len(list)-1-i] = e
	}
	return reversed, nil
}

func (db *InMemoryDB) UpdateLastQueued(ctx context.Context, monitorID string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	m, exists := db.monitors[monitorID]
	if !exists {
		return errors.New("not found")
	}
	now := time.Now()
	m.LastQueuedAt = &now
	db.monitors[monitorID] = m
	return nil
}

// InMemoryQueueCache implements the QueueCache interface
type InMemoryQueueCache struct {
	queue chan string
	cache map[string]*models.Check
	locks map[string]time.Time
	mu    sync.RWMutex
}

func NewInMemoryQueueCache() *InMemoryQueueCache {
	return &InMemoryQueueCache{
		queue: make(chan string, 1000),
		cache: make(map[string]*models.Check),
		locks: make(map[string]time.Time),
	}
}

func (qc *InMemoryQueueCache) CacheLatestStatus(ctx context.Context, check *models.Check) error {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	qc.cache[check.MonitorID.Hex()] = check
	return nil
}

func (qc *InMemoryQueueCache) GetLatestStatus(ctx context.Context, monitorID string) (*models.Check, error) {
	qc.mu.RLock()
	defer qc.mu.RUnlock()
	check, exists := qc.cache[monitorID]
	if !exists {
		return nil, nil
	}
	return check, nil
}

func (qc *InMemoryQueueCache) DeleteStatusCache(ctx context.Context, monitorID string) error {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	delete(qc.cache, monitorID)
	delete(qc.locks, monitorID)
	return nil
}

func (qc *InMemoryQueueCache) EnqueueJob(ctx context.Context, monitorID string) error {
	select {
	case qc.queue <- monitorID:
		return nil
	default:
		return errors.New("queue full")
	}
}

func (qc *InMemoryQueueCache) DequeueJob(ctx context.Context) (string, error) {
	select {
	case id := <-qc.queue:
		return id, nil
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(5 * time.Second): // mimics Redis BRPOP 5s timeout
		return "", nil
	}
}

func (qc *InMemoryQueueCache) AcquireInFlightLock(ctx context.Context, monitorID string) (bool, error) {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	now := time.Now()
	expiry, locked := qc.locks[monitorID]
	if locked && now.Before(expiry) {
		return false, nil // already locked
	}
	// set lock with 30s TTL
	qc.locks[monitorID] = now.Add(30 * time.Second)
	return true, nil
}

func (qc *InMemoryQueueCache) ReleaseInFlightLock(ctx context.Context, monitorID string) error {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	delete(qc.locks, monitorID)
	return nil
}

// Convert string ID helper for compatibility
func StringToObjectID(idStr string) primitive.ObjectID {
	id, err := primitive.ObjectIDFromHex(idStr)
	if err != nil {
		return primitive.NewObjectID()
	}
	return id
}
