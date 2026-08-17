package db

import (
	"context"
	"sentinel/internal/models"
)

// Database defines the persistence contract (implemented by MongoDB and InMemory)
type Database interface {
	CreateMonitor(ctx context.Context, m models.Monitor) error
	GetMonitors(ctx context.Context) ([]models.Monitor, error)
	GetMonitor(ctx context.Context, id string) (*models.Monitor, error)
	DeleteMonitor(ctx context.Context, id string) error
	SaveCheck(ctx context.Context, c models.Check) error
	GetLatestCheck(ctx context.Context, monitorID string) (*models.Check, error)
	SaveStatusEvent(ctx context.Context, e models.StatusEvent) error
	GetStatusEvents(ctx context.Context, monitorID string) ([]models.StatusEvent, error)
	UpdateLastQueued(ctx context.Context, monitorID string) error
}

// QueueCache defines the queueing and cache contract (implemented by Redis and InMemory)
type QueueCache interface {
	CacheLatestStatus(ctx context.Context, check *models.Check) error
	GetLatestStatus(ctx context.Context, monitorID string) (*models.Check, error)
	DeleteStatusCache(ctx context.Context, monitorID string) error
	EnqueueJob(ctx context.Context, monitorID string) error
	DequeueJob(ctx context.Context) (string, error)
	AcquireInFlightLock(ctx context.Context, monitorID string) (bool, error)
	ReleaseInFlightLock(ctx context.Context, monitorID string) error
}
