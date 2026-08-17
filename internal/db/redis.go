package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"sentinel/internal/config"
	"sentinel/internal/models"
)

type RedisClient struct {
	Client *redis.Client
}

const (
	QueueKey        = "sentinel:queue"
	CachePrefix     = "status:"
	InFlightPrefix  = "inflight:"
	CacheTTL        = 30 * time.Second
	DefaultLockTTL  = 30 * time.Second // enough time for workers to process even with a 5s HTTP timeout
)

func NewRedisClient(cfg *config.Config) (*RedisClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPass,
		DB:       0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, err
	}

	log.Printf("Connected to Redis at %s", cfg.RedisAddr)
	return &RedisClient{Client: client}, nil
}

// CacheLatestStatus caches the latest check result for a monitor with a short TTL (30s)
func (r *RedisClient) CacheLatestStatus(ctx context.Context, check *models.Check) error {
	key := fmt.Sprintf("%s%s", CachePrefix, check.MonitorID.Hex())
	data, err := json.Marshal(check)
	if err != nil {
		return err
	}
	return r.Client.Set(ctx, key, data, CacheTTL).Err()
}

// GetLatestStatus retrieves the cached check result for a monitor
func (r *RedisClient) GetLatestStatus(ctx context.Context, monitorID string) (*models.Check, error) {
	key := fmt.Sprintf("%s%s", CachePrefix, monitorID)
	data, err := r.Client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, nil // cache miss
	} else if err != nil {
		return nil, err
	}

	var check models.Check
	err = json.Unmarshal([]byte(data), &check)
	if err != nil {
		return nil, err
	}
	return &check, nil
}

// DeleteStatusCache deletes cached check results and lock keys (useful on deletion)
func (r *RedisClient) DeleteStatusCache(ctx context.Context, monitorID string) error {
	cacheKey := fmt.Sprintf("%s%s", CachePrefix, monitorID)
	lockKey := fmt.Sprintf("%s%s", InFlightPrefix, monitorID)
	return r.Client.Del(ctx, cacheKey, lockKey).Err()
}

// EnqueueJob pushes a monitor ID to the check queue
func (r *RedisClient) EnqueueJob(ctx context.Context, monitorID string) error {
	return r.Client.LPush(ctx, QueueKey, monitorID).Err()
}

// DequeueJob blocks until a monitor ID is available and returns it
func (r *RedisClient) DequeueJob(ctx context.Context) (string, error) {
	// BRPOP returns [key, value]
	res, err := r.Client.BRPop(ctx, 5*time.Second, QueueKey).Result()
	if err == redis.Nil {
		return "", nil // timeout waiting for job
	} else if err != nil {
		return "", err
	}
	if len(res) < 2 {
		return "", errors.New("invalid queue response")
	}
	return res[1], nil
}

// AcquireInFlightLock attempts to set a lock for monitor checking to prevent duplicates
func (r *RedisClient) AcquireInFlightLock(ctx context.Context, monitorID string) (bool, error) {
	key := fmt.Sprintf("%s%s", InFlightPrefix, monitorID)
	return r.Client.SetNX(ctx, key, "1", DefaultLockTTL).Result()
}

// ReleaseInFlightLock removes the lock, allowing the monitor to be queued again
func (r *RedisClient) ReleaseInFlightLock(ctx context.Context, monitorID string) error {
	key := fmt.Sprintf("%s%s", InFlightPrefix, monitorID)
	return r.Client.Del(ctx, key).Err()
}
