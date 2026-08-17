package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Monitor represents a URL being monitored
type Monitor struct {
	ID              primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	URL             string             `bson:"url" json:"url"`
	IntervalSeconds int                `bson:"interval_seconds" json:"interval_seconds"`
	CreatedAt       time.Time          `bson:"created_at" json:"created_at"`
	LastQueuedAt    *time.Time         `bson:"last_queued_at,omitempty" json:"last_queued_at,omitempty"`
}

// Check represents a single URL check execution result
type Check struct {
	ID             primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	MonitorID      primitive.ObjectID `bson:"monitor_id" json:"monitor_id"`
	Timestamp      time.Time          `bson:"timestamp" json:"timestamp"`
	StatusCode     int                `bson:"status_code" json:"status_code"`
	ResponseTimeMS int64              `bson:"response_time_ms" json:"response_time_ms"`
	IsUp           bool               `bson:"is_up" json:"is_up"`
	ErrorMsg       string             `bson:"error_msg,omitempty" json:"error_msg,omitempty"` // for debugging network errors
}

// StatusEvent logs when a monitor transitions from up <-> down
type StatusEvent struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	MonitorID primitive.ObjectID `bson:"monitor_id" json:"monitor_id"`
	Timestamp time.Time          `bson:"timestamp" json:"timestamp"`
	ChangedTo string             `bson:"changed_to" json:"changed_to"` // "up" or "down"
}

// MonitorDetail holds monitor info plus the latest check status
type MonitorDetail struct {
	Monitor
	LatestCheck *Check `json:"latest_check"`
}
