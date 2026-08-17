package db

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sentinel/internal/config"
	"sentinel/internal/models"
)

type MongoClient struct {
	Client *mongo.Client
	DB     *mongo.Database
}

func NewMongoClient(cfg *config.Config) (*MongoClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second) // Fast timeout for quick local fallback
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.MongoURI))
	if err != nil {
		return nil, err
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}

	db := client.Database(cfg.MongoDB)
	log.Printf("Connected to MongoDB at %s", cfg.MongoURI)

	return &MongoClient{
		Client: client,
		DB:     db,
	}, nil
}

func (m *MongoClient) Monitors() *mongo.Collection {
	return m.DB.Collection("monitors")
}

func (m *MongoClient) Checks() *mongo.Collection {
	return m.DB.Collection("checks")
}

func (m *MongoClient) StatusEvents() *mongo.Collection {
	return m.DB.Collection("status_events")
}

// Database interface implementation:

func (m *MongoClient) CreateMonitor(ctx context.Context, mon models.Monitor) error {
	_, err := m.Monitors().InsertOne(ctx, mon)
	return err
}

func (m *MongoClient) GetMonitors(ctx context.Context) ([]models.Monitor, error) {
	cursor, err := m.Monitors().Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var list []models.Monitor
	if err := cursor.All(ctx, &list); err != nil {
		return nil, err
	}
	if list == nil {
		list = []models.Monitor{}
	}
	return list, nil
}

func (m *MongoClient) GetMonitor(ctx context.Context, idStr string) (*models.Monitor, error) {
	id, err := primitive.ObjectIDFromHex(idStr)
	if err != nil {
		return nil, err
	}
	var mon models.Monitor
	err = m.Monitors().FindOne(ctx, bson.M{"_id": id}).Decode(&mon)
	if err != nil {
		return nil, err
	}
	return &mon, nil
}

func (m *MongoClient) DeleteMonitor(ctx context.Context, idStr string) error {
	id, err := primitive.ObjectIDFromHex(idStr)
	if err != nil {
		return err
	}
	_, err = m.Monitors().DeleteOne(ctx, bson.M{"_id": id})
	return err
}

func (m *MongoClient) SaveCheck(ctx context.Context, c models.Check) error {
	_, err := m.Checks().InsertOne(ctx, c)
	return err
}

func (m *MongoClient) GetLatestCheck(ctx context.Context, monitorID string) (*models.Check, error) {
	id, err := primitive.ObjectIDFromHex(monitorID)
	if err != nil {
		return nil, err
	}
	opts := options.FindOne().SetSort(bson.D{{Key: "timestamp", Value: -1}})
	var check models.Check
	err = m.Checks().FindOne(ctx, bson.M{"monitor_id": id}, opts).Decode(&check)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	return &check, err
}

func (m *MongoClient) SaveStatusEvent(ctx context.Context, e models.StatusEvent) error {
	_, err := m.StatusEvents().InsertOne(ctx, e)
	return err
}

func (m *MongoClient) GetStatusEvents(ctx context.Context, monitorID string) ([]models.StatusEvent, error) {
	id, err := primitive.ObjectIDFromHex(monitorID)
	if err != nil {
		return nil, err
	}
	opts := options.Find().SetSort(bson.D{{Key: "timestamp", Value: -1}}).SetLimit(50)
	cursor, err := m.StatusEvents().Find(ctx, bson.M{"monitor_id": id}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var list []models.StatusEvent
	if err := cursor.All(ctx, &list); err != nil {
		return nil, err
	}
	if list == nil {
		list = []models.StatusEvent{}
	}
	return list, nil
}

func (m *MongoClient) UpdateLastQueued(ctx context.Context, monitorID string) error {
	id, err := primitive.ObjectIDFromHex(monitorID)
	if err != nil {
		return err
	}
	_, err = m.Monitors().UpdateOne(
		ctx,
		bson.M{"_id": id},
		bson.M{"$set": bson.M{"last_queued_at": time.Now()}},
	)
	return err
}
