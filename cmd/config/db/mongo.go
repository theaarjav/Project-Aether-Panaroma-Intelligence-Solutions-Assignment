package db

import (
	"context"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var mongoDB *mongo.Database
var mongoOnce sync.Once

func GetMongoDB(url, dbName string) *mongo.Database {
	mongoOnce.Do(func() {
		mongoDB = InitMongoDB(url, dbName)
	})
	return mongoDB
}

// InitMongoDB connects to MongoDB
func InitMongoDB(uri, dbName string) *mongo.Database {
	client, err := mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatalf("Failed to create MongoDB client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	// Ping to confirm connection
	if err := client.Ping(ctx, nil); err != nil {
		log.Fatalf("Failed to ping MongoDB: %v", err)
	}

	log.Println("MongoDB connected successfully!")
	InitTimeSeriesCollection(client.Database(dbName), "request_logs")
	return client.Database(dbName)
}

// InitTimeSeriesCollection checks if a collection exists, creates it if not
func InitTimeSeriesCollection(database *mongo.Database, collectionName string) *mongo.Collection {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check if collection already exists
	collections, err := database.ListCollectionNames(ctx, map[string]interface{}{})
	if err != nil {
		log.Fatalf("Failed to list collections: %v", err)
	}

	for _, coll := range collections {
		if coll == collectionName {
			log.Printf("TimeSeries collection '%s' already exists, skipping creation.", collectionName)
			return database.Collection(collectionName)
		}
	}

	// If not found, create it as a TimeSeries collection
	clientIdField := "client_id"
	secondsField := "seconds"
	opts := options.CreateCollection().SetTimeSeriesOptions(
		&options.TimeSeriesOptions{
			TimeField:   "timestamp",    // required
			MetaField:   &clientIdField, // optional metadata
			Granularity: &secondsField,  // "seconds" | "minutes" | "hours"
		},
	)

	err = database.CreateCollection(ctx, collectionName, opts)
	if err != nil {
		log.Fatalf("Failed to create time-series collection: %v", err)
	}

	log.Printf("TimeSeries collection '%s' created successfully!", collectionName)
	return database.Collection(collectionName)
}
