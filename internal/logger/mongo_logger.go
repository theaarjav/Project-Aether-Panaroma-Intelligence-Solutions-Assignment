package logger

import (
	"context"
	"fmt"
	"log"
	"time"

	dbConfig "project-aether/cmd/config/db"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoLogger struct {
	mongoDB    *mongo.Database
	collection *mongo.Collection
}

type RequestLog struct {
	ID                string    `bson:"_id,omitempty"`
	ClientID          string    `bson:"client_id"`
	Timestamp         time.Time `bson:"timestamp"`
	Path              string    `bson:"path"`
	Method            string    `bson:"method"`
	StatusCode        int       `bson:"status_code"`
	ResponseLatencyMs int64     `bson:"response_latency_ms"`
	IsThrottled       bool      `bson:"is_throttled"`
	IsAnomalous       bool      `bson:"is_anomalous"`
	UpstreamService   string    `bson:"upstream_service"`
	UserAgent         string    `bson:"user_agent,omitempty"`
	IPAddress         string    `bson:"ip_address,omitempty"`
	RequestSize       int64     `bson:"request_size"`
	ResponseSize      int64     `bson:"response_size"`

	// Time series specific fields
	Hour  int `bson:"hour"`
	Day   int `bson:"day"`
	Month int `bson:"month"`
	Year  int `bson:"year"`
}

type ClientMetrics struct {
	ClientID           string    `bson:"_id"`
	Date               time.Time `bson:"date"`
	TotalRequests      int64     `bson:"total_requests"`
	SuccessfulRequests int64     `bson:"successful_requests"`
	ThrottledRequests  int64     `bson:"throttled_requests"`
	AnomalousRequests  int64     `bson:"anomalous_requests"`
	AvgLatencyMs       float64   `bson:"avg_latency_ms"`
	MaxLatencyMs       int64     `bson:"max_latency_ms"`
	ErrorRate          float64   `bson:"error_rate"`
	LastUpdated        time.Time `bson:"last_updated"`
}

func NewMongoLogger(mongoURL string) (*MongoLogger, error) {
	db := dbConfig.GetMongoDB(mongoURL, "aether")
	collection := db.Collection("request_logs")

	// Create indexes for better query performance
	go func() {
		ctx := context.Background()

		indexes := []mongo.IndexModel{
			{
				Keys: bson.D{
					{Key: "client_id", Value: 1},
					{Key: "timestamp", Value: -1},
				},
			},
			{
				Keys: bson.D{
					{Key: "timestamp", Value: -1},
				},
			},
			{
				Keys: bson.D{
					{Key: "client_id", Value: 1},
					{Key: "year", Value: 1},
					{Key: "month", Value: 1},
					{Key: "day", Value: 1},
				},
			},
			{
				Keys: bson.D{
					{Key: "is_anomalous", Value: 1},
					{Key: "timestamp", Value: -1},
				},
			},
		}

		for _, index := range indexes {
			_, err := collection.Indexes().CreateOne(ctx, index)
			if err != nil {
				log.Printf("Failed to create index: %v", err)
			}
		}

		// Create TTL index for automatic log cleanup (keep logs for 30 days)
		ttlIndex := mongo.IndexModel{
			Keys:    bson.D{{Key: "timestamp", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(30 * 24 * 3600), // 30 days
		}

		_, err := collection.Indexes().CreateOne(ctx, ttlIndex)
		if err != nil {
			log.Printf("Failed to create TTL index: %v", err)
		}
	}()

	return &MongoLogger{
		mongoDB:    db,
		collection: collection,
	}, nil
}

func (ml *MongoLogger) GetDB() *mongo.Database {
	return ml.mongoDB
}

func (ml *MongoLogger) LogRequest(ctx context.Context, log RequestLog) error {
	// Add time series fields for easier aggregation
	log.Hour = log.Timestamp.Hour()
	log.Day = log.Timestamp.Day()
	log.Month = int(log.Timestamp.Month())
	log.Year = log.Timestamp.Year()

	// Insert with context timeout
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := ml.collection.InsertOne(ctx, log)
	if err != nil {
		fmt.Printf("Failed to log request to MongoDB: %v", err)
		// Don't return error to avoid breaking the request flow
	}
	return nil
}

func (ml *MongoLogger) LogRequestAsync(log RequestLog) {
	go func() {
		ctx := context.Background()
		ml.LogRequest(ctx, log)
	}()
}

func (ml *MongoLogger) GetClientLogs(ctx context.Context, clientID string, since time.Time, limit int64) ([]RequestLog, error) {
	filter := bson.M{
		"client_id": clientID,
		"timestamp": bson.M{"$gte": since},
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "timestamp", Value: -1}}).
		SetLimit(limit)

	cursor, err := ml.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var logs []RequestLog
	if err := cursor.All(ctx, &logs); err != nil {
		return nil, err
	}

	return logs, nil
}

// Simple MongoDB queries - complex calculations done in Go
func (ml *MongoLogger) GetClientRequestCounts(ctx context.Context, clientID string, since time.Time) ([]RequestCount, error) {
	filter := bson.M{
		"client_id": clientID,
		"timestamp": bson.M{"$gte": since},
	}

	cursor, err := ml.collection.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "timestamp", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var requests []RequestCount
	for cursor.Next(ctx) {
		var log RequestLog
		if err := cursor.Decode(&log); err != nil {
			continue
		}

		requests = append(requests, RequestCount{
			Timestamp:   log.Timestamp,
			IsThrottled: log.IsThrottled,
			IsAnomalous: log.IsAnomalous,
			StatusCode:  log.StatusCode,
			LatencyMs:   log.ResponseLatencyMs,
		})
	}

	return requests, nil
}

type RequestCount struct {
	Timestamp   time.Time
	IsThrottled bool
	IsAnomalous bool
	StatusCode  int
	LatencyMs   int64
}

func (ml *MongoLogger) GetAggregatedMetrics(ctx context.Context, clientID string, since time.Time) (*ClientMetrics, error) {
	// Simple query to get all requests for the client
	requests, err := ml.GetClientRequestCounts(ctx, clientID, since)
	if err != nil {
		return nil, err
	}

	if len(requests) == 0 {
		return &ClientMetrics{
			ClientID:    clientID,
			Date:        since,
			LastUpdated: time.Now(),
		}, nil
	}

	// Calculate metrics in Go code
	metrics := &ClientMetrics{
		ClientID:    clientID,
		Date:        since,
		LastUpdated: time.Now(),
	}

	var totalLatency int64
	var maxLatency int64
	var errorCount int64

	for _, req := range requests {
		metrics.TotalRequests++

		if req.StatusCode < 400 {
			metrics.SuccessfulRequests++
		} else {
			errorCount++
		}

		if req.IsThrottled {
			metrics.ThrottledRequests++
		}

		if req.IsAnomalous {
			metrics.AnomalousRequests++
		}

		totalLatency += req.LatencyMs
		if req.LatencyMs > maxLatency {
			maxLatency = req.LatencyMs
		}
	}

	// Calculate averages and rates
	if metrics.TotalRequests > 0 {
		metrics.AvgLatencyMs = float64(totalLatency) / float64(metrics.TotalRequests)
		metrics.ErrorRate = float64(errorCount) / float64(metrics.TotalRequests) * 100
	}
	metrics.MaxLatencyMs = maxLatency

	return metrics, nil
}

func (ml *MongoLogger) GetSystemMetrics(ctx context.Context, since time.Time) (map[string]interface{}, error) {
	// Simple query to get all requests since timestamp
	filter := bson.M{
		"timestamp": bson.M{"$gte": since},
	}

	cursor, err := ml.collection.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	// Calculate metrics in Go
	var totalRequests int64
	var throttledRequests int64
	var anomalousRequests int64
	var totalLatency int64
	var errorCount int64
	clientSet := make(map[string]bool)
	latencies := make([]int64, 0)

	for cursor.Next(ctx) {
		var log RequestLog
		if err := cursor.Decode(&log); err != nil {
			continue
		}

		totalRequests++
		clientSet[log.ClientID] = true
		totalLatency += log.ResponseLatencyMs
		latencies = append(latencies, log.ResponseLatencyMs)

		if log.IsThrottled {
			throttledRequests++
		}
		if log.IsAnomalous {
			anomalousRequests++
		}
		if log.StatusCode >= 400 {
			errorCount++
		}
	}

	// Calculate p95 latency in Go
	p95Latency := calculatePercentile(latencies, 95)

	result := map[string]interface{}{
		"total_requests":  totalRequests,
		"unique_clients":  len(clientSet),
		"total_throttled": throttledRequests,
		"total_anomalous": anomalousRequests,
		"avg_latency":     0.0,
		"p95_latency":     p95Latency,
		"throttle_rate":   0.0,
		"anomaly_rate":    0.0,
		"error_rate":      0.0,
		"since":           since,
		"generated_at":    time.Now(),
	}

	if totalRequests > 0 {
		result["avg_latency"] = float64(totalLatency) / float64(totalRequests)
		result["throttle_rate"] = float64(throttledRequests) / float64(totalRequests)
		result["anomaly_rate"] = float64(anomalousRequests) / float64(totalRequests)
		result["error_rate"] = float64(errorCount) / float64(totalRequests)
	}

	return result, nil
}

func (ml *MongoLogger) GetTopClients(ctx context.Context, since time.Time, limit int64) ([]map[string]interface{}, error) {
	// Simple query to get all requests
	filter := bson.M{
		"timestamp": bson.M{"$gte": since},
	}

	cursor, err := ml.collection.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	// Calculate per-client metrics in Go
	clientStats := make(map[string]*ClientMetrics)

	for cursor.Next(ctx) {
		var log RequestLog
		if err := cursor.Decode(&log); err != nil {
			continue
		}

		stats, exists := clientStats[log.ClientID]
		if !exists {
			stats = &ClientMetrics{
				ClientID: log.ClientID,
				Date:     since,
			}
			clientStats[log.ClientID] = stats
		}

		stats.TotalRequests++
		if log.StatusCode < 400 {
			stats.SuccessfulRequests++
		}
		if log.IsThrottled {
			stats.ThrottledRequests++
		}
		if log.IsAnomalous {
			stats.AnomalousRequests++
		}

		// Update latency calculation
		if stats.TotalRequests == 1 {
			stats.AvgLatencyMs = float64(log.ResponseLatencyMs)
		} else {
			// Running average
			stats.AvgLatencyMs = (stats.AvgLatencyMs*float64(stats.TotalRequests-1) + float64(log.ResponseLatencyMs)) / float64(stats.TotalRequests)
		}

		if log.ResponseLatencyMs > stats.MaxLatencyMs {
			stats.MaxLatencyMs = log.ResponseLatencyMs
		}
	}

	// Convert to slice and sort by total requests
	var results []map[string]interface{}
	for clientID, stats := range clientStats {
		if stats.TotalRequests > 0 {
			stats.ErrorRate = float64(stats.TotalRequests-stats.SuccessfulRequests) / float64(stats.TotalRequests) * 100
		}

		result := map[string]interface{}{
			"client_id":          clientID,
			"total_requests":     stats.TotalRequests,
			"avg_latency":        stats.AvgLatencyMs,
			"throttled_requests": stats.ThrottledRequests,
			"anomalous_requests": stats.AnomalousRequests,
			"error_rate":         stats.ErrorRate,
			"throttle_rate":      float64(stats.ThrottledRequests) / float64(stats.TotalRequests),
			"anomaly_rate":       float64(stats.AnomalousRequests) / float64(stats.TotalRequests),
		}
		results = append(results, result)
	}

	// Sort by total requests (descending) and limit
	// Simple bubble sort for small datasets
	for i := 0; i < len(results)-1; i++ {
		for j := 0; j < len(results)-i-1; j++ {
			if results[j]["total_requests"].(int64) < results[j+1]["total_requests"].(int64) {
				results[j], results[j+1] = results[j+1], results[j]
			}
		}
	}

	if int64(len(results)) > limit {
		results = results[:limit]
	}

	return results, nil
}

// Helper function to calculate percentiles in Go
func calculatePercentile(values []int64, percentile int) float64 {
	if len(values) == 0 {
		return 0
	}

	// Simple sort
	for i := 0; i < len(values)-1; i++ {
		for j := 0; j < len(values)-i-1; j++ {
			if values[j] > values[j+1] {
				values[j], values[j+1] = values[j+1], values[j]
			}
		}
	}

	index := int(float64(len(values)) * float64(percentile) / 100.0)
	if index >= len(values) {
		index = len(values) - 1
	}

	return float64(values[index])
}
func (ml *MongoLogger) Close() {
	if ml.mongoDB != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ml.mongoDB.Client().Disconnect(ctx)
	}
}
