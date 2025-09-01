package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	dbConfig "project-aether/cmd/config/db"
	"project-aether/internal/analytics"
	"project-aether/internal/config"
	"project-aether/internal/messaging"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type AnalysisService struct {
	kafka  *messaging.KafkaConsumer
	redis  *redis.Client
	db     *sql.DB
	mongo  *mongo.Database
	config *config.AnalysisConfig

	// Analytics engine for complex calculations
	analyticsEngine *analytics.AnalyticsEngine

	// Collections
	requestLogs *mongo.Collection

	// In-memory sliding windows for real-time analysis
	clientWindows map[string]*SlidingWindow
	windowMutex   sync.RWMutex

	// Background processing
	ctx    context.Context
	cancel context.CancelFunc
}

type SlidingWindow struct {
	requests   []time.Time
	lastUpdate time.Time
	windowSize time.Duration
	mutex      sync.RWMutex
}

func (s *AnalysisService) saveBaseline(baseline *ClientBaseline) {
	// Save to Redis cache for fast access
	baselineJSON, err := json.Marshal(baseline)
	if err == nil {
		cacheKey := fmt.Sprintf("baseline:%s", baseline.ClientID)
		s.redis.Set(s.ctx, cacheKey, baselineJSON, time.Hour)
	}

	log.Printf("Updated baseline for client %s: avg=%.2f, stddev=%.2f, samples=%d",
		baseline.ClientID, baseline.AvgRequestsPerMin, baseline.StdDeviation, baseline.SampleCount)
}

// Enhanced analysis with MongoDB data
func (s *AnalysisService) analyzeClientBehaviorFromMongo(clientID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Get last hour of data for detailed analysis
	since := time.Now().Add(-time.Hour)
	filter := bson.M{
		"client_id": clientID,
		"timestamp": bson.M{"$gte": since},
	}

	cursor, err := s.requestLogs.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "timestamp", Value: 1}}))
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	// Collect request data in Go
	var requests []RequestData
	for cursor.Next(ctx) {
		var log struct {
			Timestamp   time.Time `bson:"timestamp"`
			IsThrottled bool      `bson:"is_throttled"`
			StatusCode  int       `bson:"status_code"`
			LatencyMs   int64     `bson:"response_latency_ms"`
		}
		if err := cursor.Decode(&log); err != nil {
			continue
		}

		requests = append(requests, RequestData{
			Timestamp:   log.Timestamp,
			IsThrottled: log.IsThrottled,
			StatusCode:  log.StatusCode,
			LatencyMs:   log.LatencyMs,
		})
	}

	if len(requests) < 10 {
		return nil // Not enough data for analysis
	}

	// Analyze patterns in Go code
	patterns := s.analyzeRequestPatterns(requests)

	// Store analysis results in Redis for quick access
	analysisKey := fmt.Sprintf("analysis:%s", clientID)
	analysisJSON, _ := json.Marshal(patterns)
	s.redis.Set(s.ctx, analysisKey, analysisJSON, 30*time.Minute)

	log.Printf("Analyzed behavior for client %s: %d requests, patterns detected: %v",
		clientID, len(requests), patterns.HasSuspiciousPatterns)

	return nil
}

type RequestData struct {
	Timestamp   time.Time
	IsThrottled bool
	StatusCode  int
	LatencyMs   int64
}

type BehaviorPatterns struct {
	ClientID              string    `json:"client_id"`
	RequestsPerMinute     float64   `json:"requests_per_minute"`
	AverageLatency        float64   `json:"average_latency"`
	ErrorRate             float64   `json:"error_rate"`
	HasSuspiciousPatterns bool      `json:"has_suspicious_patterns"`
	PatternDetails        []string  `json:"pattern_details"`
	AnalyzedAt            time.Time `json:"analyzed_at"`
}

func (s *AnalysisService) analyzeRequestPatterns(requests []RequestData) *BehaviorPatterns {
	if len(requests) == 0 {
		return &BehaviorPatterns{AnalyzedAt: time.Now()}
	}

	patterns := &BehaviorPatterns{
		AnalyzedAt:     time.Now(),
		PatternDetails: make([]string, 0),
	}

	// Calculate basic metrics
	totalLatency := int64(0)
	errorCount := 0

	for _, req := range requests {
		totalLatency += req.LatencyMs
		if req.StatusCode >= 400 {
			errorCount++
		}
	}

	// Calculate rates
	timeSpan := requests[len(requests)-1].Timestamp.Sub(requests[0].Timestamp)
	if timeSpan.Minutes() > 0 {
		patterns.RequestsPerMinute = float64(len(requests)) / timeSpan.Minutes()
	}

	patterns.AverageLatency = float64(totalLatency) / float64(len(requests))
	patterns.ErrorRate = float64(errorCount) / float64(len(requests)) * 100

	// Detect suspicious patterns using simple Go logic
	if patterns.RequestsPerMinute > 300 {
		patterns.HasSuspiciousPatterns = true
		patterns.PatternDetails = append(patterns.PatternDetails, "Very high request rate")
	}

	if patterns.ErrorRate > 50 {
		patterns.HasSuspiciousPatterns = true
		patterns.PatternDetails = append(patterns.PatternDetails, "High error rate")
	}

	// Check for burst patterns (many requests in short time)
	if len(requests) > 50 {
		burstCount := s.detectBurstPatterns(requests)
		if burstCount > 3 {
			patterns.HasSuspiciousPatterns = true
			patterns.PatternDetails = append(patterns.PatternDetails, fmt.Sprintf("Detected %d burst patterns", burstCount))
		}
	}

	return patterns
}

func (s *AnalysisService) detectBurstPatterns(requests []RequestData) int {
	if len(requests) < 10 {
		return 0
	}

	burstCount := 0
	windowSize := 10 * time.Second
	burstThreshold := 20 // requests in 10 seconds

	for i := 0; i < len(requests)-burstThreshold; i++ {
		windowStart := requests[i].Timestamp
		requestsInWindow := 0

		for j := i; j < len(requests) && requests[j].Timestamp.Sub(windowStart) <= windowSize; j++ {
			requestsInWindow++
		}

		if requestsInWindow >= burstThreshold {
			burstCount++
			// Skip ahead to avoid counting overlapping bursts
			i += burstThreshold - 1
		}
	}

	return burstCount
}

type TelemetryEvent struct {
	ClientID          string    `json:_id"`
	Timestamp         time.Time `json:"timestamp"`
	Path              string    `json:"path"`
	Method            string    `json:"method"`
	StatusCode        int       `json:"status_code"`
	ResponseLatencyMs int64     `json:"response_latency_ms"`
	IsThrottled       bool      `json:"is_throttled"`
	IsAnomalous       bool      `json:"is_anomalous"`
	UpstreamService   string    `json:"upstream_service"`
}

type ClientBaseline struct {
	ClientID          string    `json:"client_id"`
	AvgRequestsPerMin float64   `json:"avg_requests_per_minute"`
	StdDeviation      float64   `json:"std_deviation"`
	SampleCount       int       `json:"sample_count"`
	LastUpdated       time.Time `json:"last_updated"`
}

func main() {
	cfg := config.LoadAnalysisConfig()

	service, err := NewAnalysisService(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize analysis service: %v", err)
	}

	// Start the service
	if err := service.Start(); err != nil {
		log.Fatalf("Failed to start analysis service: %v", err)
	}

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down analysis service...")
	service.Stop()
	log.Println("Analysis service stopped")
}

func (s *AnalysisService) loadBaselines() error {
	query := `
		SELECT client_id, avg_requests_per_minute, std_deviation, sample_count, last_updated
		FROM client_baselines
		WHERE last_updated > $1
	`

	cutoff := time.Now().Add(-24 * time.Hour) // Load baselines from last 24 hours
	rows, err := s.db.Query(query, cutoff)
	if err != nil {
		return err
	}
	defer rows.Close()

	loaded := 0
	for rows.Next() {
		var baseline ClientBaseline
		err := rows.Scan(
			&baseline.ClientID,
			&baseline.AvgRequestsPerMin,
			&baseline.StdDeviation,
			&baseline.SampleCount,
			&baseline.LastUpdated,
		)
		if err != nil {
			log.Printf("Failed to scan baseline row: %v", err)
			continue
		}

		// Initialize sliding window for this client
		s.windowMutex.Lock()
		s.clientWindows[baseline.ClientID] = &SlidingWindow{
			requests:   make([]time.Time, 0),
			windowSize: time.Duration(s.config.WindowMinutes) * time.Minute,
		}
		s.windowMutex.Unlock()

		loaded++
	}

	log.Printf("Loaded %d client baselines", loaded)
	return nil
}

func NewSlidingWindow(windowSize time.Duration) *SlidingWindow {
	return &SlidingWindow{
		requests:   make([]time.Time, 0),
		windowSize: windowSize,
	}
}

func (sw *SlidingWindow) Add(timestamp time.Time) {
	sw.mutex.Lock()
	defer sw.mutex.Unlock()

	sw.requests = append(sw.requests, timestamp)
	sw.lastUpdate = time.Now()

	// Clean up old requests
	cutoff := timestamp.Add(-sw.windowSize)
	validIndex := 0
	for i, req := range sw.requests {
		if req.After(cutoff) {
			validIndex = i
			break
		}
	}
	sw.requests = sw.requests[validIndex:]
}

func (sw *SlidingWindow) GetRate() float64 {
	sw.mutex.RLock()
	defer sw.mutex.RUnlock()

	now := time.Now()
	cutoff := now.Add(-time.Minute)

	count := 0
	for _, req := range sw.requests {
		if req.After(cutoff) {
			count++
		}
	}

	return float64(count)
}

func NewAnalysisService(cfg *config.AnalysisConfig) (*AnalysisService, error) {
	// Connect to Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr: cfg.RedisURL[8:], // Remove redis:// prefix
	})

	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Connect to PostgreSQL (for client configurations)
	db := dbConfig.GetPostgresDB(cfg.DBURL, "./migrations")

	// Connect to MongoDB
	mongoDb := dbConfig.GetMongoDB(cfg.MongoURL, "aether")

	// Get collections
	requestLogs := mongoDb.Collection("request_logs")

	// Initialize analytics engine
	analyticsEngine := analytics.NewAnalyticsEngine(requestLogs)

	ctx, cancel := context.WithCancel(context.Background())

	// Initialize Kafka consumer
	brokers := strings.Split(cfg.KafkaBrokers, ",")

	service := &AnalysisService{
		redis:           redisClient,
		db:              db,
		mongo:           mongoDb,
		config:          cfg,
		analyticsEngine: analyticsEngine,
		requestLogs:     requestLogs,
		clientWindows:   make(map[string]*SlidingWindow),
		ctx:             ctx,
		cancel:          cancel,
	}

	kafkaConsumer := messaging.NewKafkaConsumer(
		brokers,
		"api-calls",
		"analysis-service",
		service, // AnalysisService implements EventHandler
	)

	service.kafka = kafkaConsumer

	return service, nil
}

func (s *AnalysisService) Start() error {
	log.Println("Starting analysis service...")

	// Load existing baselines from database
	if err := s.loadBaselines(); err != nil {
		log.Printf("Warning: Failed to load baselines: %v", err)
	}

	log.Println("Subscribed to gateway.events")

	// Start background processing
	go s.backgroundProcessor()

	// Start baseline calculation
	go s.baselineCalculator()

	log.Println("Analysis service started successfully")
	return nil
}

func (s *AnalysisService) Stop() {
	s.cancel()

	if s.redis != nil {
		s.redis.Close()
	}
	if s.db != nil {
		s.db.Close()
	}
}

func (s *AnalysisService) addToWindow(clientID string, timestamp time.Time) {
	s.windowMutex.Lock()
	defer s.windowMutex.Unlock()

	window, exists := s.clientWindows[clientID]
	if !exists {
		window = &SlidingWindow{
			requests:   make([]time.Time, 0),
			windowSize: time.Duration(s.config.WindowMinutes) * time.Minute,
		}
		s.clientWindows[clientID] = window
	}

	window.mutex.Lock()
	defer window.mutex.Unlock()

	// Add new request
	window.requests = append(window.requests, timestamp)
	window.lastUpdate = time.Now()

	// Remove old requests outside the window
	cutoff := timestamp.Add(-window.windowSize)
	validIndex := 0
	for i, req := range window.requests {
		if req.After(cutoff) {
			validIndex = i
			break
		}
	}
	window.requests = window.requests[validIndex:]
}

func (s *AnalysisService) shouldCheckAnomaly(clientID string) bool {
	s.windowMutex.RLock()
	window, exists := s.clientWindows[clientID]
	s.windowMutex.RUnlock()

	if !exists {
		return false
	}

	window.mutex.RLock()
	defer window.mutex.RUnlock()

	// Check every minute or when we have significant activity
	return len(window.requests) > 10 &&
		time.Since(window.lastUpdate) < time.Minute
}

func (s *AnalysisService) checkAnomaly(clientID string) {
	// Get current request rate (requests per minute)
	currentRate := s.getCurrentRate(clientID)
	if currentRate == 0 {
		return
	}

	// Get baseline statistics
	baseline, err := s.getBaseline(clientID)
	if err != nil || baseline.SampleCount < 10 {
		// Not enough data for anomaly detection
		return
	}

	// Calculate Z-score
	if baseline.StdDeviation == 0 {
		return // Cannot detect anomalies with zero deviation
	}

	zScore := (currentRate - baseline.AvgRequestsPerMin) / baseline.StdDeviation

	// Check if anomalous (3-sigma rule)
	threshold := float64(s.config.AnomalyThresholdSigma)
	if math.Abs(zScore) > threshold {
		log.Printf("Anomaly detected for client %s: rate=%.2f, baseline=%.2f±%.2f, z-score=%.2f",
			clientID, currentRate, baseline.AvgRequestsPerMin, baseline.StdDeviation, zScore)

		s.flagAsAnomalous(clientID)
	}
}

func (s *AnalysisService) getCurrentRate(clientID string) float64 {
	s.windowMutex.RLock()
	window, exists := s.clientWindows[clientID]
	s.windowMutex.RUnlock()

	if !exists {
		return 0
	}

	window.mutex.RLock()
	defer window.mutex.RUnlock()

	// Count requests in the last minute
	now := time.Now()
	cutoff := now.Add(-time.Minute)

	count := 0
	for _, req := range window.requests {
		if req.After(cutoff) {
			count++
		}
	}

	return float64(count)
}

func (s *AnalysisService) getBaseline(clientID string) (*ClientBaseline, error) {
	query := `
		SELECT client_id, avg_requests_per_minute, std_deviation, sample_count, last_updated
		FROM client_baselines 
		WHERE client_id = $1
	`

	var baseline ClientBaseline
	err := s.db.QueryRow(query, clientID).Scan(
		&baseline.ClientID,
		&baseline.AvgRequestsPerMin,
		&baseline.StdDeviation,
		&baseline.SampleCount,
		&baseline.LastUpdated,
	)

	if err != nil {
		return nil, err
	}

	return &baseline, nil
}

func (s *AnalysisService) flagAsAnomalous(clientID string) {
	key := fmt.Sprintf("anomaly:%s", clientID)
	ttl := time.Duration(s.config.AnomalyTTLMinutes) * time.Minute

	err := s.redis.Set(s.ctx, key, "true", ttl).Err()
	if err != nil {
		log.Printf("Failed to flag client as anomalous: %v", err)
	}
}

func (s *AnalysisService) backgroundProcessor() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.cleanupWindows()
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *AnalysisService) cleanupWindows() {
	s.windowMutex.Lock()
	defer s.windowMutex.Unlock()

	now := time.Now()
	for clientID, window := range s.clientWindows {
		window.mutex.Lock()

		// Remove windows that haven't been updated in a while
		if now.Sub(window.lastUpdate) > 10*time.Minute {
			delete(s.clientWindows, clientID)
		} else {
			// Clean up old requests
			cutoff := now.Add(-window.windowSize)
			validIndex := 0
			for i, req := range window.requests {
				if req.After(cutoff) {
					validIndex = i
					break
				}
			}
			window.requests = window.requests[validIndex:]
		}

		window.mutex.Unlock()
	}
}

func (s *AnalysisService) baselineCalculator() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.calculateBaselines()
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *AnalysisService) calculateBaselines() {
	log.Println("Calculating client baselines...")

	s.windowMutex.RLock()
	clientIDs := make([]string, 0, len(s.clientWindows))
	for clientID := range s.clientWindows {
		clientIDs = append(clientIDs, clientID)
	}
	s.windowMutex.RUnlock()

	for _, clientID := range clientIDs {
		baseline := s.calculateClientBaseline(clientID)
		if baseline != nil {
			s.saveBaseline(baseline)
		}
	}

	log.Printf("Updated baselines for %d clients", len(clientIDs))
}

func (s *AnalysisService) calculateClientBaseline(clientID string) *ClientBaseline {
	s.windowMutex.RLock()
	window, exists := s.clientWindows[clientID]
	s.windowMutex.RUnlock()

	if !exists {
		return nil
	}

	window.mutex.RLock()
	defer window.mutex.RUnlock()

	if len(window.requests) < 10 {
		return nil // Not enough data
	}

	// Calculate requests per minute over the window
	windowStart := time.Now().Add(-window.windowSize)
	minuteRates := make([]float64, 0)

	// Group requests by minute
	minuteCounts := make(map[int64]int)
	for _, req := range window.requests {
		if req.After(windowStart) {
			minute := req.Unix() / 60
			minuteCounts[minute]++
		}
	}

	// Convert to slice of rates
	for _, count := range minuteCounts {
		minuteRates = append(minuteRates, float64(count))
	}

	if len(minuteRates) < 5 {
		return nil // Need at least 5 minutes of data
	}

	// Calculate mean
	sum := 0.0
	for _, rate := range minuteRates {
		sum += rate
	}
	mean := sum / float64(len(minuteRates))

	// Calculate standard deviation
	variance := 0.0
	for _, rate := range minuteRates {
		variance += math.Pow(rate-mean, 2)
	}
	variance /= float64(len(minuteRates))
	stdDev := math.Sqrt(variance)

	return &ClientBaseline{
		ClientID:          clientID,
		AvgRequestsPerMin: mean,
		StdDeviation:      stdDev,
		SampleCount:       len(minuteRates),
		LastUpdated:       time.Now(),
	}
}

func (s *AnalysisService) HandleAPICallEvent(ctx context.Context, event messaging.APICallEvent) error {
	// Add to sliding window
	s.addToWindow(event.ClientID, event.Timestamp)

	// Check for immediate anomalies
	if s.shouldCheckAnomaly(event.ClientID) {
		go s.checkAnomaly(event.ClientID)
	}

	return nil
}
