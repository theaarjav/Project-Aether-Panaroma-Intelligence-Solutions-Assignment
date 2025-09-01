package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"project-aether/cmd/config/db"
	"project-aether/internal/config"
	"project-aether/internal/logger"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
)

type ConfigService struct {
	db          *sql.DB
	redis       *redis.Client
	mongoLogger *logger.MongoLogger
	cfg         *config.ConfigServiceConfig
}

type RateLimitConfig struct {
	ClientID          string `json:"client_id" binding:"required"`
	RequestsPerMinute int    `json:"requests_per_minute" binding:"required,min=1"`
	BurstLimit        int    `json:"burst_limit,omitempty"`
}

type ClientStats struct {
	ClientID          string    `json:"client_id"`
	RequestsPerMinute int       `json:"requests_per_minute"`
	BurstLimit        int       `json:"burst_limit"`
	StandardTokens    int       `json:"standard_tokens_remaining"`
	AnomalyTokens     int       `json:"anomaly_tokens_remaining"`
	IsAnomalous       bool      `json:"is_anomalous"`
	BaselineAvg       float64   `json:"baseline_avg,omitempty"`
	BaselineStdDev    float64   `json:"baseline_std_dev,omitempty"`
	LastUpdated       time.Time `json:"last_updated"`
}

func main() {
	cfg := config.LoadConfigServiceConfig()

	service, err := NewConfigService(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize config service: %v", err)
	}

	// Setup router
	if cfg.Environment == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())

	// Health check
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":    "healthy",
			"timestamp": time.Now().UTC(),
			"service":   "aether-config",
		})
	})

	// Configuration management endpoints
	api := r.Group("/config")
	{
		api.POST("/rate-limits", service.CreateRateLimit)
		api.GET("/rate-limits/:client_id", service.GetRateLimit)
		api.PUT("/rate-limits/:client_id", service.UpdateRateLimit)
		api.DELETE("/rate-limits/:client_id", service.DeleteRateLimit)
		api.GET("/rate-limits", service.ListRateLimits)
	}

	// Monitoring and stats endpoints
	stats := r.Group("/stats")
	{
		stats.GET("/client/:client_id", service.GetClientStats)
		stats.GET("/client/:client_id/logs", service.GetClientLogs)
		stats.GET("/client/:client_id/metrics", service.GetClientMetrics)
		stats.GET("/client/:client_id/analytics", service.GetClientAnalytics)
		stats.GET("/anomalies", service.GetAnomalousClients)
		stats.GET("/overview", service.GetSystemOverview)
	}

	// Start server
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%s", cfg.Port),
		Handler: r,
	}

	go func() {
		log.Printf("Starting Config Service on port %s", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down Config Service...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	service.Close()
	log.Println("Config Service stopped")
}

func NewConfigService(cfg *config.ConfigServiceConfig) (*ConfigService, error) {
	// Connect to database
	db := db.GetPostgresDB(cfg.DBURL, "./migrations")

	// Connect to Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr: cfg.RedisURL[8:], // Remove redis:// prefix
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Connect to MongoDB for analytics
	mongoLogger, err := logger.NewMongoLogger(cfg.MongoURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	return &ConfigService{
		db:          db,
		redis:       redisClient,
		mongoLogger: mongoLogger,
		cfg:         cfg,
	}, nil
}

func (cs *ConfigService) CreateRateLimit(c *gin.Context) {
	var config RateLimitConfig
	if err := c.ShouldBindJSON(&config); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Set default burst limit if not provided
	if config.BurstLimit == 0 {
		config.BurstLimit = max(10, config.RequestsPerMinute/6) // Default to 1/6 of per-minute limit
	}

	query := `
		INSERT INTO rate_limits (client_id, requests_per_minute, burst_limit)
		VALUES ($1, $2, $3)
		ON CONFLICT (client_id) 
		DO UPDATE SET 
			requests_per_minute = EXCLUDED.requests_per_minute,
			burst_limit = EXCLUDED.burst_limit,
			updated_at = NOW()
		RETURNING client_id, requests_per_minute, burst_limit, created_at, updated_at
	`

	var result struct {
		ClientID          string    `json:"client_id"`
		RequestsPerMinute int       `json:"requests_per_minute"`
		BurstLimit        int       `json:"burst_limit"`
		CreatedAt         time.Time `json:"created_at"`
		UpdatedAt         time.Time `json:"updated_at"`
	}

	err := cs.db.QueryRow(query, config.ClientID, config.RequestsPerMinute, config.BurstLimit).Scan(
		&result.ClientID,
		&result.RequestsPerMinute,
		&result.BurstLimit,
		&result.CreatedAt,
		&result.UpdatedAt,
	)

	if err != nil {
		log.Printf("Failed to create rate limit: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create rate limit"})
		return
	}

	// Clear any cached data for this client
	cs.invalidateClientCache(config.ClientID)

	c.JSON(http.StatusCreated, result)
}

func (cs *ConfigService) GetRateLimit(c *gin.Context) {
	clientID := c.Param("client_id")

	query := `
		SELECT client_id, requests_per_minute, burst_limit, created_at, updated_at
		FROM rate_limits 
		WHERE client_id = $1
	`

	var result struct {
		ClientID          string    `json:"client_id"`
		RequestsPerMinute int       `json:"requests_per_minute"`
		BurstLimit        int       `json:"burst_limit"`
		CreatedAt         time.Time `json:"created_at"`
		UpdatedAt         time.Time `json:"updated_at"`
	}

	err := cs.db.QueryRow(query, clientID).Scan(
		&result.ClientID,
		&result.RequestsPerMinute,
		&result.BurstLimit,
		&result.CreatedAt,
		&result.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "Rate limit not found"})
			return
		}
		log.Printf("Failed to get rate limit: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get rate limit"})
		return
	}

	c.JSON(http.StatusOK, result)
}

func (cs *ConfigService) UpdateRateLimit(c *gin.Context) {
	clientID := c.Param("client_id")

	var config RateLimitConfig
	if err := c.ShouldBindJSON(&config); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Ensure client_id matches the URL parameter
	config.ClientID = clientID

	// Set default burst limit if not provided
	if config.BurstLimit == 0 {
		config.BurstLimit = max(10, config.RequestsPerMinute/6)
	}

	query := `
		UPDATE rate_limits 
		SET requests_per_minute = $2, burst_limit = $3, updated_at = NOW()
		WHERE client_id = $1
		RETURNING client_id, requests_per_minute, burst_limit, created_at, updated_at
	`

	var result struct {
		ClientID          string    `json:"client_id"`
		RequestsPerMinute int       `json:"requests_per_minute"`
		BurstLimit        int       `json:"burst_limit"`
		CreatedAt         time.Time `json:"created_at"`
		UpdatedAt         time.Time `json:"updated_at"`
	}

	err := cs.db.QueryRow(query, config.ClientID, config.RequestsPerMinute, config.BurstLimit).Scan(
		&result.ClientID,
		&result.RequestsPerMinute,
		&result.BurstLimit,
		&result.CreatedAt,
		&result.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "Rate limit not found"})
			return
		}
		log.Printf("Failed to update rate limit: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update rate limit"})
		return
	}

	// Clear cached data for this client
	cs.invalidateClientCache(config.ClientID)

	c.JSON(http.StatusOK, result)
}

func (cs *ConfigService) DeleteRateLimit(c *gin.Context) {
	clientID := c.Param("client_id")

	query := `DELETE FROM rate_limits WHERE client_id = $1`
	result, err := cs.db.Exec(query, clientID)
	if err != nil {
		log.Printf("Failed to delete rate limit: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete rate limit"})
		return
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "Rate limit not found"})
		return
	}

	// Clear cached data for this client
	cs.invalidateClientCache(clientID)

	c.JSON(http.StatusOK, gin.H{"message": "Rate limit deleted successfully"})
}

func (cs *ConfigService) ListRateLimits(c *gin.Context) {
	// Parse query parameters
	limitStr := c.DefaultQuery("limit", "100")
	offsetStr := c.DefaultQuery("offset", "0")

	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit < 1 || limit > 1000 {
		limit = 100
	}

	offset, err := strconv.Atoi(offsetStr)
	if err != nil || offset < 0 {
		offset = 0
	}

	query := `
		SELECT client_id, requests_per_minute, burst_limit, created_at, updated_at
		FROM rate_limits 
		ORDER BY updated_at DESC
		LIMIT $1 OFFSET $2
	`

	rows, err := cs.db.Query(query, limit, offset)
	if err != nil {
		log.Printf("Failed to list rate limits: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to list rate limits"})
		return
	}
	defer rows.Close()

	var results []struct {
		ClientID          string    `json:"client_id"`
		RequestsPerMinute int       `json:"requests_per_minute"`
		BurstLimit        int       `json:"burst_limit"`
		CreatedAt         time.Time `json:"created_at"`
		UpdatedAt         time.Time `json:"updated_at"`
	}

	for rows.Next() {
		var item struct {
			ClientID          string    `json:"client_id"`
			RequestsPerMinute int       `json:"requests_per_minute"`
			BurstLimit        int       `json:"burst_limit"`
			CreatedAt         time.Time `json:"created_at"`
			UpdatedAt         time.Time `json:"updated_at"`
		}

		err := rows.Scan(
			&item.ClientID,
			&item.RequestsPerMinute,
			&item.BurstLimit,
			&item.CreatedAt,
			&item.UpdatedAt,
		)
		if err != nil {
			log.Printf("Failed to scan rate limit row: %v", err)
			continue
		}

		results = append(results, item)
	}

	// Get total count
	countQuery := `SELECT COUNT(*) FROM rate_limits`
	var total int
	cs.db.QueryRow(countQuery).Scan(&total)

	c.JSON(http.StatusOK, gin.H{
		"data":   results,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

func (cs *ConfigService) GetClientStats(c *gin.Context) {
	clientID := c.Param("client_id")
	ctx := context.Background()

	// Get rate limit configuration
	var stats ClientStats
	query := `
		SELECT client_id, requests_per_minute, burst_limit, updated_at
		FROM rate_limits 
		WHERE client_id = $1
	`

	err := cs.db.QueryRow(query, clientID).Scan(
		&stats.ClientID,
		&stats.RequestsPerMinute,
		&stats.BurstLimit,
		&stats.LastUpdated,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			c.JSON(http.StatusNotFound, gin.H{"error": "Client not found"})
			return
		}
		log.Printf("Failed to get client config: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get client stats"})
		return
	}

	// Get current token counts
	pipe := cs.redis.Pipeline()
	standardTokens := pipe.HGet(ctx, fmt.Sprintf("rate_limit:standard:%s", clientID), "tokens")
	anomalyTokens := pipe.HGet(ctx, fmt.Sprintf("rate_limit:anomaly:%s", clientID), "tokens")
	anomalyFlag := pipe.Get(ctx, fmt.Sprintf("anomaly:%s", clientID))
	_, err = pipe.Exec(ctx)

	if err == nil {
		if tokens, err := standardTokens.Result(); err == nil {
			if t, err := strconv.Atoi(tokens); err == nil {
				stats.StandardTokens = t
			}
		}

		if tokens, err := anomalyTokens.Result(); err == nil {
			if t, err := strconv.Atoi(tokens); err == nil {
				stats.AnomalyTokens = t
			}
		}

		if flag, err := anomalyFlag.Result(); err == nil {
			stats.IsAnomalous = flag == "true"
		}
	}

	// Get baseline from Redis cache
	baselineKey := fmt.Sprintf("baseline:%s", clientID)
	if _, err := cs.redis.Get(ctx, baselineKey).Result(); err == nil {
		// Parse baseline data if available
		// This would require importing the baseline struct or creating a simple parser
		stats.BaselineAvg = 0    // Placeholder - would parse from Redis
		stats.BaselineStdDev = 0 // Placeholder - would parse from Redis
	}

	c.JSON(http.StatusOK, stats)
}

func (cs *ConfigService) GetClientLogs(c *gin.Context) {
	clientID := c.Param("client_id")

	// Parse query parameters
	limitStr := c.DefaultQuery("limit", "100")
	sinceStr := c.DefaultQuery("since", "24h")

	limit, err := strconv.ParseInt(limitStr, 10, 64)
	if err != nil || limit < 1 || limit > 1000 {
		limit = 100
	}

	// Parse since parameter
	var since time.Time
	if duration, err := time.ParseDuration(sinceStr); err == nil {
		since = time.Now().Add(-duration)
	} else {
		since = time.Now().Add(-24 * time.Hour)
	}

	// Get logs from MongoDB
	logs, err := cs.mongoLogger.GetClientLogs(c.Request.Context(), clientID, since, limit)
	if err != nil {
		log.Printf("Failed to get client logs: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get client logs"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"logs":      logs,
		"count":     len(logs),
		"since":     since,
		"limit":     limit,
		"client_id": clientID,
	})
}

func (cs *ConfigService) GetClientMetrics(c *gin.Context) {
	clientID := c.Param("client_id")
	sinceStr := c.DefaultQuery("since", "24h")

	// Parse since parameter
	var since time.Time
	if duration, err := time.ParseDuration(sinceStr); err == nil {
		since = time.Now().Add(-duration)
	} else {
		since = time.Now().Add(-24 * time.Hour)
	}

	// Get aggregated metrics from MongoDB using simple Go calculations
	metrics, err := cs.mongoLogger.GetAggregatedMetrics(c.Request.Context(), clientID, since)
	if err != nil {
		log.Printf("Failed to get client metrics: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get client metrics"})
		return
	}

	c.JSON(http.StatusOK, metrics)
}

func (cs *ConfigService) GetClientAnalytics(c *gin.Context) {
	clientID := c.Param("client_id")
	sinceStr := c.DefaultQuery("since", "24h")

	var since time.Time
	if duration, err := time.ParseDuration(sinceStr); err == nil {
		since = time.Now().Add(-duration)
	} else {
		since = time.Now().Add(-24 * time.Hour)
	}

	// Use analytics engine for comprehensive analysis
	// This would require integrating the analytics engine into the config service
	// For now, return the basic metrics

	c.JSON(http.StatusOK, gin.H{
		"message":   "Comprehensive analytics available via analytics engine",
		"client_id": clientID,
		"since":     since,
	})
}

func (cs *ConfigService) GetAnomalousClients(c *gin.Context) {
	ctx := context.Background()

	// Get all anomaly keys from Redis
	keys, err := cs.redis.Keys(ctx, "anomaly:*").Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to get anomalous clients"})
		return
	}

	var anomalousClients []map[string]interface{}

	for _, key := range keys {
		clientID := key[8:] // Remove "anomaly:" prefix
		ttl, _ := cs.redis.TTL(ctx, key).Result()

		anomalousClients = append(anomalousClients, map[string]interface{}{
			"client_id":   clientID,
			"ttl_seconds": int64(ttl.Seconds()),
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"anomalous_clients": anomalousClients,
		"count":             len(anomalousClients),
	})
}

func (cs *ConfigService) GetSystemOverview(c *gin.Context) {
	ctx := context.Background()

	// Get total clients with rate limits
	var totalClients int
	cs.db.QueryRow("SELECT COUNT(*) FROM rate_limits").Scan(&totalClients)

	// Get total clients with baselines
	var clientsWithBaselines int
	cs.db.QueryRow("SELECT COUNT(*) FROM client_baselines").Scan(&clientsWithBaselines)

	// Get anomalous clients count
	anomalyKeys, _ := cs.redis.Keys(ctx, "anomaly:*").Result()
	anomalousCount := len(anomalyKeys)

	// Get active rate limit keys
	standardKeys, _ := cs.redis.Keys(ctx, "rate_limit:standard:*").Result()
	anomalyLimitKeys, _ := cs.redis.Keys(ctx, "rate_limit:anomaly:*").Result()

	overview := map[string]interface{}{
		"total_clients":          totalClients,
		"clients_with_baselines": clientsWithBaselines,
		"anomalous_clients":      anomalousCount,
		"active_standard_limits": len(standardKeys),
		"active_anomaly_limits":  len(anomalyLimitKeys),
		"timestamp":              time.Now(),
	}

	c.JSON(http.StatusOK, overview)
}

func (cs *ConfigService) invalidateClientCache(clientID string) {
	ctx := context.Background()

	// Remove rate limiting state for this client
	keys := []string{
		fmt.Sprintf("rate_limit:standard:%s", clientID),
		fmt.Sprintf("rate_limit:anomaly:%s", clientID),
	}

	for _, key := range keys {
		cs.redis.Del(ctx, key)
	}
}

func (cs *ConfigService) Close() {
	if cs.db != nil {
		cs.db.Close()
	}
	if cs.redis != nil {
		cs.redis.Close()
	}
	if cs.mongoLogger != nil {
		cs.mongoLogger.Close()
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
