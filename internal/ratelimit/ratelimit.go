package ratelimit

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
)

type RateLimiter struct {
	redis        *redis.Client
	db           *sql.DB
	configCache  map[string]*ClientConfig
	cacheMutex   sync.RWMutex
	lastUpdate   time.Time
	anomalyLimit int // Strict limit for anomalous clients
}

type ClientConfig struct {
	ClientID          string    `json:"client_id"`
	RequestsPerMinute int       `json:"requests_per_minute"`
	BurstLimit        int       `json:"burst_limit"`
	LastUpdated       time.Time `json:"last_updated"`
}

func NewRateLimiter(redisClient *redis.Client, dbURL string) *RateLimiter {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	if err := db.Ping(); err != nil {
		log.Fatalf("Database ping failed: %v", err)
	}

	rl := &RateLimiter{
		redis:        redisClient,
		db:           db,
		configCache:  make(map[string]*ClientConfig),
		anomalyLimit: 10, // Very restrictive limit for anomalous clients
	}

	// Load initial configuration
	rl.refreshConfiguration()

	// Start background configuration refresh
	go rl.startConfigurationRefresh()

	return rl
}

func (rl *RateLimiter) Allow(ctx context.Context, clientID string, isAnomalous bool) (bool, string) {
	// Get client configuration
	config := rl.getClientConfig(clientID)
	if config == nil {
		// Default configuration for unknown clients
		config = &ClientConfig{
			ClientID:          clientID,
			RequestsPerMinute: 100,
			BurstLimit:        10,
		}
	}

	// Use stricter limits for anomalous clients
	limit := config.RequestsPerMinute
	limitType := "standard"

	if isAnomalous {
		limit = rl.anomalyLimit
		limitType = "anomaly"
	}

	// Token bucket algorithm using Redis
	return rl.tokenBucketCheck(ctx, clientID, limit, config.BurstLimit, limitType)
}

func (rl *RateLimiter) tokenBucketCheck(ctx context.Context, clientID string, limit, burst int, limitType string) (bool, string) {
	key := fmt.Sprintf("rate_limit:%s:%s", limitType, clientID)
	now := time.Now().Unix()

	// Lua script for atomic token bucket check
	luaScript := `
		local key = KEYS[1]
		local limit = tonumber(ARGV[1])
		local burst = tonumber(ARGV[2])
		local now = tonumber(ARGV[3])
		local window = 60  -- 1 minute window

		-- Get current token count and last refill time
		local current = redis.call('HMGET', key, 'tokens', 'last_refill')
		local tokens = tonumber(current[1]) or burst
		local last_refill = tonumber(current[2]) or now

		-- Calculate tokens to add based on time elapsed
		local elapsed = now - last_refill
		local tokens_to_add = math.floor((elapsed / window) * limit)
		
		-- Refill tokens, capped at burst limit
		tokens = math.min(burst, tokens + tokens_to_add)

		-- Check if request can be allowed
		if tokens >= 1 then
			tokens = tokens - 1
			redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
			redis.call('EXPIRE', key, 300)  -- 5 minute TTL
			return 1
		else
			redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
			redis.call('EXPIRE', key, 300)
			return 0
		end
	`

	result, err := rl.redis.Eval(ctx, luaScript, []string{key}, limit, burst, now).Result()
	if err != nil {
		log.Printf("Rate limiting Redis error: %v", err)
		return true, limitType // Allow on Redis error (fail open)
	}

	allowed := result.(int64) == 1
	return allowed, limitType
}

func (rl *RateLimiter) getClientConfig(clientID string) *ClientConfig {
	rl.cacheMutex.RLock()
	config, exists := rl.configCache[clientID]
	rl.cacheMutex.RUnlock()

	if exists {
		return config
	}

	// Try to load from database
	return rl.loadClientConfigFromDB(clientID)
}

func (rl *RateLimiter) loadClientConfigFromDB(clientID string) *ClientConfig {
	query := `
		SELECT client_id, requests_per_minute, burst_limit, updated_at 
		FROM rate_limits 
		WHERE client_id = $1
	`

	var config ClientConfig
	err := rl.db.QueryRow(query, clientID).Scan(
		&config.ClientID,
		&config.RequestsPerMinute,
		&config.BurstLimit,
		&config.LastUpdated,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil // No configuration found
		}
		log.Printf("Failed to load client config: %v", err)
		return nil
	}

	// Cache the configuration
	rl.cacheMutex.Lock()
	rl.configCache[clientID] = &config
	rl.cacheMutex.Unlock()

	return &config
}

func (rl *RateLimiter) refreshConfiguration() {
	query := `
		SELECT client_id, requests_per_minute, burst_limit, updated_at 
		FROM rate_limits 
		WHERE updated_at > $1
	`

	rows, err := rl.db.Query(query, rl.lastUpdate)
	if err != nil {
		log.Printf("Failed to refresh configuration: %v", err)
		return
	}
	defer rows.Close()

	rl.cacheMutex.Lock()
	defer rl.cacheMutex.Unlock()

	updated := 0
	maxUpdateTime := rl.lastUpdate

	for rows.Next() {
		var config ClientConfig
		err := rows.Scan(
			&config.ClientID,
			&config.RequestsPerMinute,
			&config.BurstLimit,
			&config.LastUpdated,
		)
		if err != nil {
			log.Printf("Failed to scan configuration row: %v", err)
			continue
		}

		rl.configCache[config.ClientID] = &config
		updated++

		if config.LastUpdated.After(maxUpdateTime) {
			maxUpdateTime = config.LastUpdated
		}
	}

	rl.lastUpdate = maxUpdateTime

	if updated > 0 {
		log.Printf("Refreshed %d client configurations", updated)
	}
}

func (rl *RateLimiter) startConfigurationRefresh() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		rl.refreshConfiguration()
	}
}

func (rl *RateLimiter) GetClientStats(ctx context.Context, clientID string) (map[string]interface{}, error) {
	standardKey := fmt.Sprintf("rate_limit:standard:%s", clientID)
	anomalyKey := fmt.Sprintf("rate_limit:anomaly:%s", clientID)

	pipe := rl.redis.Pipeline()
	standardTokens := pipe.HGet(ctx, standardKey, "tokens")
	anomalyTokens := pipe.HGet(ctx, anomalyKey, "tokens")
	_, err := pipe.Exec(ctx)

	stats := map[string]interface{}{
		"client_id": clientID,
		"timestamp": time.Now(),
	}

	if err == nil {
		if tokens, err := standardTokens.Result(); err == nil {
			if t, err := strconv.Atoi(tokens); err == nil {
				stats["standard_tokens_remaining"] = t
			}
		}

		if tokens, err := anomalyTokens.Result(); err == nil {
			if t, err := strconv.Atoi(tokens); err == nil {
				stats["anomaly_tokens_remaining"] = t
			}
		}
	}

	// Get configuration
	config := rl.getClientConfig(clientID)
	if config != nil {
		stats["configured_limit"] = config.RequestsPerMinute
		stats["burst_limit"] = config.BurstLimit
	}

	// Check if client is currently flagged as anomalous
	anomalyFlag, _ := rl.redis.Get(ctx, fmt.Sprintf("anomaly:%s", clientID)).Result()
	stats["is_anomalous"] = anomalyFlag == "true"

	return stats, nil
}

func (rl *RateLimiter) Close() {
	if rl.db != nil {
		rl.db.Close()
	}
}
