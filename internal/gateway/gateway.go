package gateway

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"project-aether/internal/config"
	"project-aether/internal/logger"
	"project-aether/internal/messaging"
	"project-aether/internal/ratelimit"
)

type Gateway struct {
	config        *config.GatewayConfig
	rateLimiter   *ratelimit.RateLimiter
	mongoLogger   *logger.MongoLogger
	upstreams     []*httputil.ReverseProxy
	upstreamURLs  []*url.URL
	redis         *redis.Client
	kafkaProducer messaging.KafkaProducer

	// Metrics
	requestsTotal     *prometheus.CounterVec
	requestDuration   *prometheus.HistogramVec
	rateLimitedTotal  *prometheus.CounterVec
	anomalousRequests *prometheus.CounterVec

	// Circuit breaker state
	circuitState map[string]*CircuitBreaker
	circuitMu    sync.RWMutex
}

type CircuitBreaker struct {
	failures    int
	lastFailure time.Time
	state       string // "closed", "open", "half-open"
	threshold   int
	timeout     time.Duration
}

type TelemetryEvent struct {
	ClientID          string    `json:"client_id"`
	Timestamp         time.Time `json:"timestamp"`
	Path              string    `json:"path"`
	Method            string    `json:"method"`
	StatusCode        int       `json:"status_code"`
	ResponseLatencyMs int64     `json:"response_latency_ms"`
	IsThrottled       bool      `json:"is_throttled"`
	IsAnomalous       bool      `json:"is_anomalous"`
	UpstreamService   string    `json:"upstream_service"`
}

func NewGateway(cfg *config.GatewayConfig) (*Gateway, error) {
	// Initialize Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: cfg.RedisURL[8:], // Remove redis:// prefix
	})

	// Test Redis connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Initialize MongoDB logger
	mongoLogger, err := logger.NewMongoLogger(cfg.MongoURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	// Parse upstream URLs
	upstreamURLs := make([]*url.URL, 0)
	upstreams := make([]*httputil.ReverseProxy, 0)

	for _, urlStr := range strings.Split(cfg.UpstreamURLs, ",") {
		u, err := url.Parse(strings.TrimSpace(urlStr))
		if err != nil {
			return nil, fmt.Errorf("invalid upstream URL %s: %w", urlStr, err)
		}
		upstreamURLs = append(upstreamURLs, u)
		upstreams = append(upstreams, httputil.NewSingleHostReverseProxy(u))
	}

	// Initialize rate limiter
	rateLimiter := ratelimit.NewRateLimiter(redisClient, cfg.DBURL)

	// Initialize metrics
	requestsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_requests_total",
			Help: "Total number of requests processed by the gateway",
		},
		[]string{"client_id", "method", "path", "status_code"},
	)

	requestDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "gateway_request_duration_seconds",
			Help:    "Request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"client_id", "method", "path"},
	)

	rateLimitedTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_rate_limited_requests_total",
			Help: "Total number of rate-limited requests",
		},
		[]string{"client_id", "limit_type"},
	)

	anomalousRequests := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gateway_anomalous_requests_total",
			Help: "Total number of requests from anomalous clients",
		},
		[]string{"client_id"},
	)

	// Register metrics
	prometheus.MustRegister(requestsTotal, requestDuration, rateLimitedTotal, anomalousRequests)

	return &Gateway{
		config:            cfg,
		rateLimiter:       rateLimiter,
		mongoLogger:       mongoLogger,
		upstreams:         upstreams,
		upstreamURLs:      upstreamURLs,
		redis:             redisClient,
		requestsTotal:     requestsTotal,
		requestDuration:   requestDuration,
		rateLimitedTotal:  rateLimitedTotal,
		anomalousRequests: anomalousRequests,
		circuitState:      make(map[string]*CircuitBreaker),
	}, nil
}

func (g *Gateway) ProxyHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		startTime := time.Now()

		// Extract client ID from header
		clientID := c.GetHeader("X-Client-ID")
		if clientID == "" {
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": "Missing X-Client-ID header",
			})
			return
		}

		// Check if client is anomalous
		isAnomalous := g.isClientAnomalous(c.Request.Context(), clientID)
		if isAnomalous {
			g.anomalousRequests.WithLabelValues(clientID).Inc()
		}

		// Apply rate limiting
		allowed, limitType := g.rateLimiter.Allow(c.Request.Context(), clientID, isAnomalous)
		if !allowed {
			g.rateLimitedTotal.WithLabelValues(clientID, limitType).Inc()

			// Publish telemetry for rate-limited request
			g.publishTelemetryAsync(messaging.APICallEvent{
				ClientID:          clientID,
				Timestamp:         startTime,
				Path:              c.Request.URL.Path,
				Method:            c.Request.Method,
				StatusCode:        http.StatusTooManyRequests,
				ResponseLatencyMs: time.Since(startTime).Milliseconds(),
				IsThrottled:       true,
				IsAnomalous:       isAnomalous,
			})

			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":     "Rate limit exceeded",
				"type":      limitType,
				"client_id": clientID,
			})
			return
		}

		// Select upstream server (simple round-robin)
		upstream := g.selectUpstream(clientID)
		upstreamURL := g.upstreamURLs[upstream]

		// Check circuit breaker
		if !g.isCircuitClosed(upstreamURL.String()) {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": "Upstream service unavailable",
			})
			return
		}

		// Modify request for upstream
		c.Request.Host = upstreamURL.Host
		c.Request.URL.Host = upstreamURL.Host
		c.Request.URL.Scheme = upstreamURL.Scheme

		// Custom response writer to capture status code
		statusRecorder := &statusResponseWriter{
			ResponseWriter: c.Writer,
			statusCode:     http.StatusOK,
		}
		c.Writer = statusRecorder

		// Forward request to upstream
		g.upstreams[upstream].ServeHTTP(c.Writer, c.Request)

		// Calculate response time
		duration := time.Since(startTime)

		// Update circuit breaker based on response
		g.updateCircuitBreaker(upstreamURL.String(), statusRecorder.statusCode)

		// Record metrics
		g.requestsTotal.WithLabelValues(
			clientID,
			c.Request.Method,
			c.Request.URL.Path,
			strconv.Itoa(statusRecorder.statusCode),
		).Inc()

		g.requestDuration.WithLabelValues(
			clientID,
			c.Request.Method,
			c.Request.URL.Path,
		).Observe(duration.Seconds())

		// Publish telemetry asynchronously
		g.publishTelemetryAsync(messaging.APICallEvent{
			ClientID:          clientID,
			Timestamp:         startTime,
			Path:              c.Request.URL.Path,
			Method:            c.Request.Method,
			StatusCode:        statusRecorder.statusCode,
			ResponseLatencyMs: duration.Milliseconds(),
			IsThrottled:       false,
			IsAnomalous:       isAnomalous,
			UpstreamService:   upstreamURL.String(),
		})
	}
}

type statusResponseWriter struct {
	gin.ResponseWriter
	statusCode int
}

func (w *statusResponseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

func (g *Gateway) isClientAnomalous(ctx context.Context, clientID string) bool {
	key := fmt.Sprintf("anomaly:%s", clientID)
	result, err := g.redis.Get(ctx, key).Result()
	if err != nil {
		return false // Default to not anomalous if Redis is unavailable
	}
	return result == "true"
}

func (g *Gateway) selectUpstream(clientID string) int {
	// Simple round-robin based on client ID hash
	hash := 0
	for _, b := range clientID {
		hash = hash*31 + int(b)
	}
	return hash % len(g.upstreams)
}

func (g *Gateway) isCircuitClosed(upstream string) bool {
	g.circuitMu.RLock()
	cb, exists := g.circuitState[upstream]
	g.circuitMu.RUnlock()

	if !exists {
		return true // Default to closed (healthy) state
	}

	switch cb.state {
	case "open":
		// Check if timeout has passed to transition to half-open
		if time.Since(cb.lastFailure) > cb.timeout {
			g.circuitMu.Lock()
			cb.state = "half-open"
			g.circuitMu.Unlock()
			return true
		}
		return false
	case "half-open", "closed":
		return true
	default:
		return true
	}
}

func (g *Gateway) updateCircuitBreaker(upstream string, statusCode int) {
	g.circuitMu.Lock()
	defer g.circuitMu.Unlock()

	cb, exists := g.circuitState[upstream]
	if !exists {
		cb = &CircuitBreaker{
			threshold: 5,
			timeout:   30 * time.Second,
			state:     "closed",
		}
		g.circuitState[upstream] = cb
	}

	if statusCode >= 500 {
		cb.failures++
		cb.lastFailure = time.Now()

		if cb.failures >= cb.threshold {
			cb.state = "open"
			log.Printf("Circuit breaker opened for upstream %s", upstream)
		}
	} else {
		// Reset on successful request
		if cb.state == "half-open" {
			cb.state = "closed"
			cb.failures = 0
		} else if cb.failures > 0 {
			cb.failures = max(0, cb.failures-1)
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (g *Gateway) publishTelemetryAsync(event messaging.APICallEvent) {
	ctx := context.Background()
	go func() {
		// Publish to NATS for real-time analysis
		if err := g.kafkaProducer.PublishAPICall(ctx, event); err != nil {
			log.Printf("Failed to publish telemetry: %v", err)
		}

		// Log to MongoDB for persistent storage and time series analysis
		mongoLog := logger.RequestLog{
			ClientID:          event.ClientID,
			Timestamp:         event.Timestamp,
			Path:              event.Path,
			Method:            event.Method,
			StatusCode:        event.StatusCode,
			ResponseLatencyMs: event.ResponseLatencyMs,
			IsThrottled:       event.IsThrottled,
			IsAnomalous:       event.IsAnomalous,
			UpstreamService:   event.UpstreamService,
			RequestSize:       0, // Could be enhanced to capture actual size
			ResponseSize:      0, // Could be enhanced to capture actual size
		}

		g.mongoLogger.LogRequestAsync(mongoLog)
	}()
}

func (g *Gateway) MetricsHandler() gin.HandlerFunc {
	h := promhttp.Handler()
	return gin.WrapH(h)
}

func (g *Gateway) Close() {
	if g.redis != nil {
		g.redis.Close()
	}
	if g.mongoLogger != nil {
		g.mongoLogger.Close()
	}
}
