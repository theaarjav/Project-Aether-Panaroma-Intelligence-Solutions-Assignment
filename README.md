# Project Aether: Intelligent & Adaptive API Gateway

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Client Apps   │    │   Admin Panel   │    │  Load Balancer  │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          │ API Requests         │ Config Updates       │
          │                      │                      │
          ▼                      ▼                      ▼
┌─────────────────────────────────────────────────────────────────┐
│                    API Gateway (Proxy Service)                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐ │
│  │ Rate Limiter│  │   Router    │  │    Auth Middleware      │ │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘ │
└─────────┬───────────────────────────────────────────┬─────────┘
          │                                           │
          │ Publishes to Kafka Topic                 │ Anomaly Checks
          ▼                                           ▼
┌─────────────────┐                         ┌─────────────────┐
│   Kafka Cluster │                         │   Redis Cache   │
│ Topic: api-calls│                         │ (Anomaly Flags │
│ - Partitioned   │                         │ & Baselines)   │
│ - Replicated    │                         └─────────────────┘
│ - Persistent    │                                   ▲
└─────────┬───────┘                                   │
          │                                           │
          │ Multiple Consumers                        │
          ├─────────────────┬─────────────────────────┘
          ▼                 ▼
┌─────────────────┐ ┌───────────────────────┐
│ Analysis Service│ │Telemetry Service      │
│ - Anomaly Detect│ │ - MongoDB Store(TSDB) │
│ - Pattern Recog │ │ - Metrics Calc        │
│ - Baseline Calc │ │ - Log Archival        │
└─────────┬───────┘ └───────────────────────┘
          │
          │ Config Queries                ┌─────────────────┐
          ▼                               │   MongoDB TSDB  │
┌─────────────────┐    ┌─────────────────┐│ (Request Logs)  │
│  PostgreSQL DB  │    │ Upstream APIs   ││ - Time Series   │
│(Client Configs) │    │ (Mock Services) ││ - Aggregations  │
└─────────────────┘    └─────────────────┘└─────────────────┘
```

## System Components

### 1. API Gateway (Proxy Service)
- **Language**: Go with Gin framework for high performance
- **Responsibilities**:
  - Reverse proxy with request routing
  - Authentication via X-Client-ID header
  - Rate limiting enforcement
  - Telemetry event publishing
  - Anomaly status checking

### 2. Configuration Service
- **Database**: PostgreSQL for ACID compliance
- **Features**:
  - Client rate limit configuration storage
  - REST API for configuration management
  - Hot-reload capability without service restart

### 3. Analysis Service
- **Pattern Analysis**: Real-time statistical analysis
- **Anomaly Detection**: 3-sigma deviation detection
- **Baseline Calculation**: Rolling window statistics

### 4. Infrastructure Components
- **Message Queue**: Kafka for lightweight, high-performance messaging
- **Cache**: Redis for fast anomaly flag storage with TTL
- **Mock Upstream**: Simple HTTP services for testing

## Design Rationale

### Technology Choices

**Go + Gin Framework**:
- Superior concurrency model with goroutines
- Low memory footprint and high throughput
- Built-in HTTP/2 support
- Fast JSON serialization
- Excellent for building high-performance proxies

**PostgreSQL** (Client Configuration):
- ACID compliance for configuration data integrity
- Complex queries for client management
- Excellent concurrent read performance
- Perfect for structured configuration data

**Redis** (Anomaly Data & Caching):
- Sub-millisecond latency for anomaly flag operations
- Built-in TTL support for temporary anomaly status
- Atomic operations for race condition prevention
- Perfect for ephemeral anomaly data and baseline caching

**MongoDB** (Request Logs & Time Series):
- Optimized for high-volume writes
- Excellent time-series data handling
- Flexible schema for request metadata
- Built-in TTL for automatic log cleanup
- Superior performance for log aggregation queries

**Kafka** (Event Streaming):
- High-throughput, persistent event streaming
- Multiple consumers can process same events independently
- Built-in partitioning for horizontal scaling
- Event replay capability for historical analysis
- Perfect for audit logs and real-time analytics

### Architectural Patterns

**Event-Driven Architecture**:
- Decouples telemetry collection from request processing
- Enables horizontal scaling of analysis components
- Provides resilience through message queue buffering

**Cache-Aside Pattern**:
- Reduces database load for frequently accessed data
- Enables fast anomaly status checks
- Provides fallback to database if cache is unavailable

**Circuit Breaker Pattern**:
- Prevents cascade failures to upstream services
- Implements graceful degradation
- Provides automatic recovery mechanisms

## Key Features Implemented

### 1. High-Performance Proxy
```go
// Concurrent request handling with connection pooling
func (p *ProxyService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    // Non-blocking telemetry publishing
    go p.publishTelemetry(clientID, requestData)
    
    // Fast anomaly status check
    if p.isAnomalous(clientID) {
        // Apply strict rate limiting
    }
    
    // Efficient request forwarding
    p.reverseProxy.ServeHTTP(w, r)
}
```

### 2. Dynamic Configuration
```go
// Hot-reload configuration without restart
func (c *ConfigService) UpdateRateLimit(clientID string, limit int) error {
    // Update database
    // Invalidate cache
    // Broadcast configuration change
}
```

### 3. Real-time Analytics
```go
// Streaming analytics with sliding windows
func (a *AnalysisService) processEvents() {
    for event := range a.eventStream {
        // Calculate rolling statistics
        baseline := a.calculateBaseline(event.ClientID)
        
        // Detect anomalies using 3-sigma rule
        if a.isAnomalous(event, baseline) {
            a.flagAsAnomalous(event.ClientID)
        }
    }
}
```

### 4. Adaptive Throttling
```go
// Multi-tier rate limiting
type RateLimiter struct {
    standardLimits map[string]*TokenBucket
    anomalyLimits  map[string]*TokenBucket
    cache          *redis.Client
}

func (rl *RateLimiter) Allow(clientID string) bool {
    if rl.isAnomalous(clientID) {
        return rl.anomalyLimits[clientID].Allow()
    }
    return rl.standardLimits[clientID].Allow()
}
```

## Performance Optimizations

### 1. Connection Pooling
- HTTP client connection reuse
- Database connection pooling
- Redis connection pooling

### 2. Asynchronous Processing
- Non-blocking telemetry publishing
- Background anomaly detection
- Concurrent request handling

### 3. Efficient Data Structures
- Token bucket rate limiting
- Ring buffers for sliding windows
- Memory-efficient event processing

### 4. Caching Strategy
- Configuration caching with TTL
- Anomaly status caching
- Baseline statistics caching

## Trade-offs & Production Considerations

### Shortcuts Taken (MVP Focus)
1. **Simplified Anomaly Detection**: Using basic 3-sigma rule instead of ML models
2. **In-Memory Baselines**: Production would use time-series database
3. **Basic Authentication**: Production needs OAuth2/JWT
4. **Simple Routing**: Production needs advanced path matching
5. **Minimal Monitoring**: Production needs comprehensive observability

### Production Enhancements Needed

**Scalability**:
- Horizontal pod autoscaling
- Database sharding for high-traffic clients
- Message queue partitioning
- Distributed caching with Redis Cluster

**Reliability**:
- Circuit breakers for all external dependencies
- Graceful shutdown handling
- Health checks and readiness probes
- Distributed tracing (Jaeger/Zipkin)

**Security**:
- TLS termination
- API key rotation
- Request signing validation
- Rate limiting by IP + Client ID

**Observability**:
- Prometheus metrics
- Structured logging with correlation IDs
- Grafana dashboards
- AlertManager integration

**Advanced Features**:
- Machine learning anomaly detection
- Geographic load balancing
- A/B testing capabilities
- Cost-based rate limiting

## Local Development Setup

### Prerequisites
- Docker & Docker Compose
- Go 1.21+ (for development)
- Make (optional, for convenient commands)

### Quick Start
```bash
# Clone the repository
git clone <repository-url>
cd project-aether

# Start all services
docker-compose up -d

# Verify services are running
curl {{GATEWAY_URL}}/health

# View logs
docker-compose logs -f
```

### Service Endpoints
- **API Gateway**: {GATEWAY_URL}
- **Configuration API**: {CONFIG_URL}
- **Analysis Service**: Internal (Kafka consumer)
- **Telemetry Service**: Internal (Kafka consumer)
- **PostgreSQL**: {POSTGRES_DB_CONN_URL}
- **Redis**: {REDIS_URL}
- **MongoDB**: {MONGO_DB_URL}
- **Kafka**: {KAFKA_URL}

### Testing the System

#### 1. Configure Rate Limits
```bash
# Set rate limit for client-123
curl -X POST {{CONFIG_URL}}/config/rate-limits \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "client-123",
    "requests_per_minute": 100
  }'
```

#### 2. Send Test Requests
```bash
# Normal request
curl -H "X-Client-ID: client-123" \
  {{GATEWAY_URL}}/api/users

# Generate high traffic to trigger anomaly
for i in {1..200}; do
  curl -H "X-Client-ID: client-123" \
    {{GATEWAY_URL}}/api/users &
done
```

#### 3. Monitor Anomaly Detection
```bash
# Check if client is flagged as anomalous
redis-cli GET anomaly:client-123
```

## Configuration

### Environment Variables
```bash
# API Gateway
GATEWAY_PORT=8080
UPSTREAM_URLS=http://upstream1:3001,http://upstream2:3002
KAFKA_BROKERS=kafka:9092
REDIS_URL=redis://redis:6379
DB_URL=postgres://user:pass@postgres:5432/aether
MONGO_URL=mongodb://mongo:27017/aether

# Analysis Service
ANALYSIS_WINDOW_MINUTES=60
ANOMALY_THRESHOLD_SIGMA=3
ANOMALY_TTL_MINUTES=5

# Telemetry Service
KAFKA_BROKERS=kafka:9092
MONGO_URL=mongodb://mongo:27017/aether
```

### Database Schema

**PostgreSQL (Client Configuration)**:
```sql
-- Client information and authentication
CREATE TABLE clients (
    client_id VARCHAR(255) PRIMARY KEY,
    client_name VARCHAR(500),
    email VARCHAR(320),
    tier VARCHAR(50) DEFAULT 'standard',
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT NOW()
);

-- Rate limiting rules
CREATE TABLE rate_limits (
    client_id VARCHAR(255) PRIMARY KEY,
    requests_per_minute INTEGER NOT NULL,
    burst_limit INTEGER DEFAULT 10,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

**Redis (Anomaly Data & Caching)**:
```
# Anomaly flags (TTL: 5-10 minutes)
anomaly:{client_id} -> "true"

# Rate limiting tokens (TTL: 5 minutes)
rate_limit:standard:{client_id} -> {tokens: N, last_refill: timestamp}
rate_limit:anomaly:{client_id} -> {tokens: N, last_refill: timestamp}

# Cached baselines (TTL: 1 hour)
baseline:{client_id} -> {avg: N, std_dev: N, sample_count: N}
```

**MongoDB (Request Logs & Time Series)**:
```javascript
// Request logs collection with time series optimization
{
  _id: ObjectId,
  client_id: "client-123",
  timestamp: ISODate,
  path: "/api/users",
  method: "GET",
  status_code: 200,
  response_latency_ms: 45,
  is_throttled: false,
  is_anomalous: false,
  upstream_service: "upstream1",
  // Time series fields for aggregation
  hour: 14,
  day: 31,
  month: 8,
  year: 2025
}
```

## Monitoring & Observability

### Health Checks
- `/health` - Overall system health
- `/metrics` - Prometheus metrics endpoint
- `/ready` - Kubernetes readiness probe

### Key Metrics
- Request throughput (requests/second)
- Response latency percentiles (p50, p95, p99)
- Rate limit hit rates
- Anomaly detection accuracy
- Cache hit rates
- Message queue lag

### Logging Structure
```json
{
  "timestamp": "2025-08-31T10:00:00Z",
  "level": "INFO",
  "service": "proxy",
  "client_id": "client-123",
  "request_id": "req-456",
  "path": "/api/users",
  "method": "GET",
  "status_code": 200,
  "latency_ms": 45,
  "rate_limited": false,
  "anomalous": false
}
```

## Security Considerations

### Authentication & Authorization
- Client ID validation against database
- Rate limiting by authenticated client only
- Admin API protection (internal network only)

### Data Protection
- No sensitive data in telemetry events
- Encrypted database connections
- Redis AUTH enabled in production

### Network Security
- Internal service communication only
- No direct database access from gateway
- Firewall rules for service isolation

## Performance Benchmarks

### Target Performance (Single Gateway Instance)
- **Throughput**: 10,000+ requests/second
- **Latency**: p95 < 10ms (excluding upstream)
- **Memory**: < 512MB under normal load
- **CPU**: < 2 cores under normal load

### Load Testing
```bash
# Install vegeta for load testing
go install github.com/tsenart/vegeta@latest

# Run load test
echo "GET {{GATEWAY_URL}}/api/users" | \
  vegeta attack -header "X-Client-ID: client-123" \
  -rate=1000 -duration=30s | \
  vegeta report
```

## Development Workflow

### Running Tests
```bash
# Unit tests
go test ./...

# Integration tests
docker-compose -f docker-compose.test.yml up --abort-on-container-exit

# Load tests
make load-test
```

### Code Quality
```bash
# Linting
golangci-lint run

# Formatting
gofmt -s -w .

# Security scanning
gosec ./...
```

### Debugging
```bash
# View real-time logs
docker-compose logs -f gateway

# Access Redis CLI
docker-compose exec redis redis-cli

# Access PostgreSQL
docker-compose exec postgres psql -U aether
```

## Deployment

### Docker Images
- Multi-stage builds for minimal image sizes
- Non-root user execution
- Health check definitions
- Proper signal handling

### Kubernetes Deployment
```yaml
# Example deployment snippet
apiVersion: apps/v1
kind: Deployment
metadata:
  name: aether-gateway
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: gateway
        image: aether/gateway:latest
        resources:
          requests:
            memory: "256Mi"
            cpu: "500m"
          limits:
            memory: "512Mi"
            cpu: "1000m"
```

## Future Enhancements

### Advanced Analytics
- Machine learning anomaly detection (isolation forests, LSTM)
- Behavioral clustering for client segmentation
- Predictive scaling based on traffic patterns

### Enhanced Security
- OAuth2/JWT token validation
- Request signing and verification
- Geographic access controls
- DDoS protection integration

### Operational Excellence
- Blue-green deployments
- Canary releases
- Chaos engineering tests
- Cost optimization features

## Contributing

### Code Style
- Follow Go best practices and idioms
- Use gofmt and golint
- Write comprehensive tests
- Document public APIs

### Git Workflow
- Feature branches from main
- Squash commits before merge
- Conventional commit messages
- Pull request reviews required

---

This implementation demonstrates a production-ready approach to building intelligent API gateways with modern DevOps practices and scalable architecture patterns.