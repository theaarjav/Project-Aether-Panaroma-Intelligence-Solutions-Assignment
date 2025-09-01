package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

type UpstreamService struct {
	serviceName string
	port        string
}

type MockUser struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Email   string `json:"email"`
	Service string `json:"service"`
}

type MockOrder struct {
	ID      int       `json:"id"`
	UserID  int       `json:"user_id"`
	Amount  float64   `json:"amount"`
	Status  string    `json:"status"`
	Service string    `json:"service"`
	Created time.Time `json:"created"`
}

func main() {
	port := getEnvOrDefault("PORT", "3001")
	serviceName := getEnvOrDefault("SERVICE_NAME", "upstream1")

	service := &UpstreamService{
		serviceName: serviceName,
		port:        port,
	}

	// Set up Gin
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())

	// Health check
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":  "healthy",
			"service": service.serviceName,
			"port":    service.port,
			"time":    time.Now(),
		})
	})

	// Mock API endpoints
	api := r.Group("/api")
	{
		// User endpoints
		api.GET("/users", service.getUsers)
		api.GET("/users/:id", service.getUser)
		api.POST("/users", service.createUser)

		// Order endpoints
		api.GET("/orders", service.getOrders)
		api.GET("/orders/:id", service.getOrder)
		api.POST("/orders", service.createOrder)

		// Analytics endpoints (simulate heavy computation)
		api.GET("/analytics/reports", service.getAnalyticsReport)
		api.GET("/analytics/metrics", service.getMetrics)
	}

	// Catch-all endpoint for any other paths
	r.NoRoute(func(c *gin.Context) {
		// Simulate random processing delay
		delay := time.Duration(rand.Intn(100)) * time.Millisecond
		time.Sleep(delay)

		c.JSON(http.StatusOK, gin.H{
			"message":   "Mock response from upstream service",
			"service":   service.serviceName,
			"path":      c.Request.URL.Path,
			"method":    c.Request.Method,
			"timestamp": time.Now(),
			"delay_ms":  delay.Milliseconds(),
		})
	})

	log.Printf("Starting %s on port %s", serviceName, port)
	log.Fatal(http.ListenAndServe(":"+port, r))
}

func (s *UpstreamService) getUsers(c *gin.Context) {
	// Simulate some processing time
	time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)

	users := []MockUser{
		{ID: 1, Name: "Alice Johnson", Email: "alice@example.com", Service: s.serviceName},
		{ID: 2, Name: "Bob Smith", Email: "bob@example.com", Service: s.serviceName},
		{ID: 3, Name: "Carol Davis", Email: "carol@example.com", Service: s.serviceName},
	}

	c.JSON(http.StatusOK, gin.H{
		"users":   users,
		"service": s.serviceName,
		"total":   len(users),
	})
}

func (s *UpstreamService) getUser(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid user ID"})
		return
	}

	// Simulate database lookup delay
	time.Sleep(time.Duration(rand.Intn(30)) * time.Millisecond)

	user := MockUser{
		ID:      id,
		Name:    fmt.Sprintf("User %d", id),
		Email:   fmt.Sprintf("user%d@example.com", id),
		Service: s.serviceName,
	}

	c.JSON(http.StatusOK, user)
}

func (s *UpstreamService) createUser(c *gin.Context) {
	var newUser MockUser
	if err := c.ShouldBindJSON(&newUser); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Simulate database write delay
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)

	newUser.ID = rand.Intn(10000) + 1000
	newUser.Service = s.serviceName

	c.JSON(http.StatusCreated, newUser)
}

func (s *UpstreamService) getOrders(c *gin.Context) {
	// Simulate some processing time
	time.Sleep(time.Duration(rand.Intn(75)) * time.Millisecond)

	orders := []MockOrder{
		{
			ID:      1,
			UserID:  1,
			Amount:  99.99,
			Status:  "completed",
			Service: s.serviceName,
			Created: time.Now().Add(-2 * time.Hour),
		},
		{
			ID:      2,
			UserID:  2,
			Amount:  149.50,
			Status:  "pending",
			Service: s.serviceName,
			Created: time.Now().Add(-1 * time.Hour),
		},
	}

	c.JSON(http.StatusOK, gin.H{
		"orders":  orders,
		"service": s.serviceName,
		"total":   len(orders),
	})
}

func (s *UpstreamService) getOrder(c *gin.Context) {
	idStr := c.Param("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid order ID"})
		return
	}

	// Simulate database lookup
	time.Sleep(time.Duration(rand.Intn(40)) * time.Millisecond)

	order := MockOrder{
		ID:      id,
		UserID:  rand.Intn(100) + 1,
		Amount:  float64(rand.Intn(500)) + 10.00,
		Status:  []string{"pending", "completed", "cancelled"}[rand.Intn(3)],
		Service: s.serviceName,
		Created: time.Now().Add(-time.Duration(rand.Intn(24)) * time.Hour),
	}

	c.JSON(http.StatusOK, order)
}

func (s *UpstreamService) createOrder(c *gin.Context) {
	var newOrder MockOrder
	if err := c.ShouldBindJSON(&newOrder); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Simulate order processing delay
	time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)

	newOrder.ID = rand.Intn(10000) + 1000
	newOrder.Status = "pending"
	newOrder.Service = s.serviceName
	newOrder.Created = time.Now()

	c.JSON(http.StatusCreated, newOrder)
}

func (s *UpstreamService) getAnalyticsReport(c *gin.Context) {
	// Simulate heavy computation
	time.Sleep(time.Duration(rand.Intn(500)+200) * time.Millisecond)

	report := map[string]interface{}{
		"service":           s.serviceName,
		"total_requests":    rand.Intn(10000) + 1000,
		"avg_response_time": float64(rand.Intn(100)+50) + rand.Float64(),
		"success_rate":      0.95 + rand.Float64()*0.05,
		"top_endpoints": []string{
			"/api/users",
			"/api/orders",
			"/api/analytics/metrics",
		},
		"generated_at": time.Now(),
	}

	// Occasionally simulate server errors for circuit breaker testing
	if rand.Float64() < 0.05 { // 5% chance of error
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Simulated server error",
			"service": s.serviceName,
		})
		return
	}

	c.JSON(http.StatusOK, report)
}

func (s *UpstreamService) getMetrics(c *gin.Context) {
	// Simulate metric collection
	time.Sleep(time.Duration(rand.Intn(150)) * time.Millisecond)

	metrics := map[string]interface{}{
		"service":          s.serviceName,
		"cpu_usage":        rand.Float64() * 100,
		"memory_mb":        rand.Intn(512) + 128,
		"requests_per_sec": rand.Intn(100) + 10,
		"error_rate":       rand.Float64() * 0.1,
		"uptime_hours":     rand.Intn(720) + 1,
		"timestamp":        time.Now(),
	}

	c.JSON(http.StatusOK, metrics)
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
