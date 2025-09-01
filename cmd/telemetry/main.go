package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"project-aether/internal/config"
	"project-aether/internal/logger"
	"project-aether/internal/messaging"
)

type TelemetryService struct {
	config        *config.TelemetryConfig
	kafkaConsumer *messaging.KafkaConsumer
	mongoLogger   *logger.MongoLogger
	ctx           context.Context
	cancel        context.CancelFunc
}

func main() {
	cfg := config.LoadTelemetryConfig()

	service, err := NewTelemetryService(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize telemetry service: %v", err)
	}

	// Start the service
	if err := service.Start(); err != nil {
		log.Fatalf("Failed to start telemetry service: %v", err)
	}

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down telemetry service...")
	service.Stop()
	log.Println("Telemetry service stopped")
}

func NewTelemetryService(cfg *config.TelemetryConfig) (*TelemetryService, error) {
	// Initialize MongoDB logger
	mongoLogger, err := logger.NewMongoLogger(cfg.MongoURL)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Initialize Kafka consumer
	brokers := strings.Split(cfg.KafkaBrokers, ",")

	service := &TelemetryService{
		config:      cfg,
		mongoLogger: mongoLogger,
		ctx:         ctx,
		cancel:      cancel,
	}

	kafkaConsumer := messaging.NewKafkaConsumer(
		brokers,
		"api-calls",
		"telemetry-service",
		service, // TelemetryService implements EventHandler
	)

	service.kafkaConsumer = kafkaConsumer

	return service, nil
}

func (ts *TelemetryService) Start() error {
	log.Println("Starting telemetry service...")

	// Start Kafka consumer in background
	go func() {
		if err := ts.kafkaConsumer.StartBatch(ts.ctx); err != nil {
			if err != context.Canceled {
				log.Printf("Kafka consumer error: %v", err)
			}
		}
	}()

	log.Println("Telemetry service started successfully")
	return nil
}

func (ts *TelemetryService) Stop() {
	ts.cancel()

	if ts.kafkaConsumer != nil {
		// Log final stats
		stats := ts.kafkaConsumer.GetStats()
		log.Printf("Kafka consumer final stats: Messages=%d, Bytes=%d, Lag=%d",
			stats.Messages, stats.Bytes, stats.Lag)

		ts.kafkaConsumer.Close()
	}
	if ts.mongoLogger != nil {
		ts.mongoLogger.Close()
	}
}

// Implement EventHandler interface
func (ts *TelemetryService) HandleAPICallEvent(ctx context.Context, event messaging.APICallEvent) error {
	// Convert Kafka event to MongoDB log entry
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
		UserAgent:         event.UserAgent,
		IPAddress:         event.IPAddress,
		RequestSize:       event.RequestSize,
		ResponseSize:      event.ResponseSize,
	}

	// Store in MongoDB
	return ts.mongoLogger.LogRequest(ctx, mongoLog)
}

// Implement BatchEventHandler interface for efficient batch processing
func (ts *TelemetryService) HandleAPICallEventBatch(ctx context.Context, events []messaging.APICallEvent) error {
	if len(events) == 0 {
		return nil
	}

	// Convert all events to MongoDB format
	mongoLogs := make([]interface{}, len(events))
	for i, event := range events {
		mongoLogs[i] = logger.RequestLog{
			ClientID:          event.ClientID,
			Timestamp:         event.Timestamp,
			Path:              event.Path,
			Method:            event.Method,
			StatusCode:        event.StatusCode,
			ResponseLatencyMs: event.ResponseLatencyMs,
			IsThrottled:       event.IsThrottled,
			IsAnomalous:       event.IsAnomalous,
			UpstreamService:   event.UpstreamService,
			UserAgent:         event.UserAgent,
			IPAddress:         event.IPAddress,
			RequestSize:       event.RequestSize,
			ResponseSize:      event.ResponseSize,
		}
	}

	// Batch insert into MongoDB
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	mongoClient := ts.mongoLogger.GetClient()
	collection := mongoClient.Database("aether").Collection("request_logs")

	_, err := collection.InsertMany(ctx, mongoLogs)
	if err != nil {
		log.Printf("Failed to batch insert %d events: %v", len(events), err)
		return err
	}

	log.Printf("Successfully stored %d API call events to MongoDB", len(events))
	return nil
}
