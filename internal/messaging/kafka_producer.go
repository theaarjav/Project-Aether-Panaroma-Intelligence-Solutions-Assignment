package messaging

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	writer *kafka.Writer
	topic  string
}

type APICallEvent struct {
	EventID            string                 `json:"event_id"`
	ClientID           string                 `json:"client_id"`
	Timestamp          time.Time              `json:"timestamp"`
	Path               string                 `json:"path"`
	Method             string                 `json:"method"`
	StatusCode         int                    `json:"status_code"`
	ResponseLatencyMs  int64                  `json:"response_latency_ms"`
	IsThrottled        bool                   `json:"is_throttled"`
	IsAnomalous        bool                   `json:"is_anomalous"`
	UpstreamService    string                 `json:"upstream_service"`
	RequestSize        int64                  `json:"request_size,omitempty"`
	ResponseSize       int64                  `json:"response_size,omitempty"`
	UserAgent          string                 `json:"user_agent,omitempty"`
	IPAddress          string                 `json:"ip_address,omitempty"`
	Headers            map[string]string      `json:"headers,omitempty"`
	QueryParams        map[string]string      `json:"query_params,omitempty"`
	AdditionalMetadata map[string]interface{} `json:"additional_metadata,omitempty"`
}

func NewKafkaProducer(brokers []string, topic string) (*KafkaProducer, error) {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // Partition by client_id for ordering
		BatchSize:    100,           // Batch messages for efficiency
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafka.RequireOne, // At least one acknowledgment
		Async:        true,             // Non-blocking writes
	}

	// Test connection by creating topic if needed
	conn, err := kafka.DialLeader(context.Background(), "tcp", brokers[0], topic, 0)
	if err != nil {
		// Try to create topic if it doesn't exist
		controller, err := kafka.Dial("tcp", brokers[0])
		if err != nil {
			return nil, err
		}
		defer controller.Close()

		topicConfigs := []kafka.TopicConfig{
			{
				Topic:             topic,
				NumPartitions:     6, // Multiple partitions for scalability
				ReplicationFactor: 1, // Single replica for development
			},
		}

		err = controller.CreateTopics(topicConfigs...)
		if err != nil {
			log.Printf("Topic might already exist: %v", err)
		}
	} else {
		conn.Close()
	}

	return &KafkaProducer{
		writer: writer,
		topic:  topic,
	}, nil
}

func (kp *KafkaProducer) PublishAPICall(ctx context.Context, event APICallEvent) error {
	// Use client_id as the key for consistent partitioning
	// This ensures all events for a client go to the same partition (ordering)
	key := event.ClientID

	// Serialize event to JSON
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}

	// Create Kafka message
	message := kafka.Message{
		Key:   []byte(key),
		Value: value,
		Time:  event.Timestamp,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte("api_call")},
			{Key: "client_id", Value: []byte(event.ClientID)},
			{Key: "method", Value: []byte(event.Method)},
		},
	}

	// Write message (async)
	return kp.writer.WriteMessages(ctx, message)
}

func (kp *KafkaProducer) PublishAPICallAsync(event APICallEvent) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := kp.PublishAPICall(ctx, event); err != nil {
			log.Printf("Failed to publish API call event: %v", err)
		}
	}()
}

func (kp *KafkaProducer) PublishBatch(ctx context.Context, events []APICallEvent) error {
	messages := make([]kafka.Message, len(events))

	for i, event := range events {
		value, err := json.Marshal(event)
		if err != nil {
			continue // Skip malformed events
		}

		messages[i] = kafka.Message{
			Key:   []byte(event.ClientID),
			Value: value,
			Time:  event.Timestamp,
			Headers: []kafka.Header{
				{Key: "event_type", Value: []byte("api_call")},
				{Key: "client_id", Value: []byte(event.ClientID)},
			},
		}
	}

	return kp.writer.WriteMessages(ctx, messages...)
}

func (kp *KafkaProducer) GetStats() kafka.WriterStats {
	return kp.writer.Stats()
}

func (kp *KafkaProducer) Close() error {
	return kp.writer.Close()
}
