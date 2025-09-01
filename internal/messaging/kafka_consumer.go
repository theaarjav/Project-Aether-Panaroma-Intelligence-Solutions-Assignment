package messaging

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaConsumer struct {
	reader  *kafka.Reader
	topic   string
	groupID string
	handler EventHandler
}

type EventHandler interface {
	HandleAPICallEvent(ctx context.Context, event APICallEvent) error
}

func NewKafkaConsumer(brokers []string, topic, groupID string, handler EventHandler) *KafkaConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,    // Minimum bytes to fetch
		MaxBytes:       10e6, // 10MB max
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset, // Start from latest for new consumers

		// Error handling
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			log.Printf("Kafka consumer error: "+msg, args...)
		}),

		// Performance tuning
		ReadBatchTimeout: 100 * time.Millisecond,
		MaxWait:          500 * time.Millisecond,
	})

	return &KafkaConsumer{
		reader:  reader,
		topic:   topic,
		groupID: groupID,
		handler: handler,
	}
}

func (kc *KafkaConsumer) Start(ctx context.Context) error {
	log.Printf("Starting Kafka consumer for topic: %s, group: %s", kc.topic, kc.groupID)

	for {
		select {
		case <-ctx.Done():
			log.Println("Kafka consumer shutting down...")
			return ctx.Err()
		default:
			// Read message with timeout
			message, err := kc.reader.FetchMessage(ctx)
			if err != nil {
				if err == context.Canceled {
					return nil
				}
				log.Printf("Error fetching message: %v", err)
				time.Sleep(time.Second) // Brief pause before retry
				continue
			}

			// Process message
			if err := kc.processMessage(ctx, message); err != nil {
				log.Printf("Error processing message: %v", err)
				// Don't commit if processing failed
				continue
			}

			// Commit message after successful processing
			if err := kc.reader.CommitMessages(ctx, message); err != nil {
				log.Printf("Error committing message: %v", err)
			}
		}
	}
}

func (kc *KafkaConsumer) processMessage(ctx context.Context, message kafka.Message) error {
	// Parse the API call event
	var event APICallEvent
	if err := json.Unmarshal(message.Value, &event); err != nil {
		log.Printf("Failed to unmarshal message: %v", err)
		return err
	}

	// Add message metadata
	if event.AdditionalMetadata == nil {
		event.AdditionalMetadata = make(map[string]interface{})
	}
	event.AdditionalMetadata["kafka_partition"] = message.Partition
	event.AdditionalMetadata["kafka_offset"] = message.Offset
	event.AdditionalMetadata["kafka_timestamp"] = message.Time

	// Handle the event
	return kc.handler.HandleAPICallEvent(ctx, event)
}

func (kc *KafkaConsumer) StartBatch(ctx context.Context) error {
	log.Printf("Starting batch Kafka consumer for topic: %s, group: %s", kc.topic, kc.groupID)

	for {
		select {
		case <-ctx.Done():
			log.Println("Batch Kafka consumer shutting down...")
			return ctx.Err()
		default:
			// Read batch of messages
			messages, err := kc.readBatch(ctx, 100) // Read up to 100 messages
			if err != nil {
				if err == context.Canceled {
					return nil
				}
				log.Printf("Error reading batch: %v", err)
				time.Sleep(time.Second)
				continue
			}

			if len(messages) == 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			// Process batch
			events := make([]APICallEvent, 0, len(messages))
			var lastMessage kafka.Message

			for _, message := range messages {
				var event APICallEvent
				if err := json.Unmarshal(message.Value, &event); err != nil {
					log.Printf("Failed to unmarshal batch message: %v", err)
					continue
				}

				// Add message metadata
				if event.AdditionalMetadata == nil {
					event.AdditionalMetadata = make(map[string]interface{})
				}
				event.AdditionalMetadata["kafka_partition"] = message.Partition
				event.AdditionalMetadata["kafka_offset"] = message.Offset

				events = append(events, event)
				lastMessage = message
			}

			// Process batch
			if err := kc.processBatch(ctx, events); err != nil {
				log.Printf("Error processing batch: %v", err)
				continue
			}

			// Commit the last message (commits all previous in same partition)
			if err := kc.reader.CommitMessages(ctx, lastMessage); err != nil {
				log.Printf("Error committing batch: %v", err)
			}
		}
	}
}

func (kc *KafkaConsumer) readBatch(ctx context.Context, maxSize int) ([]kafka.Message, error) {
	messages := make([]kafka.Message, 0, maxSize)

	for len(messages) < maxSize {
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		message, err := kc.reader.FetchMessage(ctx)
		cancel()

		if err != nil {
			if len(messages) > 0 {
				// Return what we have if we got at least some messages
				return messages, nil
			}
			return nil, err
		}

		messages = append(messages, message)
	}

	return messages, nil
}

func (kc *KafkaConsumer) processBatch(ctx context.Context, events []APICallEvent) error {
	// If handler supports batch processing, use it
	if batchHandler, ok := kc.handler.(BatchEventHandler); ok {
		return batchHandler.HandleAPICallEventBatch(ctx, events)
	}

	// Otherwise, process individually
	for _, event := range events {
		if err := kc.handler.HandleAPICallEvent(ctx, event); err != nil {
			return err
		}
	}

	return nil
}

func (kc *KafkaConsumer) GetStats() kafka.ReaderStats {
	return kc.reader.Stats()
}

func (kc *KafkaConsumer) Close() error {
	return kc.reader.Close()
}

// BatchEventHandler interface for handlers that can process events in batches
type BatchEventHandler interface {
	EventHandler
	HandleAPICallEventBatch(ctx context.Context, events []APICallEvent) error
}
