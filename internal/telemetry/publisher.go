package telemetry

import (
	"encoding/json"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

type Publisher struct {
	nats *nats.Conn
}

type Event interface{}

func NewPublisher(nc *nats.Conn) *Publisher {
	return &Publisher{
		nats: nc,
	}
}

func (p *Publisher) Publish(subject string, event Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return p.nats.Publish(subject, data)
}

func (p *Publisher) PublishWithTimeout(subject string, event Event, timeout time.Duration) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	// Create a channel for the result
	done := make(chan error, 1)

	go func() {
		done <- p.nats.Publish(subject, data)
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		log.Printf("Telemetry publish timeout for subject: %s", subject)
		return nil // Don't fail the request due to telemetry timeout
	}
}

func (p *Publisher) Close() {
	if p.nats != nil {
		p.nats.Close()
	}
}
