package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type messageWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

// KafkaConfig contains the producer settings required by StableRail.
type KafkaConfig struct {
	Brokers []string
}

// KafkaProducer publishes events with the aggregate ID as the message key.
// Kafka therefore preserves ordering for events belonging to one aggregate.
type KafkaProducer struct {
	writer messageWriter
}

func NewKafkaProducer(config KafkaConfig) (*KafkaProducer, error) {
	if len(config.Brokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}
	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(config.Brokers...),
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
		},
	}, nil
}

func (p *KafkaProducer) Publish(ctx context.Context, topic Topic, event Event) error {
	if topic == "" {
		return errors.New("Kafka topic is required")
	}
	if err := event.Validate(); err != nil {
		return fmt.Errorf("validate event: %w", err)
	}

	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	message := kafka.Message{
		Topic: string(topic),
		Key:   []byte(event.AggregateID),
		Value: value,
		Time:  event.OccurredAt,
		Headers: []kafka.Header{
			{Key: "event-id", Value: []byte(event.ID)},
			{Key: "event-type", Value: []byte(event.Type)},
			{Key: "event-version", Value: []byte(fmt.Sprintf("%d", event.Version))},
		},
	}

	if err := p.writer.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("publish event %s: %w", event.ID, err)
	}
	return nil
}

func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}
