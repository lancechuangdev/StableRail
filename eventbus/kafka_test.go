package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

type recordingWriter struct {
	messages []kafka.Message
	err      error
	closed   bool
}

func (w *recordingWriter) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	w.messages = append(w.messages, messages...)
	return w.err
}

func (w *recordingWriter) Close() error {
	w.closed = true
	return nil
}

func TestKafkaProducerPublishesVersionedEvent(t *testing.T) {
	writer := &recordingWriter{}
	producer := &KafkaProducer{writer: writer}
	event := Event{
		ID:            "evt-1",
		Type:          "payment.created",
		Version:       1,
		AggregateID:   "pay-1",
		AggregateType: "payment",
		OccurredAt:    time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC),
		Payload:       json.RawMessage(`{"amount_minor":2500,"currency":"USD"}`),
	}

	if err := producer.Publish(context.Background(), PayoutEventsTopic, event); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	if len(writer.messages) != 1 {
		t.Fatalf("expected one message, got %d", len(writer.messages))
	}

	message := writer.messages[0]
	if message.Topic != "payout-events" || string(message.Key) != "pay-1" {
		t.Fatalf("unexpected routing: topic=%q key=%q", message.Topic, message.Key)
	}

	var decoded Event
	if err := json.Unmarshal(message.Value, &decoded); err != nil {
		t.Fatalf("decode message: %v", err)
	}
	if decoded.ID != event.ID || decoded.Version != 1 {
		t.Fatalf("unexpected event envelope: %+v", decoded)
	}
}

func TestKafkaProducerPublishesToMultipleTopics(t *testing.T) {
	writer := &recordingWriter{}
	producer := &KafkaProducer{writer: writer}
	event := Event{
		ID: "evt-1", Type: "payment.created", Version: 1,
		AggregateID: "pay-1", AggregateType: "payment",
		OccurredAt: time.Now().UTC(), Payload: json.RawMessage(`{}`),
	}

	if err := producer.Publish(context.Background(), PayoutEventsTopic, event); err != nil {
		t.Fatalf("publish payment event: %v", err)
	}
	event.ID = "evt-2"
	event.Type = "settlement.requested"
	if err := producer.Publish(context.Background(), Topic("settlement-events"), event); err != nil {
		t.Fatalf("publish settlement event: %v", err)
	}

	if len(writer.messages) != 2 {
		t.Fatalf("expected two messages, got %d", len(writer.messages))
	}
	if writer.messages[0].Topic != "payout-events" || writer.messages[1].Topic != "settlement-events" {
		t.Fatalf("unexpected topics: %q, %q", writer.messages[0].Topic, writer.messages[1].Topic)
	}
}

func TestKafkaProducerRejectsInvalidEvent(t *testing.T) {
	writer := &recordingWriter{}
	producer := &KafkaProducer{writer: writer}

	if err := producer.Publish(context.Background(), PayoutEventsTopic, Event{}); err == nil {
		t.Fatal("expected invalid event error")
	}
	if len(writer.messages) != 0 {
		t.Fatal("invalid event was written")
	}
}

func TestKafkaProducerReturnsWriterError(t *testing.T) {
	writer := &recordingWriter{err: errors.New("broker unavailable")}
	producer := &KafkaProducer{writer: writer}
	event := Event{
		ID: "evt-1", Type: "payment.created", Version: 1,
		AggregateID: "pay-1", AggregateType: "payment",
		OccurredAt: time.Now().UTC(), Payload: json.RawMessage(`{}`),
	}

	if err := producer.Publish(context.Background(), PayoutEventsTopic, event); err == nil {
		t.Fatal("expected writer error")
	}
}

func TestNewKafkaProducerValidatesConfig(t *testing.T) {
	if _, err := NewKafkaProducer(KafkaConfig{}); err == nil {
		t.Fatal("expected missing broker error")
	}
	producer, err := NewKafkaProducer(KafkaConfig{Brokers: []string{"localhost:9092"}})
	if err != nil {
		t.Fatalf("valid config returned error: %v", err)
	}
	defer producer.Close()
}

func TestKafkaProducerRejectsEmptyTopic(t *testing.T) {
	writer := &recordingWriter{}
	producer := &KafkaProducer{writer: writer}
	event := Event{
		ID: "evt-1", Type: "payment.created", Version: 1,
		AggregateID: "pay-1", AggregateType: "payment",
		OccurredAt: time.Now().UTC(), Payload: json.RawMessage(`{}`),
	}

	if err := producer.Publish(context.Background(), "", event); err == nil {
		t.Fatal("expected missing topic error")
	}
	if len(writer.messages) != 0 {
		t.Fatal("event with empty topic was written")
	}
}
