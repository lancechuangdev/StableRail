package eventbus

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestSchemaRegistryUpcastsSequentiallyWithoutMutatingInput(t *testing.T) {
	registry := NewSchemaRegistry()
	err := registry.Register("payment.created", 3, map[int]Upcaster{
		1: func(payload json.RawMessage) (json.RawMessage, error) {
			var value map[string]any
			if err := json.Unmarshal(payload, &value); err != nil {
				return nil, err
			}
			value["currency"] = "USD"
			return json.Marshal(value)
		},
		2: func(payload json.RawMessage) (json.RawMessage, error) {
			var value map[string]any
			if err := json.Unmarshal(payload, &value); err != nil {
				return nil, err
			}
			value["country"] = "US"
			return json.Marshal(value)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	original := testSchemaEvent(1, `{"amount_minor":2500}`)
	result, err := registry.Upcast(original)
	if err != nil {
		t.Fatal(err)
	}
	if result.Version != 3 {
		t.Fatalf("version = %d, want 3", result.Version)
	}
	var payload map[string]any
	if err := json.Unmarshal(result.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["currency"] != "USD" || payload["country"] != "US" {
		t.Fatalf("payload = %#v", payload)
	}
	if original.Version != 1 || string(original.Payload) != `{"amount_minor":2500}` {
		t.Fatal("input event was mutated")
	}
}

func TestSchemaRegistryRejectsIncompleteChain(t *testing.T) {
	registry := NewSchemaRegistry()
	err := registry.Register("payment.created", 3, map[int]Upcaster{1: func(p json.RawMessage) (json.RawMessage, error) { return p, nil }})
	if err == nil || !strings.Contains(err.Error(), "version 2") {
		t.Fatalf("error = %v", err)
	}
}

func TestSchemaRegistryRejectsNewerEvent(t *testing.T) {
	registry := NewSchemaRegistry()
	if err := registry.Register("payment.created", 1, nil); err != nil {
		t.Fatal(err)
	}
	_, err := registry.Upcast(testSchemaEvent(2, `{}`))
	if err == nil || !strings.Contains(err.Error(), "newer than supported") {
		t.Fatalf("error = %v", err)
	}
}

func TestSchemaRegistryReportsUpcasterFailure(t *testing.T) {
	registry := NewSchemaRegistry()
	want := errors.New("legacy payload is missing amount")
	if err := registry.Register("payment.created", 2, map[int]Upcaster{1: func(json.RawMessage) (json.RawMessage, error) { return nil, want }}); err != nil {
		t.Fatal(err)
	}
	_, err := registry.Upcast(testSchemaEvent(1, `{}`))
	if !errors.Is(err, want) {
		t.Fatalf("error = %v", err)
	}
}

func testSchemaEvent(version int, payload string) Event {
	return Event{ID: "evt-1", Type: "payment.created", Version: version, AggregateID: "pay-1", AggregateType: "payment", OccurredAt: time.Now().UTC(), Payload: json.RawMessage(payload)}
}
