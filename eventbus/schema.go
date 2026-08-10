package eventbus

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// Upcaster converts one payload version to the immediately following version.
// It must return valid JSON and must not change the event envelope identity.
type Upcaster func(json.RawMessage) (json.RawMessage, error)

// SchemaRegistry describes the payload versions understood by a consumer.
// Registration is normally performed once during application startup.
type SchemaRegistry struct {
	mu      sync.RWMutex
	schemas map[string]*eventSchema
}

type eventSchema struct {
	latest    int
	upcasters map[int]Upcaster
}

func NewSchemaRegistry() *SchemaRegistry {
	return &SchemaRegistry{schemas: make(map[string]*eventSchema)}
}

// Register declares the latest supported payload version for an event type.
// Every older supported version must have an upcaster to its next version.
func (r *SchemaRegistry) Register(eventType string, latest int, upcasters map[int]Upcaster) error {
	if r == nil {
		return errors.New("schema registry is required")
	}
	if eventType == "" {
		return errors.New("event type is required")
	}
	if latest < 1 {
		return errors.New("latest event version must be at least 1")
	}
	steps := make(map[int]Upcaster, len(upcasters))
	for version, upcast := range upcasters {
		if version < 1 || version >= latest {
			return fmt.Errorf("upcaster version %d for %s is outside [1, %d)", version, eventType, latest)
		}
		if upcast == nil {
			return fmt.Errorf("upcaster from version %d for %s is nil", version, eventType)
		}
		steps[version] = upcast
	}
	for version := 1; version < latest; version++ {
		if steps[version] == nil {
			return fmt.Errorf("upcaster from version %d for %s is required", version, eventType)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.schemas[eventType]; exists {
		return fmt.Errorf("schema for event type %s is already registered", eventType)
	}
	r.schemas[eventType] = &eventSchema{latest: latest, upcasters: steps}
	return nil
}

// Upcast returns the event represented at the consumer's latest registered
// payload version. The input event and its payload are not mutated.
func (r *SchemaRegistry) Upcast(event Event) (Event, error) {
	if r == nil {
		return Event{}, errors.New("schema registry is required")
	}
	if err := event.Validate(); err != nil {
		return Event{}, fmt.Errorf("validate event: %w", err)
	}
	r.mu.RLock()
	schema, ok := r.schemas[event.Type]
	r.mu.RUnlock()
	if !ok {
		return Event{}, fmt.Errorf("event type %s has no registered schema", event.Type)
	}
	if event.Version > schema.latest {
		return Event{}, fmt.Errorf("event %s version %d is newer than supported version %d", event.Type, event.Version, schema.latest)
	}

	result := event
	result.Payload = append(json.RawMessage(nil), event.Payload...)
	for result.Version < schema.latest {
		upcast := schema.upcasters[result.Version]
		payload, err := upcast(result.Payload)
		if err != nil {
			return Event{}, fmt.Errorf("upcast %s from version %d: %w", result.Type, result.Version, err)
		}
		if !json.Valid(payload) {
			return Event{}, fmt.Errorf("upcast %s from version %d produced invalid JSON", result.Type, result.Version)
		}
		result.Payload = append(json.RawMessage(nil), payload...)
		result.Version++
	}
	return result, nil
}
