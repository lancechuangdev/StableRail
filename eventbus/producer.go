package eventbus

import "context"

// Topic identifies a Kafka destination without exposing the Kafka client to
// domain and outbox code.
type Topic string

// Producer publishes domain events. The transactional outbox will depend on
// this interface rather than on a Kafka client directly.
type Producer interface {
	Publish(context.Context, Topic, Event) error
	Close() error
}
