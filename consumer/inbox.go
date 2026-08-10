package consumer

import (
	"context"
	"database/sql"
	"stablerail/eventbus"
	"stablerail/inbox"
)

// InboxProcessor adapts the transactional inbox while retaining its SQL
// transaction for handlers that need atomic side effects.
type InboxProcessor struct {
	Inbox   *inbox.Processor
	Handler func(context.Context, *sql.Tx, eventbus.Event) error
}

func (p InboxProcessor) Process(ctx context.Context, name string, event eventbus.Event, _ func(context.Context, eventbus.Event) error) (bool, error) {
	return p.Inbox.Process(ctx, name, event, p.Handler)
}
