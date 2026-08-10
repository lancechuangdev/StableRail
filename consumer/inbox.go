package consumer

import (
	"context"
	"stablerail/eventbus"
	"stablerail/inbox"
)

// InboxProcessor adapts the transactional inbox while retaining its SQL
// transaction for handlers that need atomic side effects.
type InboxProcessor struct {
	Inbox   *inbox.Processor
	Handler inbox.Handler
}

func (p InboxProcessor) Process(ctx context.Context, name string, event eventbus.Event) (bool, error) {
	return p.Inbox.Process(ctx, name, event, p.Handler)
}
