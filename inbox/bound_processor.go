package inbox

import (
	"context"

	"stablerail/eventbus"
)

// BoundProcessor binds a transactional inbox processor to one handler. It
// satisfies the consumer loop's processor contract without coupling the
// consumer package to SQL-backed inbox behavior.
type BoundProcessor struct {
	Processor *Processor
	Handler   Handler
}

func (p BoundProcessor) Process(ctx context.Context, consumer string, event eventbus.Event) (bool, error) {
	return p.Processor.Process(ctx, consumer, event, p.Handler)
}
