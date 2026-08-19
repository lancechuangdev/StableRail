// Package workflow contains infrastructure shared by direction-specific settlement coordinators.
package workflow

import (
	"context"
	"errors"
	"time"

	"stablerail/eventbus"
)

const CommandTopic eventbus.Topic = "payment-commands"

// TimeoutWorker periodically expires overdue sagas until its context is
// canceled. Its function signature can be passed directly to an application
// runtime without coupling workflow infrastructure to that runtime.
type Expirer interface {
	ExpireOnce(context.Context) (int, error)
}

func TimeoutWorker(coordinator Expirer, interval time.Duration) func(context.Context) error {
	return func(ctx context.Context) error {
		for {
			if _, err := coordinator.ExpireOnce(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return ctx.Err()
				}
				return err
			}
			timer := time.NewTimer(interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
}
