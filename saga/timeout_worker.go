package saga

import (
	"context"
	"errors"
	"time"
)

// TimeoutWorker periodically expires overdue sagas until its context is
// canceled. Its function signature can be passed directly to an application
// runtime without coupling the saga package to that runtime.
func TimeoutWorker(coordinator *Coordinator, interval time.Duration) func(context.Context) error {
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
