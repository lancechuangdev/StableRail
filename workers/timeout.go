package workers

import (
	"context"
	"errors"
	"time"
)

// TimeoutWorker periodically expires overdue sagas until its context is
// canceled. Its function signature can be passed directly to an application
// runtime without coupling either domain coordinator to process scheduling.
type Expirer interface {
	ExpireOnce(context.Context) (int, error)
}

func TimeoutWorker(expirer Expirer, interval time.Duration) func(context.Context) error {
	return func(ctx context.Context) error {
		for {
			if _, err := expirer.ExpireOnce(ctx); err != nil {
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
