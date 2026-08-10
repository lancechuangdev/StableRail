package app

import (
	"context"
	"errors"
	"time"

	"stablerail/saga"
)

func SagaTimeoutWorker(coordinator *saga.Coordinator, interval time.Duration) Runner {
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
