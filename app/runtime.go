package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Runner func(context.Context) error

// Run starts all background workers and the HTTP server. The first component
// failure cancels its siblings; cancellation triggers a bounded HTTP drain.
func Run(ctx context.Context, server *http.Server, shutdownTimeout time.Duration, workers ...Runner) error {
	if server == nil || server.Handler == nil {
		return errors.New("HTTP server and handler are required")
	}
	if shutdownTimeout <= 0 {
		return errors.New("shutdown timeout must be positive")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, len(workers)+1)
	var wg sync.WaitGroup

	start := func(run Runner) { wg.Add(1); go func() { defer wg.Done(); errs <- run(ctx) }() }
	for _, worker := range workers {
		if worker == nil {
			return errors.New("nil application worker")
		}
		start(worker)
	}

	start(func(context.Context) error {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	})

	var result error
	select {
	case <-ctx.Done():
		result = ctx.Err()
	case err := <-errs:
		if err != nil && !errors.Is(err, context.Canceled) {
			result = fmt.Errorf("application component failed: %w", err)
		}
		cancel()
	}

	shutdownCtx, stop := context.WithTimeout(context.Background(), shutdownTimeout)
	defer stop()

	if err := server.Shutdown(shutdownCtx); err != nil && result == nil {
		result = fmt.Errorf("shutdown HTTP server: %w", err)
	}
	cancel()

	wg.Wait()
	if errors.Is(result, context.Canceled) {
		return nil
	}
	return result
}
