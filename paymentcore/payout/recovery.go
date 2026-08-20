package payout

import (
	"context"
	"time"
)

func (s *Service) RecoverUnknownOnce(ctx context.Context) (int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT payment_id,idempotency_key FROM payouts WHERE provider=$1 AND provider_status='unknown' ORDER BY updated_at LIMIT 100`, s.executionProvider.Name())
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var requests []executionRequest
	for rows.Next() {
		var request executionRequest
		if err := rows.Scan(&request.paymentID, &request.idempotencyKey); err != nil {
			return 0, err
		}
		requests = append(requests, request)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for i, request := range requests {
		if _, err := s.executePayout(ctx, request); err != nil {
			return i, err
		}
	}
	return len(requests), nil
}

func (s *Service) RunRecovery(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Minute
	}
	for {
		if _, err := s.RecoverUnknownOnce(ctx); err != nil {
			return err
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}
