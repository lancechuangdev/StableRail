package payout

import (
	"context"
	"time"
)

func (s *Service) RecoverUnknownOnce(ctx context.Context) (int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT o.payment_id,o.idempotency_key,p.amount_minor,p.currency FROM payouts o JOIN payments p ON p.id=o.payment_id WHERE o.provider=$1 AND o.provider_status='unknown' ORDER BY o.updated_at LIMIT 100`, s.executionProvider.Name())
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var requests []ExecuteRequest
	for rows.Next() {
		var request ExecuteRequest
		if err := rows.Scan(&request.PaymentID, &request.IdempotencyKey, &request.AmountMinor, &request.Currency); err != nil {
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
