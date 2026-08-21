package paymentcore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type AuditRecord struct {
	PaymentID string
	Event     string
	Message   string
	At        time.Time
}

type TimelineRecord struct {
	PaymentID     string
	PaymentStatus PaymentStatus
	Note          string
	At            time.Time
}

type HistoryService struct{}

func NewHistoryService() *HistoryService { return &HistoryService{} }

func (*HistoryService) RecordAudit(ctx context.Context, tx *sql.Tx, record AuditRecord) error {
	if tx == nil {
		return errors.New("history transaction is required")
	}
	if record.PaymentID == "" || record.Event == "" {
		return errors.New("audit payment ID and event are required")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO payment_audit_events(payment_id,event,message,occurred_at) VALUES($1,$2,$3,$4)`, record.PaymentID, record.Event, record.Message, record.At); err != nil {
		return fmt.Errorf("insert payment audit event: %w", err)
	}
	return nil
}

func (*HistoryService) RecordTimeline(ctx context.Context, tx *sql.Tx, record TimelineRecord) error {
	if tx == nil {
		return errors.New("history transaction is required")
	}
	if record.PaymentID == "" || record.PaymentStatus == "" {
		return errors.New("timeline payment ID and status are required")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO payment_timeline_entries(payment_id,payment_status,note,occurred_at) VALUES($1,$2,$3,$4)`, record.PaymentID, record.PaymentStatus, record.Note, record.At); err != nil {
		return fmt.Errorf("insert payment timeline entry: %w", err)
	}
	return nil
}

func (s *HistoryService) Record(ctx context.Context, tx *sql.Tx, audit AuditRecord, timeline TimelineRecord) error {
	if err := s.RecordAudit(ctx, tx, audit); err != nil {
		return err
	}
	return s.RecordTimeline(ctx, tx, timeline)
}
