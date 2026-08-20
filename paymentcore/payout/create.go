package payout

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"stablerail/paymentcore"
)

type Payment = paymentcore.Payment
type PaymentStatus = paymentcore.PaymentStatus
type FundsStatus = paymentcore.FundsStatus
type AuditEvent = paymentcore.AuditEvent
type TimelineEntry = paymentcore.TimelineEntry
type LedgerEntry = paymentcore.LedgerEntry

const (
	PaymentDirectionPayout  = paymentcore.PaymentDirectionPayout
	PaymentStatusCreated    = paymentcore.PaymentStatusCreated
	PaymentStatusProcessing = paymentcore.PaymentStatusProcessing
	PaymentStatusSucceeded  = paymentcore.PaymentStatusSucceeded
	PaymentStatusFailed     = paymentcore.PaymentStatusFailed
	FundsStatusAvailable    = paymentcore.FundsStatusAvailable
	FundsStatusReserved     = paymentcore.FundsStatusReserved
	FundsStatusConsumed     = paymentcore.FundsStatusConsumed
)

var (
	ErrPaymentNotFound     = paymentcore.ErrPaymentNotFound
	ErrIdempotencyConflict = paymentcore.ErrIdempotencyConflict
)

type CreatePaymentRequest struct {
	ExternalReference       string
	Currency                string
	AmountMinor             int64
	TenantID                string
	IdempotencyKey          string
	QuoteID                 string
	SourceAccountID         string
	DestinationInstrumentID string
}

func (r CreatePaymentRequest) Validate() error {
	if strings.TrimSpace(r.TenantID) == "" || strings.TrimSpace(r.IdempotencyKey) == "" || strings.TrimSpace(r.ExternalReference) == "" {
		return errors.New("tenant, idempotency key, and external reference are required")
	}
	if len(strings.TrimSpace(r.Currency)) < 3 || len(strings.TrimSpace(r.Currency)) > 10 || r.AmountMinor <= 0 {
		return errors.New("payout payments require currency and positive amount_minor")
	}
	direct := strings.TrimSpace(r.SourceAccountID) != "" || strings.TrimSpace(r.DestinationInstrumentID) != ""
	if strings.TrimSpace(r.QuoteID) != "" {
		if direct {
			return errors.New("payout quote and direct resource IDs cannot be combined")
		}
		return nil
	}
	if strings.TrimSpace(r.SourceAccountID) == "" || strings.TrimSpace(r.DestinationInstrumentID) == "" {
		return errors.New("direct payout requires source_account_id and destination_instrument_id")
	}
	return nil
}

func (s *Service) CreatePayment(ctx context.Context, request CreatePaymentRequest) (*Payment, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	return s.createPayout(ctx, request)
}

// createPayout creates the shared payment aggregate and binds its outbound
// provider-resource route atomically. Provider submission happens asynchronously.
func (s *Service) createPayout(ctx context.Context, request CreatePaymentRequest) (*Payment, error) {
	externalRef, currency, amountMinor := request.ExternalReference, request.Currency, request.AmountMinor
	tenantID, idempotencyKey, payoutQuoteID := request.TenantID, request.IdempotencyKey, request.QuoteID
	if payoutQuoteID == "" {
		quote, err := s.CreateQuote(ctx, QuoteRequest{
			IdempotencyKey:          idempotencyKey + ":implicit-quote",
			TenantID:                tenantID,
			SourceAccountID:         request.SourceAccountID,
			DestinationInstrumentID: request.DestinationInstrumentID,
			SourceCurrency:          currency,
			DestinationCurrency:     currency,
			CurrencyType:            "sender",
			AmountMinor:             amountMinor,
		})
		if err != nil {
			return nil, fmt.Errorf("create implicit payout quote: %w", err)
		}
		payoutQuoteID = quote.ID
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin create payment transaction: %w", err)
	}
	defer tx.Rollback()

	paymentID, err := s.newID("pay_")
	if err != nil {
		return nil, fmt.Errorf("generate payment ID: %w", err)
	}
	now := s.now()
	payment := &Payment{
		ID: paymentID, Direction: PaymentDirectionPayout, ExternalReference: externalRef, Currency: currency,
		AmountMinor: amountMinor, TenantID: tenantID, PaymentStatus: PaymentStatusCreated, FundsStatus: FundsStatusAvailable,
		IdempotencyKey: idempotencyKey, QuoteID: payoutQuoteID, CreatedAt: now, UpdatedAt: now,
	}
	var quoteTenant, quoteCurrency string
	var quoteAmount int64
	var quoteStatus string
	var expiresAt time.Time
	err = tx.QueryRowContext(ctx, `SELECT tenant_id,source_currency,sender_amount_minor,status,expires_at FROM payment_quotes WHERE direction='payout' AND id=$1 FOR UPDATE`, payoutQuoteID).Scan(&quoteTenant, &quoteCurrency, &quoteAmount, &quoteStatus, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errors.New("payout quote not found")
	}
	if err != nil {
		return nil, fmt.Errorf("lock payout quote: %w", err)
	}
	if quoteStatus == "accepted" {
		existing, lookupErr := getPaymentByIdempotencyKey(ctx, tx, idempotencyKey)
		if lookupErr == nil {
			if !paymentRequestMatches(existing, externalRef, currency, amountMinor, tenantID, payoutQuoteID) {
				return nil, ErrIdempotencyConflict
			}
			if err := tx.Commit(); err != nil {
				return nil, fmt.Errorf("commit idempotent payout payment lookup: %w", err)
			}
			return existing, nil
		}
		if !errors.Is(lookupErr, sql.ErrNoRows) {
			return nil, lookupErr
		}
		return nil, errors.New("payout quote already accepted")
	}
	if quoteStatus != "open" || !expiresAt.After(now) {
		if _, err := tx.ExecContext(ctx, `UPDATE payment_quotes SET status='expired',updated_at=$1 WHERE id=$2 AND status='open'`, now, payoutQuoteID); err != nil {
			return nil, fmt.Errorf("expire payout quote: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit payout quote expiration: %w", err)
		}
		return nil, errors.New("payout quote expired")
	}
	if quoteTenant != tenantID || quoteCurrency != currency || quoteAmount != amountMinor {
		return nil, errors.New("payment tenant, amount, or currency does not match payout quote")
	}

	result, err := tx.ExecContext(ctx, `
		INSERT INTO payments
			(id, direction, external_reference, currency, amount_minor, tenant_id, payment_status, funds_status,
			 idempotency_key, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)
		ON CONFLICT (idempotency_key) DO NOTHING`,
		payment.ID, payment.Direction, payment.ExternalReference, payment.Currency, payment.AmountMinor,
		payment.TenantID, payment.PaymentStatus, payment.FundsStatus, payment.IdempotencyKey, now,
	)
	if err != nil {
		return nil, fmt.Errorf("insert payment: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("inspect payment insert: %w", err)
	}
	if rows == 0 {
		existing, err := getPaymentByIdempotencyKey(ctx, tx, idempotencyKey)
		if err != nil {
			return nil, err
		}
		if !paymentRequestMatches(existing, externalRef, currency, amountMinor, tenantID, payoutQuoteID) {
			return nil, ErrIdempotencyConflict
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit idempotent payment lookup: %w", err)
		}
		return existing, nil
	}
	result, err = tx.ExecContext(ctx, `UPDATE payment_quotes SET status='accepted',payment_id=$1,updated_at=$2 WHERE id=$3 AND status='open'`, payment.ID, now, payoutQuoteID)
	if err != nil {
		return nil, fmt.Errorf("accept payout quote: %w", err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		return nil, errors.New("payout quote already accepted")
	}
	if err := insertHistory(ctx, tx, payment.ID, "created", "payment intent created", PaymentStatusCreated, "payment created", now); err != nil {
		return nil, err
	}
	payment.AuditLog = []AuditEvent{{Event: "created", Message: "payment intent created", At: now}}
	payment.Timeline = []TimelineEntry{{PaymentStatus: PaymentStatusCreated, At: now, Note: "payment created"}}

	payload, err := json.Marshal(struct {
		ExternalReference string `json:"external_reference"`
		Currency          string `json:"currency"`
		AmountMinor       int64  `json:"amount_minor"`
		TenantID          string `json:"tenant_id"`
		PaymentStatus     string `json:"payment_status"`
		FundsStatus       string `json:"funds_status"`
	}{externalRef, currency, amountMinor, tenantID, string(PaymentStatusCreated), string(FundsStatusAvailable)})
	if err != nil {
		return nil, fmt.Errorf("marshal payment created payload: %w", err)
	}
	if err := s.enqueue(ctx, tx, payment.ID, "payment.created", payload, now); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit create payment transaction: %w", err)
	}
	return clonePayment(payment), nil
}

func getPaymentByIdempotencyKey(ctx context.Context, tx *sql.Tx, key string) (*Payment, error) {
	payment := &Payment{}
	err := tx.QueryRowContext(ctx, `
		SELECT id, direction, external_reference, currency, amount_minor, tenant_id, payment_status, funds_status,
		       idempotency_key, created_at, updated_at
		FROM payments WHERE idempotency_key = $1`, key,
	).Scan(
		&payment.ID, &payment.Direction, &payment.ExternalReference, &payment.Currency, &payment.AmountMinor,
		&payment.TenantID, &payment.PaymentStatus, &payment.FundsStatus, &payment.IdempotencyKey,
		&payment.CreatedAt, &payment.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("get idempotent payment: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT id FROM payment_quotes WHERE payment_id=$1`, payment.ID).Scan(&payment.QuoteID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("get idempotent payment payout quote: %w", err)
	}
	return payment, nil
}

func paymentRequestMatches(payment *Payment, externalRef, currency string, amountMinor int64, tenantID, payoutQuoteID string) bool {
	return payment.ExternalReference == externalRef && payment.Currency == currency && payment.AmountMinor == amountMinor && payment.TenantID == tenantID && payment.QuoteID == payoutQuoteID
}

func clonePayment(payment *Payment) *Payment {
	clone := *payment
	clone.LedgerEntries = append([]LedgerEntry(nil), payment.LedgerEntries...)
	clone.AuditLog = append([]AuditEvent(nil), payment.AuditLog...)
	clone.Timeline = append([]TimelineEntry(nil), payment.Timeline...)
	return &clone
}
