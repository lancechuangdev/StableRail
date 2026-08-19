package payout

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrSubmissionUnknown = errors.New("payout submission outcome is unknown")

// Service owns provider-neutral payout persistence and delegates only the
// provider API operation to an adapter. The attempt is committed before the
// external call so ambiguous outcomes can be recovered idempotently.
type Service struct {
	db       *sql.DB
	provider Provider
	now      func() time.Time
	newID    func(string) (string, error)
}

func NewService(db *sql.DB, provider Provider) (*Service, error) {
	if db == nil || provider == nil {
		return nil, errors.New("payout database and provider are required")
	}
	return &Service{db: db, provider: provider, now: func() time.Time { return time.Now().UTC() }, newID: func(prefix string) (string, error) {
		b := make([]byte, 16)
		_, err := rand.Read(b)
		return prefix + hex.EncodeToString(b), err
	}}, nil
}

func (s *Service) Name() string { return s.provider.Name() }

var ErrQuoteIdempotencyConflict = errors.New("idempotency key is bound to another payout quote")

func (s *Service) CreateQuote(ctx context.Context, request QuoteRequest) (QuoteResult, error) {
	if err := request.Validate(); err != nil {
		return QuoteResult{}, err
	}
	var existing QuoteResult
	var sourceAccountID, destinationInstrumentID, currencyType string
	var coverFees bool
	var requestAmount int64
	err := s.db.QueryRowContext(ctx, `SELECT id,provider,provider_quote_id,status,source_currency,destination_currency,sender_amount_minor,receiver_amount_minor,commercial_rate,provider_rate,flat_fee_minor,partner_fee_minor,billing_fee_minor,expires_at,provider_payload,source_resource_id,destination_resource_id,currency_type,cover_fees,request_amount_minor FROM payment_quotes WHERE direction='payout' AND tenant_id=$1 AND idempotency_key=$2`, request.TenantID, request.IdempotencyKey).Scan(&existing.ID, &existing.Provider, &existing.ProviderQuoteID, &existing.Status, &existing.SourceCurrency, &existing.DestinationCurrency, &existing.SenderAmountMinor, &existing.ReceiverAmountMinor, &existing.CommercialRate, &existing.ProviderRate, &existing.FlatFeeMinor, &existing.PartnerFeeMinor, &existing.BillingFeeMinor, &existing.ExpiresAt, &existing.Payload, &sourceAccountID, &destinationInstrumentID, &currencyType, &coverFees, &requestAmount)
	if err == nil {
		existing.Direction = "payout"
		if sourceAccountID != request.SourceAccountID || destinationInstrumentID != request.DestinationInstrumentID || existing.SourceCurrency != request.SourceCurrency || existing.DestinationCurrency != request.DestinationCurrency || currencyType != request.CurrencyType || coverFees != request.CoverFees || requestAmount != request.RequestAmountMinor {
			return QuoteResult{}, ErrQuoteIdempotencyConflict
		}
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return QuoteResult{}, fmt.Errorf("lookup payout quote: %w", err)
	}
	result, err := s.provider.CreatePayoutQuote(ctx, request)
	if err != nil {
		return QuoteResult{}, err
	}
	if result.ProviderQuoteID == "" || result.SourceCurrency == "" || result.DestinationCurrency == "" || result.SenderAmountMinor <= 0 || result.ReceiverAmountMinor <= 0 || !result.ExpiresAt.After(s.now()) {
		return QuoteResult{}, errors.New("provider returned an invalid payout quote")
	}
	id, err := s.newID("pqi_")
	if err != nil {
		return QuoteResult{}, err
	}
	payload := result.Payload
	if len(payload) == 0 {
		payload, _ = json.Marshal(result)
	}
	now := s.now()
	_, err = s.db.ExecContext(ctx, `INSERT INTO payment_quotes(id,direction,provider,provider_quote_id,tenant_id,idempotency_key,source_resource_id,destination_resource_id,payment_method,source_currency,destination_currency,currency_type,cover_fees,request_amount_minor,sender_amount_minor,receiver_amount_minor,commercial_rate,provider_rate,flat_fee_minor,partner_fee_minor,billing_fee_minor,status,expires_at,provider_payload,created_at,updated_at) VALUES($1,'payout',$2,$3,$4,$5,$6,$7,'provider_quote',$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,'open',$20,$21,$22,$22)`, id, s.provider.Name(), result.ProviderQuoteID, request.TenantID, request.IdempotencyKey, request.SourceAccountID, request.DestinationInstrumentID, result.SourceCurrency, result.DestinationCurrency, request.CurrencyType, request.CoverFees, request.RequestAmountMinor, result.SenderAmountMinor, result.ReceiverAmountMinor, result.CommercialRate, result.ProviderRate, result.FlatFeeMinor, result.PartnerFeeMinor, result.BillingFeeMinor, result.ExpiresAt, payload, now)
	if err != nil {
		return QuoteResult{}, fmt.Errorf("store payout quote: %w", err)
	}
	result.Direction, result.ID, result.Provider, result.Status, result.Payload = "payout", id, s.provider.Name(), "open", payload
	return result, nil
}

type attempt struct {
	request           Request
	providerReference string
	providerStatus    string
	new               bool
}

func (s *Service) CreatePayout(ctx context.Context, request Request) (Result, error) {
	if err := request.Validate(); err != nil {
		return Result{}, err
	}
	a, err := s.prepare(ctx, request)
	if err != nil {
		return Result{}, err
	}
	if !a.new {
		if a.providerReference != "" {
			if err := s.setPaymentFundsStatus(ctx, request.PaymentID, "reserved"); err != nil {
				return Result{}, err
			}
			return mapPersistedResult(a.providerReference, a.providerStatus)
		}
		if a.providerStatus != "unknown" {
			return Result{}, ErrSubmissionUnknown
		}
	}

	result, err := s.provider.ExecutePayout(ctx, a.request)
	if err != nil {
		status := "unknown"
		var providerErr *ProviderError
		if errors.As(err, &providerErr) && !providerErr.Retryable {
			status = "submission_failed"
		}
		if updateErr := s.recordError(ctx, request.PaymentID, status, err); updateErr != nil {
			return Result{}, fmt.Errorf("execute payout: %w (also failed to record outcome: %v)", err, updateErr)
		}
		if status == "unknown" {
			return Result{}, fmt.Errorf("%w: %v", ErrSubmissionUnknown, err)
		}
		return Result{}, err
	}
	if err := result.Validate(); err != nil {
		return Result{}, err
	}
	payload := result.Payload
	if len(payload) == 0 {
		payload, _ = json.Marshal(result)
	}
	providerStatus := persistedStatus(result)
	now := s.now()
	if _, err := s.db.ExecContext(ctx, `UPDATE payouts SET provider_payout_id=$1,provider_status=$2,provider_payload=$3,last_error=NULL,updated_at=$4,submitted_at=$4 WHERE payment_id=$5 AND provider_status IN ('submission_pending','unknown')`, result.ProviderReference, providerStatus, payload, now, request.PaymentID); err != nil {
		return Result{}, fmt.Errorf("record payout response: %w", err)
	}
	if err := s.setPaymentFundsStatus(ctx, request.PaymentID, "reserved"); err != nil {
		return Result{}, err
	}
	return result, nil
}

func (s *Service) prepare(ctx context.Context, request Request) (attempt, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return attempt{}, fmt.Errorf("begin payout submission: %w", err)
	}
	defer tx.Rollback()
	var quoteID, providerQuoteID, tenantID, sourceAccountID, destinationInstrumentID, method, sourceCurrency, destinationCurrency string
	var sourceAmount, destinationAmount int64
	err = tx.QueryRowContext(ctx, `SELECT q.id,q.provider_quote_id,q.tenant_id,q.source_resource_id,q.destination_resource_id,COALESCE(d.metadata->>'rail',d.metadata->>'kind','unknown'),q.sender_amount_minor,q.source_currency,q.receiver_amount_minor,q.destination_currency FROM payment_quotes q JOIN provider_resources d ON d.id=q.destination_resource_id WHERE q.direction='payout' AND q.payment_id=$1 AND q.provider=$2 AND q.status='accepted' FOR UPDATE`, request.PaymentID, s.provider.Name()).Scan(&quoteID, &providerQuoteID, &tenantID, &sourceAccountID, &destinationInstrumentID, &method, &sourceAmount, &sourceCurrency, &destinationAmount, &destinationCurrency)
	if errors.Is(err, sql.ErrNoRows) {
		return attempt{}, fmt.Errorf("payment has no accepted %s quote", s.provider.Name())
	}
	if err != nil {
		return attempt{}, fmt.Errorf("lock accepted payout quote: %w", err)
	}
	now := s.now()
	res, err := tx.ExecContext(ctx, `INSERT INTO payouts(payment_id,quote_id,tenant_id,source_account_id,destination_instrument_id,payout_method,source_amount_minor,source_currency,destination_amount_minor,destination_currency,provider,provider_status,idempotency_key,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'submission_pending',$12,$13,$13) ON CONFLICT(payment_id) DO NOTHING`, request.PaymentID, quoteID, tenantID, sourceAccountID, destinationInstrumentID, method, sourceAmount, sourceCurrency, destinationAmount, destinationCurrency, s.provider.Name(), request.IdempotencyKey, now)
	if err != nil {
		return attempt{}, fmt.Errorf("create payout submission: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return attempt{}, err
	}
	a := attempt{new: rows == 1}
	if !a.new {
		var storedKey string
		err = tx.QueryRowContext(ctx, `SELECT COALESCE(provider_payout_id,''),provider_status,idempotency_key FROM payouts WHERE payment_id=$1 FOR UPDATE`, request.PaymentID).Scan(&a.providerReference, &a.providerStatus, &storedKey)
		if err != nil {
			return attempt{}, fmt.Errorf("get existing payout submission: %w", err)
		}
		if storedKey != request.IdempotencyKey {
			return attempt{}, errors.New("payment is already bound to a different payout idempotency key")
		}
	}
	request.TenantID, request.QuoteID, request.ProviderQuoteID, request.SourceAccountID = tenantID, quoteID, providerQuoteID, sourceAccountID
	request.AmountMinor, request.Currency = sourceAmount, sourceCurrency
	a.request = request
	if err := tx.Commit(); err != nil {
		return attempt{}, fmt.Errorf("commit payout submission: %w", err)
	}
	return a, nil
}

func (s *Service) RecoverUnknownOnce(ctx context.Context) (int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT o.payment_id,o.idempotency_key,p.amount_minor,p.currency FROM payouts o JOIN payments p ON p.id=o.payment_id WHERE o.provider=$1 AND o.provider_status='unknown' ORDER BY o.updated_at LIMIT 100`, s.provider.Name())
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var requests []Request
	for rows.Next() {
		var r Request
		if err := rows.Scan(&r.PaymentID, &r.IdempotencyKey, &r.AmountMinor, &r.Currency); err != nil {
			return 0, err
		}
		requests = append(requests, r)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for i, r := range requests {
		if _, err := s.CreatePayout(ctx, r); err != nil {
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

func (s *Service) recordError(ctx context.Context, paymentID, status string, cause error) error {
	_, err := s.db.ExecContext(ctx, `UPDATE payouts SET provider_status=$1,last_error=$2,updated_at=$3 WHERE payment_id=$4 AND provider_status IN ('submission_pending','unknown')`, status, cause.Error(), s.now(), paymentID)
	return err
}

func (s *Service) setPaymentFundsStatus(ctx context.Context, paymentID, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE payments SET funds_status=$1,updated_at=$2 WHERE id=$3 AND payment_status='processing'`, status, s.now(), paymentID)
	if err != nil {
		return fmt.Errorf("update payment funds status: %w", err)
	}
	return nil
}

func persistedStatus(r Result) string {
	switch r.Status {
	case StatusSucceeded:
		return "completed"
	case StatusOnHold:
		return "on_hold"
	case StatusFailed:
		if strings.TrimSpace(r.FailureCode) != "" {
			return r.FailureCode
		}
		return "failed"
	default:
		return "processing"
	}
}

func mapPersistedResult(reference, status string) (Result, error) {
	r := Result{ProviderReference: reference}
	switch status {
	case "completed":
		r.Status = StatusSucceeded
	case "processing", "submission_pending", "unknown":
		r.Status = StatusPending
	case "on_hold":
		r.Status = StatusOnHold
	case "failed", "refunded", "submission_failed":
		r.Status, r.FailureCode = StatusFailed, status
	default:
		return Result{}, fmt.Errorf("unsupported payout status %q", status)
	}
	return r, r.Validate()
}
