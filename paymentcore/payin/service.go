package payin

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"stablerail/eventbus"
	"stablerail/paymentcore"
)

var ErrNotFound = errors.New("payin not found")
var ErrIdempotencyConflict = errors.New("idempotency key is bound to another payin quote")

const EventsTopic eventbus.Topic = "payin-events"

type Service struct {
	db       *sql.DB
	provider Provider
	now      func() time.Time
	newID    func(string) (string, error)
}

func NewService(db *sql.DB, provider Provider) (*Service, error) {
	if db == nil || provider == nil {
		return nil, errors.New("payin database and provider are required")
	}
	return &Service{db: db, provider: provider, now: func() time.Time { return time.Now().UTC() }, newID: func(prefix string) (string, error) {
		b := make([]byte, 16)
		_, err := rand.Read(b)
		return prefix + hex.EncodeToString(b), err
	}}, nil
}

func (s *Service) CreateQuote(ctx context.Context, r QuoteRequest) (*Quote, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	var existing Quote
	var existingCurrencyType string
	var existingCoverFees bool
	var existingRequestAmount int64
	err := s.db.QueryRowContext(ctx, `SELECT id,provider,tenant_id,payment_method,COALESCE(source_resource_id,''),destination_resource_id,source_currency,destination_currency,sender_amount_minor,receiver_amount_minor,status,expires_at,created_at,updated_at,currency_type,cover_fees,request_amount_minor FROM payment_quotes WHERE direction='payin' AND tenant_id=$1 AND idempotency_key=$2`, r.TenantID, r.IdempotencyKey).Scan(&existing.ID, &existing.Provider, &existing.TenantID, &existing.FundingMethod, &existing.SourceInstrumentID, &existing.DestinationAccountID, &existing.SourceCurrency, &existing.DestinationCurrency, &existing.SenderAmountMinor, &existing.ReceiverAmountMinor, &existing.Status, &existing.ExpiresAt, &existing.CreatedAt, &existing.UpdatedAt, &existingCurrencyType, &existingCoverFees, &existingRequestAmount)
	if err == nil {
		existing.Direction = "payin"
		if existing.FundingMethod != r.FundingMethod || existing.SourceInstrumentID != r.SourceInstrumentID || existing.DestinationAccountID != r.DestinationAccountID || existingCurrencyType != r.CurrencyType || existingCoverFees != r.CoverFees || existingRequestAmount != r.AmountMinor || existing.SourceCurrency != r.SourceCurrency || existing.DestinationCurrency != r.DestinationCurrency {
			return nil, ErrIdempotencyConflict
		}
		return &existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("lookup payin quote: %w", err)
	}
	result, err := s.provider.CreatePayinQuote(ctx, r)
	if err != nil {
		return nil, err
	}
	id, err := s.newID("pqi_")
	if err != nil {
		return nil, err
	}
	now := s.now()
	payload := result.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO payment_quotes(id,direction,provider,provider_quote_id,tenant_id,idempotency_key,payment_method,currency_type,cover_fees,request_amount_minor,source_resource_id,destination_resource_id,source_currency,destination_currency,sender_amount_minor,receiver_amount_minor,status,expires_at,provider_payload,created_at,updated_at) VALUES($1,'payin',$2,$3,$4,$5,$6,$7,$8,$9,NULLIF($10,''),$11,$12,$13,$14,$15,'open',$16,$17,$18,$18)`, id, s.provider.Name(), result.ProviderQuoteID, r.TenantID, r.IdempotencyKey, r.FundingMethod, r.CurrencyType, r.CoverFees, r.AmountMinor, r.SourceInstrumentID, r.DestinationAccountID, result.SourceCurrency, result.DestinationCurrency, result.SenderAmountMinor, result.ReceiverAmountMinor, result.ExpiresAt, payload, now)
	if err != nil {
		return nil, fmt.Errorf("store payin quote: %w", err)
	}
	return &Quote{Direction: "payin", ID: id, Provider: s.provider.Name(), TenantID: r.TenantID, FundingMethod: r.FundingMethod, SourceInstrumentID: r.SourceInstrumentID, DestinationAccountID: r.DestinationAccountID, SourceCurrency: result.SourceCurrency, DestinationCurrency: result.DestinationCurrency, SenderAmountMinor: result.SenderAmountMinor, ReceiverAmountMinor: result.ReceiverAmountMinor, Status: "open", ExpiresAt: result.ExpiresAt, CreatedAt: now, UpdatedAt: now}, nil
}

func (s *Service) CreatePayin(ctx context.Context, tenantID, quoteID, idempotencyKey string) (*Payin, error) {
	return s.createPayin(ctx, tenantID, quoteID, idempotencyKey, "payin:"+quoteID)
}

// CreatePayment creates the public payment aggregate and its provider pay-in
// operation atomically. Clients track the returned payment, not the payins row.
func (s *Service) CreatePayment(ctx context.Context, tenantID, quoteID, idempotencyKey, externalReference string) (*paymentcore.Payment, error) {
	p, err := s.createPayin(ctx, tenantID, quoteID, idempotencyKey, externalReference)
	if err != nil {
		return nil, err
	}
	return &paymentcore.Payment{ID: p.PaymentID, Direction: paymentcore.PaymentDirectionPayin, ExternalReference: externalReference, Currency: p.DestinationCurrency, AmountMinor: p.DestinationAmountMinor, TenantID: tenantID, PaymentStatus: paymentcore.PaymentStatusCreated, FundsStatus: paymentcore.FundsStatusPending, CreatedAt: p.CreatedAt, UpdatedAt: p.UpdatedAt}, nil
}

func (s *Service) createPayin(ctx context.Context, tenantID, quoteID, idempotencyKey, externalReference string) (*Payin, error) {
	if tenantID == "" || quoteID == "" || idempotencyKey == "" {
		return nil, errors.New("tenant, quote, and idempotency key are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var keyedQuote string
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(quote_id,'') FROM payins WHERE tenant_id=$1 AND idempotency_key=$2`, tenantID, idempotencyKey).Scan(&keyedQuote)
	if err == nil && keyedQuote != quoteID {
		return nil, ErrIdempotencyConflict
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	var providerQuote, status, quoteTenant, fundingMethod, sourceInstrumentID, destinationAccountID, sourceCurrency, destinationCurrency string
	var sourceAmountMinor, destinationAmountMinor int64
	var expires time.Time
	err = tx.QueryRowContext(ctx, `SELECT provider_quote_id,status,tenant_id,expires_at,payment_method,COALESCE(source_resource_id,''),destination_resource_id,sender_amount_minor,source_currency,receiver_amount_minor,destination_currency FROM payment_quotes WHERE direction='payin' AND id=$1 FOR UPDATE`, quoteID).Scan(&providerQuote, &status, &quoteTenant, &expires, &fundingMethod, &sourceInstrumentID, &destinationAccountID, &sourceAmountMinor, &sourceCurrency, &destinationAmountMinor, &destinationCurrency)
	if errors.Is(err, sql.ErrNoRows) || quoteTenant != tenantID {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var existing Payin
	err = tx.QueryRowContext(ctx, `SELECT id,payment_id,COALESCE(quote_id,''),provider,COALESCE(provider_payin_id,''),funding_method,COALESCE(source_instrument_id,''),destination_account_id,source_amount_minor,source_currency,destination_amount_minor,destination_currency,status,instructions,created_at,updated_at FROM payins WHERE quote_id=$1`, quoteID).Scan(&existing.ID, &existing.PaymentID, &existing.QuoteID, &existing.Provider, &existing.ProviderPayinID, &existing.FundingMethod, &existing.SourceInstrumentID, &existing.DestinationAccountID, &existing.SourceAmountMinor, &existing.SourceCurrency, &existing.DestinationAmountMinor, &existing.DestinationCurrency, &existing.Status, &existing.Instructions, &existing.CreatedAt, &existing.UpdatedAt)
	if err == nil {
		return &existing, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if status != "open" || !expires.After(s.now()) {
		return nil, errors.New("payin quote expired or already accepted")
	}
	id, err := s.newID("pin_")
	if err != nil {
		return nil, err
	}
	now := s.now()
	paymentID, err := s.newID("pay_")
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO payments(id,direction,external_reference,currency,amount_minor,tenant_id,payment_status,funds_status,idempotency_key,created_at,updated_at) VALUES($1,'payin',$2,$3,$4,$5,'created','pending',$6,$7,$7)`, paymentID, externalReference, destinationCurrency, destinationAmountMinor, tenantID, idempotencyKey, now)
	if err != nil {
		return nil, fmt.Errorf("store payin payment: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO payment_audit_events(payment_id,event,message,occurred_at) VALUES($1,'created','payin payment intent created',$2)`, paymentID, now); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO payment_timeline_entries(payment_id,payment_status,note,occurred_at) VALUES($1,'created','payin payment created',$2)`, paymentID, now); err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO payins(id,payment_id,quote_id,tenant_id,idempotency_key,funding_method,source_instrument_id,destination_account_id,source_amount_minor,source_currency,destination_amount_minor,destination_currency,provider,provider_payin_id,status,instructions,provider_payload,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,NULLIF($7,''),$8,$9,$10,$11,$12,$13,NULL,'created','{}','{}',$14,$14)`, id, paymentID, quoteID, tenantID, idempotencyKey, fundingMethod, sourceInstrumentID, destinationAccountID, sourceAmountMinor, sourceCurrency, destinationAmountMinor, destinationCurrency, s.provider.Name(), now)
	if err != nil {
		return nil, fmt.Errorf("store payin: %w", err)
	}
	accepted, err := tx.ExecContext(ctx, `UPDATE payment_quotes SET status='accepted',payment_id=$1,updated_at=$2 WHERE id=$3 AND direction='payin' AND status='open'`, paymentID, now, quoteID)
	if err != nil {
		return nil, err
	}
	if rows, err := accepted.RowsAffected(); err != nil || rows != 1 {
		return nil, errors.New("payin quote was consumed concurrently")
	}
	eventID := "evt_" + id + "_created"
	eventBody, _ := json.Marshal(map[string]string{"payin_id": id})
	if _, err := tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,'payin.created',$3,$4,'payment',$5,$6)`, eventID, EventsTopic, eventbus.PayinCreatedVersion, paymentID, eventBody, now); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &Payin{ID: id, PaymentID: paymentID, QuoteID: quoteID, Provider: s.provider.Name(), FundingMethod: fundingMethod, SourceInstrumentID: sourceInstrumentID, DestinationAccountID: destinationAccountID, SourceAmountMinor: sourceAmountMinor, SourceCurrency: sourceCurrency, DestinationAmountMinor: destinationAmountMinor, DestinationCurrency: destinationCurrency, Status: StatusCreated, Instructions: json.RawMessage(`{}`), CreatedAt: now, UpdatedAt: now}, nil
}

// ExecutePayin is called by the Kafka command worker. The durable pay-in row
// exists before the provider call, and retries reuse the original idempotency key.
func (s *Service) ExecutePayin(ctx context.Context, payinID string) (ExecuteResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ExecuteResult{}, err
	}
	defer tx.Rollback()
	var providerQuote, key, status string
	var providerID sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT q.provider_quote_id,p.idempotency_key,p.status,p.provider_payin_id FROM payins p JOIN payment_quotes q ON q.id=p.quote_id WHERE p.id=$1 AND p.provider=$2 FOR UPDATE`, payinID, s.provider.Name()).Scan(&providerQuote, &key, &status, &providerID)
	if errors.Is(err, sql.ErrNoRows) {
		return ExecuteResult{}, ErrNotFound
	}
	if err != nil {
		return ExecuteResult{}, err
	}
	if providerID.Valid && providerID.String != "" {
		var instructions, payload json.RawMessage
		if err := tx.QueryRowContext(ctx, `SELECT instructions,provider_payload FROM payins WHERE id=$1`, payinID).Scan(&instructions, &payload); err != nil {
			return ExecuteResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return ExecuteResult{}, err
		}
		return ExecuteResult{ProviderPayinID: providerID.String, Status: PayinStatus(status), Instructions: instructions, Payload: payload}, nil
	}
	if status != "created" && status != "submission_pending" && status != "unknown" {
		return ExecuteResult{}, fmt.Errorf("payin cannot execute from status %s", status)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE payins SET status='submission_pending',updated_at=$1 WHERE id=$2`, s.now(), payinID); err != nil {
		return ExecuteResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return ExecuteResult{}, err
	}
	result, err := s.provider.ExecutePayin(ctx, ExecuteRequest{IdempotencyKey: key, QuoteID: providerQuote})
	if err != nil {
		state := "unknown"
		var providerErr *ProviderError
		if errors.As(err, &providerErr) && !providerErr.Retryable {
			state = "failed"
		}
		_, updateErr := s.db.ExecContext(ctx, `UPDATE payins SET status=$1,failure_reason=$2,updated_at=$3 WHERE id=$4 AND status='submission_pending'`, state, err.Error(), s.now(), payinID)
		if updateErr != nil {
			return ExecuteResult{}, fmt.Errorf("execute payin: %w (record outcome: %v)", err, updateErr)
		}
		return ExecuteResult{}, err
	}
	if err := result.Validate(); err != nil {
		return ExecuteResult{}, err
	}
	return result, nil
}

// ApplyResult records a provider result and its accounting/merchant effects in
// the inbox transaction that consumed the execution command.
func (s *Service) ApplyResult(ctx context.Context, tx *sql.Tx, payinID, correlationID string, result ExecuteResult, now time.Time) error {
	payload, instructions := result.Payload, result.Instructions
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	if len(instructions) == 0 {
		instructions = json.RawMessage(`{}`)
	}
	var tenantID, paymentID string
	if err := tx.QueryRowContext(ctx, `SELECT tenant_id,payment_id FROM payins WHERE id=$1 FOR UPDATE`, payinID).Scan(&tenantID, &paymentID); err != nil {
		return err
	}
	lifecycleStatus := result.Status
	if lifecycleStatus == StatusSucceeded {
		lifecycleStatus = StatusReceived
	}
	if _, err := tx.ExecContext(ctx, `UPDATE payins SET provider_payin_id=NULLIF($1,''),status=$2,instructions=$3,provider_payload=$4,failure_reason=NULLIF($5,''),updated_at=$6 WHERE id=$7`, result.ProviderPayinID, lifecycleStatus, instructions, payload, result.FailureReason, now, payinID); err != nil {
		return err
	}
	paymentStatus, fundsStatus := paymentcore.PaymentStatusProcessing, paymentcore.FundsStatusPending
	if lifecycleStatus == StatusReceived {
		fundsStatus = paymentcore.FundsStatusReceived
	}
	if lifecycleStatus == StatusFailed {
		paymentStatus, fundsStatus = paymentcore.PaymentStatusFailed, paymentcore.FundsStatusReturned
	}
	eventType, eventID := "payin."+string(lifecycleStatus), "evt_"+payinID+"_"+string(lifecycleStatus)
	if _, err := tx.ExecContext(ctx, `UPDATE payments SET payment_status=$1,funds_status=$2,updated_at=$3 WHERE id=$4`, paymentStatus, fundsStatus, now, paymentID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO payment_timeline_entries(payment_id,payment_status,note,occurred_at) VALUES($1,$2,$3,$4)`, paymentID, paymentStatus, eventType, now); err != nil {
		return err
	}
	body, _ := json.Marshal(map[string]any{"id": eventID, "type": eventType, "payin_id": payinID, "correlation_id": correlationID, "reason": result.FailureReason, "occurred_at": now, "data": map[string]any{"status": result.Status}})
	if _, err := tx.ExecContext(ctx, `INSERT INTO webhook_deliveries(id,endpoint_id,event_id,payment_id,event_type,payload,next_attempt_at,created_at) SELECT 'whd_'||md5(id||$1),id,$1,$2,$3,$4,$5,$5 FROM webhook_endpoints WHERE tenant_id=$6 AND active ON CONFLICT(endpoint_id,event_id) DO NOTHING`, eventID, paymentID, eventType, body, now, tenantID); err != nil {
		return err
	}
	version := map[PayinStatus]int{StatusProcessing: eventbus.PayinProcessingVersion, StatusOnHold: eventbus.PayinOnHoldVersion, StatusReceived: eventbus.PayinReceivedVersion, StatusFailed: eventbus.PayinFailedVersion, StatusRefunded: eventbus.PayinRefundedVersion}[lifecycleStatus]
	_, err := tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5,'payment',$6,$7) ON CONFLICT(id) DO NOTHING`, eventID, EventsTopic, eventType, version, paymentID, body, now)
	return err
}

func (s *Service) RecordLedger(ctx context.Context, tx *sql.Tx, payinID, correlationID string, now time.Time) error {
	var tenantID, paymentID, status, currency string
	var amount int64
	if err := tx.QueryRowContext(ctx, `SELECT tenant_id,payment_id,status,destination_amount_minor,destination_currency FROM payins WHERE id=$1 FOR UPDATE`, payinID).Scan(&tenantID, &paymentID, &status, &amount, &currency); err != nil {
		return err
	}
	if status == "succeeded" {
		return nil
	}
	if status != "received" {
		return fmt.Errorf("payin %s cannot record ledger from status %s", payinID, status)
	}
	journalID := "jrn_" + payinID + "_succeeded"
	res, err := tx.ExecContext(ctx, `INSERT INTO ledger_transactions(id,payment_id,event_type,occurred_at) VALUES($1,$2,'payin.succeeded',$3) ON CONFLICT(payment_id,event_type) DO NOTHING`, journalID, paymentID, now)
	if err != nil {
		return err
	}
	rows, _ := res.RowsAffected()
	if rows == 1 {
		for _, line := range []struct{ suffix, account, side string }{{"debit", "cash:operating", "debit"}, {"credit", "settlement:payable", "credit"}} {
			if _, err := tx.ExecContext(ctx, `INSERT INTO ledger_entries(id,transaction_id,account_code,side,amount_minor,currency) VALUES($1,$2,$3,$4,$5,$6)`, journalID+":"+line.suffix, journalID, line.account, line.side, amount, currency); err != nil {
				return err
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE payins SET status='succeeded',updated_at=$1 WHERE id=$2`, now, payinID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE payments SET payment_status='succeeded',funds_status='received',updated_at=$1 WHERE id=$2`, now, paymentID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO payment_timeline_entries(payment_id,payment_status,note,occurred_at) VALUES($1,'succeeded','payin ledger recorded',$2)`, paymentID, now); err != nil {
		return err
	}
	eventID, eventType := "evt_"+payinID+"_succeeded", "payin.succeeded"
	body, _ := json.Marshal(map[string]any{"id": eventID, "type": eventType, "payin_id": payinID, "correlation_id": correlationID, "occurred_at": now, "data": map[string]string{"status": "succeeded"}})
	if _, err := tx.ExecContext(ctx, `INSERT INTO webhook_deliveries(id,endpoint_id,event_id,payment_id,event_type,payload,next_attempt_at,created_at) SELECT 'whd_'||md5(id||$1),id,$1,$2,$3,$4,$5,$5 FROM webhook_endpoints WHERE tenant_id=$6 AND active ON CONFLICT(endpoint_id,event_id) DO NOTHING`, eventID, paymentID, eventType, body, now, tenantID); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5,'payment',$6,$7) ON CONFLICT(id) DO NOTHING`, eventID, EventsTopic, eventType, eventbus.PayinSucceededVersion, paymentID, body, now)
	return err
}

func (s *Service) Get(ctx context.Context, tenantID, id string) (*Payin, error) {
	var p Payin
	err := s.db.QueryRowContext(ctx, `SELECT id,COALESCE(quote_id,''),provider,COALESCE(provider_payin_id,''),funding_method,COALESCE(source_instrument_id,''),destination_account_id,source_amount_minor,source_currency,destination_amount_minor,destination_currency,status,instructions,created_at,updated_at FROM payins WHERE id=$1 AND tenant_id=$2`, id, tenantID).Scan(&p.ID, &p.QuoteID, &p.Provider, &p.ProviderPayinID, &p.FundingMethod, &p.SourceInstrumentID, &p.DestinationAccountID, &p.SourceAmountMinor, &p.SourceCurrency, &p.DestinationAmountMinor, &p.DestinationCurrency, &p.Status, &p.Instructions, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &p, err
}
