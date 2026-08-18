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
)

var ErrNotFound = errors.New("payin not found")
var ErrIdempotencyConflict = errors.New("idempotency key is bound to another payin quote")

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
	err := s.db.QueryRowContext(ctx, `SELECT id,provider,tenant_id,funding_method,COALESCE(source_instrument_id,''),destination_account_id,source_currency,destination_currency,sender_amount_minor,receiver_amount_minor,status,expires_at,created_at,updated_at,currency_type,cover_fees,request_amount_minor FROM payin_quotes WHERE tenant_id=$1 AND idempotency_key=$2`, r.TenantID, r.IdempotencyKey).Scan(&existing.ID, &existing.Provider, &existing.TenantID, &existing.FundingMethod, &existing.SourceInstrumentID, &existing.DestinationAccountID, &existing.SourceCurrency, &existing.DestinationCurrency, &existing.SenderAmountMinor, &existing.ReceiverAmountMinor, &existing.Status, &existing.ExpiresAt, &existing.CreatedAt, &existing.UpdatedAt, &existingCurrencyType, &existingCoverFees, &existingRequestAmount)
	if err == nil {
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
	_, err = s.db.ExecContext(ctx, `INSERT INTO payin_quotes(id,provider,provider_quote_id,tenant_id,idempotency_key,funding_method,currency_type,cover_fees,request_amount_minor,source_instrument_id,destination_account_id,source_currency,destination_currency,sender_amount_minor,receiver_amount_minor,status,expires_at,provider_payload,created_at,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,NULLIF($10,''),$11,$12,$13,$14,$15,'open',$16,$17,$18,$18)`, id, s.provider.Name(), result.ProviderQuoteID, r.TenantID, r.IdempotencyKey, r.FundingMethod, r.CurrencyType, r.CoverFees, r.AmountMinor, r.SourceInstrumentID, r.DestinationAccountID, result.SourceCurrency, result.DestinationCurrency, result.SenderAmountMinor, result.ReceiverAmountMinor, result.ExpiresAt, payload, now)
	if err != nil {
		return nil, fmt.Errorf("store payin quote: %w", err)
	}
	return &Quote{ID: id, Provider: s.provider.Name(), TenantID: r.TenantID, FundingMethod: r.FundingMethod, SourceInstrumentID: r.SourceInstrumentID, DestinationAccountID: r.DestinationAccountID, SourceCurrency: result.SourceCurrency, DestinationCurrency: result.DestinationCurrency, SenderAmountMinor: result.SenderAmountMinor, ReceiverAmountMinor: result.ReceiverAmountMinor, Status: "open", ExpiresAt: result.ExpiresAt, CreatedAt: now, UpdatedAt: now}, nil
}

func (s *Service) CreatePayin(ctx context.Context, tenantID, quoteID, idempotencyKey string) (*Payin, error) {
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
	err = tx.QueryRowContext(ctx, `SELECT provider_quote_id,status,tenant_id,expires_at,funding_method,COALESCE(source_instrument_id,''),destination_account_id,sender_amount_minor,source_currency,receiver_amount_minor,destination_currency FROM payin_quotes WHERE id=$1 FOR UPDATE`, quoteID).Scan(&providerQuote, &status, &quoteTenant, &expires, &fundingMethod, &sourceInstrumentID, &destinationAccountID, &sourceAmountMinor, &sourceCurrency, &destinationAmountMinor, &destinationCurrency)
	if errors.Is(err, sql.ErrNoRows) || quoteTenant != tenantID {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var existing Payin
	err = tx.QueryRowContext(ctx, `SELECT id,COALESCE(quote_id,''),provider,provider_payin_id,funding_method,COALESCE(source_instrument_id,''),destination_account_id,source_amount_minor,source_currency,destination_amount_minor,destination_currency,status,instructions,created_at,updated_at FROM payins WHERE quote_id=$1`, quoteID).Scan(&existing.ID, &existing.QuoteID, &existing.Provider, &existing.ProviderPayinID, &existing.FundingMethod, &existing.SourceInstrumentID, &existing.DestinationAccountID, &existing.SourceAmountMinor, &existing.SourceCurrency, &existing.DestinationAmountMinor, &existing.DestinationCurrency, &existing.Status, &existing.Instructions, &existing.CreatedAt, &existing.UpdatedAt)
	if err == nil {
		return &existing, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if status != "open" || !expires.After(s.now()) {
		return nil, errors.New("payin quote expired or already accepted")
	}
	result, err := s.provider.ExecutePayin(ctx, ExecuteRequest{IdempotencyKey: idempotencyKey, QuoteID: providerQuote})
	if err != nil {
		return nil, err
	}
	if err := result.Validate(); err != nil {
		return nil, err
	}
	id, err := s.newID("pin_")
	if err != nil {
		return nil, err
	}
	now := s.now()
	instructions := result.Instructions
	if len(instructions) == 0 {
		instructions = json.RawMessage(`{}`)
	}
	payload := result.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO payins(id,quote_id,tenant_id,idempotency_key,funding_method,source_instrument_id,destination_account_id,source_amount_minor,source_currency,destination_amount_minor,destination_currency,provider,provider_payin_id,status,instructions,provider_payload,created_at,updated_at) VALUES($1,$2,$3,$4,$5,NULLIF($6,''),$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$17)`, id, quoteID, tenantID, idempotencyKey, fundingMethod, sourceInstrumentID, destinationAccountID, sourceAmountMinor, sourceCurrency, destinationAmountMinor, destinationCurrency, s.provider.Name(), result.ProviderPayinID, result.Status, instructions, payload, now)
	if err != nil {
		return nil, fmt.Errorf("store payin: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `UPDATE payin_quotes SET status='accepted',updated_at=$1 WHERE id=$2 AND status='open'`, now, quoteID); err != nil {
		return nil, err
	}
	if result.Status == StatusSucceeded {
		journalID := "jrn_" + id + "_succeeded"
		if _, err := tx.ExecContext(ctx, `INSERT INTO ledger_transactions(id,payment_id,payin_id,event_type,occurred_at) VALUES($1,NULL,$2,'payin.succeeded',$3)`, journalID, id, now); err != nil {
			return nil, err
		}
		for _, line := range []struct{ suffix, account, side string }{{"debit", "cash:operating", "debit"}, {"credit", "settlement:payable", "credit"}} {
			if _, err := tx.ExecContext(ctx, `INSERT INTO ledger_entries(id,transaction_id,account_code,side,amount_minor,currency) VALUES($1,$2,$3,$4,$5,$6)`, journalID+":"+line.suffix, journalID, line.account, line.side, destinationAmountMinor, destinationCurrency); err != nil {
				return nil, err
			}
		}
	}
	eventType := "payin." + string(result.Status)
	eventID := "evt_" + id + "_" + string(result.Status)
	eventBody, _ := json.Marshal(map[string]any{"id": eventID, "type": eventType, "payin_id": id, "occurred_at": now, "data": map[string]any{"quote_id": quoteID, "status": result.Status}})
	if _, err := tx.ExecContext(ctx, `INSERT INTO webhook_deliveries(id,endpoint_id,event_id,payment_id,payin_id,event_type,payload,next_attempt_at,created_at) SELECT 'whd_'||md5(id||$1),id,$1,NULL,$2,$3,$4,$5,$5 FROM webhook_endpoints WHERE tenant_id=$6 AND active ON CONFLICT(endpoint_id,event_id) DO NOTHING`, eventID, id, eventType, eventBody, now, tenantID); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &Payin{ID: id, QuoteID: quoteID, Provider: s.provider.Name(), ProviderPayinID: result.ProviderPayinID, FundingMethod: fundingMethod, SourceInstrumentID: sourceInstrumentID, DestinationAccountID: destinationAccountID, SourceAmountMinor: sourceAmountMinor, SourceCurrency: sourceCurrency, DestinationAmountMinor: destinationAmountMinor, DestinationCurrency: destinationCurrency, Status: result.Status, Instructions: instructions, CreatedAt: now, UpdatedAt: now}, nil
}

func (s *Service) Get(ctx context.Context, tenantID, id string) (*Payin, error) {
	var p Payin
	err := s.db.QueryRowContext(ctx, `SELECT id,COALESCE(quote_id,''),provider,provider_payin_id,funding_method,COALESCE(source_instrument_id,''),destination_account_id,source_amount_minor,source_currency,destination_amount_minor,destination_currency,status,instructions,created_at,updated_at FROM payins WHERE id=$1 AND tenant_id=$2`, id, tenantID).Scan(&p.ID, &p.QuoteID, &p.Provider, &p.ProviderPayinID, &p.FundingMethod, &p.SourceInstrumentID, &p.DestinationAccountID, &p.SourceAmountMinor, &p.SourceCurrency, &p.DestinationAmountMinor, &p.DestinationCurrency, &p.Status, &p.Instructions, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &p, err
}
