package payin

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"stablerail/eventbus"
	"stablerail/paymentcore"
)

type CreatePaymentRequest struct {
	TenantID             string
	IdempotencyKey       string
	ExternalReference    string
	QuoteID              string
	AmountMinor          int64
	Currency             string
	FundingMethod        string
	SourceInstrumentID   string
	DestinationAccountID string
}

func (r CreatePaymentRequest) Validate() error {
	if strings.TrimSpace(r.TenantID) == "" || strings.TrimSpace(r.IdempotencyKey) == "" || strings.TrimSpace(r.ExternalReference) == "" {
		return errors.New("tenant, idempotency key, and external reference are required")
	}
	direct := r.AmountMinor != 0 || r.Currency != "" || r.FundingMethod != "" || r.SourceInstrumentID != "" || r.DestinationAccountID != ""
	if r.QuoteID != "" {
		if direct {
			return errors.New("payin quote and direct payment fields cannot be combined")
		}
		return nil
	}
	if r.AmountMinor <= 0 || len(strings.TrimSpace(r.Currency)) < 3 || strings.TrimSpace(r.FundingMethod) == "" || strings.TrimSpace(r.DestinationAccountID) == "" {
		return errors.New("direct payin requires positive amount, currency, funding method, and destination account")
	}
	return nil
}

// CreatePayment creates the shared payment aggregate and its inbound operation
// atomically. Clients track the returned payment rather than the payins row.
func (s *Service) CreatePayment(ctx context.Context, request CreatePaymentRequest) (*paymentcore.Payment, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	request.Currency = strings.ToUpper(strings.TrimSpace(request.Currency))
	p, err := s.createPayin(ctx, request)
	if err != nil {
		return nil, err
	}
	return &paymentcore.Payment{ID: p.PaymentID, Direction: paymentcore.PaymentDirectionPayin, ExternalReference: request.ExternalReference, Currency: p.DestinationCurrency, AmountMinor: p.DestinationAmountMinor, TenantID: request.TenantID, PaymentStatus: paymentcore.PaymentStatusCreated, FundsStatus: paymentcore.FundsStatusPending, CreatedAt: p.CreatedAt, UpdatedAt: p.UpdatedAt}, nil
}

func (s *Service) createPayin(ctx context.Context, request CreatePaymentRequest) (*Payin, error) {
	tenantID, quoteID, idempotencyKey, externalReference := request.TenantID, request.QuoteID, request.IdempotencyKey, request.ExternalReference
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var keyedQuote, keyedExternalReference string
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(p.quote_id,''),pm.external_reference FROM payins p JOIN payments pm ON pm.id=p.payment_id WHERE p.tenant_id=$1 AND p.idempotency_key=$2`, tenantID, idempotencyKey).Scan(&keyedQuote, &keyedExternalReference)
	if err == nil && (keyedQuote != quoteID || keyedExternalReference != externalReference) {
		return nil, ErrIdempotencyConflict
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	var providerQuote, status, fundingMethod, sourceInstrumentID, destinationAccountID, sourceCurrency, destinationCurrency string
	var sourceAmountMinor, destinationAmountMinor int64
	var expires time.Time
	if quoteID != "" {
		var quoteTenant string
		err = tx.QueryRowContext(ctx, `SELECT provider_quote_id,status,tenant_id,expires_at,payment_method,COALESCE(source_resource_id,''),destination_resource_id,sender_amount_minor,source_currency,receiver_amount_minor,destination_currency FROM payment_quotes WHERE direction='payin' AND id=$1 FOR UPDATE`, quoteID).Scan(&providerQuote, &status, &quoteTenant, &expires, &fundingMethod, &sourceInstrumentID, &destinationAccountID, &sourceAmountMinor, &sourceCurrency, &destinationAmountMinor, &destinationCurrency)
		if errors.Is(err, sql.ErrNoRows) || quoteTenant != tenantID {
			return nil, ErrNotFound
		}
		if err != nil {
			return nil, err
		}
	} else {
		status = "open"
		fundingMethod, sourceInstrumentID, destinationAccountID = request.FundingMethod, request.SourceInstrumentID, request.DestinationAccountID
		sourceAmountMinor, destinationAmountMinor = request.AmountMinor, request.AmountMinor
		sourceCurrency, destinationCurrency = request.Currency, request.Currency
	}
	var existing Payin
	lookup := `SELECT id,payment_id,COALESCE(quote_id,''),provider,COALESCE(provider_payin_id,''),funding_method,COALESCE(source_instrument_id,''),destination_account_id,source_amount_minor,source_currency,destination_amount_minor,destination_currency,status,instructions,created_at,updated_at FROM payins WHERE quote_id=$1`
	lookupArg := quoteID
	if quoteID == "" {
		lookup = `SELECT id,payment_id,COALESCE(quote_id,''),provider,COALESCE(provider_payin_id,''),funding_method,COALESCE(source_instrument_id,''),destination_account_id,source_amount_minor,source_currency,destination_amount_minor,destination_currency,status,instructions,created_at,updated_at FROM payins WHERE tenant_id=$1 AND idempotency_key=$2`
		err = tx.QueryRowContext(ctx, lookup, tenantID, idempotencyKey).Scan(&existing.ID, &existing.PaymentID, &existing.QuoteID, &existing.Provider, &existing.ProviderPayinID, &existing.FundingMethod, &existing.SourceInstrumentID, &existing.DestinationAccountID, &existing.SourceAmountMinor, &existing.SourceCurrency, &existing.DestinationAmountMinor, &existing.DestinationCurrency, &existing.Status, &existing.Instructions, &existing.CreatedAt, &existing.UpdatedAt)
	} else {
		err = tx.QueryRowContext(ctx, lookup, lookupArg).Scan(&existing.ID, &existing.PaymentID, &existing.QuoteID, &existing.Provider, &existing.ProviderPayinID, &existing.FundingMethod, &existing.SourceInstrumentID, &existing.DestinationAccountID, &existing.SourceAmountMinor, &existing.SourceCurrency, &existing.DestinationAmountMinor, &existing.DestinationCurrency, &existing.Status, &existing.Instructions, &existing.CreatedAt, &existing.UpdatedAt)
	}
	if err == nil {
		if existing.QuoteID != quoteID || existing.FundingMethod != fundingMethod || existing.SourceInstrumentID != sourceInstrumentID || existing.DestinationAccountID != destinationAccountID || existing.SourceAmountMinor != sourceAmountMinor || existing.SourceCurrency != sourceCurrency || existing.DestinationAmountMinor != destinationAmountMinor || existing.DestinationCurrency != destinationCurrency {
			return nil, ErrIdempotencyConflict
		}
		return &existing, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if quoteID != "" && (status != "open" || !expires.After(s.now())) {
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
	_, err = tx.ExecContext(ctx, `INSERT INTO payins(id,payment_id,quote_id,tenant_id,idempotency_key,funding_method,source_instrument_id,destination_account_id,source_amount_minor,source_currency,destination_amount_minor,destination_currency,provider,provider_payin_id,status,instructions,provider_payload,created_at,updated_at) VALUES($1,$2,NULLIF($3,''),$4,$5,$6,NULLIF($7,''),$8,$9,$10,$11,$12,$13,NULL,'created','{}','{}',$14,$14)`, id, paymentID, quoteID, tenantID, idempotencyKey, fundingMethod, sourceInstrumentID, destinationAccountID, sourceAmountMinor, sourceCurrency, destinationAmountMinor, destinationCurrency, s.quoteProvider.Name(), now)
	if err != nil {
		return nil, fmt.Errorf("store payin: %w", err)
	}
	if quoteID != "" {
		accepted, err := tx.ExecContext(ctx, `UPDATE payment_quotes SET status='accepted',payment_id=$1,updated_at=$2 WHERE id=$3 AND direction='payin' AND status='open'`, paymentID, now, quoteID)
		if err != nil {
			return nil, err
		}
		if rows, err := accepted.RowsAffected(); err != nil || rows != 1 {
			return nil, errors.New("payin quote was consumed concurrently")
		}
	}
	eventID := "evt_" + id + "_created"
	eventBody, _ := json.Marshal(map[string]string{"payin_id": id})
	if _, err := tx.ExecContext(ctx, `INSERT INTO outbox_events(id,topic,event_type,event_version,aggregate_id,aggregate_type,payload,occurred_at) VALUES($1,$2,'payin.created',$3,$4,'payin',$5,$6)`, eventID, eventbus.PayinEventsTopic, eventbus.PayinCreatedVersion, paymentID, eventBody, now); err != nil {
		return nil, err
	}
	if err := enqueuePublicPaymentEvent(ctx, tx, eventID+"_payment", paymentID, "payment.created", eventbus.PaymentCreatedVersion, eventBody, now); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &Payin{ID: id, PaymentID: paymentID, QuoteID: quoteID, Provider: s.quoteProvider.Name(), FundingMethod: fundingMethod, SourceInstrumentID: sourceInstrumentID, DestinationAccountID: destinationAccountID, SourceAmountMinor: sourceAmountMinor, SourceCurrency: sourceCurrency, DestinationAmountMinor: destinationAmountMinor, DestinationCurrency: destinationCurrency, Status: StatusCreated, Instructions: json.RawMessage(`{}`), CreatedAt: now, UpdatedAt: now}, nil
}
