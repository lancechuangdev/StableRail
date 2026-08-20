// Package paymentapi exposes the payment command service over HTTP.
package paymentapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"stablerail/paymentcore"
	"stablerail/paymentcore/payin"
	"stablerail/paymentcore/payout"
)

type PaymentStore interface {
	CreatePayment(context.Context, string, string, int64, string, string) (*paymentcore.Payment, error)
	CreatePaymentWithPayoutQuote(context.Context, string, string, int64, string, string, string) (*paymentcore.Payment, error)
	CreateRefund(context.Context, string, string, string, int64, string, string) (*paymentcore.Refund, error)
	GetPayment(context.Context, string) (*paymentcore.Payment, error)
	Timeline(context.Context, string) ([]paymentcore.TimelineEntry, error)
}
type DestinationPaymentStore interface {
	CreatePaymentWithDestination(context.Context, string, string, int64, string, string, paymentcore.Destination) (*paymentcore.Payment, error)
}
type Health interface{ PingContext(context.Context) error }
type PayoutQuoteService interface {
	CreateQuote(context.Context, payout.QuoteRequest) (payout.QuoteResult, error)
}
type PayinPaymentService interface {
	CreatePayment(context.Context, string, string, string, string) (*paymentcore.Payment, error)
	CreateQuote(context.Context, payin.QuoteRequest) (*payin.Quote, error)
}

type Handler struct {
	payments     PaymentStore
	health       Health
	payoutQuotes PayoutQuoteService
	payins       PayinPaymentService
}

func NewHandler(payments PaymentStore, health Health, payoutQuotes PayoutQuoteService, payinServices ...PayinPaymentService) (http.Handler, error) {
	if payments == nil || health == nil {
		return nil, errors.New("payment API and health dependencies are required")
	}
	h := &Handler{payments: payments, health: health, payoutQuotes: payoutQuotes}
	if len(payinServices) > 0 {
		h.payins = payinServices[0]
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/payments", h.create)
	if payoutQuotes != nil || h.payins != nil {
		mux.HandleFunc("POST /v1/payment-quotes", h.createPaymentQuote)
	}
	mux.HandleFunc("GET /v1/payments/{id}", h.get)
	mux.HandleFunc("GET /v1/payments/{id}/timeline", h.timeline)
	mux.HandleFunc("POST /v1/payments/{id}/refunds", h.createRefund)
	mux.HandleFunc("GET /healthz", h.live)
	mux.HandleFunc("GET /readyz", h.ready)
	return mux, nil
}

type createRefundRequest struct {
	AmountMinor   int64  `json:"amount_minor"`
	Reason        string `json:"reason"`
	PayoutQuoteID string `json:"payout_quote_id,omitempty"`
}

func (h *Handler) createRefund(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" || len(key) > 255 {
		problem(w, http.StatusBadRequest, "a valid Idempotency-Key header is required")
		return
	}
	var input createRefundRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		problem(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		problem(w, http.StatusBadRequest, "request body must contain one JSON object")
		return
	}
	input.Reason = strings.TrimSpace(input.Reason)
	if input.AmountMinor <= 0 || input.Reason == "" {
		problem(w, http.StatusBadRequest, "positive amount_minor and reason are required")
		return
	}
	tenantID, ok := TenantIDFromContext(r.Context())
	if !ok {
		problem(w, http.StatusUnauthorized, "authentication required")
		return
	}
	refund, err := h.payments.CreateRefund(r.Context(), r.PathValue("id"), tenantID, key, input.AmountMinor, input.Reason, strings.TrimSpace(input.PayoutQuoteID))
	if err != nil {
		switch {
		case errors.Is(err, paymentcore.ErrPaymentNotFound):
			problem(w, http.StatusNotFound, "payment not found")
		case errors.Is(err, paymentcore.ErrIdempotencyConflict), errors.Is(err, paymentcore.ErrPaymentNotRefundable), errors.Is(err, paymentcore.ErrRefundAmountExceeded):
			problem(w, http.StatusConflict, err.Error())
		default:
			problem(w, http.StatusInternalServerError, "could not create refund")
		}
		return
	}
	writeJSON(w, http.StatusAccepted, refund)
}

type createRequest struct {
	Direction         paymentcore.PaymentDirection `json:"direction"`
	ExternalReference string                       `json:"external_reference"`
	Currency          string                       `json:"currency"`
	AmountMinor       int64                        `json:"amount_minor"`
	TenantID          string                       `json:"tenant_id"`
	Destination       *paymentcore.Destination     `json:"destination,omitempty"`
	QuoteID           string                       `json:"quote_id,omitempty"`
	PayoutQuoteID     string                       `json:"payout_quote_id,omitempty"` // deprecated input alias
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" || len(key) > 255 {
		problem(w, http.StatusBadRequest, "a valid Idempotency-Key header is required")
		return
	}
	var input createRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		problem(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		problem(w, http.StatusBadRequest, "request body must contain one JSON object")
		return
	}
	input.Currency = strings.ToUpper(strings.TrimSpace(input.Currency))
	if input.Direction != paymentcore.PaymentDirectionPayin && input.Direction != paymentcore.PaymentDirectionPayout {
		problem(w, http.StatusBadRequest, "direction must be payin or payout")
		return
	}
	if input.QuoteID == "" {
		input.QuoteID = input.PayoutQuoteID
	}
	tenantID := strings.TrimSpace(input.TenantID)
	if authenticated, ok := TenantIDFromContext(r.Context()); ok {
		if tenantID != "" && tenantID != authenticated {
			problem(w, http.StatusForbidden, "tenant_id does not match the authenticated tenant")
			return
		}
		tenantID = authenticated
	}
	if strings.TrimSpace(input.ExternalReference) == "" || tenantID == "" || (input.Direction == paymentcore.PaymentDirectionPayout && (len(input.Currency) < 3 || len(input.Currency) > 10 || input.AmountMinor <= 0)) {
		problem(w, http.StatusBadRequest, "external_reference, currency, positive amount_minor, and an authenticated tenant are required")
		return
	}
	if input.Destination != nil {
		if err := input.Destination.Validate(); err != nil {
			problem(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if input.Destination != nil && input.QuoteID != "" {
		problem(w, http.StatusBadRequest, "destination and payout_quote_id cannot be combined")
		return
	}
	var p *paymentcore.Payment
	var err error
	if input.Direction == paymentcore.PaymentDirectionPayin {
		if h.payins == nil || input.QuoteID == "" || input.Destination != nil {
			problem(w, http.StatusBadRequest, "payin payments require quote_id")
			return
		}
		p, err = h.payins.CreatePayment(r.Context(), tenantID, input.QuoteID, key, input.ExternalReference)
		if err != nil {
			problem(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusAccepted, p)
		return
	}
	if input.Destination != nil {
		store, ok := h.payments.(DestinationPaymentStore)
		if !ok {
			problem(w, http.StatusBadRequest, "payment destinations are not supported")
			return
		}
		p, err = store.CreatePaymentWithDestination(r.Context(), input.ExternalReference, input.Currency, input.AmountMinor, tenantID, key, *input.Destination)
	} else if input.QuoteID != "" {
		p, err = h.payments.CreatePaymentWithPayoutQuote(r.Context(), input.ExternalReference, input.Currency, input.AmountMinor, tenantID, key, input.QuoteID)
	} else {
		p, err = h.payments.CreatePayment(r.Context(), input.ExternalReference, input.Currency, input.AmountMinor, tenantID, key)
	}
	if err != nil {
		if errors.Is(err, paymentcore.ErrIdempotencyConflict) {
			problem(w, http.StatusConflict, err.Error())
			return
		}
		problem(w, http.StatusInternalServerError, "could not create payment")
		return
	}
	writeJSON(w, http.StatusCreated, p)
}

type createPayoutQuoteRequest struct {
	Direction               paymentcore.PaymentDirection `json:"direction"`
	FundingMethod           string                       `json:"funding_method,omitempty"`
	TenantID                string                       `json:"tenant_id"`
	SourceAccountID         string                       `json:"source_account_id"`
	DestinationInstrumentID string                       `json:"destination_instrument_id"`
	SourceCurrency          string                       `json:"source_currency"`
	DestinationCurrency     string                       `json:"destination_currency"`
	CurrencyType            string                       `json:"currency_type"`
	CoverFees               bool                         `json:"cover_fees"`
	RequestAmountMinor      int64                        `json:"request_amount_minor"`
}

func (h *Handler) createPaymentQuote(w http.ResponseWriter, r *http.Request) {
	var probe struct {
		Direction paymentcore.PaymentDirection `json:"direction"`
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil || json.Unmarshal(body, &probe) != nil {
		problem(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}
	r.Body = io.NopCloser(strings.NewReader(string(body)))
	if probe.Direction == paymentcore.PaymentDirectionPayin {
		h.createPayinQuote(w, r)
		return
	}
	if probe.Direction != paymentcore.PaymentDirectionPayout {
		problem(w, http.StatusBadRequest, "direction must be payin or payout")
		return
	}
	h.createPayoutQuote(w, r)
}

func (h *Handler) createPayinQuote(w http.ResponseWriter, r *http.Request) {
	if h.payins == nil {
		problem(w, http.StatusNotImplemented, "payin quotes are not configured")
		return
	}
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	tenant, ok := TenantIDFromContext(r.Context())
	if !ok || key == "" {
		problem(w, http.StatusBadRequest, "authentication and Idempotency-Key are required")
		return
	}
	var in struct {
		Direction            paymentcore.PaymentDirection `json:"direction"`
		FundingMethod        string                       `json:"funding_method"`
		CurrencyType         string                       `json:"currency_type"`
		SourceInstrumentID   string                       `json:"source_instrument_id,omitempty"`
		DestinationAccountID string                       `json:"destination_account_id"`
		SourceCurrency       string                       `json:"source_currency"`
		DestinationCurrency  string                       `json:"destination_currency"`
		AmountMinor          int64                        `json:"amount_minor"`
		CoverFees            bool                         `json:"cover_fees"`
	}
	if decodePayin(w, r, &in) != nil {
		return
	}
	q, err := h.payins.CreateQuote(r.Context(), payin.QuoteRequest{IdempotencyKey: key, TenantID: tenant, FundingMethod: in.FundingMethod, CurrencyType: in.CurrencyType, SourceInstrumentID: in.SourceInstrumentID, DestinationAccountID: in.DestinationAccountID, SourceCurrency: in.SourceCurrency, DestinationCurrency: in.DestinationCurrency, AmountMinor: in.AmountMinor, CoverFees: in.CoverFees})
	if err != nil {
		problem(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, q)
}

func decodePayin(w http.ResponseWriter, r *http.Request, out any) error {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		problem(w, http.StatusBadRequest, "invalid JSON request body")
		return err
	}
	var extra any
	if err := d.Decode(&extra); !errors.Is(err, io.EOF) {
		problem(w, http.StatusBadRequest, "request body must contain one JSON object")
		return errors.New("extra JSON")
	}
	return nil
}

func (h *Handler) createPayoutQuote(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" || len(key) > 255 {
		problem(w, http.StatusBadRequest, "a valid Idempotency-Key header is required")
		return
	}
	var input createPayoutQuoteRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		problem(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		problem(w, http.StatusBadRequest, "request body must contain one JSON object")
		return
	}
	tenantID := strings.TrimSpace(input.TenantID)
	if authenticated, ok := TenantIDFromContext(r.Context()); ok {
		if tenantID != "" && tenantID != authenticated {
			problem(w, http.StatusForbidden, "tenant_id does not match the authenticated tenant")
			return
		}
		tenantID = authenticated
	}
	q, err := h.payoutQuotes.CreateQuote(r.Context(), payout.QuoteRequest{IdempotencyKey: key, TenantID: tenantID, SourceAccountID: input.SourceAccountID, DestinationInstrumentID: input.DestinationInstrumentID, SourceCurrency: input.SourceCurrency, DestinationCurrency: input.DestinationCurrency, CurrencyType: input.CurrencyType, CoverFees: input.CoverFees, RequestAmountMinor: input.RequestAmountMinor})
	if err != nil {
		problem(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, q)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	p, err := h.payments.GetPayment(r.Context(), r.PathValue("id"))
	if err != nil {
		h.storeError(w, err)
		return
	}
	if tenantID, ok := TenantIDFromContext(r.Context()); ok && p.TenantID != tenantID {
		problem(w, http.StatusNotFound, "payment not found")
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (h *Handler) timeline(w http.ResponseWriter, r *http.Request) {
	if tenantID, ok := TenantIDFromContext(r.Context()); ok {
		payment, err := h.payments.GetPayment(r.Context(), r.PathValue("id"))
		if err != nil {
			h.storeError(w, err)
			return
		}
		if payment.TenantID != tenantID {
			problem(w, http.StatusNotFound, "payment not found")
			return
		}
	}
	timeline, err := h.payments.Timeline(r.Context(), r.PathValue("id"))
	if err != nil {
		h.storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"payment_id": r.PathValue("id"), "timeline": timeline})
}

func (h *Handler) storeError(w http.ResponseWriter, err error) {
	if errors.Is(err, paymentcore.ErrPaymentNotFound) {
		problem(w, http.StatusNotFound, "payment not found")
		return
	}
	problem(w, http.StatusInternalServerError, "payment service unavailable")
}
func (h *Handler) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
func (h *Handler) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2_000_000_000)
	defer cancel()
	if err := h.health.PingContext(ctx); err != nil {
		problem(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
func problem(w http.ResponseWriter, status int, detail string) {
	writeJSON(w, status, map[string]string{"error": detail})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		_ = fmt.Errorf("encode response: %w", err)
	}
}
