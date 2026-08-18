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
	"stablerail/settlement/blindpay"
)

type PaymentStore interface {
	CreatePayment(context.Context, string, string, int64, string, string) (*paymentcore.Payment, error)
	CreatePaymentWithPayoutQuote(context.Context, string, string, int64, string, string, string) (*paymentcore.Payment, error)
	GetPayment(context.Context, string) (*paymentcore.Payment, error)
	Timeline(context.Context, string) ([]paymentcore.TimelineEntry, error)
}
type DestinationPaymentStore interface {
	CreatePaymentWithDestination(context.Context, string, string, int64, string, string, paymentcore.Destination) (*paymentcore.Payment, error)
}
type RefundStore interface {
	CreateRefund(context.Context, string, string, string, int64, string) (*paymentcore.Refund, error)
}

type Health interface{ PingContext(context.Context) error }

type BlindPayPayoutQuoteService interface {
	Create(context.Context, blindpay.PayoutQuoteRequest) (*blindpay.PayoutQuote, error)
}

type Handler struct {
	payments     PaymentStore
	health       Health
	payoutQuotes BlindPayPayoutQuoteService
}

func NewHandler(payments PaymentStore, health Health, payoutQuotes BlindPayPayoutQuoteService) (http.Handler, error) {
	if payments == nil || health == nil {
		return nil, errors.New("payment API and health dependencies are required")
	}
	h := &Handler{payments: payments, health: health, payoutQuotes: payoutQuotes}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/payments", h.create)
	if payoutQuotes != nil {
		mux.HandleFunc("POST /v1/blindpay/payout-quotes", h.createPayoutQuote)
	}
	mux.HandleFunc("GET /v1/payments/{id}", h.get)
	mux.HandleFunc("GET /v1/payments/{id}/timeline", h.timeline)
	mux.HandleFunc("POST /v1/payments/{id}/refunds", h.createRefund)
	mux.HandleFunc("GET /healthz", h.live)
	mux.HandleFunc("GET /readyz", h.ready)
	return mux, nil
}

type createRefundRequest struct {
	AmountMinor int64  `json:"amount_minor"`
	Reason      string `json:"reason"`
}

func (h *Handler) createRefund(w http.ResponseWriter, r *http.Request) {
	store, ok := h.payments.(RefundStore)
	if !ok {
		problem(w, http.StatusNotImplemented, "refunds are not supported")
		return
	}
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
	refund, err := store.CreateRefund(r.Context(), r.PathValue("id"), tenantID, key, input.AmountMinor, input.Reason)
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
	ExternalReference string                   `json:"external_reference"`
	Currency          string                   `json:"currency"`
	AmountMinor       int64                    `json:"amount_minor"`
	TenantID          string                   `json:"tenant_id"`
	Destination       *paymentcore.Destination `json:"destination,omitempty"`
	PayoutQuoteID     string                   `json:"payout_quote_id,omitempty"`
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
	tenantID := strings.TrimSpace(input.TenantID)
	if authenticated, ok := TenantIDFromContext(r.Context()); ok {
		if tenantID != "" && tenantID != authenticated {
			problem(w, http.StatusForbidden, "tenant_id does not match the authenticated tenant")
			return
		}
		tenantID = authenticated
	}
	if strings.TrimSpace(input.ExternalReference) == "" || len(input.Currency) < 3 || len(input.Currency) > 10 || input.AmountMinor <= 0 || tenantID == "" {
		problem(w, http.StatusBadRequest, "external_reference, currency, positive amount_minor, and an authenticated tenant are required")
		return
	}
	if input.Destination != nil {
		if err := input.Destination.Validate(); err != nil {
			problem(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if input.Destination != nil && input.PayoutQuoteID != "" {
		problem(w, http.StatusBadRequest, "destination and payout_quote_id cannot be combined")
		return
	}
	var p *paymentcore.Payment
	var err error
	if input.Destination != nil {
		store, ok := h.payments.(DestinationPaymentStore)
		if !ok {
			problem(w, http.StatusBadRequest, "payment destinations are not supported")
			return
		}
		p, err = store.CreatePaymentWithDestination(r.Context(), input.ExternalReference, input.Currency, input.AmountMinor, tenantID, key, *input.Destination)
	} else if input.PayoutQuoteID != "" {
		p, err = h.payments.CreatePaymentWithPayoutQuote(r.Context(), input.ExternalReference, input.Currency, input.AmountMinor, tenantID, key, input.PayoutQuoteID)
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
	TenantID            string `json:"tenant_id"`
	BankAccountID       string `json:"bank_account_id"`
	ManagedWalletID     string `json:"managed_wallet_id"`
	DestinationCurrency string `json:"destination_currency"`
	CurrencyType        string `json:"currency_type"`
	CoverFees           bool   `json:"cover_fees"`
	RequestAmountMinor  int64  `json:"request_amount_minor"`
	PartnerFeeID        string `json:"partner_fee_id,omitempty"`
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
	q, err := h.payoutQuotes.Create(r.Context(), blindpay.PayoutQuoteRequest{IdempotencyKey: key, TenantID: tenantID, BankAccountID: input.BankAccountID, ManagedWalletID: input.ManagedWalletID, DestinationCurrency: input.DestinationCurrency, CurrencyType: input.CurrencyType, CoverFees: input.CoverFees, RequestAmountMinor: input.RequestAmountMinor, PartnerFeeID: input.PartnerFeeID})
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
