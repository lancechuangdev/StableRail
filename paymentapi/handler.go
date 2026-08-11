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
	"stablerail/quote"
)

type PaymentStore interface {
	CreatePayment(context.Context, string, string, int64, string, string) (*paymentcore.Payment, error)
	CreatePaymentWithQuote(context.Context, string, string, int64, string, string, string) (*paymentcore.Payment, error)
	GetPayment(context.Context, string) (*paymentcore.Payment, error)
	Timeline(context.Context, string) ([]paymentcore.TimelineEntry, error)
}
type DestinationPaymentStore interface {
	CreatePaymentWithDestination(context.Context, string, string, int64, string, string, string, paymentcore.Destination) (*paymentcore.Payment, error)
}

type QuoteService interface {
	Create(context.Context, string, string, int64) (*quote.Quote, error)
	Get(context.Context, string) (*quote.Quote, error)
}

type Health interface{ PingContext(context.Context) error }

type Handler struct {
	payments PaymentStore
	quotes   QuoteService
	health   Health
}

func NewHandler(payments PaymentStore, quotes QuoteService, health Health) (http.Handler, error) {
	if payments == nil || quotes == nil || health == nil {
		return nil, errors.New("payment API, quote, and health dependencies are required")
	}
	h := &Handler{payments: payments, quotes: quotes, health: health}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/payments", h.create)
	mux.HandleFunc("POST /v1/quotes", h.createQuote)
	mux.HandleFunc("GET /v1/quotes/{id}", h.getQuote)
	mux.HandleFunc("GET /v1/payments/{id}", h.get)
	mux.HandleFunc("GET /v1/payments/{id}/timeline", h.timeline)
	mux.HandleFunc("GET /healthz", h.live)
	mux.HandleFunc("GET /readyz", h.ready)
	return mux, nil
}

type createRequest struct {
	ExternalReference string                   `json:"external_reference"`
	Currency          string                   `json:"currency"`
	AmountMinor       int64                    `json:"amount_minor"`
	CustomerID        string                   `json:"customer_id"`
	QuoteID           string                   `json:"quote_id,omitempty"`
	Destination       *paymentcore.Destination `json:"destination,omitempty"`
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
	if strings.TrimSpace(input.ExternalReference) == "" || len(input.Currency) != 3 || input.AmountMinor <= 0 || strings.TrimSpace(input.CustomerID) == "" {
		problem(w, http.StatusBadRequest, "external_reference, three-letter currency, positive amount_minor, and customer_id are required")
		return
	}
	if input.Destination != nil {
		if err := input.Destination.Validate(); err != nil {
			problem(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	var p *paymentcore.Payment
	var err error
	if input.Destination != nil {
		store, ok := h.payments.(DestinationPaymentStore)
		if !ok {
			problem(w, http.StatusBadRequest, "payment destinations are not supported")
			return
		}
		p, err = store.CreatePaymentWithDestination(r.Context(), input.ExternalReference, input.Currency, input.AmountMinor, input.CustomerID, key, input.QuoteID, *input.Destination)
	} else if input.QuoteID != "" {
		p, err = h.payments.CreatePaymentWithQuote(r.Context(), input.ExternalReference, input.Currency, input.AmountMinor, input.CustomerID, key, input.QuoteID)
	} else {
		p, err = h.payments.CreatePayment(r.Context(), input.ExternalReference, input.Currency, input.AmountMinor, input.CustomerID, key)
	}
	if err != nil {
		if errors.Is(err, quote.ErrNotFound) {
			problem(w, http.StatusNotFound, "quote not found")
			return
		}
		if errors.Is(err, quote.ErrExpired) || errors.Is(err, quote.ErrAccepted) {
			problem(w, http.StatusConflict, err.Error())
			return
		}
		if strings.Contains(err.Error(), "do not match quote") {
			problem(w, http.StatusBadRequest, err.Error())
			return
		}
		problem(w, http.StatusInternalServerError, "could not create payment")
		return
	}
	writeJSON(w, http.StatusCreated, p)
}

type createQuoteRequest struct {
	SourceCurrency      string `json:"source_currency"`
	DestinationCurrency string `json:"destination_currency"`
	SourceAmountMinor   int64  `json:"source_amount_minor"`
}

func (h *Handler) createQuote(w http.ResponseWriter, r *http.Request) {
	var input createQuoteRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		problem(w, http.StatusBadRequest, "invalid JSON request body")
		return
	}
	q, err := h.quotes.Create(
		r.Context(),
		input.SourceCurrency,
		input.DestinationCurrency,
		input.SourceAmountMinor,
	)
	if err != nil {
		problem(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, q)
}

func (h *Handler) getQuote(w http.ResponseWriter, r *http.Request) {
	q, err := h.quotes.Get(r.Context(), r.PathValue("id"))
	if errors.Is(err, quote.ErrNotFound) {
		problem(w, http.StatusNotFound, "quote not found")
		return
	}
	if err != nil {
		problem(w, http.StatusInternalServerError, "quote service unavailable")
		return
	}
	writeJSON(w, http.StatusOK, q)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	p, err := h.payments.GetPayment(r.Context(), r.PathValue("id"))
	if err != nil {
		h.storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (h *Handler) timeline(w http.ResponseWriter, r *http.Request) {
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
