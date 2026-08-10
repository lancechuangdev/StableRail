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
)

type Store interface {
	CreatePayment(context.Context, string, string, int64, string, string) (*paymentcore.Payment, error)
	GetPayment(context.Context, string) (*paymentcore.Payment, error)
	Timeline(context.Context, string) ([]paymentcore.TimelineEntry, error)
}

type Health interface{ PingContext(context.Context) error }

type Handler struct {
	store  Store
	health Health
}

func NewHandler(store Store, health Health) (http.Handler, error) {
	if store == nil || health == nil {
		return nil, errors.New("payment API dependencies are required")
	}
	h := &Handler{store: store, health: health}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/payments", h.create)
	mux.HandleFunc("GET /v1/payments/{id}", h.get)
	mux.HandleFunc("GET /v1/payments/{id}/timeline", h.timeline)
	mux.HandleFunc("GET /healthz", h.live)
	mux.HandleFunc("GET /readyz", h.ready)
	return mux, nil
}

type createRequest struct {
	ExternalReference string `json:"external_reference"`
	Currency          string `json:"currency"`
	AmountMinor       int64  `json:"amount_minor"`
	CustomerID        string `json:"customer_id"`
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
	p, err := h.store.CreatePayment(r.Context(), input.ExternalReference, input.Currency, input.AmountMinor, input.CustomerID, key)
	if err != nil {
		problem(w, http.StatusInternalServerError, "could not create payment")
		return
	}
	writeJSON(w, http.StatusCreated, p)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	p, err := h.store.GetPayment(r.Context(), r.PathValue("id"))
	if err != nil {
		h.storeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (h *Handler) timeline(w http.ResponseWriter, r *http.Request) {
	timeline, err := h.store.Timeline(r.Context(), r.PathValue("id"))
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
