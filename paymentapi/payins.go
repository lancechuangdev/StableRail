package paymentapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"stablerail/paymentcore/payin"
)

func NewPayinHandler(service *payin.Service) (http.Handler, error) {
	if service == nil {
		return nil, errors.New("payin service is required")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/payin-quotes", func(w http.ResponseWriter, r *http.Request) {
		tenant, ok := TenantIDFromContext(r.Context())
		if !ok {
			problem(w, 401, "authentication required")
			return
		}
		key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		var in struct {
			FundingMethod        string `json:"funding_method"`
			CurrencyType         string `json:"currency_type"`
			SourceInstrumentID   string `json:"source_instrument_id,omitempty"`
			DestinationAccountID string `json:"destination_account_id"`
			SourceCurrency       string `json:"source_currency"`
			DestinationCurrency  string `json:"destination_currency"`
			AmountMinor          int64  `json:"amount_minor"`
			CoverFees            bool   `json:"cover_fees"`
		}
		if decodePayin(w, r, &in) != nil {
			return
		}
		q, err := service.CreateQuote(r.Context(), payin.QuoteRequest{IdempotencyKey: key, TenantID: tenant, FundingMethod: in.FundingMethod, CurrencyType: in.CurrencyType, SourceInstrumentID: in.SourceInstrumentID, DestinationAccountID: in.DestinationAccountID, SourceCurrency: in.SourceCurrency, DestinationCurrency: in.DestinationCurrency, AmountMinor: in.AmountMinor, CoverFees: in.CoverFees})
		if err != nil {
			problem(w, 400, err.Error())
			return
		}
		writeJSON(w, 201, q)
	})
	mux.HandleFunc("POST /v1/payins", func(w http.ResponseWriter, r *http.Request) {
		tenant, ok := TenantIDFromContext(r.Context())
		if !ok {
			problem(w, 401, "authentication required")
			return
		}
		key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		var in struct {
			QuoteID string `json:"quote_id"`
		}
		if decodePayin(w, r, &in) != nil {
			return
		}
		p, err := service.CreatePayin(r.Context(), tenant, in.QuoteID, key)
		if err != nil {
			if errors.Is(err, payin.ErrNotFound) {
				problem(w, 404, "payin quote not found")
			} else {
				problem(w, 400, err.Error())
			}
			return
		}
		writeJSON(w, 202, p)
	})
	mux.HandleFunc("GET /v1/payins/{id}", func(w http.ResponseWriter, r *http.Request) {
		tenant, ok := TenantIDFromContext(r.Context())
		if !ok {
			problem(w, 401, "authentication required")
			return
		}
		p, err := service.Get(r.Context(), tenant, r.PathValue("id"))
		if err != nil {
			if errors.Is(err, payin.ErrNotFound) {
				problem(w, 404, "payin not found")
			} else {
				problem(w, 500, "payin service unavailable")
			}
			return
		}
		writeJSON(w, 200, p)
	})
	return mux, nil
}
func decodePayin(w http.ResponseWriter, r *http.Request, out any) error {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		problem(w, 400, "invalid JSON request body")
		return err
	}
	var extra any
	if err := d.Decode(&extra); !errors.Is(err, io.EOF) {
		problem(w, 400, "request body must contain one JSON object")
		return errors.New("extra JSON")
	}
	return nil
}
