package paymentapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"stablerail/paymentcore"
	"stablerail/paymentcore/payout"
)

type fakeStore struct {
	payment                 *paymentcore.Payment
	err                     error
	key                     string
	tenantID                string
	refund                  *paymentcore.Refund
	paymentID               string
	amount                  int64
	reason                  string
	sourceAccountID         string
	destinationInstrumentID string
}

func (f *fakeStore) CreatePayment(_ context.Context, request payout.CreatePaymentRequest) (*paymentcore.Payment, error) {
	f.key, f.tenantID = request.IdempotencyKey, request.TenantID
	f.sourceAccountID, f.destinationInstrumentID = request.SourceAccountID, request.DestinationInstrumentID
	return f.payment, f.err
}

func (f *fakeStore) CreateQuote(context.Context, payout.QuoteRequest) (payout.Quote, error) {
	return payout.Quote{}, nil
}

func (f *fakeStore) CreateRefund(_ context.Context, paymentID, tenantID, key string, amount int64, reason, _ string) (*paymentcore.Refund, error) {
	f.paymentID, f.tenantID, f.key, f.amount, f.reason = paymentID, tenantID, key, amount, reason
	return f.refund, f.err
}

func TestCreatePaymentDerivesAuthenticatedTenant(t *testing.T) {
	store := &fakeStore{payment: &paymentcore.Payment{ID: "pay_1", PaymentStatus: paymentcore.PaymentStatusCreated}}
	h, _ := NewHandler(store, store, nil, fakeHealth{})
	req := httptest.NewRequest(http.MethodPost, "/v1/payments", strings.NewReader(`{"direction":"payout","external_reference":"order-1","quote_id":"quote-1","currency":"USD","amount_minor":1250}`))
	req = req.WithContext(context.WithValue(req.Context(), tenantContextKey{}, "tenant-authenticated"))
	req.Header.Set("Idempotency-Key", "request-1")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusAccepted || store.tenantID != "tenant-authenticated" {
		t.Fatalf("status=%d tenant=%q", res.Code, store.tenantID)
	}
}

func TestGetPaymentHidesAnotherTenantsPayment(t *testing.T) {
	store := &fakeStore{payment: &paymentcore.Payment{ID: "pay_1", TenantID: "tenant-other"}}
	h, _ := NewHandler(store, store, nil, fakeHealth{})
	req := httptest.NewRequest(http.MethodGet, "/v1/payments/pay_1", nil)
	req = req.WithContext(context.WithValue(req.Context(), tenantContextKey{}, "tenant-authenticated"))
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusNotFound {
		t.Fatalf("status=%d", res.Code)
	}
}
func (f *fakeStore) GetPayment(context.Context, string) (*paymentcore.Payment, error) {
	return f.payment, f.err
}
func (f *fakeStore) Timeline(context.Context, string) ([]paymentcore.TimelineEntry, error) {
	if f.payment == nil {
		return nil, f.err
	}
	return f.payment.Timeline, f.err
}

type fakeHealth struct{ err error }

func (f fakeHealth) PingContext(context.Context) error { return f.err }

func TestCreateRefundUsesAuthenticatedTenantAndIdempotency(t *testing.T) {
	store := &fakeStore{refund: &paymentcore.Refund{ID: "ref_1", PaymentID: "pay_1", RefundPaymentID: "pay_refund_1"}}
	h, _ := NewHandler(store, store, nil, fakeHealth{})
	req := httptest.NewRequest(http.MethodPost, "/v1/payments/pay_1/refunds", strings.NewReader(`{"amount_minor":500,"reason":"duplicate order"}`))
	req = req.WithContext(context.WithValue(req.Context(), tenantContextKey{}, "tenant_1"))
	req.Header.Set("Idempotency-Key", "refund-key")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusAccepted || store.paymentID != "pay_1" || store.tenantID != "tenant_1" || store.key != "refund-key" || store.amount != 500 || store.reason != "duplicate order" {
		t.Fatalf("status=%d request=%+v body=%s", res.Code, store, res.Body.String())
	}
}

type fakePayoutQuoteService struct {
	request payout.QuoteRequest
	err     error
}

type fakePayoutService struct {
	*fakeStore
	quotes *fakePayoutQuoteService
}

func (f *fakePayoutService) CreateQuote(ctx context.Context, request payout.QuoteRequest) (payout.Quote, error) {
	return f.quotes.CreateQuote(ctx, request)
}

func (f *fakePayoutQuoteService) CreateQuote(_ context.Context, request payout.QuoteRequest) (payout.Quote, error) {
	f.request = request
	if f.err != nil {
		return payout.Quote{}, f.err
	}
	return payout.Quote{ID: "qu_test", Provider: "blindpay", Status: "open", ExpiresAt: time.Now().Add(time.Minute)}, nil
}

func TestCreatePayment(t *testing.T) {
	store := &fakeStore{payment: &paymentcore.Payment{ID: "pay_1", PaymentStatus: paymentcore.PaymentStatusCreated}}
	h, _ := NewHandler(store, store, nil, fakeHealth{})
	req := httptest.NewRequest(http.MethodPost, "/v1/payments", strings.NewReader(`{"direction":"payout","external_reference":"order-1","funding_method":"bank","source_account_id":"account-1","destination_instrument_id":"instrument-1","currency":"usd","amount_minor":1250,"tenant_id":"cus-1"}`))
	req.Header.Set("Idempotency-Key", "request-1")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body=%s", res.Code, res.Body.String())
	}
	if store.key != "request-1" || store.sourceAccountID != "account-1" || store.destinationInstrumentID != "instrument-1" {
		t.Fatalf("unexpected request: %+v", store)
	}
}

func TestCreatePaymentValidation(t *testing.T) {
	store := &fakeStore{}
	h, _ := NewHandler(store, store, nil, fakeHealth{})
	res := httptest.NewRecorder()
	h.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/v1/payments", strings.NewReader(`{}`)))
	if res.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", res.Code)
	}
}

func TestCreatePaymentRequiresExplicitDirection(t *testing.T) {
	store := &fakeStore{}
	h, _ := NewHandler(store, store, nil, fakeHealth{})
	for _, body := range []string{
		`{"external_reference":"order-1","currency":"USD","amount_minor":1250,"tenant_id":"cus-1"}`,
		`{"direction":"transfer","external_reference":"order-1","currency":"USD","amount_minor":1250,"tenant_id":"cus-1"}`,
	} {
		req := httptest.NewRequest(http.MethodPost, "/v1/payments", strings.NewReader(body))
		req.Header.Set("Idempotency-Key", "request-1")
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		if res.Code != http.StatusBadRequest || !strings.Contains(res.Body.String(), "direction must be payin or payout") {
			t.Fatalf("body=%s: status=%d response=%s", body, res.Code, res.Body.String())
		}
	}
}

func TestCreatePaymentRejectsDifferentRequestForIdempotencyKey(t *testing.T) {
	store := &fakeStore{err: paymentcore.ErrIdempotencyConflict}
	h, _ := NewHandler(store, store, nil, fakeHealth{})
	req := httptest.NewRequest(http.MethodPost, "/v1/payments", strings.NewReader(`{"direction":"payout","external_reference":"different","quote_id":"quote-1","currency":"USD","amount_minor":1250,"tenant_id":"cus-1"}`))
	req.Header.Set("Idempotency-Key", "request-1")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusConflict {
		t.Fatalf("status = %d, body=%s", res.Code, res.Body.String())
	}
}

func TestCreateBlindPayPayoutQuote(t *testing.T) {
	quotes := &fakePayoutQuoteService{}
	payouts := &fakePayoutService{fakeStore: &fakeStore{}, quotes: quotes}
	h, _ := NewHandler(payouts, payouts, nil, fakeHealth{})
	req := httptest.NewRequest(http.MethodPost, "/v1/payment-quotes", strings.NewReader(`{"direction":"payout","funding_method":"bank","tenant_id":"tenant-1","source_account_id":"acct_test","destination_instrument_id":"instrument_test","source_currency":"USDC","destination_currency":"BRL","currency_type":"sender","cover_fees":true,"request_amount_minor":2500}`))
	req.Header.Set("Idempotency-Key", "quote-request-1")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("status = %d, body=%s", res.Code, res.Body.String())
	}
	if quotes.request.IdempotencyKey != "quote-request-1" || quotes.request.SourceAccountID != "acct_test" {
		t.Fatalf("unexpected quote request: %+v", quotes.request)
	}
}

func TestGetNotFound(t *testing.T) {
	store := &fakeStore{err: fmtNotFound()}
	h, _ := NewHandler(store, store, nil, fakeHealth{})
	res := httptest.NewRecorder()
	h.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/v1/payments/missing", nil))
	if res.Code != http.StatusNotFound {
		t.Fatalf("status = %d", res.Code)
	}
}
func fmtNotFound() error { return errors.Join(paymentcore.ErrPaymentNotFound, errors.New("missing")) }

func TestReadiness(t *testing.T) {
	store := &fakeStore{}
	h, _ := NewHandler(store, store, nil, fakeHealth{err: errors.New("down")})
	res := httptest.NewRecorder()
	h.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", res.Code)
	}
}
