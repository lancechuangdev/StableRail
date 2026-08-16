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
	"stablerail/settlement/blindpay"
)

type fakeStore struct {
	payment  *paymentcore.Payment
	err      error
	key      string
	tenantID string
}

func (f *fakeStore) CreatePayment(_ context.Context, _, _ string, _ int64, tenantID, key string) (*paymentcore.Payment, error) {
	f.key, f.tenantID = key, tenantID
	return f.payment, f.err
}

func TestCreatePaymentDerivesAuthenticatedTenant(t *testing.T) {
	store := &fakeStore{payment: &paymentcore.Payment{ID: "pay_1", PaymentStatus: paymentcore.PaymentStatusCreated}}
	h, _ := NewHandler(store, fakeHealth{}, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/payments", strings.NewReader(`{"external_reference":"order-1","currency":"USD","amount_minor":1250}`))
	req = req.WithContext(context.WithValue(req.Context(), tenantContextKey{}, "tenant-authenticated"))
	req.Header.Set("Idempotency-Key", "request-1")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusCreated || store.tenantID != "tenant-authenticated" {
		t.Fatalf("status=%d tenant=%q", res.Code, store.tenantID)
	}
}

func TestGetPaymentHidesAnotherTenantsPayment(t *testing.T) {
	store := &fakeStore{payment: &paymentcore.Payment{ID: "pay_1", TenantID: "tenant-other"}}
	h, _ := NewHandler(store, fakeHealth{}, nil)
	req := httptest.NewRequest(http.MethodGet, "/v1/payments/pay_1", nil)
	req = req.WithContext(context.WithValue(req.Context(), tenantContextKey{}, "tenant-authenticated"))
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusNotFound {
		t.Fatalf("status=%d", res.Code)
	}
}
func (f *fakeStore) CreatePaymentWithPayoutQuote(ctx context.Context, a, b string, c int64, d, e, _ string) (*paymentcore.Payment, error) {
	return f.CreatePayment(ctx, a, b, c, d, e)
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

type fakePayoutQuoteService struct {
	request blindpay.PayoutQuoteRequest
	err     error
}

func (f *fakePayoutQuoteService) Create(_ context.Context, request blindpay.PayoutQuoteRequest) (*blindpay.PayoutQuote, error) {
	f.request = request
	if f.err != nil {
		return nil, f.err
	}
	return &blindpay.PayoutQuote{ID: "qu_test", Provider: "blindpay", Status: "open", ExpiresAt: time.Now().Add(time.Minute)}, nil
}

func TestCreatePayment(t *testing.T) {
	store := &fakeStore{payment: &paymentcore.Payment{ID: "pay_1", PaymentStatus: paymentcore.PaymentStatusCreated}}
	h, _ := NewHandler(store, fakeHealth{}, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/payments", strings.NewReader(`{"external_reference":"order-1","currency":"usd","amount_minor":1250,"tenant_id":"cus-1"}`))
	req.Header.Set("Idempotency-Key", "request-1")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("status = %d, body=%s", res.Code, res.Body.String())
	}
	if store.key != "request-1" {
		t.Fatalf("key = %q", store.key)
	}
}

func TestCreatePaymentValidation(t *testing.T) {
	store := &fakeStore{}
	h, _ := NewHandler(store, fakeHealth{}, nil)
	res := httptest.NewRecorder()
	h.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/v1/payments", strings.NewReader(`{}`)))
	if res.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", res.Code)
	}
}

func TestCreatePaymentRejectsDifferentRequestForIdempotencyKey(t *testing.T) {
	store := &fakeStore{err: paymentcore.ErrIdempotencyConflict}
	h, _ := NewHandler(store, fakeHealth{}, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/payments", strings.NewReader(`{"external_reference":"different","currency":"USD","amount_minor":1250,"tenant_id":"cus-1"}`))
	req.Header.Set("Idempotency-Key", "request-1")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusConflict {
		t.Fatalf("status = %d, body=%s", res.Code, res.Body.String())
	}
}

func TestCreateBlindPayPayoutQuote(t *testing.T) {
	quotes := &fakePayoutQuoteService{}
	h, _ := NewHandler(&fakeStore{}, fakeHealth{}, quotes)
	req := httptest.NewRequest(http.MethodPost, "/v1/blindpay/payout-quotes", strings.NewReader(`{"tenant_id":"tenant-1","bank_account_id":"ba_test","managed_wallet_id":"bl_test","destination_currency":"BRL","currency_type":"sender","cover_fees":true,"request_amount_minor":2500}`))
	req.Header.Set("Idempotency-Key", "quote-request-1")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("status = %d, body=%s", res.Code, res.Body.String())
	}
	if quotes.request.IdempotencyKey != "quote-request-1" || quotes.request.BankAccountID != "ba_test" {
		t.Fatalf("unexpected quote request: %+v", quotes.request)
	}
}

func TestGetNotFound(t *testing.T) {
	store := &fakeStore{err: fmtNotFound()}
	h, _ := NewHandler(store, fakeHealth{}, nil)
	res := httptest.NewRecorder()
	h.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/v1/payments/missing", nil))
	if res.Code != http.StatusNotFound {
		t.Fatalf("status = %d", res.Code)
	}
}
func fmtNotFound() error { return errors.Join(paymentcore.ErrPaymentNotFound, errors.New("missing")) }

func TestReadiness(t *testing.T) {
	store := &fakeStore{}
	h, _ := NewHandler(store, fakeHealth{err: errors.New("down")}, nil)
	res := httptest.NewRecorder()
	h.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", res.Code)
	}
}
