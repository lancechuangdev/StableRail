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
)

type fakeStore struct {
	payment *paymentcore.Payment
	err     error
	key     string
}

func (f *fakeStore) CreatePayment(_ context.Context, _, _ string, _ int64, _, key string) (*paymentcore.Payment, error) {
	f.key = key
	return f.payment, f.err
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

func TestCreatePayment(t *testing.T) {
	store := &fakeStore{payment: &paymentcore.Payment{ID: "pay_1", State: paymentcore.StateCreated}}
	h, _ := NewHandler(store, fakeHealth{})
	req := httptest.NewRequest(http.MethodPost, "/v1/payments", strings.NewReader(`{"external_reference":"order-1","currency":"usd","amount_minor":1250,"customer_id":"cus-1"}`))
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
	h, _ := NewHandler(&fakeStore{}, fakeHealth{})
	res := httptest.NewRecorder()
	h.ServeHTTP(res, httptest.NewRequest(http.MethodPost, "/v1/payments", strings.NewReader(`{}`)))
	if res.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", res.Code)
	}
}

func TestGetNotFound(t *testing.T) {
	h, _ := NewHandler(&fakeStore{err: fmtNotFound()}, fakeHealth{})
	res := httptest.NewRecorder()
	h.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/v1/payments/missing", nil))
	if res.Code != http.StatusNotFound {
		t.Fatalf("status = %d", res.Code)
	}
}
func fmtNotFound() error { return errors.Join(paymentcore.ErrPaymentNotFound, errors.New("missing")) }

func TestReadiness(t *testing.T) {
	h, _ := NewHandler(&fakeStore{}, fakeHealth{err: errors.New("down")})
	res := httptest.NewRecorder()
	h.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", res.Code)
	}
	_ = time.Second
}
