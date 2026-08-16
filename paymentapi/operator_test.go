package paymentapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeManualReviewResolver struct {
	paymentID, action, operator, note string
	err                               error
}

func (f *fakeManualReviewResolver) ResolveManualReview(_ context.Context, paymentID, action, operator, note string) error {
	f.paymentID, f.action, f.operator, f.note = paymentID, action, operator, note
	return f.err
}

func TestOperatorHandlerResolvesManualReview(t *testing.T) {
	resolver := &fakeManualReviewResolver{}
	h, err := NewOperatorHandler("secret", resolver)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("POST /v1/operator/payments/{id}/manual-review", h)
	req := httptest.NewRequest(http.MethodPost, "/v1/operator/payments/pay_1/manual-review", strings.NewReader(`{"action":"return","operator":"alice","note":"provider confirmed return"}`))
	req.Header.Set("Authorization", "Bearer secret")
	res := httptest.NewRecorder()
	mux.ServeHTTP(res, req)
	if res.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
	if resolver.paymentID != "pay_1" || resolver.action != "return" || resolver.operator != "alice" || resolver.note != "provider confirmed return" {
		t.Fatalf("unexpected resolution: %+v", resolver)
	}
}

func TestOperatorHandlerRequiresAuthentication(t *testing.T) {
	h, _ := NewOperatorHandler("secret", &fakeManualReviewResolver{})
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d", res.Code)
	}
}
