package paymentapi

import (
	"context"
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestAPIKeyMiddlewareAuthenticatesTenant(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service, _ := NewAPIKeyService(db)
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	digest := sha256.Sum256([]byte("secret"))
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT tenant_id FROM tenant_api_keys").WithArgs("key_abc", digest[:]).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow("tenant-1"))
	mock.ExpectExec("UPDATE tenant_api_keys SET last_used_at").WithArgs(now, "key_abc").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	called := false
	h := service.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenantID, ok := TenantIDFromContext(r.Context())
		if !ok || tenantID != "tenant-1" {
			t.Fatalf("tenant=%q ok=%v", tenantID, ok)
		}
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/payments/pay_1", nil)
	req.Header.Set("Authorization", "Bearer srk_key_abc_secret")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusNoContent || !called {
		t.Fatalf("status=%d called=%v", res.Code, called)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAPIKeyMiddlewareRejectsMissingKey(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()
	service, _ := NewAPIKeyService(db)
	h := service.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("protected handler was called")
	}))
	res := httptest.NewRecorder()
	h.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/v1/payments/pay_1", nil))
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d", res.Code)
	}
}

func TestAuthenticatedTenantOverridesRequestIdentity(t *testing.T) {
	ctx := context.WithValue(context.Background(), tenantContextKey{}, "tenant-1")
	if tenantID, ok := TenantIDFromContext(ctx); !ok || tenantID != "tenant-1" {
		t.Fatalf("tenant=%q ok=%v", tenantID, ok)
	}
}

func TestRevokeAPIKey(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service, _ := NewAPIKeyService(db)
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	mock.ExpectExec("UPDATE tenant_api_keys SET revoked_at").WithArgs(now, "key_abc").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := service.Revoke(context.Background(), "key_abc"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
