package paymentapi

import (
	"context"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestRegisterWebhookEndpointReturnsSecretOnce(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service, _ := NewWebhookEndpointService(db)
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	service.random = rand.Read

	mock.ExpectBegin()
	mock.ExpectExec("SELECT pg_advisory_xact_lock").WithArgs("tenant-1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT count\\(\\*\\),COALESCE").WithArgs("tenant-1", "https://merchant.example/webhooks").
		WillReturnRows(sqlmock.NewRows([]string{"count", "duplicate"}).AddRow(0, false))
	mock.ExpectExec("INSERT INTO webhook_endpoints").WithArgs(sqlmock.AnyArg(), "tenant-1", "https://merchant.example/webhooks", sqlmock.AnyArg(), now).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	endpoint, secret, err := service.Register(context.Background(), "tenant-1", "https://merchant.example/webhooks")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(endpoint.ID, "whe_") || !strings.HasPrefix(secret, "whsec_") {
		t.Fatalf("endpoint=%q secret=%q", endpoint.ID, secret)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestListWebhookEndpointsDoesNotExposeSecret(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service, _ := NewWebhookEndpointService(db)
	handler, _ := NewWebhookEndpointHandler(service)
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery("SELECT id,url,active,created_at FROM webhook_endpoints").WithArgs("tenant-1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "url", "active", "created_at"}).AddRow("whe_1", "https://merchant.example/webhooks", true, now))

	req := httptest.NewRequest(http.MethodGet, "/v1/webhook-endpoints", nil)
	req = req.WithContext(context.WithValue(req.Context(), tenantContextKey{}, "tenant-1"))
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusOK || strings.Contains(res.Body.String(), "secret") || !strings.Contains(res.Body.String(), "whe_1") {
		t.Fatalf("status=%d body=%s", res.Code, res.Body.String())
	}
}

func TestDisableWebhookEndpointIsTenantScoped(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service, _ := NewWebhookEndpointService(db)
	mock.ExpectExec("UPDATE webhook_endpoints SET active=FALSE").WithArgs("whe_1", "tenant-1").WillReturnResult(sqlmock.NewResult(0, 0))
	if err := service.Disable(context.Background(), "tenant-1", "whe_1"); err != ErrWebhookEndpointNotFound {
		t.Fatalf("Disable() error = %v", err)
	}
}

func TestWebhookURLValidationRejectsUnsafeAddresses(t *testing.T) {
	for _, rawURL := range []string{"http://example.com/hook", "https://localhost/hook", "https://127.0.0.1/hook", "https://10.0.0.1/hook", "https://example.com/hook#fragment"} {
		if _, err := validateWebhookURL(rawURL, false); err == nil {
			t.Fatalf("accepted unsafe URL %q", rawURL)
		}
	}
	if got, err := validateWebhookURL("https://merchant.example/webhooks", false); err != nil || got == "" {
		t.Fatalf("valid URL rejected: %q, %v", got, err)
	}
}
