package paymentapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"stablerail/paymentcore"
	"stablerail/postgresdb"
)

// Run with STABLERAIL_E2E_DATABASE_URL pointing at a database with migrations
// applied. This covers HTTP -> payment transaction -> outbox -> query.
func TestPostgresPaymentHTTPEndToEnd(t *testing.T) {
	url := os.Getenv("STABLERAIL_E2E_DATABASE_URL")
	if url == "" {
		t.Skip("STABLERAIL_E2E_DATABASE_URL is not set")
	}
	db, err := postgresdb.Open(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	handler, err := NewHandler(paymentcore.NewPostgresService(db), db, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	key := fmt.Sprintf("e2e-%d", time.Now().UnixNano())
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/payments", strings.NewReader(`{"external_reference":"e2e-order","currency":"USD","amount_minor":4200,"customer_id":"e2e-customer"}`))
	req.Header.Set("Idempotency-Key", key)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d", response.StatusCode)
	}
	var created paymentcore.Payment
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/v1/payments/" + created.ID, "/v1/payments/" + created.ID + "/timeline"} {
		got, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		got.Body.Close()
		if got.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d", path, got.StatusCode)
		}
	}
}
