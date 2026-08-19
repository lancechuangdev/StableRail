//go:build e2e

package testenv

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Environment struct {
	BaseURL       string
	OperatorToken string
	DB            *sql.DB
	HTTP          *http.Client
}

type Tenant struct {
	ID, APIKey string
	Env        *Environment
}

type Payment struct {
	ID                string `json:"id"`
	ExternalReference string `json:"external_reference"`
	Currency          string `json:"currency"`
	AmountMinor       int64  `json:"amount_minor"`
	TenantID          string `json:"tenant_id"`
	PaymentStatus     string `json:"payment_status"`
	FundsStatus       string `json:"funds_status"`
}

type TimelineEntry struct {
	PaymentStatus string `json:"payment_status"`
}

func Open(t *testing.T) *Environment {
	t.Helper()
	baseURL := strings.TrimRight(os.Getenv("STABLERAIL_E2E_BASE_URL"), "/")
	databaseURL := os.Getenv("STABLERAIL_E2E_DATABASE_URL")
	operatorToken := os.Getenv("STABLERAIL_E2E_OPERATOR_TOKEN")
	if baseURL == "" || databaseURL == "" || operatorToken == "" {
		t.Skip("STABLERAIL_E2E_BASE_URL, STABLERAIL_E2E_DATABASE_URL, and STABLERAIL_E2E_OPERATOR_TOKEN are required")
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	env := &Environment{BaseURL: baseURL, OperatorToken: operatorToken, DB: db, HTTP: &http.Client{Timeout: 5 * time.Second}}
	env.waitReady(t)
	return env
}

func (e *Environment) NewTenant(t *testing.T) *Tenant {
	t.Helper()
	id := "tenant-e2e-" + unique()
	requestBody := map[string]string{"name": "local e2e"}
	response := e.request(t, http.MethodPost, "/v1/operator/tenants/"+id+"/api-keys", e.OperatorToken, "", requestBody)
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("issue tenant key: status=%d body=%s", response.StatusCode, readBody(response.Body))
	}
	var result struct {
		APIKey string `json:"api_key"`
	}
	decode(t, response.Body, &result)
	if result.APIKey == "" {
		t.Fatal("operator API returned an empty API key")
	}
	return &Tenant{ID: id, APIKey: result.APIKey, Env: e}
}

func (tenant *Tenant) CreatePayment(t *testing.T, key, reference string, amount int64) (*Payment, int) {
	t.Helper()
	body := map[string]any{"external_reference": reference, "currency": "USD", "amount_minor": amount}
	response := tenant.Env.request(t, http.MethodPost, "/v1/payments", tenant.APIKey, key, body)
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		return nil, response.StatusCode
	}
	var payment Payment
	decode(t, response.Body, &payment)
	return &payment, response.StatusCode
}

func (tenant *Tenant) Get(t *testing.T, path string) (*http.Response, []byte) {
	t.Helper()
	response := tenant.Env.request(t, http.MethodGet, path, tenant.APIKey, "", nil)
	defer response.Body.Close()
	return response, []byte(readBody(response.Body))
}

func (tenant *Tenant) Post(t *testing.T, path string, body any) (*http.Response, []byte) {
	t.Helper()
	response := tenant.Env.request(t, http.MethodPost, path, tenant.APIKey, "", body)
	defer response.Body.Close()
	return response, []byte(readBody(response.Body))
}

func (e *Environment) OperatorPost(t *testing.T, path string, body any) (*http.Response, []byte) {
	t.Helper()
	response := e.request(t, http.MethodPost, path, e.OperatorToken, "", body)
	defer response.Body.Close()
	return response, []byte(readBody(response.Body))
}

func (tenant *Tenant) WaitForPaymentStatus(t *testing.T, paymentID, wanted string) Payment {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var last Payment
	for time.Now().Before(deadline) {
		response, body := tenant.Get(t, "/v1/payments/"+paymentID)
		if response.StatusCode == http.StatusOK {
			if err := json.Unmarshal(body, &last); err != nil {
				t.Fatal(err)
			}
			if last.PaymentStatus == wanted {
				return last
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("payment %s did not reach status %s; last status=%s", paymentID, wanted, last.PaymentStatus)
	return Payment{}
}

func (e *Environment) WaitForSagaState(t *testing.T, paymentID, wanted string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		if err := e.DB.QueryRow(`SELECT state FROM settlement_sagas WHERE payment_id=$1`, paymentID).Scan(&last); err == nil && last == wanted {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("saga for payment %s did not reach %s; last state=%s", paymentID, wanted, last)
}

func (e *Environment) WaitForCount(t *testing.T, query string, wanted int, args ...any) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	last := -1
	for time.Now().Before(deadline) {
		if err := e.DB.QueryRow(query, args...).Scan(&last); err == nil && last == wanted {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("query count=%d, want %d", last, wanted)
}

func (e *Environment) waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		response, err := e.HTTP.Get(e.BaseURL + "/readyz")
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("StableRail did not become ready")
}

func (e *Environment) WaitReady(t *testing.T) { e.waitReady(t) }

func (e *Environment) request(t *testing.T, method, path, bearer, idempotencyKey string, body any) *http.Response {
	t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(context.Background(), method, e.BaseURL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+bearer)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := e.HTTP.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func unique() string { return fmt.Sprintf("%d", time.Now().UnixNano()) }

func decode(t *testing.T, reader io.Reader, value any) {
	t.Helper()
	if err := json.NewDecoder(reader).Decode(value); err != nil {
		t.Fatal(err)
	}
}

func readBody(reader io.Reader) string {
	body, _ := io.ReadAll(io.LimitReader(reader, 1<<20))
	return string(body)
}
