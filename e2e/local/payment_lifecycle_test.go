//go:build e2e

package local_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"stablerail/e2e/internal/testenv"
)

func TestLOCAL001SuccessfulPaymentLifecycle(t *testing.T) {
	env := testenv.Open(t)
	tenant := env.NewTenant(t)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	payment, status := tenant.CreatePayment(t, "local-001-"+suffix, "order-local-001-"+suffix, 2500)
	if status != http.StatusCreated {
		t.Fatalf("create payment status=%d", status)
	}
	settled := tenant.WaitForPaymentState(t, payment.ID, "settled")
	if settled.TenantID != tenant.ID || settled.AmountMinor != 2500 || settled.Currency != "USD" {
		t.Fatalf("unexpected settled payment: %+v", settled)
	}

	env.WaitForSagaState(t, payment.ID, "completed")

	rows, err := env.DB.Query(`
		SELECT
			t.event_type,
			COALESCE(
				SUM(CASE WHEN e.side = 'debit' THEN e.amount_minor ELSE 0 END),
				0
			),
			COALESCE(
				SUM(CASE WHEN e.side = 'credit' THEN e.amount_minor ELSE 0 END),
				0
			)
		FROM ledger_transactions t
		JOIN ledger_entries e
			ON e.transaction_id = t.id
		WHERE t.payment_id = $1
		GROUP BY t.event_type
	`, payment.ID)
	
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	journalCount := 0
	for rows.Next() {
		var eventType string
		var debit, credit int64
		if err := rows.Scan(&eventType, &debit, &credit); err != nil {
			t.Fatal(err)
		}
		if debit != credit || debit != 2500 {
			t.Fatalf("unbalanced %s journal: debit=%d credit=%d", eventType, debit, credit)
		}
		journalCount++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if journalCount != 2 {
		t.Fatalf("journal count=%d, want 2", journalCount)
	}

	response, body := tenant.Get(t, "/v1/payments/"+payment.ID+"/timeline")
	if response.StatusCode != http.StatusOK {
		t.Fatalf("timeline status=%d body=%s", response.StatusCode, body)
	}
	var timeline struct {
		Entries []testenv.TimelineEntry `json:"timeline"`
	}
	if err := json.Unmarshal(body, &timeline); err != nil {
		t.Fatal(err)
	}
	want := []string{"created", "processing", "settled"}
	if len(timeline.Entries) != len(want) {
		t.Fatalf("timeline=%v, want states %v", timeline.Entries, want)
	}
	for i, state := range want {
		if timeline.Entries[i].State != state {
			t.Fatalf("timeline[%d]=%q, want %q", i, timeline.Entries[i].State, state)
		}
	}
}

func TestLOCAL002PaymentIdempotency(t *testing.T) {
	env := testenv.Open(t)
	tenant := env.NewTenant(t)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	key, reference := "local-002-"+suffix, "order-local-002-"+suffix

	first, status := tenant.CreatePayment(t, key, reference, 1250)
	if status != http.StatusCreated {
		t.Fatalf("first create status=%d", status)
	}
	second, status := tenant.CreatePayment(t, key, reference, 1250)
	if status != http.StatusCreated || second.ID != first.ID {
		t.Fatalf("idempotent create status=%d first=%s second=%s", status, first.ID, second.ID)
	}
	if _, status := tenant.CreatePayment(t, key, reference, 1251); status != http.StatusConflict {
		t.Fatalf("changed request status=%d, want 409", status)
	}

	var count int
	if err := env.DB.QueryRow(`SELECT count(*) FROM payments WHERE idempotency_key=$1`, key).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("payment count=%d, want 1", count)
	}
}

func TestLOCAL003TenantIsolation(t *testing.T) {
	env := testenv.Open(t)
	owner, stranger := env.NewTenant(t), env.NewTenant(t)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	payment, status := owner.CreatePayment(t, "local-003-"+suffix, "order-local-003-"+suffix, 900)
	if status != http.StatusCreated {
		t.Fatalf("create status=%d", status)
	}

	for _, path := range []string{"/v1/payments/" + payment.ID, "/v1/payments/" + payment.ID + "/timeline"} {
		if response, body := owner.Get(t, path); response.StatusCode != http.StatusOK {
			t.Fatalf("owner GET %s status=%d body=%s", path, response.StatusCode, body)
		}
		if response, body := stranger.Get(t, path); response.StatusCode != http.StatusNotFound {
			t.Fatalf("cross-tenant GET %s status=%d body=%s", path, response.StatusCode, body)
		}
	}
}
