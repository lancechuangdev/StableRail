//go:build e2e

package local_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"testing"
	"time"

	"stablerail/e2e/internal/testenv"
	"stablerail/notification"
)

func TestLOCAL001SuccessfulPaymentLifecycle(t *testing.T) {
	env := testenv.Open(t)
	tenant := env.NewTenant(t)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	payment, status := tenant.CreatePayment(t, "local-001-"+suffix, "order-local-001-"+suffix, 2500)
	if status != http.StatusCreated {
		t.Fatalf("create payment status=%d", status)
	}
	succeeded := tenant.WaitForPaymentStatus(t, payment.ID, "succeeded")
	if succeeded.TenantID != tenant.ID || succeeded.AmountMinor != 2500 || succeeded.Currency != "USD" || succeeded.FundsStatus != "consumed" {
		t.Fatalf("unexpected succeeded payment: %+v", succeeded)
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
	want := []string{"created", "processing", "succeeded"}
	if len(timeline.Entries) != len(want) {
		t.Fatalf("timeline=%v, want states %v", timeline.Entries, want)
	}
	for i, state := range want {
		if timeline.Entries[i].PaymentStatus != state {
			t.Fatalf("timeline[%d]=%q, want %q", i, timeline.Entries[i].PaymentStatus, state)
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

func TestLOCAL004PolicyRejection(t *testing.T) {
	env, tenant := testenv.Open(t), (*testenv.Tenant)(nil)
	tenant = env.NewTenant(t)
	payment, status := tenant.CreatePayment(t, "local-004-"+fmt.Sprint(time.Now().UnixNano()), "order-local-004", 4004)
	if status != http.StatusCreated {
		t.Fatalf("create status=%d", status)
	}
	tenant.WaitForPaymentStatus(t, payment.ID, "failed")
	env.WaitForSagaState(t, payment.ID, "failed")
	env.WaitForCount(t, `SELECT count(*) FROM settlement_submissions WHERE payment_id=$1`, 0, payment.ID)
	env.WaitForCount(t, `SELECT count(*) FROM ledger_transactions WHERE payment_id=$1`, 0, payment.ID)
}

func TestLOCAL005SettlementFailureKeepsFundsReserved(t *testing.T) {
	env := testenv.Open(t)
	tenant := env.NewTenant(t)
	payment, status := tenant.CreatePayment(t, "local-005-"+fmt.Sprint(time.Now().UnixNano()), "order-local-005", 5005)
	if status != http.StatusCreated {
		t.Fatalf("create status=%d", status)
	}
	failed := tenant.WaitForPaymentStatus(t, payment.ID, "failed")
	if failed.FundsStatus != "reserved" {
		t.Fatalf("funds_status=%s, want reserved", failed.FundsStatus)
	}
	env.WaitForSagaState(t, payment.ID, "failed")
	env.WaitForCount(t, `SELECT count(*) FROM ledger_transactions WHERE payment_id=$1 AND event_type='payment.processing'`, 1, payment.ID)
	env.WaitForCount(t, `SELECT count(*) FROM ledger_transactions WHERE payment_id=$1 AND event_type='payment.released'`, 0, payment.ID)
}

func TestLOCAL007ManualReviewResolution(t *testing.T) {
	env := testenv.Open(t)
	tenant := env.NewTenant(t)
	payment, status := tenant.CreatePayment(t, "local-007-"+fmt.Sprint(time.Now().UnixNano()), "order-local-007", 7007)
	if status != http.StatusCreated {
		t.Fatalf("create status=%d", status)
	}
	env.WaitForSagaState(t, payment.ID, "manual_review")
	response, body := env.OperatorPost(t, "/v1/operator/payments/"+payment.ID+"/manual-review", map[string]string{"action": "complete", "operator": "local-e2e", "note": "review passed"})
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("manual review status=%d body=%s", response.StatusCode, body)
	}
	tenant.WaitForPaymentStatus(t, payment.ID, "succeeded")
	env.WaitForSagaState(t, payment.ID, "completed")
	env.WaitForCount(t, `SELECT count(*) FROM saga_manual_review_actions a JOIN settlement_sagas s ON s.id=a.saga_id WHERE s.payment_id=$1`, 1, payment.ID)
}

func TestLOCAL008IndependentSignedWebhooks(t *testing.T) {
	env := testenv.Open(t)
	tenant := env.NewTenant(t)
	type received struct {
		endpoint             int
		timestamp, signature string
		body                 []byte
	}
	deliveries := make(chan received, 20)
	servers := make([]*httptest.Server, 2)
	secrets := make([]string, 2)
	for i := range servers {
		i := i
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			deliveries <- received{i, r.Header.Get("X-StableRail-Timestamp"), r.Header.Get("X-StableRail-Signature"), body}
			w.WriteHeader(http.StatusNoContent)
		}))
		defer servers[i].Close()
		response, body := tenant.Post(t, "/v1/webhook-endpoints", map[string]string{"url": servers[i].URL})
		if response.StatusCode != http.StatusCreated {
			t.Fatalf("register endpoint %d status=%d body=%s", i, response.StatusCode, body)
		}
		var created struct {
			Secret string `json:"signing_secret"`
		}
		if err := json.Unmarshal(body, &created); err != nil {
			t.Fatal(err)
		}
		secrets[i] = created.Secret
	}
	payment, status := tenant.CreatePayment(t, "local-008-"+fmt.Sprint(time.Now().UnixNano()), "order-local-008", 8008)
	if status != http.StatusCreated {
		t.Fatalf("create status=%d", status)
	}
	tenant.WaitForPaymentStatus(t, payment.ID, "succeeded")
	seen := [2]int{}
	deadline := time.After(60 * time.Second)
	for seen[0] < 3 || seen[1] < 3 {
		select {
		case delivery := <-deliveries:
			if delivery.signature != notification.Signature(secrets[delivery.endpoint], delivery.timestamp, delivery.body) {
				t.Fatalf("endpoint %d signature is invalid", delivery.endpoint)
			}
			if !bytes.Contains(delivery.body, []byte(payment.ID)) {
				t.Fatalf("webhook does not identify payment: %s", delivery.body)
			}
			seen[delivery.endpoint]++
		case <-deadline:
			t.Fatalf("webhook counts=%v, want at least three each", seen)
		}
	}
}

func TestLOCAL009RestartCompletesDurableWorkOnce(t *testing.T) {
	env := testenv.Open(t)
	tenant := env.NewTenant(t)
	binary, rawPID := os.Getenv("STABLERAIL_E2E_SERVER_BINARY"), os.Getenv("STABLERAIL_E2E_SERVER_PID")
	if binary == "" || rawPID == "" {
		t.Skip("runner-managed server is required")
	}
	payment, status := tenant.CreatePayment(t, "local-009-"+fmt.Sprint(time.Now().UnixNano()), "order-local-009", 9009)
	if status != http.StatusCreated {
		t.Fatalf("create status=%d", status)
	}
	env.WaitForSagaState(t, payment.ID, "awaiting_settlement")
	response, body := env.OperatorPost(t, "/v1/operator/mock-settlements/"+payment.ID, map[string]any{"status": "completed", "delay_milliseconds": 2000})
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("completion trigger status=%d body=%s", response.StatusCode, body)
	}
	pid, err := strconv.Atoi(rawPID)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	command := exec.Command(binary)
	command.Env = os.Environ()
	command.Stdout, command.Stderr = io.Discard, io.Discard
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	if pidFile := os.Getenv("STABLERAIL_E2E_SERVER_PID_FILE"); pidFile != "" {
		if err := os.WriteFile(pidFile, []byte(strconv.Itoa(command.Process.Pid)), 0600); err != nil {
			t.Fatal(err)
		}
	}
	env.WaitReady(t)
	tenant.WaitForPaymentStatus(t, payment.ID, "succeeded")
	env.WaitForSagaState(t, payment.ID, "completed")
	env.WaitForCount(t, `SELECT count(*) FROM settlement_submissions WHERE payment_id=$1`, 1, payment.ID)
	env.WaitForCount(t, `SELECT count(*) FROM ledger_transactions WHERE payment_id=$1 AND event_type='payment.succeeded'`, 1, payment.ID)
}
