//go:build e2e

package blindpay_test

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

func newPayment(t *testing.T, amount int64, scenario string) (*testenv.Environment, *testenv.Tenant, testenv.PayoutQuote, *testenv.Payment) {
	t.Helper()
	env := testenv.Open(t)
	tenant := env.NewTenant(t)
	profile := env.SeedBlindPayProfile(t, tenant)
	suffix := fmt.Sprint(time.Now().UnixNano())
	quote := tenant.CreatePayoutQuote(t, profile, "bp-quote-"+scenario+"-"+suffix, amount)
	payment, status := tenant.CreatePaymentWithQuote(t, "bp-payment-"+scenario+"-"+suffix, "order-bp-"+scenario+"-"+suffix, quote)
	if status != http.StatusCreated {
		t.Fatalf("create payment status=%d", status)
	}
	return env, tenant, quote, payment
}

func complete(t *testing.T, env *testenv.Environment, tenant *testenv.Tenant, quote testenv.PayoutQuote, payment *testenv.Payment) {
	t.Helper()
	env.WaitForSagaState(t, payment.ID, "awaiting_settlement")
	env.WaitForCount(t, `SELECT count(*) FROM blindpay_payouts WHERE payment_id=$1 AND provider_payout_id=$2 AND provider_status='processing'`, 1, payment.ID, testenv.PayoutID(quote.ID))
	env.SendBlindPayWebhook(t, testenv.PayoutID(quote.ID), "completed")
	tenant.WaitForPaymentState(t, payment.ID, "succeeded")
	env.WaitForSagaState(t, payment.ID, "completed")
}

func TestBLINDPAY001SuccessfulPaymentLifecycle(t *testing.T) {
	env, tenant, quote, payment := newPayment(t, 2500, "001")
	complete(t, env, tenant, quote, payment)
	env.WaitForCount(t, `SELECT count(*) FROM blindpay_payouts WHERE payment_id=$1 AND provider_status='completed'`, 1, payment.ID)
	rows, err := env.DB.Query(`SELECT t.event_type,SUM(CASE WHEN e.side='debit' THEN e.amount_minor ELSE 0 END),SUM(CASE WHEN e.side='credit' THEN e.amount_minor ELSE 0 END) FROM ledger_transactions t JOIN ledger_entries e ON e.transaction_id=t.id WHERE t.payment_id=$1 GROUP BY t.event_type`, payment.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var event string
		var debit, credit int64
		if err := rows.Scan(&event, &debit, &credit); err != nil {
			t.Fatal(err)
		}
		if debit != credit || debit != 2500 {
			t.Fatalf("unbalanced %s journal debit=%d credit=%d", event, debit, credit)
		}
		count++
	}
	if count != 2 {
		t.Fatalf("journal count=%d, want 2", count)
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
		t.Fatalf("timeline=%v", timeline.Entries)
	}
	for i, state := range want {
		if timeline.Entries[i].State != state {
			t.Fatalf("timeline[%d]=%s want %s", i, timeline.Entries[i].State, state)
		}
	}
}

func TestBLINDPAY002PaymentIdempotency(t *testing.T) {
	env := testenv.Open(t)
	tenant := env.NewTenant(t)
	profile := env.SeedBlindPayProfile(t, tenant)
	suffix := fmt.Sprint(time.Now().UnixNano())
	quote := tenant.CreatePayoutQuote(t, profile, "bp-quote-002-"+suffix, 1250)
	key := "bp-payment-002-" + suffix
	first, status := tenant.CreatePaymentWithQuote(t, key, "order-bp-002", quote)
	if status != http.StatusCreated {
		t.Fatalf("first status=%d", status)
	}
	second, status := tenant.CreatePaymentWithQuote(t, key, "order-bp-002", quote)
	if status != http.StatusCreated || second.ID != first.ID {
		t.Fatalf("repeat status=%d first=%s second=%s", status, first.ID, second.ID)
	}
	if _, status := tenant.CreatePaymentWithQuoteAmount(t, key, "order-bp-002", quote, 1251); status != http.StatusConflict {
		t.Fatalf("changed request status=%d", status)
	}
	env.WaitForCount(t, `SELECT count(*) FROM payments WHERE idempotency_key=$1`, 1, key)
}

func TestBLINDPAY003TenantIsolation(t *testing.T) {
	env := testenv.Open(t)
	owner, stranger := env.NewTenant(t), env.NewTenant(t)
	profile := env.SeedBlindPayProfile(t, owner)
	suffix := fmt.Sprint(time.Now().UnixNano())
	quote := owner.CreatePayoutQuote(t, profile, "bp-quote-003-"+suffix, 900)
	payment, status := owner.CreatePaymentWithQuote(t, "bp-payment-003-"+suffix, "order-bp-003", quote)
	if status != http.StatusCreated {
		t.Fatalf("create status=%d", status)
	}
	for _, path := range []string{"/v1/payments/" + payment.ID, "/v1/payments/" + payment.ID + "/timeline"} {
		if response, body := owner.Get(t, path); response.StatusCode != http.StatusOK {
			t.Fatalf("owner %s status=%d body=%s", path, response.StatusCode, body)
		}
		if response, body := stranger.Get(t, path); response.StatusCode != http.StatusNotFound {
			t.Fatalf("stranger %s status=%d body=%s", path, response.StatusCode, body)
		}
	}
}

func TestBLINDPAY004PolicyRejection(t *testing.T) {
	env, tenant, _, payment := newPayment(t, 4004, "004")
	tenant.WaitForPaymentState(t, payment.ID, "failed")
	env.WaitForSagaState(t, payment.ID, "failed")
	env.WaitForCount(t, `SELECT count(*) FROM blindpay_payouts WHERE payment_id=$1`, 0, payment.ID)
	env.WaitForCount(t, `SELECT count(*) FROM ledger_transactions WHERE payment_id=$1`, 0, payment.ID)
}

func TestBLINDPAY005SettlementFailureReleasesFunds(t *testing.T) {
	env, tenant, quote, payment := newPayment(t, 66600, "005")
	env.WaitForSagaState(t, payment.ID, "awaiting_settlement")
	env.WaitForCount(t, `SELECT count(*) FROM blindpay_payouts WHERE payment_id=$1 AND provider_payout_id=$2 AND provider_status='processing'`, 1, payment.ID, testenv.PayoutID(quote.ID))
	env.SendBlindPayWebhook(t, testenv.PayoutID(quote.ID), "failed")
	tenant.WaitForPaymentState(t, payment.ID, "failed")
	env.WaitForSagaState(t, payment.ID, "ledger_released")
	for _, event := range []string{"payment.processing", "payment.released"} {
		env.WaitForCount(t, `SELECT count(*) FROM ledger_transactions WHERE payment_id=$1 AND event_type=$2`, 1, payment.ID, event)
	}
}

func TestBLINDPAY006TerminalReturn(t *testing.T) {
	env, tenant, quote, payment := newPayment(t, 77700, "006")
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer receiver.Close()
	if response, body := tenant.Post(t, "/v1/webhook-endpoints", map[string]string{"url": receiver.URL}); response.StatusCode != http.StatusCreated {
		t.Fatalf("register webhook status=%d body=%s", response.StatusCode, body)
	}
	env.SendBlindPayWebhook(t, testenv.PayoutID(quote.ID), "refunded")
	tenant.WaitForPaymentState(t, payment.ID, "returned")
	env.WaitForSagaState(t, payment.ID, "returned")
	env.WaitForCount(t, `SELECT count(*) FROM ledger_transactions WHERE payment_id=$1 AND event_type='payment.released'`, 1, payment.ID)
	env.WaitForCount(t, `SELECT count(*) FROM webhook_deliveries WHERE payment_id=$1 AND event_type='payment.returned' AND status='delivered'`, 1, payment.ID)
}

func TestBLINDPAY007ManualReviewResolution(t *testing.T) {
	env, tenant, quote, payment := newPayment(t, 7007, "007")
	env.WaitForSagaState(t, payment.ID, "awaiting_settlement")
	env.WaitForCount(t, `SELECT count(*) FROM blindpay_payouts WHERE payment_id=$1 AND provider_payout_id=$2 AND provider_status='processing'`, 1, payment.ID, testenv.PayoutID(quote.ID))
	env.SendBlindPayWebhook(t, testenv.PayoutID(quote.ID), "on_hold")
	env.WaitForSagaState(t, payment.ID, "manual_review")
	response, body := env.OperatorPost(t, "/v1/operator/payments/"+payment.ID+"/manual-review", map[string]string{"action": "complete", "operator": "blindpay-e2e", "note": "review passed"})
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("manual review status=%d body=%s", response.StatusCode, body)
	}
	tenant.WaitForPaymentState(t, payment.ID, "succeeded")
	env.WaitForSagaState(t, payment.ID, "completed")
	env.WaitForCount(t, `SELECT count(*) FROM saga_manual_review_actions a JOIN payment_sagas s ON s.id=a.saga_id WHERE s.payment_id=$1`, 1, payment.ID)
}

func TestBLINDPAY008IndependentSignedTenantWebhooks(t *testing.T) {
	env := testenv.Open(t)
	tenant := env.NewTenant(t)
	profile := env.SeedBlindPayProfile(t, tenant)
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
			t.Fatalf("register %d status=%d body=%s", i, response.StatusCode, body)
		}
		var created struct {
			Secret string `json:"signing_secret"`
		}
		if err := json.Unmarshal(body, &created); err != nil {
			t.Fatal(err)
		}
		secrets[i] = created.Secret
	}
	suffix := fmt.Sprint(time.Now().UnixNano())
	quote := tenant.CreatePayoutQuote(t, profile, "bp-quote-008-"+suffix, 8008)
	payment, status := tenant.CreatePaymentWithQuote(t, "bp-payment-008-"+suffix, "order-bp-008", quote)
	if status != http.StatusCreated {
		t.Fatalf("create status=%d", status)
	}
	complete(t, env, tenant, quote, payment)
	seen := [2]int{}
	deadline := time.After(60 * time.Second)
	for seen[0] < 3 || seen[1] < 3 {
		select {
		case delivery := <-deliveries:
			if delivery.signature != notification.Signature(secrets[delivery.endpoint], delivery.timestamp, delivery.body) {
				t.Fatalf("endpoint %d invalid signature", delivery.endpoint)
			}
			if !bytes.Contains(delivery.body, []byte(payment.ID)) {
				t.Fatalf("wrong payment body=%s", delivery.body)
			}
			seen[delivery.endpoint]++
		case <-deadline:
			t.Fatalf("webhook counts=%v", seen)
		}
	}
}

func TestBLINDPAY009RestartCompletesDurableWorkOnce(t *testing.T) {
	env := testenv.Open(t)
	tenant := env.NewTenant(t)
	profile := env.SeedBlindPayProfile(t, tenant)
	binary, rawPID := os.Getenv("STABLERAIL_E2E_SERVER_BINARY"), os.Getenv("STABLERAIL_E2E_SERVER_PID")
	if binary == "" || rawPID == "" {
		t.Skip("runner-managed server required")
	}
	suffix := fmt.Sprint(time.Now().UnixNano())
	quote := tenant.CreatePayoutQuote(t, profile, "bp-quote-009-"+suffix, 9009)
	env.SendBlindPayWebhook(t, testenv.PayoutID(quote.ID), "completed")
	payment, status := tenant.CreatePaymentWithQuote(t, "bp-payment-009-"+suffix, "order-bp-009", quote)
	if status != http.StatusCreated {
		t.Fatalf("create status=%d", status)
	}
	env.WaitForSagaState(t, payment.ID, "awaiting_settlement")
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
	tenant.WaitForPaymentState(t, payment.ID, "succeeded")
	env.WaitForSagaState(t, payment.ID, "completed")
	env.WaitForCount(t, `SELECT count(*) FROM blindpay_payouts WHERE payment_id=$1`, 1, payment.ID)
	env.WaitForCount(t, `SELECT count(*) FROM ledger_transactions WHERE payment_id=$1 AND event_type='payment.succeeded'`, 1, payment.ID)
}
