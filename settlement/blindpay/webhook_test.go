package blindpay

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"stablerail/eventbus"
	"stablerail/paymentcore"
)

func TestWebhookVerifierAcceptsV1Signature(t *testing.T) {
	key := []byte("test-signing-key")
	verifier, err := NewWebhookVerifier("whsec_" + base64.StdEncoding.EncodeToString(key))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 12, 20, 0, 0, 0, time.UTC)
	verifier.now = func() time.Time { return now }
	body := []byte(`{"webhook_event":"payout.complete","id":"po_test","status":"completed"}`)
	timestamp := "1786564800"
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte("msg_test." + timestamp + "."))
	_, _ = mac.Write(body)
	signature := "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if err := verifier.Verify("msg_test", timestamp, signature, body); err != nil {
		t.Fatal(err)
	}
}

func TestPayoutWebhookServicePersistsCompletionAndEnqueuesSagaEvent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service, err := NewWebhookService(db)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 12, 20, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	body := []byte(`{"webhook_event":"payout.complete","id":"po_test","status":"completed"}`)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO blindpay_webhook_events").
		WithArgs("msg_test", "payout.complete", "po_test", body, now).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT payment_id,provider_status FROM payouts").
		WithArgs("po_test").
		WillReturnRows(sqlmock.NewRows([]string{"payment_id", "provider_status"}).AddRow("pay_test", "processing"))
	mock.ExpectExec("UPDATE payouts SET provider_status").
		WithArgs("completed", body, now, "pay_test").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT correlation_id FROM payment_sagas").
		WithArgs("pay_test").
		WillReturnRows(sqlmock.NewRows([]string{"correlation_id"}).AddRow("corr_test"))
	mock.ExpectExec("INSERT INTO outbox_events").
		WithArgs("evt_blindpay_msg_test", paymentcore.PaymentEventsTopic, "settlement.completed", 1, "pay_test", sqlmock.AnyArg(), now).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := service.Process(context.Background(), "msg_test", body); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPayoutWebhookServiceMapsRefundedToReturnedOutcome(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service, _ := NewWebhookService(db)
	now := time.Date(2026, time.August, 12, 20, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	body := []byte(`{"webhook_event":"payout.complete","id":"po_test","status":"refunded"}`)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO blindpay_webhook_events").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT payment_id,provider_status FROM payouts").WillReturnRows(sqlmock.NewRows([]string{"payment_id", "provider_status"}).AddRow("pay_test", "processing"))
	mock.ExpectExec("UPDATE payouts SET provider_status").WithArgs("refunded", body, now, "pay_test").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT correlation_id FROM payment_sagas").WillReturnRows(sqlmock.NewRows([]string{"correlation_id"}).AddRow("corr_test"))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_blindpay_msg_refund", paymentcore.PaymentEventsTopic, "settlement.returned", 1, "pay_test", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := service.Process(context.Background(), "msg_refund", body); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCompletedPayoutCanAdvanceOnlyToRefunded(t *testing.T) {
	if !payoutStatusCanAdvance("completed", "refunded") {
		t.Fatal("completed payout must allow a later provider return")
	}
	if payoutStatusCanAdvance("completed", "failed") {
		t.Fatal("completed payout must not regress to failed")
	}
	if payoutStatusCanAdvance("refunded", "completed") {
		t.Fatal("refunded payout must be terminal")
	}
}

func TestPayoutWebhookRecordsPostSuccessReturnWithoutChangingPayment(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service, _ := NewWebhookService(db)
	now := time.Date(2026, time.August, 16, 20, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	body := []byte(`{"webhook_event":"payout.complete","id":"po_test","status":"refunded"}`)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO blindpay_webhook_events").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT payment_id,provider_status FROM payouts").WillReturnRows(sqlmock.NewRows([]string{"payment_id", "provider_status"}).AddRow("pay_test", "completed"))
	mock.ExpectExec("UPDATE payouts SET provider_status").WithArgs("refunded", body, now, "pay_test").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT payment_status,funds_status,amount_minor,currency").WithArgs("pay_test").WillReturnRows(sqlmock.NewRows([]string{"payment_status", "funds_status", "amount_minor", "currency"}).AddRow("succeeded", "consumed", 2500, "USD"))
	mock.ExpectExec("INSERT INTO ledger_transactions").WithArgs("jrn_ret_blindpay_msg_return", "pay_test", now).WillReturnResult(sqlmock.NewResult(0, 1))
	for _, line := range []struct{ id, account, side string }{
		{"jrn_ret_blindpay_msg_return:debit", "cash:operating", "debit"},
		{"jrn_ret_blindpay_msg_return:credit", "settlement:payable", "credit"},
	} {
		mock.ExpectExec("INSERT INTO ledger_entries").WithArgs(line.id, "jrn_ret_blindpay_msg_return", line.account, line.side, int64(2500), "USD").WillReturnResult(sqlmock.NewResult(0, 1))
	}
	mock.ExpectExec("INSERT INTO payment_returns").WithArgs("ret_blindpay_msg_return", "pay_test", "blindpay", "msg_return", int64(2500), "USD", "provider returned funds after completed payout", "jrn_ret_blindpay_msg_return", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_audit_events").WithArgs("pay_test", "provider returned funds after completed payout", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO payment_timeline_entries").WithArgs("pay_test", "provider returned funds after completed payout", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_blindpay_msg_return", paymentcore.PaymentEventsTopic, eventbus.PaymentReturnSucceededVersion, "pay_test", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if err := service.Process(context.Background(), "msg_return", body); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestReconcileOnceRepairsInitiallyUnmatchedTerminalWebhook(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service, _ := NewWebhookService(db)
	now := time.Date(2026, time.August, 13, 20, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	body := []byte(`{"webhook_event":"payout.complete","id":"po_early","status":"completed"}`)
	mock.ExpectQuery("SELECT w.svix_id,w.payload").WillReturnRows(sqlmock.NewRows([]string{"svix_id", "payload"}).AddRow("msg_early", body))
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO blindpay_webhook_events").WithArgs("msg_early", "payout.complete", "po_early", body, now).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT payment_id,provider_status FROM payouts").WithArgs("po_early").WillReturnRows(sqlmock.NewRows([]string{"payment_id", "provider_status"}).AddRow("pay_early", "processing"))
	mock.ExpectExec("UPDATE payouts SET provider_status").WithArgs("completed", body, now, "pay_early").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT correlation_id FROM payment_sagas").WithArgs("pay_early").WillReturnRows(sqlmock.NewRows([]string{"correlation_id"}).AddRow("corr_early"))
	mock.ExpectExec("INSERT INTO outbox_events").WithArgs("evt_blindpay_msg_early", paymentcore.PaymentEventsTopic, "settlement.completed", 1, "pay_early", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectQuery("FROM blindpay_webhook_events w WHERE w.webhook_event LIKE 'payin").WillReturnRows(sqlmock.NewRows([]string{"svix_id", "payload"}))

	count, err := service.ReconcileOnce(context.Background())
	if err != nil || count != 1 {
		t.Fatalf("ReconcileOnce = (%d, %v)", count, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPayinCompletionUpdatesStatusPostsLedgerAndNotifiesTenant(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	service, _ := NewWebhookService(db)
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	body := []byte(`{"webhook_event":"payin.complete","id":"pi_test","status":"completed"}`)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO blindpay_webhook_events").WithArgs("msg_payin", "payin.complete", "pi_test", body, now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT p.id,p.status,q.receiver_amount_minor").WithArgs("pi_test").WillReturnRows(sqlmock.NewRows([]string{"id", "status", "amount", "currency"}).AddRow("pin_1", "processing", int64(9900), "USDC"))
	mock.ExpectExec("UPDATE payins SET status").WithArgs("succeeded", body, "completed", now, "pin_1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO webhook_deliveries").WithArgs("evt_blindpay_pi_test_succeeded", "pin_1", "payin.succeeded", sqlmock.AnyArg(), now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO ledger_transactions").WithArgs("jrn_pin_1_succeeded", "pin_1", "payin.succeeded", now).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO ledger_entries").WithArgs("jrn_pin_1_succeeded:debit", "jrn_pin_1_succeeded", paymentcore.CashOperatingAccount, "debit", int64(9900), "USDC").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO ledger_entries").WithArgs("jrn_pin_1_succeeded:credit", "jrn_pin_1_succeeded", paymentcore.SettlementAccount, "credit", int64(9900), "USDC").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO provider_webhook_applications").WithArgs("msg_payin", now, "pi_test").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := service.Process(context.Background(), "msg_payin", body); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
