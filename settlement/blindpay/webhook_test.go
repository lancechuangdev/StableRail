package blindpay

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

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
	service, err := NewPayoutWebhookService(db)
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
	mock.ExpectQuery("SELECT payment_id,provider_status FROM blindpay_payouts").
		WithArgs("po_test").
		WillReturnRows(sqlmock.NewRows([]string{"payment_id", "provider_status"}).AddRow("pay_test", "processing"))
	mock.ExpectExec("UPDATE blindpay_payouts SET provider_status").
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
