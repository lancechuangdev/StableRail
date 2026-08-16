package notification

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"stablerail/eventbus"
)

func TestSignatureCoversTimestampAndExactBody(t *testing.T) {
	body := []byte(`{"type":"payment.succeeded"}`)
	mac := hmac.New(sha256.New, []byte("secret"))
	mac.Write([]byte("1700000000."))
	mac.Write(body)
	want := "v1=" + hex.EncodeToString(mac.Sum(nil))
	if got := Signature("secret", "1700000000", body); got != want {
		t.Fatalf("Signature() = %q, want %q", got, want)
	}
	if Signature("secret", "1700000001", body) == want {
		t.Fatal("signature did not cover timestamp")
	}
}

func TestNewDispatcherRejectsInvalidRetryWindow(t *testing.T) {
	_, err := NewDispatcher(&sql.DB{}, nil, Config{InitialBackoff: 2 * time.Second, MaxBackoff: time.Second})
	if err == nil {
		t.Fatal("expected invalid retry window to be rejected")
	}
}

func TestEventHandlerCreatesReturnDelivery(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	event := eventbus.Event{ID: "evt_returned", Type: "payment.funds_returned", Version: 1, AggregateID: "pay_1", AggregateType: "payment", Payload: []byte(`{"reason":"provider returned payout funds"}`), OccurredAt: now}
	mock.ExpectQuery("SELECT tenant_id FROM payments").WithArgs("pay_1").WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow("cus_1"))
	mock.ExpectExec("INSERT INTO webhook_deliveries").WithArgs("evt_returned", "pay_1", "payment.funds_returned", sqlmock.AnyArg(), now, "cus_1").WillReturnResult(sqlmock.NewResult(0, 1))

	if err := EventHandler()(context.Background(), tx, event); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEventHandlerIgnoresProviderCompletionUntilPaymentSettles(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	tx, _ := db.Begin()
	event := eventbus.Event{ID: "evt_provider", Type: "settlement.completed", Version: 1, AggregateID: "pay_1", AggregateType: "payment", Payload: []byte(`{}`), OccurredAt: time.Now()}
	if err := EventHandler()(context.Background(), tx, event); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
