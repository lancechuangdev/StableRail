package notification

import (
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"testing"
	"time"
)

func TestSignatureCoversTimestampAndExactBody(t *testing.T) {
	body := []byte(`{"type":"payment.settled"}`)
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
