package settlement

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCircleProviderCreatesRecipientForBlockchainDestination(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.Header.Get("Authorization") != "Bearer key" {
			t.Fatal("missing bearer authentication")
		}
		if r.URL.Path == "/v1/addressBook/recipients" {
			_, _ = w.Write([]byte(`{"data":{"id":"recipient-1"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"data":{"id":"payout-1","status":"pending"}}`))
	}))
	defer server.Close()
	p, err := NewCircleProvider(CircleConfig{APIKey: "key", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Submit(context.Background(), SettlementRequest{IdempotencyKey: "evt", PaymentID: "pay", AmountMinor: 1234, Currency: "USD", Destination: &Destination{Type: "blockchain_address", Chain: "BASE", Address: "0xabc"}})
	if err != nil {
		t.Fatal(err)
	}
	if got.ProviderReference != "payout-1" || got.Status != StatusPending {
		t.Fatalf("result = %#v", got)
	}
	if len(paths) != 2 || paths[0] != "/v1/addressBook/recipients" || paths[1] != "/v1/payouts" {
		t.Fatalf("paths = %v", paths)
	}
}

func TestCircleStatusUsesDocumentedPayoutStatuses(t *testing.T) {
	if circleStatus("complete") != StatusSucceeded || circleStatus("failed") != StatusFailed || circleStatus("pending") != StatusPending {
		t.Fatal("documented Circle statuses mapped incorrectly")
	}
	if circleStatus("confirmed") != StatusPending {
		t.Fatal("unsupported status must not be treated as success")
	}
}
