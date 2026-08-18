package blindpay

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func response(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}
func testClient(t *testing.T, fn roundTripFunc) *Client {
	t.Helper()
	c, err := NewClient(Config{APIKey: "secret", InstanceID: "in_test", BaseURL: "https://blindpay.test", HTTPClient: &http.Client{Transport: fn}})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestCreateQuoteContract(t *testing.T) {
	c := testClient(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/instances/in_test/quotes" || r.Header.Get("Authorization") != "Bearer secret" || r.Header.Get("Idempotency-Key") != "idem-1" {
			t.Fatalf("unexpected request: %s, headers=%v", r.URL.Path, r.Header)
		}
		return response(http.StatusOK, `{"id":"qu_test","expires_at":1712958191000,"commercial_quotation":1.001,"blindpay_quotation":0.998,"sender_amount":10000,"receiver_amount":9980,"partner_fee_amount":0,"flat_fee":20,"billing_fee_amount":null,"contract":{"address":"0xtoken","abi":[],"functionName":"approve","blindpayContractAddress":"0xspender","amount":"100000000","network":{"name":"base","chainId":8453}}}`), nil
	})
	q, err := c.CreateQuote(context.Background(), QuoteRequest{IdempotencyKey: "idem-1", BankAccountID: "ba_test", CurrencyType: "sender", RequestAmount: 10000, Network: "base", Token: "USDC"})
	if err != nil {
		t.Fatal(err)
	}
	if q.ID != "qu_test" || q.CommercialQuotation.String() != "1.001" || q.Contract == nil || q.Contract.Network.ChainID != 8453 || !strings.Contains(string(q.RawPayload), `"id":"qu_test"`) {
		t.Fatalf("unexpected quote: %+v", q)
	}
}

func TestCreatePayoutContract(t *testing.T) {
	c := testClient(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/instances/in_test/payouts/evm" {
			t.Fatalf("path=%s", r.URL.Path)
		}
		return response(http.StatusOK, `{"id":"po_test","status":"processing","sender_wallet_address":"0xabc","customer_id":"re_test","bank_account_id":"ba_test"}`), nil
	})
	p, err := c.CreateEVMPayout(context.Background(), PayoutRequest{QuoteID: "qu_test", SenderWalletAddress: "0xabc"})
	if err != nil || p.ID != "po_test" || p.Status != "processing" {
		t.Fatalf("payout=%+v err=%v", p, err)
	}
}

func TestCreatePayinQuoteAndPayinContracts(t *testing.T) {
	calls := 0
	c := testClient(t, func(r *http.Request) (*http.Response, error) {
		calls++
		switch r.URL.Path {
		case "/instances/in_test/payin-quotes":
			return response(http.StatusOK, `{"id":"pq_test","expires_at":1912958191000,"sender_amount":10000,"receiver_amount":9900}`), nil
		case "/instances/in_test/payins/evm":
			return response(http.StatusOK, `{"id":"pi_test","status":"processing","memo_code":"ABC123"}`), nil
		default:
			t.Fatalf("path=%s", r.URL.Path)
			return nil, nil
		}
	})
	q, err := c.CreatePayinQuote(context.Background(), PayinQuoteRequest{IdempotencyKey: "quote-1", WalletID: "bl_test", CurrencyType: "sender", RequestAmount: 10000, PaymentMethod: "ach", Token: "USDB"})
	if err != nil || q.ID != "pq_test" {
		t.Fatalf("quote=%+v err=%v", q, err)
	}
	p, err := c.CreatePayin(context.Background(), PayinRequest{IdempotencyKey: "payin-1", PayinQuoteID: q.ID})
	if err != nil || p.ID != "pi_test" || !strings.Contains(string(p.Instructions), "memo_code") {
		t.Fatalf("payin=%+v err=%v", p, err)
	}
	if calls != 2 {
		t.Fatalf("calls=%d", calls)
	}
}

func TestGetManagedWalletBalanceContract(t *testing.T) {
	c := testClient(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/instances/in_test/customers/re_test/wallets/bl_test/balance" {
			t.Fatalf("path=%s", r.URL.Path)
		}
		return response(http.StatusOK, `{"USDB":{"address":"0xabc","id":"","symbol":"USDB","amount":2500}}`), nil
	})
	balance, err := c.GetManagedWalletBalance(context.Background(), "re_test", "bl_test")
	if err != nil {
		t.Fatal(err)
	}
	if balance["USDB"].Amount != 2500 || balance["USDB"].Address != "0xabc" {
		t.Fatalf("unexpected balance: %+v", balance)
	}
}

func TestErrorClassification(t *testing.T) {
	c := testClient(t, func(*http.Request) (*http.Response, error) {
		return response(http.StatusBadRequest, `{"code":"please_accept_terms_of_service","message":"accept terms"}`), nil
	})
	_, err := c.CreateQuote(context.Background(), QuoteRequest{BankAccountID: "ba_test", CurrencyType: "sender", RequestAmount: 1, Network: "base", Token: "USDC"})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Kind != ErrorUserAction {
		t.Fatalf("error=%v", err)
	}
}

func TestRetryableServerError(t *testing.T) {
	c := testClient(t, func(*http.Request) (*http.Response, error) {
		return response(http.StatusServiceUnavailable, "down"), nil
	})
	_, err := c.GetPayout(context.Background(), "po_test")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Kind != ErrorRetryable {
		t.Fatalf("error=%v", err)
	}
}
