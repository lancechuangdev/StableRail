//go:build e2e

package testenv

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

const blindPayWebhookKey = "blindpay-e2e-secret"

type PayoutQuote struct {
	ID                string `json:"id"`
	SenderAmountMinor int64  `json:"sender_amount_minor"`
}

type BlindPayProfile struct{ CustomerID, BankAccountID, WalletID string }

func (e *Environment) SeedBlindPayProfile(t *testing.T, tenant *Tenant) BlindPayProfile {
	t.Helper()
	suffix := strings.TrimPrefix(tenant.ID, "tenant-e2e-")
	profile := BlindPayProfile{CustomerID: "re_" + suffix, BankAccountID: "ba_" + suffix, WalletID: "bl_" + suffix}
	now := time.Now().UTC()
	if _, err := e.DB.Exec(`INSERT INTO blindpay_customers(tenant_id,provider_customer_id,kyc_status,created_at,updated_at) VALUES($1,$2,'approved',$3,$3)`, tenant.ID, profile.CustomerID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := e.DB.Exec(`INSERT INTO blindpay_bank_accounts(provider_bank_account_id,tenant_id,rail,display_name,account_last_four,status,created_at,updated_at) VALUES($1,$2,'ach','E2E bank','1234','approved',$3,$3)`, profile.BankAccountID, tenant.ID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := e.DB.Exec(`INSERT INTO blindpay_managed_wallets(provider_wallet_id,tenant_id,network,address,display_name,status,created_at,updated_at) VALUES($1,$2,'base_sepolia',$3,'E2E wallet','active',$4,$4)`, profile.WalletID, tenant.ID, "0x"+suffix, now); err != nil {
		t.Fatal(err)
	}
	return profile
}

func (tenant *Tenant) CreatePayoutQuote(t *testing.T, profile BlindPayProfile, key string, amount int64) PayoutQuote {
	t.Helper()
	body := map[string]any{"bank_account_id": profile.BankAccountID, "managed_wallet_id": profile.WalletID, "destination_currency": "USD", "currency_type": "sender", "cover_fees": false, "request_amount_minor": amount}
	response := tenant.Env.request(t, http.MethodPost, "/v1/blindpay/payout-quotes", tenant.APIKey, key, body)
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create payout quote status=%d body=%s", response.StatusCode, readBody(response.Body))
	}
	var quote PayoutQuote
	decode(t, response.Body, &quote)
	return quote
}

func (tenant *Tenant) CreatePaymentWithQuote(t *testing.T, key, reference string, quote PayoutQuote) (*Payment, int) {
	return tenant.CreatePaymentWithQuoteAmount(t, key, reference, quote, quote.SenderAmountMinor)
}

func (tenant *Tenant) CreatePaymentWithQuoteAmount(t *testing.T, key, reference string, quote PayoutQuote, amount int64) (*Payment, int) {
	t.Helper()
	body := map[string]any{"external_reference": reference, "currency": "USDB", "amount_minor": amount, "payout_quote_id": quote.ID}
	response := tenant.Env.request(t, http.MethodPost, "/v1/payments", tenant.APIKey, key, body)
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		return nil, response.StatusCode
	}
	var payment Payment
	decode(t, response.Body, &payment)
	return &payment, response.StatusCode
}

func (e *Environment) SendBlindPayWebhook(t *testing.T, payoutID, status string) {
	t.Helper()
	messageID := fmt.Sprintf("msg_e2e_%d", time.Now().UnixNano())
	body, _ := json.Marshal(map[string]string{"webhook_event": "payout.complete", "id": payoutID, "status": status})
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	mac := hmac.New(sha256.New, []byte(blindPayWebhookKey))
	_, _ = mac.Write([]byte(messageID + "." + timestamp + "."))
	_, _ = mac.Write(body)
	request, err := http.NewRequest(http.MethodPost, e.BaseURL+"/v1/providers/blindpay/webhooks", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("svix-id", messageID)
	request.Header.Set("svix-timestamp", timestamp)
	request.Header.Set("svix-signature", "v1,"+base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	response, err := e.HTTP.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("BlindPay webhook status=%d body=%s", response.StatusCode, readBody(response.Body))
	}
}

func PayoutID(quoteID string) string { return "po_" + strings.TrimPrefix(quoteID, "qu_") }
