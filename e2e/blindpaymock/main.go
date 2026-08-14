package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

type mock struct {
	mu          sync.Mutex
	amounts     map[string]int64
	payoutCalls map[string]int
}

func main() {
	m := &mock{amounts: map[string]int64{}, payoutCalls: map[string]int{}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/instances/in_e2e/quotes", m.quote)
	mux.HandleFunc("GET /v1/instances/in_e2e/customers/{customer}/wallets/{wallet}/balance", m.balance)
	mux.HandleFunc("POST /v1/instances/in_e2e/payouts/evm", m.payout)
	mux.HandleFunc("GET /__e2e/payout-calls/{quote}", m.calls)
	log.Fatal(http.ListenAndServe(":18081", mux))
}

func (m *mock) authorize(w http.ResponseWriter, r *http.Request) bool {
	if r.Header.Get("Authorization") != "Bearer blindpay-e2e-key" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

func stableID(prefix, value string) string {
	sum := sha256.Sum256([]byte(value))
	return prefix + hex.EncodeToString(sum[:12])
}

func (m *mock) quote(w http.ResponseWriter, r *http.Request) {
	if !m.authorize(w, r) {
		return
	}
	var input struct {
		RequestAmount int64 `json:"request_amount"`
	}
	if json.NewDecoder(r.Body).Decode(&input) != nil || input.RequestAmount <= 0 {
		http.Error(w, "bad quote", http.StatusBadRequest)
		return
	}
	id := stableID("qu_", r.Header.Get("Idempotency-Key"))
	m.mu.Lock()
	m.amounts[id] = input.RequestAmount
	m.mu.Unlock()
	writeJSON(w, map[string]any{"id": id, "expires_at": time.Now().Add(5 * time.Minute).UnixMilli(), "commercial_quotation": "1", "blindpay_quotation": "1", "sender_amount": input.RequestAmount, "receiver_amount": input.RequestAmount, "partner_fee_amount": 0, "flat_fee": 0})
}

func (m *mock) balance(w http.ResponseWriter, r *http.Request) {
	if !m.authorize(w, r) {
		return
	}
	writeJSON(w, map[string]any{"USDB": map[string]any{"id": "asset_usdb", "address": "0xe2e", "amount": int64(1_000_000_000)}})
}

func (m *mock) payout(w http.ResponseWriter, r *http.Request) {
	if !m.authorize(w, r) {
		return
	}
	var input struct {
		QuoteID string `json:"quote_id"`
		Sender  string `json:"sender_wallet_address"`
	}
	if json.NewDecoder(r.Body).Decode(&input) != nil || !strings.HasPrefix(input.QuoteID, "qu_") {
		http.Error(w, "bad payout", http.StatusBadRequest)
		return
	}
	m.mu.Lock()
	m.payoutCalls[input.QuoteID]++
	m.mu.Unlock()
	writeJSON(w, map[string]any{"id": "po_" + strings.TrimPrefix(input.QuoteID, "qu_"), "status": "processing", "sender_wallet_address": input.Sender, "customer_id": "re_e2e", "bank_account_id": "ba_e2e"})
}

func (m *mock) calls(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	count := m.payoutCalls[r.PathValue("quote")]
	m.mu.Unlock()
	writeJSON(w, map[string]int{"count": count})
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
