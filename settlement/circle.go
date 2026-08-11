package settlement

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type CircleConfig struct {
	APIKey, BaseURL string
	HTTPClient      *http.Client
}
type CircleProvider struct {
	apiKey, baseURL string
	client          *http.Client
	newUUID         func(string) string
}

func NewCircleProvider(c CircleConfig) (*CircleProvider, error) {
	if strings.TrimSpace(c.APIKey) == "" {
		return nil, errors.New("Circle API key is required")
	}
	if c.BaseURL == "" {
		c.BaseURL = "https://api.circle.com"
	}
	if c.HTTPClient == nil {
		c.HTTPClient = http.DefaultClient
	}
	return &CircleProvider{c.APIKey, strings.TrimRight(c.BaseURL, "/"), c.HTTPClient, uuidV4For}, nil
}
func (*CircleProvider) Name() string { return "circle" }
func (p *CircleProvider) Submit(ctx context.Context, r SettlementRequest) (SettlementResult, error) {
	if err := r.Validate(); err != nil {
		return SettlementResult{}, err
	}
	if r.Destination == nil {
		return SettlementResult{}, errors.New("Circle settlement requires a payment destination")
	}
	var recipient string
	var err error
	switch r.Destination.Type {
	case "circle_recipient":
		recipient = r.Destination.RecipientID
	case "blockchain_address":
		recipient, err = p.createRecipient(ctx, r.Destination)
		if err != nil {
			return SettlementResult{}, err
		}
	default:
		return SettlementResult{}, errors.New("unsupported Circle destination")
	}
	key := p.newUUID(r.IdempotencyKey)
	var result struct {
		Data struct {
			ID        string `json:"id"`
			Status    string `json:"status"`
			ErrorCode string `json:"errorCode"`
		} `json:"data"`
	}
	body := map[string]any{"idempotencyKey": key, "destination": map[string]string{"type": "address_book", "id": recipient}, "amount": map[string]string{"amount": formatMinor(r.AmountMinor), "currency": r.Currency}, "purposeOfTransfer": "PMT001"}
	if err := p.do(ctx, "/v1/payouts", body, &result); err != nil {
		return SettlementResult{}, err
	}
	if result.Data.ID == "" {
		return SettlementResult{}, errors.New("Circle payout response omitted ID")
	}
	status := circleStatus(result.Data.Status)
	failureCode := result.Data.ErrorCode
	if status == StatusFailed && failureCode == "" {
		failureCode = "circle_payout_failed"
	}
	return SettlementResult{ProviderReference: result.Data.ID, Status: status, FailureCode: failureCode}, nil
}
func (p *CircleProvider) createRecipient(ctx context.Context, d *Destination) (string, error) {
	key := p.newUUID("recipient:" + d.Chain + ":" + d.Address)
	var result struct {
		Data struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := p.do(ctx, "/v1/addressBook/recipients", map[string]any{"idempotencyKey": key, "chain": d.Chain, "address": d.Address}, &result); err != nil {
		return "", err
	}
	if result.Data.ID == "" {
		return "", errors.New("Circle recipient response omitted ID")
	}
	return result.Data.ID, nil
}
func (p *CircleProvider) do(ctx context.Context, path string, body, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+p.apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("Circle request: %w", err)
	}
	defer resp.Body.Close()
	raw, err = io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		retryable := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
		return &ProviderError{Message: fmt.Sprintf("Circle API %s: %s", resp.Status, strings.TrimSpace(string(raw))), Retryable: retryable}
	}
	if err = json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode Circle response: %w", err)
	}
	return nil
}
func circleStatus(s string) Status {
	switch strings.ToLower(s) {
	case "complete":
		return StatusSucceeded
	case "failed":
		return StatusFailed
	default:
		return StatusPending
	}
}
func formatMinor(n int64) string { return fmt.Sprintf("%d.%02d", n/100, n%100) }
func uuidV4For(key string) string {
	sum := sha256.Sum256([]byte(key))
	b := sum[:16]
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	s := hex.EncodeToString(b)
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:]
}
