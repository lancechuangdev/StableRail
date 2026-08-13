// Package blindpay implements the StableRail settlement adapter for BlindPay's payout API.
package blindpay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Config struct {
	APIKey, InstanceID, BaseURL string
	HTTPClient                  *http.Client
}

type Client struct {
	apiKey, instanceID, baseURL string
	httpClient                  *http.Client
}

func NewClient(c Config) (*Client, error) {
	if strings.TrimSpace(c.APIKey) == "" || !strings.HasPrefix(c.InstanceID, "in_") {
		return nil, errors.New("BlindPay API key and valid instance ID are required")
	}
	if c.BaseURL == "" {
		c.BaseURL = "https://api.blindpay.com/v1"
	}
	u, err := url.Parse(c.BaseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, errors.New("invalid BlindPay base URL")
	}
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &Client{apiKey: c.APIKey, instanceID: c.InstanceID, baseURL: strings.TrimRight(c.BaseURL, "/"), httpClient: c.HTTPClient}, nil
}

type ErrorKind string

const (
	ErrorRetryable    ErrorKind = "retryable"
	ErrorPermanent    ErrorKind = "permanent"
	ErrorUserAction   ErrorKind = "user_action_required"
	ErrorUnauthorized ErrorKind = "unauthorized"
)

type APIError struct {
	StatusCode    int
	Code, Message string
	Kind          ErrorKind
}

func (e *APIError) Error() string {
	return fmt.Sprintf("BlindPay API (%d, %s): %s", e.StatusCode, e.Code, e.Message)
}

type QuoteRequest struct {
	IdempotencyKey string `json:"-"`
	BankAccountID  string `json:"bank_account_id"`
	CurrencyType   string `json:"currency_type"`
	CoverFees      bool   `json:"cover_fees"`
	RequestAmount  int64  `json:"request_amount"`
	Network        string `json:"network"`
	Token          string `json:"token"`
	PartnerFeeID   string `json:"partner_fee_id,omitempty"`
}

type Contract struct {
	Address, FunctionName, BlindPayContractAddress, Amount string
	ABI                                                    json.RawMessage
	Network                                                struct {
		Name    string `json:"name"`
		ChainID int64  `json:"chainId"`
	} `json:"network"`
}

func (c *Contract) UnmarshalJSON(data []byte) error {
	type wire struct {
		Address                 string          `json:"address"`
		FunctionName            string          `json:"functionName"`
		BlindPayContractAddress string          `json:"blindpayContractAddress"`
		Amount                  string          `json:"amount"`
		ABI                     json.RawMessage `json:"abi"`
		Network                 struct {
			Name    string `json:"name"`
			ChainID int64  `json:"chainId"`
		} `json:"network"`
	}
	var w wire
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	c.Address, c.FunctionName, c.BlindPayContractAddress, c.Amount, c.ABI, c.Network = w.Address, w.FunctionName, w.BlindPayContractAddress, w.Amount, w.ABI, w.Network
	return nil
}

type Quote struct {
	ID                                                      string      `json:"id"`
	ExpiresAt                                               int64       `json:"expires_at"`
	CommercialQuotation                                     json.Number `json:"commercial_quotation"`
	BlindPayQuotation                                       json.Number `json:"blindpay_quotation"`
	SenderAmount, ReceiverAmount, PartnerFeeAmount, FlatFee int64
	BillingFeeAmount                                        *int64
	Contract                                                *Contract `json:"contract"`
}

func (q *Quote) UnmarshalJSON(data []byte) error {
	type wire struct {
		ID                  string      `json:"id"`
		ExpiresAt           int64       `json:"expires_at"`
		CommercialQuotation json.Number `json:"commercial_quotation"`
		BlindPayQuotation   json.Number `json:"blindpay_quotation"`
		SenderAmount        int64       `json:"sender_amount"`
		ReceiverAmount      int64       `json:"receiver_amount"`
		PartnerFeeAmount    int64       `json:"partner_fee_amount"`
		FlatFee             int64       `json:"flat_fee"`
		BillingFeeAmount    *int64      `json:"billing_fee_amount"`
		Contract            *Contract   `json:"contract"`
	}
	var w wire
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	*q = Quote(w)
	return nil
}

func (c *Client) CreateQuote(ctx context.Context, r QuoteRequest) (Quote, error) {
	if r.BankAccountID == "" || r.RequestAmount <= 0 || (r.CurrencyType != "sender" && r.CurrencyType != "receiver") || r.Network == "" || r.Token == "" {
		return Quote{}, errors.New("invalid BlindPay payout quote request")
	}
	var out Quote
	err := c.do(ctx, http.MethodPost, "/quotes", r.IdempotencyKey, r, &out)
	if err == nil && (!strings.HasPrefix(out.ID, "qu_") || out.ExpiresAt <= 0 || out.SenderAmount <= 0 || out.ReceiverAmount <= 0) {
		err = errors.New("BlindPay returned an invalid payout quote")
	}
	return out, err
}

type PayoutRequest struct {
	IdempotencyKey      string `json:"-"`
	QuoteID             string `json:"quote_id"`
	SenderWalletAddress string `json:"sender_wallet_address"`
}
type Payout struct {
	ID                  string `json:"id"`
	Status              string `json:"status"`
	SenderWalletAddress string `json:"sender_wallet_address"`
	CustomerID          string `json:"customer_id"`
	BankAccountID       string `json:"bank_account_id"`
}

func (c *Client) CreateEVMPayout(ctx context.Context, r PayoutRequest) (Payout, error) {
	if !strings.HasPrefix(r.QuoteID, "qu_") || r.SenderWalletAddress == "" {
		return Payout{}, errors.New("invalid BlindPay payout request")
	}
	var out Payout
	err := c.do(ctx, http.MethodPost, "/payouts/evm", r.IdempotencyKey, r, &out)
	if err == nil && (!strings.HasPrefix(out.ID, "po_") || out.Status == "") {
		err = errors.New("BlindPay returned an invalid payout")
	}
	return out, err
}

func (c *Client) GetPayout(ctx context.Context, id string) (Payout, error) {
	if !strings.HasPrefix(id, "po_") {
		return Payout{}, errors.New("invalid BlindPay payout ID")
	}
	var out Payout
	err := c.do(ctx, http.MethodGet, "/payouts/"+url.PathEscape(id), "", nil, &out)
	return out, err
}

func (c *Client) do(ctx context.Context, method, path, idempotencyKey string, body, out any) error {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+"/instances/"+url.PathEscape(c.instanceID)+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	res, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 2<<20))
	if err != nil {
		return err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return classifyAPIError(res.StatusCode, raw)
	}
	if out != nil && len(raw) > 0 {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(out); err != nil {
			return fmt.Errorf("decode BlindPay response: %w", err)
		}
	}
	return nil
}

func classifyAPIError(status int, raw []byte) error {
	var payload struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Error   string `json:"error"`
	}
	_ = json.Unmarshal(raw, &payload)
	if payload.Message == "" {
		payload.Message = payload.Error
	}
	if payload.Message == "" {
		payload.Message = strings.TrimSpace(string(raw))
	}
	kind := ErrorPermanent
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		kind = ErrorUnauthorized
	} else if status == http.StatusTooManyRequests || status >= 500 {
		kind = ErrorRetryable
	} else if payload.Code == "please_accept_terms_of_service" || strings.Contains(strings.ToLower(payload.Message), "kyc") || strings.Contains(strings.ToLower(payload.Message), "compliance") {
		kind = ErrorUserAction
	}
	return &APIError{StatusCode: status, Code: payload.Code, Message: payload.Message, Kind: kind}
}
