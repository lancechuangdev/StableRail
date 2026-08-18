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
	Contract                                                *Contract       `json:"contract"`
	RawPayload                                              json.RawMessage `json:"-"`
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
	q.ID = w.ID
	q.ExpiresAt = w.ExpiresAt
	q.CommercialQuotation = w.CommercialQuotation
	q.BlindPayQuotation = w.BlindPayQuotation
	q.SenderAmount = w.SenderAmount
	q.ReceiverAmount = w.ReceiverAmount
	q.PartnerFeeAmount = w.PartnerFeeAmount
	q.FlatFee = w.FlatFee
	q.BillingFeeAmount = w.BillingFeeAmount
	q.Contract = w.Contract
	return nil
}

func (c *Client) CreateQuote(ctx context.Context, r QuoteRequest) (Quote, error) {
	if r.BankAccountID == "" || r.RequestAmount <= 0 || (r.CurrencyType != "sender" && r.CurrencyType != "receiver") || r.Network == "" || r.Token == "" {
		return Quote{}, errors.New("invalid BlindPay payout quote request")
	}
	var out Quote
	raw, err := c.do(ctx, http.MethodPost, "/quotes", r.IdempotencyKey, r, &out)
	out.RawPayload = raw
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
	ID                  string          `json:"id"`
	Status              string          `json:"status"`
	SenderWalletAddress string          `json:"sender_wallet_address"`
	CustomerID          string          `json:"customer_id"`
	BankAccountID       string          `json:"bank_account_id"`
	RawPayload          json.RawMessage `json:"-"`
}

type ManagedWalletAsset struct {
	Address, ID, Symbol string
	Amount              int64
}

// GetManagedWalletBalance returns raw token amounts, expressed in minor units.
func (c *Client) GetManagedWalletBalance(ctx context.Context, customerID, walletID string) (map[string]ManagedWalletAsset, error) {
	if !strings.HasPrefix(customerID, "re_") || !strings.HasPrefix(walletID, "bl_") {
		return nil, errors.New("invalid BlindPay customer or managed wallet ID")
	}
	var out map[string]ManagedWalletAsset
	_, err := c.do(ctx, http.MethodGet, "/customers/"+url.PathEscape(customerID)+"/wallets/"+url.PathEscape(walletID)+"/balance", "", nil, &out)
	if err != nil {
		return nil, err
	}
	for symbol, asset := range out {
		asset.Symbol = strings.ToUpper(asset.Symbol)
		if asset.Symbol == "" {
			asset.Symbol = strings.ToUpper(symbol)
		}
		out[strings.ToUpper(symbol)] = asset
	}
	return out, nil
}

func (c *Client) CreateEVMPayout(ctx context.Context, r PayoutRequest) (Payout, error) {
	if !strings.HasPrefix(r.QuoteID, "qu_") || r.SenderWalletAddress == "" {
		return Payout{}, errors.New("invalid BlindPay payout request")
	}
	var out Payout
	raw, err := c.do(ctx, http.MethodPost, "/payouts/evm", r.IdempotencyKey, r, &out)
	out.RawPayload = raw
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
	_, err := c.do(ctx, http.MethodGet, "/payouts/"+url.PathEscape(id), "", nil, &out)
	return out, err
}

type PayinQuoteRequest struct {
	IdempotencyKey     string `json:"-"`
	WalletID           string `json:"wallet_id,omitempty"`
	BlockchainWalletID string `json:"blockchain_wallet_id,omitempty"`
	CurrencyType       string `json:"currency_type"`
	CoverFees          bool   `json:"cover_fees"`
	RequestAmount      int64  `json:"request_amount"`
	PaymentMethod      string `json:"payment_method"`
	Token              string `json:"token"`
}
type PayinQuote struct {
	ID                           string `json:"id"`
	ExpiresAt                    int64  `json:"expires_at"`
	SenderAmount, ReceiverAmount int64
	RawPayload                   json.RawMessage `json:"-"`
}

func (q *PayinQuote) UnmarshalJSON(data []byte) error {
	type wire struct {
		ID             string `json:"id"`
		ExpiresAt      int64  `json:"expires_at"`
		SenderAmount   int64  `json:"sender_amount"`
		ReceiverAmount int64  `json:"receiver_amount"`
	}
	var w wire
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	q.ID, q.ExpiresAt, q.SenderAmount, q.ReceiverAmount = w.ID, w.ExpiresAt, w.SenderAmount, w.ReceiverAmount
	return nil
}
func (c *Client) CreatePayinQuote(ctx context.Context, r PayinQuoteRequest) (PayinQuote, error) {
	if (r.WalletID == "") == (r.BlockchainWalletID == "") || r.RequestAmount <= 0 {
		return PayinQuote{}, errors.New("invalid BlindPay payin quote request")
	}
	var out PayinQuote
	raw, err := c.do(ctx, http.MethodPost, "/payin-quotes", r.IdempotencyKey, r, &out)
	out.RawPayload = raw
	if err == nil && (!strings.HasPrefix(out.ID, "pq_") || out.ExpiresAt <= 0) {
		err = errors.New("BlindPay returned an invalid payin quote")
	}
	return out, err
}

type PayinRequest struct {
	IdempotencyKey string `json:"-"`
	PayinQuoteID   string `json:"payin_quote_id"`
}
type Payin struct {
	ID           string          `json:"id"`
	Status       string          `json:"status"`
	RawPayload   json.RawMessage `json:"-"`
	Instructions json.RawMessage `json:"-"`
}

func (c *Client) CreatePayin(ctx context.Context, r PayinRequest) (Payin, error) {
	if !strings.HasPrefix(r.PayinQuoteID, "pq_") {
		return Payin{}, errors.New("invalid BlindPay payin request")
	}
	var out Payin
	raw, err := c.do(ctx, http.MethodPost, "/payins/evm", r.IdempotencyKey, r, &out)
	out.RawPayload = raw
	out.Instructions = raw
	if err == nil && (!strings.HasPrefix(out.ID, "pi_") || out.Status == "") {
		err = errors.New("BlindPay returned an invalid payin")
	}
	return out, err
}

func (c *Client) do(ctx context.Context, method, path, idempotencyKey string, body, out any) (json.RawMessage, error) {
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+"/instances/"+url.PathEscape(c.instanceID)+path, reader)
	if err != nil {
		return nil, err
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
		return nil, err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 2<<20))
	if err != nil {
		return nil, err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return raw, classifyAPIError(res.StatusCode, raw)
	}
	if out != nil && len(raw) > 0 {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(out); err != nil {
			return raw, fmt.Errorf("decode BlindPay response: %w", err)
		}
	}
	return raw, nil
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
