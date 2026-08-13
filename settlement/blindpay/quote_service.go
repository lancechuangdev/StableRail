package blindpay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type PayoutQuoteRequest struct {
	IdempotencyKey                           string
	TenantID, BankAccountID, ManagedWalletID string
	DestinationCurrency                      string
	CurrencyType                             string
	CoverFees                                bool
	RequestAmountMinor                       int64
	PartnerFeeID                             string
}

type PayoutQuote struct {
	ID                    string    `json:"id"`
	Provider              string    `json:"provider"`
	ProviderQuoteID       string    `json:"provider_quote_id"`
	TenantID              string    `json:"tenant_id"`
	ProviderBankAccountID string    `json:"bank_account_id"`
	ProviderWalletID      string    `json:"managed_wallet_id"`
	SourceCurrency        string    `json:"source_currency"`
	DestinationCurrency   string    `json:"destination_currency"`
	CurrencyType          string    `json:"currency_type"`
	CoverFees             bool      `json:"cover_fees"`
	SenderAmountMinor     int64     `json:"sender_amount_minor"`
	ReceiverAmountMinor   int64     `json:"receiver_amount_minor"`
	CommercialRate        string    `json:"commercial_rate"`
	ProviderRate          string    `json:"provider_rate"`
	FlatFeeMinor          int64     `json:"flat_fee_minor"`
	PartnerFeeMinor       int64     `json:"partner_fee_minor"`
	BillingFeeMinor       *int64    `json:"billing_fee_minor,omitempty"`
	Status                string    `json:"status"`
	ExpiresAt             time.Time `json:"expires_at"`
	CreatedAt             time.Time `json:"created_at"`
}

type QuoteService struct {
	client         quoteClient
	repo           *Repository
	network, token string
	now            func() time.Time
}

type quoteClient interface {
	CreateQuote(context.Context, QuoteRequest) (Quote, error)
	GetManagedWalletBalance(context.Context, string, string) (map[string]ManagedWalletAsset, error)
}

func NewQuoteService(client quoteClient, repo *Repository, network, token string) (*QuoteService, error) {
	if client == nil || repo == nil || network == "" || token == "" {
		return nil, errors.New("BlindPay quote client, repository, network, and token are required")
	}
	return &QuoteService{client: client, repo: repo, network: network, token: token, now: func() time.Time { return time.Now().UTC() }}, nil
}

func (s *QuoteService) Create(ctx context.Context, r PayoutQuoteRequest) (*PayoutQuote, error) {
	r.DestinationCurrency = strings.ToUpper(strings.TrimSpace(r.DestinationCurrency))
	if r.IdempotencyKey == "" || r.TenantID == "" || !strings.HasPrefix(r.BankAccountID, "ba_") || !strings.HasPrefix(r.ManagedWalletID, "bl_") || len(r.DestinationCurrency) != 3 || r.RequestAmountMinor <= 0 || (r.CurrencyType != "sender" && r.CurrencyType != "receiver") {
		return nil, errors.New("invalid BlindPay payout quote request")
	}
	profile, err := s.repo.GetApprovedPayoutProfile(ctx, r.TenantID, r.BankAccountID, r.ManagedWalletID)
	if err != nil {
		return nil, err
	}
	if profile.ManagedWallet.Network != s.network {
		return nil, fmt.Errorf("managed wallet network %q does not match configured BlindPay network %q", profile.ManagedWallet.Network, s.network)
	}
	providerQuote, err := s.client.CreateQuote(ctx, QuoteRequest{IdempotencyKey: r.IdempotencyKey, BankAccountID: r.BankAccountID, CurrencyType: r.CurrencyType, CoverFees: r.CoverFees, RequestAmount: r.RequestAmountMinor, Network: s.network, Token: s.token, PartnerFeeID: r.PartnerFeeID})
	if err != nil {
		return nil, fmt.Errorf("create BlindPay payout quote: %w", err)
	}
	expiresAt := time.UnixMilli(providerQuote.ExpiresAt).UTC()
	if !expiresAt.After(s.now()) {
		return nil, errors.New("BlindPay returned an expired payout quote")
	}
	balance, err := s.client.GetManagedWalletBalance(ctx, profile.Customer.ProviderCustomerID, r.ManagedWalletID)
	if err != nil {
		return nil, fmt.Errorf("get BlindPay managed wallet balance: %w", err)
	}
	asset, ok := balance[strings.ToUpper(s.token)]
	if !ok || asset.Amount < providerQuote.SenderAmount {
		return nil, fmt.Errorf("managed wallet has insufficient %s balance for payout", s.token)
	}
	raw := providerQuote.RawPayload
	if len(raw) == 0 {
		raw, err = json.Marshal(providerQuote)
		if err != nil {
			return nil, err
		}
	}
	q := &PayoutQuote{ID: providerQuote.ID, Provider: "blindpay", ProviderQuoteID: providerQuote.ID, TenantID: r.TenantID, ProviderBankAccountID: r.BankAccountID, ProviderWalletID: r.ManagedWalletID, SourceCurrency: s.token, DestinationCurrency: r.DestinationCurrency, CurrencyType: r.CurrencyType, CoverFees: r.CoverFees, SenderAmountMinor: providerQuote.SenderAmount, ReceiverAmountMinor: providerQuote.ReceiverAmount, CommercialRate: providerQuote.CommercialQuotation.String(), ProviderRate: providerQuote.BlindPayQuotation.String(), FlatFeeMinor: providerQuote.FlatFee, PartnerFeeMinor: providerQuote.PartnerFeeAmount, BillingFeeMinor: providerQuote.BillingFeeAmount, Status: "open", ExpiresAt: expiresAt, CreatedAt: s.now()}
	if err := s.repo.SavePayoutQuote(ctx, *q, raw); err != nil {
		return nil, err
	}
	return q, nil
}
