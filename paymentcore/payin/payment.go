package payin

import (
	"context"
	"errors"
	"strings"

	"stablerail/paymentcore"
)

type CreatePaymentRequest struct {
	TenantID             string
	IdempotencyKey       string
	ExternalReference    string
	QuoteID              string
	AmountMinor          int64
	Currency             string
	FundingMethod        string
	SourceInstrumentID   string
	DestinationAccountID string
}

func (r CreatePaymentRequest) Validate() error {
	if strings.TrimSpace(r.TenantID) == "" || strings.TrimSpace(r.IdempotencyKey) == "" || strings.TrimSpace(r.ExternalReference) == "" {
		return errors.New("tenant, idempotency key, and external reference are required")
	}
	direct := r.AmountMinor != 0 || r.Currency != "" || r.FundingMethod != "" || r.SourceInstrumentID != "" || r.DestinationAccountID != ""
	if r.QuoteID != "" {
		if direct {
			return errors.New("payin quote and direct payment fields cannot be combined")
		}
		return nil
	}
	if r.AmountMinor <= 0 || len(strings.TrimSpace(r.Currency)) < 3 || strings.TrimSpace(r.FundingMethod) == "" || strings.TrimSpace(r.DestinationAccountID) == "" {
		return errors.New("direct payin requires positive amount, currency, funding method, and destination account")
	}
	return nil
}

// CreatePayment creates the shared payment aggregate and its inbound operation
// atomically. Clients track the returned payment rather than the payins row.
func (s *Service) CreatePayment(ctx context.Context, request CreatePaymentRequest) (*paymentcore.Payment, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	request.Currency = strings.ToUpper(strings.TrimSpace(request.Currency))
	p, err := s.createPayin(ctx, request)
	if err != nil {
		return nil, err
	}
	return &paymentcore.Payment{ID: p.PaymentID, Direction: paymentcore.PaymentDirectionPayin, ExternalReference: request.ExternalReference, Currency: p.DestinationCurrency, AmountMinor: p.DestinationAmountMinor, TenantID: request.TenantID, PaymentStatus: paymentcore.PaymentStatusCreated, FundsStatus: paymentcore.FundsStatusPending, CreatedAt: p.CreatedAt, UpdatedAt: p.UpdatedAt}, nil
}
