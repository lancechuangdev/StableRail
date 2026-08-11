package quote

import (
	"context"
	"errors"
	"time"
)

// Price is authoritative pricing returned by a quote engine or FX provider.
type Price struct {
	Rate     string
	FeeMinor int64
	ValidFor time.Duration
}

// PricingProvider isolates quote creation from a specific market-data or FX vendor.
type PricingProvider interface {
	Price(context.Context, string, string, int64) (Price, error)
}

// DeterministicProvider supplies stable local pricing for development and tests.
type DeterministicProvider struct {
	Quote Price
}

func (p DeterministicProvider) Price(_ context.Context, sourceCurrency, destinationCurrency string, sourceAmountMinor int64) (Price, error) {
	if sourceCurrency == "" || destinationCurrency == "" || sourceCurrency == destinationCurrency || sourceAmountMinor <= 0 {
		return Price{}, errors.New("invalid pricing request")
	}
	if _, err := ParseRate(p.Quote.Rate); err != nil {
		return Price{}, err
	}
	if p.Quote.FeeMinor < 0 || p.Quote.ValidFor <= 0 {
		return Price{}, errors.New("invalid deterministic price")
	}
	return p.Quote, nil
}
