package quote

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Repository persists and retrieves immutable quote snapshots.
type Repository interface {
	Save(context.Context, *Quote, int64) error
	Get(context.Context, string) (*Quote, int64, error)
}

// Service coordinates pricing, quote calculation, and persistence.
type Service struct {
	pricing PricingProvider
	repo    Repository
	now     func() time.Time
	newID   func() (string, error)
}

func NewService(pricing PricingProvider, repo Repository) (*Service, error) {
	if pricing == nil || repo == nil {
		return nil, errors.New("pricing provider and quote repository are required")
	}
	return &Service{
		pricing: pricing,
		repo:    repo,
		now:     func() time.Time { return time.Now().UTC() },
		newID: func() (string, error) {
			b := make([]byte, 16)
			if _, err := rand.Read(b); err != nil {
				return "", err
			}
			return "quo_" + hex.EncodeToString(b), nil
		},
	}, nil
}

func (s *Service) Create(ctx context.Context, sourceCurrency, destinationCurrency string, sourceAmountMinor int64) (*Quote, error) {
	sourceCurrency, destinationCurrency = strings.ToUpper(strings.TrimSpace(sourceCurrency)), strings.ToUpper(strings.TrimSpace(destinationCurrency))
	if len(sourceCurrency) != 3 || len(destinationCurrency) != 3 || sourceCurrency == destinationCurrency || sourceAmountMinor <= 0 {
		return nil, errors.New("invalid quote request")
	}
	price, err := s.pricing.Price(ctx, sourceCurrency, destinationCurrency, sourceAmountMinor)
	if err != nil {
		return nil, fmt.Errorf("price quote: %w", err)
	}
	if price.ValidFor <= 0 || price.ValidFor > 24*time.Hour || price.FeeMinor < 0 {
		return nil, errors.New("pricing provider returned an invalid price")
	}
	scaled, err := ParseRate(price.Rate)
	if err != nil {
		return nil, fmt.Errorf("pricing provider returned an invalid rate: %w", err)
	}
	destinationAmount, err := DestinationAmount(sourceAmountMinor, scaled, price.FeeMinor)
	if err != nil {
		return nil, err
	}
	id, err := s.newID()
	if err != nil {
		return nil, fmt.Errorf("generate quote ID: %w", err)
	}
	now := s.now()
	q := &Quote{ID: id, SourceCurrency: sourceCurrency, DestinationCurrency: destinationCurrency, SourceAmountMinor: sourceAmountMinor, DestinationAmountMinor: destinationAmount, Rate: FormatRate(scaled), FeeMinor: price.FeeMinor, Status: StatusOpen, CreatedAt: now, ExpiresAt: now.Add(price.ValidFor)}
	if err := s.repo.Save(ctx, q, scaled); err != nil {
		return nil, fmt.Errorf("save quote: %w", err)
	}
	return q, nil
}

func (s *Service) Get(ctx context.Context, id string) (*Quote, error) {
	q, scaled, err := s.repo.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	q.Rate = FormatRate(scaled)
	if q.Status == StatusOpen && !q.ExpiresAt.After(s.now()) {
		q.Status = StatusExpired
	}
	return q, nil
}
