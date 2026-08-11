package quote

import (
	"context"
	"testing"
	"time"
)

type memoryRepository struct {
	quote  *Quote
	scaled int64
}

func (r *memoryRepository) Save(_ context.Context, q *Quote, scaled int64) error {
	clone := *q
	r.quote, r.scaled = &clone, scaled
	return nil
}
func (r *memoryRepository) Get(context.Context, string) (*Quote, int64, error) {
	clone := *r.quote
	return &clone, r.scaled, nil
}

func TestServiceCreatesProviderPricedQuote(t *testing.T) {
	repo := &memoryRepository{}
	service, err := NewService(DeterministicProvider{Quote: Price{Rate: "1.234567891", FeeMinor: 25, ValidFor: time.Minute}}, repo)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	service.now = func() time.Time { return now }
	service.newID = func() (string, error) { return "quo_test", nil }
	got, err := service.Create(context.Background(), "usd", "eur", 10_001)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "quo_test" || got.DestinationAmountMinor != 12_322 || got.ExpiresAt != now.Add(time.Minute) {
		t.Fatalf("unexpected quote: %+v", got)
	}
	if repo.scaled != 1_234_567_891 {
		t.Fatalf("scaled rate = %d", repo.scaled)
	}
}

func TestServiceReportsExpiredQuoteWithoutMutatingSnapshot(t *testing.T) {
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	repo := &memoryRepository{quote: &Quote{ID: "quo_test", Status: StatusOpen, ExpiresAt: now.Add(-time.Second)}, scaled: 920_000_000}
	service, _ := NewService(DeterministicProvider{Quote: Price{Rate: "1", ValidFor: time.Minute}}, repo)
	service.now = func() time.Time { return now }
	got, err := service.Get(context.Background(), "quo_test")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != StatusExpired || got.Rate != "0.92" {
		t.Fatalf("unexpected quote: %+v", got)
	}
}
