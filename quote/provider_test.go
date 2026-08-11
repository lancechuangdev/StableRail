package quote

import (
	"context"
	"testing"
	"time"
)

func TestDeterministicProvider(t *testing.T) {
	want := Price{Rate: "0.92", FeeMinor: 25, ValidFor: time.Minute}
	got, err := (DeterministicProvider{Quote: want}).Price(context.Background(), "USD", "EUR", 10_000)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("price = %+v, want %+v", got, want)
	}
}
