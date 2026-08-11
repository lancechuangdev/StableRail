package quote

import "testing"

func TestDestinationAmountPrecisionAndRounding(t *testing.T) {
	rate, err := ParseRate("1.234567891")
	if err != nil {
		t.Fatal(err)
	}
	got, err := DestinationAmount(10_001, rate, 25)
	if err != nil {
		t.Fatal(err)
	}
	if got != 12_322 {
		t.Fatalf("destination = %d, want 12322", got)
	}
	if FormatRate(rate) != "1.234567891" {
		t.Fatalf("formatted rate = %q", FormatRate(rate))
	}
}

func TestParseRateRejectsExcessPrecision(t *testing.T) {
	if _, err := ParseRate("1.0000000001"); err == nil {
		t.Fatal("expected precision error")
	}
}
