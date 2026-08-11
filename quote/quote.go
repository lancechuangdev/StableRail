// Package quote implements immutable, expiring FX prices without floating-point arithmetic.
package quote

import (
	"errors"
	"math/big"
	"regexp"
	"strings"
	"time"
)

const RateScale int64 = 1_000_000_000

var (
	ErrNotFound = errors.New("quote not found")
	ErrExpired  = errors.New("quote expired")
	ErrAccepted = errors.New("quote already accepted")
	decimalRate = regexp.MustCompile(`^[0-9]+(?:\.[0-9]{1,9})?$`)
)

type Status string

const (
	StatusOpen     Status = "open"
	StatusAccepted Status = "accepted"
	StatusExpired  Status = "expired"
)

type Quote struct {
	ID                     string    `json:"id"`
	SourceCurrency         string    `json:"source_currency"`
	DestinationCurrency    string    `json:"destination_currency"`
	SourceAmountMinor      int64     `json:"source_amount_minor"`
	DestinationAmountMinor int64     `json:"destination_amount_minor"`
	Rate                   string    `json:"rate"`
	FeeMinor               int64     `json:"fee_minor"`
	Status                 Status    `json:"status"`
	ExpiresAt              time.Time `json:"expires_at"`
	CreatedAt              time.Time `json:"created_at"`
}

// ParseRate converts a positive decimal with at most nine fractional digits to a fixed-point value.
func ParseRate(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if !decimalRate.MatchString(value) {
		return 0, errors.New("rate must be a positive decimal with at most 9 fractional digits")
	}
	parts := strings.SplitN(value, ".", 2)
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	fraction += strings.Repeat("0", 9-len(fraction))
	n := new(big.Int)
	if _, ok := n.SetString(parts[0]+fraction, 10); !ok || !n.IsInt64() || n.Sign() <= 0 {
		return 0, errors.New("rate is out of range")
	}
	return n.Int64(), nil
}

func FormatRate(scaled int64) string {
	whole, fraction := scaled/RateScale, scaled%RateScale
	if fraction == 0 {
		return new(big.Int).SetInt64(whole).String()
	}
	return new(big.Int).SetInt64(whole).String() + "." + strings.TrimRight(leftPad9(fraction), "0")
}

func leftPad9(v int64) string {
	s := new(big.Int).SetInt64(v).String()
	return strings.Repeat("0", 9-len(s)) + s
}

// DestinationAmount rounds source*rate to the nearest minor unit (half up), then deducts the fee.
func DestinationAmount(source, scaledRate, fee int64) (int64, error) {
	if source <= 0 || scaledRate <= 0 || fee < 0 {
		return 0, errors.New("invalid quote amounts")
	}
	product := new(big.Int).Mul(big.NewInt(source), big.NewInt(scaledRate))
	product.Add(product, big.NewInt(RateScale/2))
	product.Quo(product, big.NewInt(RateScale))
	product.Sub(product, big.NewInt(fee))
	if !product.IsInt64() || product.Sign() <= 0 {
		return 0, errors.New("destination amount is out of range")
	}
	return product.Int64(), nil
}
