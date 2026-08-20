package payin

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"
)

var ErrNotFound = errors.New("payin not found")
var ErrIdempotencyConflict = errors.New("idempotency key is bound to another payin quote")

type Service struct {
	db                *sql.DB
	quoteProvider     QuoteProvider
	executionProvider ExecutionProvider
	now               func() time.Time
	newID             func(string) (string, error)
}

func NewService(db *sql.DB, quoteProvider QuoteProvider, executionProvider ExecutionProvider) (*Service, error) {
	if db == nil || quoteProvider == nil || executionProvider == nil {
		return nil, errors.New("payin database, quote provider, and execution provider are required")
	}
	if quoteProvider.Name() != executionProvider.Name() {
		return nil, errors.New("payin quote and execution providers must match")
	}
	return &Service{db: db, quoteProvider: quoteProvider, executionProvider: executionProvider, now: func() time.Time { return time.Now().UTC() }, newID: func(prefix string) (string, error) {
		b := make([]byte, 16)
		_, err := rand.Read(b)
		return prefix + hex.EncodeToString(b), err
	}}, nil
}
