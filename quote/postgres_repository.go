package quote

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type PostgresRepository struct{ db *sql.DB }

func NewPostgresRepository(db *sql.DB) (*PostgresRepository, error) {
	if db == nil {
		return nil, errors.New("quote database is required")
	}
	return &PostgresRepository{db: db}, nil
}

func (r *PostgresRepository) Save(ctx context.Context, q *Quote, scaledRate int64) error {
	_, err := r.db.ExecContext(ctx, `INSERT INTO quotes (id, source_currency, destination_currency, source_amount_minor, destination_amount_minor, rate_scaled, fee_minor, status, expires_at, created_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, q.ID, q.SourceCurrency, q.DestinationCurrency, q.SourceAmountMinor, q.DestinationAmountMinor, scaledRate, q.FeeMinor, q.Status, q.ExpiresAt, q.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert quote: %w", err)
	}
	return nil
}

func (r *PostgresRepository) Get(ctx context.Context, id string) (*Quote, int64, error) {
	q := &Quote{}
	var scaled int64
	err := r.db.QueryRowContext(ctx, `SELECT id, source_currency, destination_currency, source_amount_minor, destination_amount_minor, rate_scaled, fee_minor, status, expires_at, created_at FROM quotes WHERE id=$1`, id).Scan(&q.ID, &q.SourceCurrency, &q.DestinationCurrency, &q.SourceAmountMinor, &q.DestinationAmountMinor, &scaled, &q.FeeMinor, &q.Status, &q.ExpiresAt, &q.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, 0, ErrNotFound
	}
	if err != nil {
		return nil, 0, fmt.Errorf("get quote: %w", err)
	}
	return q, scaled, nil
}
