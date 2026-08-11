package quote

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresRepositorySavesQuoteSnapshot(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo, _ := NewPostgresRepository(db)
	now := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	q := &Quote{ID: "quo_test", SourceCurrency: "USD", DestinationCurrency: "EUR", SourceAmountMinor: 10_000, DestinationAmountMinor: 9_175, FeeMinor: 25, Status: StatusOpen, CreatedAt: now, ExpiresAt: now.Add(time.Minute)}
	mock.ExpectExec("INSERT INTO quotes").WithArgs(q.ID, q.SourceCurrency, q.DestinationCurrency, q.SourceAmountMinor, q.DestinationAmountMinor, int64(920_000_000), q.FeeMinor, q.Status, q.ExpiresAt, q.CreatedAt).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := repo.Save(context.Background(), q, 920_000_000); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
