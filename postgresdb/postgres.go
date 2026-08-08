// Package postgresdb owns PostgreSQL connection setup for StableRail.
package postgresdb

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// Open creates and verifies a PostgreSQL connection pool. The caller owns the
// returned pool and must close it during application shutdown.
func Open(ctx context.Context, dataSourceName string) (*sql.DB, error) {
	if dataSourceName == "" {
		return nil, fmt.Errorf("PostgreSQL data source name is required")
	}

	db, err := sql.Open("pgx", dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}
	return db, nil
}
