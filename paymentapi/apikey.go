package paymentapi

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type tenantContextKey struct{}

var ErrInvalidAPIKey = errors.New("invalid API key")

func TenantIDFromContext(ctx context.Context) (string, bool) {
	value, ok := ctx.Value(tenantContextKey{}).(string)
	return value, ok && value != ""
}

type APIKeyService struct {
	db     *sql.DB
	now    func() time.Time
	random func([]byte) (int, error)
}

func NewAPIKeyService(db *sql.DB) (*APIKeyService, error) {
	if db == nil {
		return nil, errors.New("API key database is required")
	}
	return &APIKeyService{db: db, now: func() time.Time { return time.Now().UTC() }, random: rand.Read}, nil
}

// Issue creates a key whose secret is returned once. Only its SHA-256 digest is stored.
func (s *APIKeyService) Issue(ctx context.Context, tenantID, name string) (string, string, error) {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(name) == "" {
		return "", "", errors.New("tenant ID and key name are required")
	}
	idBytes, secretBytes := make([]byte, 12), make([]byte, 32)
	if _, err := s.random(idBytes); err != nil {
		return "", "", fmt.Errorf("generate API key ID: %w", err)
	}
	if _, err := s.random(secretBytes); err != nil {
		return "", "", fmt.Errorf("generate API key secret: %w", err)
	}
	id := "key_" + hex.EncodeToString(idBytes)
	secret := base64.RawURLEncoding.EncodeToString(secretBytes)
	digest := sha256.Sum256([]byte(secret))
	if _, err := s.db.ExecContext(ctx, `INSERT INTO tenant_api_keys(id,tenant_id,name,secret_hash,created_at) VALUES($1,$2,$3,$4,$5)`, id, tenantID, name, digest[:], s.now()); err != nil {
		return "", "", fmt.Errorf("store API key: %w", err)
	}
	return id, "srk_" + id + "_" + secret, nil
}

func (s *APIKeyService) Revoke(ctx context.Context, id string) error {
	if strings.TrimSpace(id) == "" {
		return errors.New("API key ID is required")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE tenant_api_keys SET revoked_at=$1 WHERE id=$2 AND revoked_at IS NULL`, s.now(), id)
	if err != nil {
		return fmt.Errorf("revoke API key: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect API key revocation: %w", err)
	}
	if rows != 1 {
		return errors.New("active API key not found")
	}
	return nil
}

func (s *APIKeyService) authenticate(ctx context.Context, token string) (string, error) {
	rest, ok := strings.CutPrefix(token, "srk_")
	if !ok {
		return "", ErrInvalidAPIKey
	}
	separator := strings.LastIndex(rest, "_")
	if separator <= 0 || separator == len(rest)-1 {
		return "", ErrInvalidAPIKey
	}
	id, secret := rest[:separator], rest[separator+1:]
	digest := sha256.Sum256([]byte(secret))
	var tenantID string
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	if err := tx.QueryRowContext(ctx, `SELECT tenant_id FROM tenant_api_keys WHERE id=$1 AND secret_hash=$2 AND revoked_at IS NULL FOR UPDATE`, id, digest[:]).Scan(&tenantID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrInvalidAPIKey
		}
		return "", fmt.Errorf("authenticate API key: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE tenant_api_keys SET last_used_at=$1 WHERE id=$2`, s.now(), id); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return tenantID, nil
}

func (s *APIKeyService) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			next.ServeHTTP(w, r)
			return
		}
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok {
			problem(w, http.StatusUnauthorized, "valid tenant API key is required")
			return
		}
		tenantID, err := s.authenticate(r.Context(), token)
		if err != nil {
			if errors.Is(err, ErrInvalidAPIKey) {
				problem(w, http.StatusUnauthorized, "valid tenant API key is required")
			} else {
				problem(w, http.StatusServiceUnavailable, "API key service unavailable")
			}
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), tenantContextKey{}, tenantID)))
	})
}
