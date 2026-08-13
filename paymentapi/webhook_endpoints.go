package paymentapi

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxActiveWebhookEndpoints = 5

var (
	ErrWebhookEndpointLimit     = errors.New("tenant already has the maximum number of active webhook endpoints")
	ErrWebhookEndpointDuplicate = errors.New("tenant already has an active webhook endpoint for this URL")
	ErrWebhookEndpointNotFound  = errors.New("active webhook endpoint not found")
)

type WebhookEndpoint struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"created_at"`
}

type WebhookEndpointService struct {
	db     *sql.DB
	now    func() time.Time
	random func([]byte) (int, error)
}

func NewWebhookEndpointService(db *sql.DB) (*WebhookEndpointService, error) {
	if db == nil {
		return nil, errors.New("webhook endpoint database is required")
	}
	return &WebhookEndpointService{db: db, now: func() time.Time { return time.Now().UTC() }, random: rand.Read}, nil
}

func (s *WebhookEndpointService) Register(ctx context.Context, tenantID, rawURL string) (*WebhookEndpoint, string, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, "", errors.New("tenant ID is required")
	}
	endpointURL, err := validateWebhookURL(rawURL)
	if err != nil {
		return nil, "", err
	}
	idBytes, secretBytes := make([]byte, 12), make([]byte, 32)
	if _, err := s.random(idBytes); err != nil {
		return nil, "", fmt.Errorf("generate webhook endpoint ID: %w", err)
	}
	if _, err := s.random(secretBytes); err != nil {
		return nil, "", fmt.Errorf("generate webhook signing secret: %w", err)
	}
	endpoint := &WebhookEndpoint{ID: "whe_" + hex.EncodeToString(idBytes), URL: endpointURL, Active: true, CreatedAt: s.now()}
	secret := "whsec_" + base64.RawURLEncoding.EncodeToString(secretBytes)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, "", err
	}
	defer tx.Rollback()
	// Serialize registrations per tenant so the endpoint limit cannot be raced.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, tenantID); err != nil {
		return nil, "", fmt.Errorf("lock webhook registrations: %w", err)
	}
	var count int
	var duplicate bool
	if err := tx.QueryRowContext(ctx, `SELECT count(*),COALESCE(bool_or(url=$2),FALSE) FROM webhook_endpoints WHERE tenant_id=$1 AND active`, tenantID, endpointURL).Scan(&count, &duplicate); err != nil {
		return nil, "", fmt.Errorf("inspect webhook endpoints: %w", err)
	}
	if duplicate {
		return nil, "", ErrWebhookEndpointDuplicate
	}
	if count >= maxActiveWebhookEndpoints {
		return nil, "", ErrWebhookEndpointLimit
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO webhook_endpoints(id,tenant_id,url,secret,active,created_at) VALUES($1,$2,$3,$4,TRUE,$5)`, endpoint.ID, tenantID, endpoint.URL, secret, endpoint.CreatedAt); err != nil {
		return nil, "", fmt.Errorf("register webhook endpoint: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, "", err
	}
	return endpoint, secret, nil
}

func (s *WebhookEndpointService) List(ctx context.Context, tenantID string) ([]WebhookEndpoint, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,url,active,created_at FROM webhook_endpoints WHERE tenant_id=$1 ORDER BY created_at,id`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list webhook endpoints: %w", err)
	}
	defer rows.Close()
	endpoints := make([]WebhookEndpoint, 0)
	for rows.Next() {
		var endpoint WebhookEndpoint
		if err := rows.Scan(&endpoint.ID, &endpoint.URL, &endpoint.Active, &endpoint.CreatedAt); err != nil {
			return nil, err
		}
		endpoints = append(endpoints, endpoint)
	}
	return endpoints, rows.Err()
}

func (s *WebhookEndpointService) Disable(ctx context.Context, tenantID, id string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE webhook_endpoints SET active=FALSE WHERE id=$1 AND tenant_id=$2 AND active`, id, tenantID)
	if err != nil {
		return fmt.Errorf("disable webhook endpoint: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return ErrWebhookEndpointNotFound
	}
	return nil
}

func NewWebhookEndpointHandler(service *WebhookEndpointService) (http.Handler, error) {
	if service == nil {
		return nil, errors.New("webhook endpoint service is required")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/webhook-endpoints", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			URL string `json:"url"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			problem(w, http.StatusBadRequest, "invalid JSON request body")
			return
		}
		var extra any
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			problem(w, http.StatusBadRequest, "request body must contain one JSON object")
			return
		}
		tenantID, ok := TenantIDFromContext(r.Context())
		if !ok {
			problem(w, http.StatusUnauthorized, "valid tenant API key is required")
			return
		}
		endpoint, secret, err := service.Register(r.Context(), tenantID, input.URL)
		if err != nil {
			switch {
			case errors.Is(err, ErrWebhookEndpointDuplicate), errors.Is(err, ErrWebhookEndpointLimit):
				problem(w, http.StatusConflict, err.Error())
			case errors.Is(err, errInvalidWebhookURL):
				problem(w, http.StatusBadRequest, err.Error())
			default:
				problem(w, http.StatusInternalServerError, "could not register webhook endpoint")
			}
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{"id": endpoint.ID, "url": endpoint.URL, "active": endpoint.Active, "created_at": endpoint.CreatedAt, "signing_secret": secret})
	})
	mux.HandleFunc("GET /v1/webhook-endpoints", func(w http.ResponseWriter, r *http.Request) {
		tenantID, ok := TenantIDFromContext(r.Context())
		if !ok {
			problem(w, http.StatusUnauthorized, "valid tenant API key is required")
			return
		}
		endpoints, err := service.List(r.Context(), tenantID)
		if err != nil {
			problem(w, http.StatusInternalServerError, "could not list webhook endpoints")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"data": endpoints})
	})
	mux.HandleFunc("DELETE /v1/webhook-endpoints/{id}", func(w http.ResponseWriter, r *http.Request) {
		tenantID, ok := TenantIDFromContext(r.Context())
		if !ok {
			problem(w, http.StatusUnauthorized, "valid tenant API key is required")
			return
		}
		if err := service.Disable(r.Context(), tenantID, r.PathValue("id")); err != nil {
			if errors.Is(err, ErrWebhookEndpointNotFound) {
				problem(w, http.StatusNotFound, "active webhook endpoint not found")
			} else {
				problem(w, http.StatusInternalServerError, "could not disable webhook endpoint")
			}
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	return mux, nil
}

var errInvalidWebhookURL = errors.New("webhook URL must be an HTTPS public address")

func validateWebhookURL(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return "", errInvalidWebhookURL
	}
	host := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".local") {
		return "", errInvalidWebhookURL
	}
	if ip := net.ParseIP(host); ip != nil {
		if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
			return "", errInvalidWebhookURL
		}
	}
	return parsed.String(), nil
}
