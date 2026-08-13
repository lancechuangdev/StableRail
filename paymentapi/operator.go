package paymentapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"stablerail/saga"
)

type ManualReviewResolver interface {
	ResolveManualReview(context.Context, string, string, string, string) error
}

func NewOperatorHandler(token string, resolver ManualReviewResolver) (http.Handler, error) {
	if strings.TrimSpace(token) == "" || resolver == nil {
		return nil, errors.New("operator token and manual review resolver are required")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !validOperatorToken(r, token) {
			problem(w, http.StatusUnauthorized, "valid operator bearer token is required")
			return
		}
		var input struct {
			Action   string `json:"action"`
			Operator string `json:"operator"`
			Note     string `json:"note"`
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
		input.Action, input.Operator, input.Note = strings.TrimSpace(input.Action), strings.TrimSpace(input.Operator), strings.TrimSpace(input.Note)
		if input.Operator == "" || input.Note == "" || (input.Action != "retry" && input.Action != "complete" && input.Action != "fail" && input.Action != "refund") {
			problem(w, http.StatusBadRequest, "action must be retry, complete, fail, or refund; operator and note are required")
			return
		}
		if err := resolver.ResolveManualReview(r.Context(), r.PathValue("id"), input.Action, input.Operator, input.Note); err != nil {
			switch {
			case errors.Is(err, saga.ErrSagaNotFound):
				problem(w, http.StatusNotFound, err.Error())
			case errors.Is(err, saga.ErrNotInManualReview):
				problem(w, http.StatusConflict, err.Error())
			default:
				problem(w, http.StatusInternalServerError, "could not resolve manual review")
			}
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"payment_id": r.PathValue("id"), "action": input.Action, "status": "accepted"})
	}), nil
}

func NewAPIKeyOperatorHandler(token string, keys *APIKeyService) (http.Handler, error) {
	if strings.TrimSpace(token) == "" || keys == nil {
		return nil, errors.New("operator token and API key service are required")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !validOperatorToken(r, token) {
			problem(w, http.StatusUnauthorized, "valid operator bearer token is required")
			return
		}
		var input struct {
			Name string `json:"name"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil || strings.TrimSpace(input.Name) == "" {
			problem(w, http.StatusBadRequest, "key name is required")
			return
		}
		var extra any
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			problem(w, http.StatusBadRequest, "request body must contain one JSON object")
			return
		}
		id, key, err := keys.Issue(r.Context(), r.PathValue("id"), strings.TrimSpace(input.Name))
		if err != nil {
			problem(w, http.StatusInternalServerError, "could not issue API key")
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{"id": id, "tenant_id": r.PathValue("id"), "name": strings.TrimSpace(input.Name), "api_key": key})
	}), nil
}

func NewAPIKeyRevokeOperatorHandler(token string, keys *APIKeyService) (http.Handler, error) {
	if strings.TrimSpace(token) == "" || keys == nil {
		return nil, errors.New("operator token and API key service are required")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !validOperatorToken(r, token) {
			problem(w, http.StatusUnauthorized, "valid operator bearer token is required")
			return
		}
		if err := keys.Revoke(r.Context(), r.PathValue("id")); err != nil {
			problem(w, http.StatusNotFound, "active API key not found")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}), nil
}

func validOperatorToken(r *http.Request, token string) bool {
	provided, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	providedHash, tokenHash := sha256.Sum256([]byte(provided)), sha256.Sum256([]byte(token))
	return ok && subtle.ConstantTimeCompare(providedHash[:], tokenHash[:]) == 1
}
