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
		provided, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		providedHash, tokenHash := sha256.Sum256([]byte(provided)), sha256.Sum256([]byte(token))
		if !ok || subtle.ConstantTimeCompare(providedHash[:], tokenHash[:]) != 1 {
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
