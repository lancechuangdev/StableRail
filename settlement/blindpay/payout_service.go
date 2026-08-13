package blindpay

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrPayoutSubmissionUnknown = errors.New("BlindPay payout submission outcome is unknown")

type managedWalletPayoutClient interface {
	CreateEVMPayout(context.Context, PayoutRequest) (Payout, error)
}

// PayoutService submits a payment's already-bound quote from its managed wallet.
// It commits the submission record before making the provider call.
type PayoutService struct {
	db     *sql.DB
	client managedWalletPayoutClient
	now    func() time.Time
}

func NewPayoutService(db *sql.DB, client managedWalletPayoutClient) (*PayoutService, error) {
	if db == nil || client == nil {
		return nil, errors.New("BlindPay payout database and client are required")
	}
	return &PayoutService{db: db, client: client, now: func() time.Time { return time.Now().UTC() }}, nil
}

type PayoutSubmission struct {
	PaymentID, QuoteID, ProviderPayoutID, ProviderStatus string
	IdempotencyKey, SenderWalletID, SenderWalletAddress  string
	CreatedAt, UpdatedAt                                 time.Time
	SubmittedAt                                          *time.Time
}

type payoutAttempt struct {
	PayoutSubmission
	new bool
}

// SubmitPayment creates one durable attempt for a payment and then submits it to
// BlindPay. A retryable transport failure is deliberately marked unknown rather
// than being blindly retried, because BlindPay may have accepted the request.
func (s *PayoutService) SubmitPayment(ctx context.Context, paymentID, idempotencyKey string) (*PayoutSubmission, error) {
	if strings.TrimSpace(paymentID) == "" || strings.TrimSpace(idempotencyKey) == "" {
		return nil, errors.New("payment ID and idempotency key are required")
	}
	attempt, err := s.prepare(ctx, paymentID, idempotencyKey)
	if err != nil {
		return nil, err
	}
	if !attempt.new {
		if attempt.ProviderPayoutID != "" {
			return &attempt.PayoutSubmission, nil
		}
		return &attempt.PayoutSubmission, ErrPayoutSubmissionUnknown
	}

	payout, err := s.client.CreateEVMPayout(ctx, PayoutRequest{
		IdempotencyKey:      idempotencyKey,
		QuoteID:             attempt.QuoteID,
		SenderWalletAddress: attempt.SenderWalletAddress,
	})
	if err != nil {
		status := "unknown"
		var apiErr *APIError
		if errors.As(err, &apiErr) && (apiErr.Kind == ErrorPermanent || apiErr.Kind == ErrorUserAction || apiErr.Kind == ErrorUnauthorized) {
			status = "submission_failed"
		}
		if updateErr := s.recordError(ctx, paymentID, status, err); updateErr != nil {
			return nil, fmt.Errorf("submit BlindPay payout: %w (also failed to record outcome: %v)", err, updateErr)
		}
		if status == "unknown" {
			return nil, fmt.Errorf("%w: %v", ErrPayoutSubmissionUnknown, err)
		}
		return nil, fmt.Errorf("submit BlindPay payout: %w", err)
	}

	raw := payout.RawPayload
	if len(raw) == 0 {
		raw, err = json.Marshal(payout)
		if err != nil {
			return nil, fmt.Errorf("marshal BlindPay payout response: %w", err)
		}
	}
	now := s.now()
	_, err = s.db.ExecContext(ctx, `UPDATE blindpay_payouts SET provider_payout_id=$1,provider_status=$2,provider_payload=$3,last_error=NULL,updated_at=$4,submitted_at=$4 WHERE payment_id=$5 AND provider_status='submission_pending'`, payout.ID, payout.Status, raw, now, paymentID)
	if err != nil {
		return nil, fmt.Errorf("record BlindPay payout response: %w", err)
	}
	return &PayoutSubmission{PaymentID: paymentID, QuoteID: attempt.QuoteID, ProviderPayoutID: payout.ID, ProviderStatus: payout.Status, IdempotencyKey: idempotencyKey, SenderWalletID: attempt.SenderWalletID, SenderWalletAddress: attempt.SenderWalletAddress, CreatedAt: attempt.CreatedAt, UpdatedAt: now, SubmittedAt: &now}, nil
}

func (s *PayoutService) prepare(ctx context.Context, paymentID, idempotencyKey string) (payoutAttempt, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return payoutAttempt{}, fmt.Errorf("begin BlindPay payout submission: %w", err)
	}
	defer tx.Rollback()

	var quoteID, walletID, walletAddress string
	err = tx.QueryRowContext(ctx, `SELECT q.id,q.provider_wallet_id,w.address FROM blindpay_quotes q JOIN blindpay_managed_wallets w ON w.provider_wallet_id=q.provider_wallet_id WHERE q.payment_id=$1 AND q.status='accepted' FOR UPDATE`, paymentID).Scan(&quoteID, &walletID, &walletAddress)
	if errors.Is(err, sql.ErrNoRows) {
		return payoutAttempt{}, errors.New("payment has no accepted BlindPay quote")
	}
	if err != nil {
		return payoutAttempt{}, fmt.Errorf("lock accepted BlindPay quote: %w", err)
	}

	now := s.now()
	result, err := tx.ExecContext(ctx, `INSERT INTO blindpay_payouts(payment_id,quote_id,provider_status,idempotency_key,sender_wallet_id,sender_wallet_address,created_at,updated_at) VALUES($1,$2,'submission_pending',$3,$4,$5,$6,$6) ON CONFLICT(payment_id) DO NOTHING`, paymentID, quoteID, idempotencyKey, walletID, walletAddress, now)
	if err != nil {
		return payoutAttempt{}, fmt.Errorf("create BlindPay payout submission: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return payoutAttempt{}, fmt.Errorf("inspect BlindPay payout submission: %w", err)
	}
	attempt := payoutAttempt{PayoutSubmission: PayoutSubmission{PaymentID: paymentID, QuoteID: quoteID, IdempotencyKey: idempotencyKey, SenderWalletID: walletID, SenderWalletAddress: walletAddress, ProviderStatus: "submission_pending", CreatedAt: now, UpdatedAt: now}, new: rows == 1}
	if !attempt.new {
		var submittedAt sql.NullTime
		err = tx.QueryRowContext(ctx, `SELECT quote_id,COALESCE(provider_payout_id,''),provider_status,idempotency_key,sender_wallet_id,sender_wallet_address,created_at,updated_at,submitted_at FROM blindpay_payouts WHERE payment_id=$1 FOR UPDATE`, paymentID).Scan(&attempt.QuoteID, &attempt.ProviderPayoutID, &attempt.ProviderStatus, &attempt.IdempotencyKey, &attempt.SenderWalletID, &attempt.SenderWalletAddress, &attempt.CreatedAt, &attempt.UpdatedAt, &submittedAt)
		if err != nil {
			return payoutAttempt{}, fmt.Errorf("get existing BlindPay payout submission: %w", err)
		}
		if attempt.IdempotencyKey != idempotencyKey {
			return payoutAttempt{}, errors.New("payment is already bound to a different BlindPay payout idempotency key")
		}
		if submittedAt.Valid {
			attempt.SubmittedAt = &submittedAt.Time
		}
	}
	if err := tx.Commit(); err != nil {
		return payoutAttempt{}, fmt.Errorf("commit BlindPay payout submission: %w", err)
	}
	return attempt, nil
}

func (s *PayoutService) recordError(ctx context.Context, paymentID, status string, cause error) error {
	_, err := s.db.ExecContext(ctx, `UPDATE blindpay_payouts SET provider_status=$1,last_error=$2,updated_at=$3 WHERE payment_id=$4 AND provider_status='submission_pending'`, status, cause.Error(), s.now(), paymentID)
	return err
}
