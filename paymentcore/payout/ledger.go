package payout

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"stablerail/paymentcore"
)

func transitionAccounts(state paymentcore.PaymentStatus) (string, string, error) {
	switch state {
	case paymentcore.PaymentStatusProcessing:
		return paymentcore.CashOperatingAccount, paymentcore.SettlementAccount, nil
	case paymentcore.PaymentStatusSucceeded:
		return paymentcore.SettlementAccount, paymentcore.CashOperatingAccount, nil
	default:
		return "", "", fmt.Errorf("no ledger posting defined for state %s", state)
	}
}

func (s *Service) insertJournal(ctx context.Context, tx *sql.Tx, paymentID, eventType, debitAccount, creditAccount string, amountMinor int64, currency string, now time.Time) error {
	journalID, err := s.newID("jrn_")
	if err != nil {
		return fmt.Errorf("generate journal ID: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO ledger_transactions (id, payment_id, event_type, occurred_at) VALUES ($1, $2, $3, $4)`, journalID, paymentID, eventType, now); err != nil {
		return fmt.Errorf("insert ledger transaction: %w", err)
	}
	lines := []struct {
		id, account string
		side        paymentcore.EntrySide
	}{{journalID + ":debit", debitAccount, paymentcore.EntryDebit}, {journalID + ":credit", creditAccount, paymentcore.EntryCredit}}
	for _, line := range lines {
		if _, err := tx.ExecContext(ctx, `INSERT INTO ledger_entries (id, transaction_id, account_code, side, amount_minor, currency) VALUES ($1, $2, $3, $4, $5, $6)`, line.id, journalID, line.account, line.side, amountMinor, currency); err != nil {
			return fmt.Errorf("insert %s ledger line: %w", line.side, err)
		}
	}
	return nil
}
