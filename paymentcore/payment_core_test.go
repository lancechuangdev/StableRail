package paymentcore

import (
	"fmt"
	"sync"
	"testing"
)

func TestCreateAndSettlePaymentLifecycle(t *testing.T) {
	service := NewService()

	payment, err := service.CreatePayment("order-100", "USD", 2500, "tenant-1", "idem-001")
	if err != nil {
		t.Fatalf("CreatePayment returned error: %v", err)
	}
	if payment.State != StateCreated {
		t.Fatalf("expected state %q, got %q", StateCreated, payment.State)
	}

	if err := service.Process(payment.ID); err != nil {
		t.Fatalf("Process returned error: %v", err)
	}
	payment, err = service.GetPayment(payment.ID)
	if err != nil {
		t.Fatalf("GetPayment returned error: %v", err)
	}
	if payment.State != StateProcessing {
		t.Fatalf("expected state %q, got %q", StateProcessing, payment.State)
	}

	if err := service.Settle(payment.ID); err != nil {
		t.Fatalf("Settle returned error: %v", err)
	}
	payment, err = service.GetPayment(payment.ID)
	if err != nil {
		t.Fatalf("GetPayment returned error: %v", err)
	}
	if payment.State != StateSucceeded {
		t.Fatalf("expected state %q, got %q", StateSucceeded, payment.State)
	}
	if len(payment.LedgerEntries) != 4 {
		t.Fatalf("expected four ledger lines, got %d", len(payment.LedgerEntries))
	}
	processingDebit, processingCredit := payment.LedgerEntries[0], payment.LedgerEntries[1]
	if processingDebit.AccountCode != CashOperatingAccount || processingDebit.Side != EntryDebit ||
		processingCredit.AccountCode != SettlementAccount || processingCredit.Side != EntryCredit {
		t.Fatalf("unexpected processing journal: %+v %+v", processingDebit, processingCredit)
	}
	settlementDebit, settlementCredit := payment.LedgerEntries[2], payment.LedgerEntries[3]
	if settlementDebit.AccountCode != SettlementAccount || settlementDebit.Side != EntryDebit ||
		settlementCredit.AccountCode != CashOperatingAccount || settlementCredit.Side != EntryCredit {
		t.Fatalf("unexpected settlement journal: %+v %+v", settlementDebit, settlementCredit)
	}
	for _, entry := range payment.LedgerEntries {
		if entry.AmountMinor != payment.AmountMinor || entry.Currency != payment.Currency || entry.PaymentID != payment.ID {
			t.Fatalf("ledger entry does not balance to payment: %+v", entry)
		}
	}
	if processingDebit.TransactionID != processingCredit.TransactionID || settlementDebit.TransactionID != settlementCredit.TransactionID {
		t.Fatal("debit and credit lines must share a journal transaction")
	}
	assertBalancedJournal(t, payment.LedgerEntries)

	if len(payment.AuditLog) < 3 {
		t.Fatalf("expected at least 3 audit events, got %d", len(payment.AuditLog))
	}

	timeline := service.Timeline(payment.ID)
	if len(timeline) < 3 {
		t.Fatalf("expected at least 3 timeline entries, got %d", len(timeline))
	}
}

func assertBalancedJournal(t *testing.T, entries []LedgerEntry) {
	t.Helper()
	totals := make(map[string]struct{ debit, credit int64 })
	for _, entry := range entries {
		total := totals[entry.TransactionID]
		if entry.Side == EntryDebit {
			total.debit += entry.AmountMinor
		} else if entry.Side == EntryCredit {
			total.credit += entry.AmountMinor
		} else {
			t.Fatalf("unknown ledger side %q", entry.Side)
		}
		totals[entry.TransactionID] = total
	}
	for transactionID, total := range totals {
		if total.debit != total.credit {
			t.Fatalf("journal %s is unbalanced: debits=%d credits=%d", transactionID, total.debit, total.credit)
		}
	}
}

func TestReturnedPaymentsAreSnapshots(t *testing.T) {
	service := NewService()
	payment, err := service.CreatePayment("order-300", "USD", 500, "tenant-3", "idem-snapshot")
	if err != nil {
		t.Fatalf("CreatePayment returned error: %v", err)
	}

	payment.State = StateFailed
	payment.AuditLog[0].Message = "modified"

	stored, err := service.GetPayment(payment.ID)
	if err != nil {
		t.Fatalf("GetPayment returned error: %v", err)
	}
	if stored.State != StateCreated || stored.AuditLog[0].Message == "modified" {
		t.Fatal("mutating a returned payment changed service state")
	}
}

func TestConcurrentCreatesHaveUniqueIDs(t *testing.T) {
	service := NewService()
	const count = 100

	ids := make(chan string, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payment, err := service.CreatePayment(
				fmt.Sprintf("order-%d", i), "USD", 100, "customer", fmt.Sprintf("idem-%d", i),
			)
			if err != nil {
				errs <- err
				return
			}
			ids <- payment.ID
		}(i)
	}
	wg.Wait()
	close(ids)
	close(errs)

	for err := range errs {
		t.Errorf("CreatePayment returned error: %v", err)
	}
	seen := make(map[string]struct{}, count)
	for id := range ids {
		if _, exists := seen[id]; exists {
			t.Errorf("duplicate payment ID %q", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != count {
		t.Fatalf("expected %d payment IDs, got %d", count, len(seen))
	}
}

func TestDuplicateIdempotencyKeyReturnsExistingPayment(t *testing.T) {
	service := NewService()

	first, err := service.CreatePayment("order-200", "USD", 1000, "tenant-2", "idem-dup")
	if err != nil {
		t.Fatalf("CreatePayment returned error: %v", err)
	}

	second, err := service.CreatePayment("order-201", "USD", 1000, "tenant-2", "idem-dup")
	if err != nil {
		t.Fatalf("duplicate create returned error: %v", err)
	}

	if second.ID != first.ID {
		t.Fatalf("expected existing payment %q, got %q", first.ID, second.ID)
	}
	if second.ExternalReference != "order-200" {
		t.Fatalf("expected original external reference %q, got %q", "order-200", second.ExternalReference)
	}
}
