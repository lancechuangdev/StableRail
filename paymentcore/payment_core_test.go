package paymentcore

import (
	"fmt"
	"sync"
	"testing"
)

func TestCreateAndSettlePaymentLifecycle(t *testing.T) {
	service := NewService()

	payment, err := service.CreatePayment("order-100", "USD", 2500, "customer-1", "idem-001")
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
	if payment.State != StateSettled {
		t.Fatalf("expected state %q, got %q", StateSettled, payment.State)
	}

	if len(payment.AuditLog) < 3 {
		t.Fatalf("expected at least 3 audit events, got %d", len(payment.AuditLog))
	}

	timeline := service.Timeline(payment.ID)
	if len(timeline) < 3 {
		t.Fatalf("expected at least 3 timeline entries, got %d", len(timeline))
	}
}

func TestReturnedPaymentsAreSnapshots(t *testing.T) {
	service := NewService()
	payment, err := service.CreatePayment("order-300", "USD", 500, "customer-3", "idem-snapshot")
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

	first, err := service.CreatePayment("order-200", "USD", 1000, "customer-2", "idem-dup")
	if err != nil {
		t.Fatalf("CreatePayment returned error: %v", err)
	}

	second, err := service.CreatePayment("order-201", "USD", 1000, "customer-2", "idem-dup")
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
