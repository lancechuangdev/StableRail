package policy

import (
	"context"
	"testing"
)

func TestDeterministicEvaluator(t *testing.T) {
	evaluator := DeterministicEvaluator{}
	decision, err := evaluator.Evaluate(context.Background(), PolicyRequest{OperationID: "pay_1", Direction: "payout", AmountMinor: 10, Currency: "USD"})
	if err != nil || !decision.Approved {
		t.Fatalf("decision=%#v err=%v", decision, err)
	}
	decision, err = evaluator.Evaluate(context.Background(), PolicyRequest{})
	if err != nil || decision.Approved || decision.Reason == "" {
		t.Fatalf("decision=%#v err=%v", decision, err)
	}
}
