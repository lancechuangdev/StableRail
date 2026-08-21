package payout

import (
	"testing"

	"stablerail/paymentcore"
)

func TestPersistedStatusNormalizesProviderFailureCodes(t *testing.T) {
	tests := []struct {
		code string
		want string
	}{
		{code: "local_failure", want: "failed"},
		{code: "declined", want: "failed"},
		{code: "submission_failed", want: "submission_failed"},
		{code: "refunded", want: "refunded"},
	}
	for _, test := range tests {
		result := paymentcore.ExecutionResult{Status: paymentcore.ExecutionFailed, FailureCode: test.code}
		if got := persistedStatus(result); got != test.want {
			t.Errorf("persistedStatus(failure_code=%q)=%q, want %q", test.code, got, test.want)
		}
	}
}
