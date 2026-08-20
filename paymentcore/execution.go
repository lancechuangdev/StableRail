package paymentcore

// ExecutionStatus is the provider-neutral outcome of an asynchronous
// settlement attempt. Direction-specific lifecycle detail is persisted by the
// payin or payout operation.
type ExecutionStatus string

const (
	ExecutionPending   ExecutionStatus = "pending"
	ExecutionOnHold    ExecutionStatus = "on_hold"
	ExecutionSucceeded ExecutionStatus = "succeeded"
	ExecutionFailed    ExecutionStatus = "failed"
)
