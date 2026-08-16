package eventbus

// Payload versions are explicit per event type. A type's version is advanced
// only when its payload schema changes, never for envelope-only changes.
const (
	PaymentCreatedVersion      = 1
	PaymentProcessingVersion   = 1
	PaymentSucceededVersion    = 1
	PaymentFailedVersion       = 1
	PaymentReturnedVersion     = 1
	PolicyEvaluateVersion      = 1
	LedgerReserveVersion       = 1
	SettlementExecuteVersion   = 1
	PaymentFailVersion         = 1
	PaymentSettleVersion       = 1
	PaymentReturnVersion       = 1
	LedgerReleaseVersion       = 1
	DeadLetterVersion          = 1
	PolicyApprovedVersion      = 1
	PolicyRejectedVersion      = 1
	LedgerReservedVersion      = 1
	LedgerFailedVersion        = 1
	LedgerReleasedVersion      = 1
	SettlementCompletedVersion = 1
	SettlementFailedVersion    = 1
	SettlementReturnedVersion  = 1
	SettlementOnHoldVersion    = 1
)
