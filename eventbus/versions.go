package eventbus

// Payload versions are explicit per event type. A type's version is advanced
// only when its payload schema changes, never for envelope-only changes.
const (
	PaymentCreatedVersion    = 1
	PaymentProcessingVersion = 1
	PaymentSettledVersion    = 1
	PolicyEvaluateVersion    = 1
	LedgerReserveVersion     = 1
	SettlementExecuteVersion = 1
	PaymentFailVersion       = 1
	LedgerReleaseVersion     = 1
	DeadLetterVersion        = 1
)
