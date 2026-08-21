package eventbus

// Payload versions are explicit per event type. A type's version is advanced
// only when its payload schema changes, never for envelope-only changes.
const (
	PaymentCreatedVersion          = 1
	PaymentProcessingVersion       = 1
	PaymentSucceededVersion        = 1
	PaymentFailedVersion           = 1
	PayoutCreatedVersion           = 1
	PayoutSubmittedVersion         = 1
	PayoutCompletedVersion         = 1
	PayoutFailedVersion            = 1
	PayoutFundsReturnedVersion     = 1
	PayoutReturnCompletedVersion   = 1
	PayoutPolicyApprovedVersion    = 1
	PayoutPolicyRejectedVersion    = 1
	PayoutFundsReservedVersion     = 1
	PayoutLedgerFailedVersion      = 1
	PayoutFundsReleasedVersion     = 1
	PayoutProviderCompletedVersion = 1
	PayoutProviderFailedVersion    = 1
	PayoutProviderReturnedVersion  = 1
	PayoutOnHoldVersion            = 1
	PolicyEvaluateVersion          = 1
	LedgerReserveVersion           = 1
	SettlementExecuteVersion       = 1
	PaymentFailVersion             = 1
	PaymentSettleVersion           = 1
	PaymentReturnVersion           = 1
	LedgerReleaseVersion           = 1
	DeadLetterVersion              = 1
	PayinCreatedVersion            = 1
	PayinExecuteVersion            = 1
	PayinProcessingVersion         = 1
	PayinOnHoldVersion             = 1
	PayinSucceededVersion          = 1
	PayinFailedVersion             = 1
	PayinRefundedVersion           = 1
	PayinPolicyEvaluateVersion     = 1
	PayinPolicyApprovedVersion     = 1
	PayinLedgerRecordVersion       = 1
	PayinReceivedVersion           = 1
)
