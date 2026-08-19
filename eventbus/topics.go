package eventbus

// Kafka topics used by StableRail. Keeping the topology here makes producer
// and consumer routing discoverable without coupling it to a domain package.
const (
	PayoutEventsTopic       Topic = "payout-events"
	PayinEventsTopic        Topic = "payin-events"
	SettlementCommandsTopic Topic = "settlement-commands"
	DeadLetterTopic         Topic = "stablerail-dead-letter"
)
