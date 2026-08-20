package payin

import "testing"

func TestCreatePaymentRequestSupportsQuotedAndDirectModes(t *testing.T) {
	tests := []struct {
		name    string
		request CreatePaymentRequest
		wantErr bool
	}{
		{name: "quoted", request: CreatePaymentRequest{TenantID: "tenant_1", IdempotencyKey: "idem_1", ExternalReference: "order_1", QuoteID: "quote_1"}},
		{name: "direct", request: CreatePaymentRequest{TenantID: "tenant_1", IdempotencyKey: "idem_1", ExternalReference: "order_1", AmountMinor: 1000, Currency: "USD", FundingMethod: "ach", DestinationAccountID: "account_1"}},
		{name: "mixed", request: CreatePaymentRequest{TenantID: "tenant_1", IdempotencyKey: "idem_1", ExternalReference: "order_1", QuoteID: "quote_1", AmountMinor: 1000, Currency: "USD", FundingMethod: "ach", DestinationAccountID: "account_1"}, wantErr: true},
		{name: "incomplete direct", request: CreatePaymentRequest{TenantID: "tenant_1", IdempotencyKey: "idem_1", ExternalReference: "order_1", AmountMinor: 1000, Currency: "USD"}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.request.Validate(); (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}
