package app

import "testing"

func TestBlindPayConfigValidate(t *testing.T) {
	valid := BlindPayConfig{
		APIKey: "secret", InstanceID: "in_test", BaseURL: "https://api.blindpay.com/v1",
		WebhookSecret: "whsec_test", Network: "base", Token: "USDC", ManagedWalletID: "bl_test", ManagedWalletAddress: "0xabc",
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	valid.InstanceID = "invalid"
	if err := valid.Validate(); err == nil {
		t.Fatal("invalid instance ID accepted")
	}
}

func TestConfigFromEnvNestsBlindPay(t *testing.T) {
	t.Setenv("STABLERAIL_DATABASE_URL", "postgres://example")
	t.Setenv("STABLERAIL_SETTLEMENT_PROVIDER", "blindpay")
	t.Setenv("STABLERAIL_BLINDPAY_API_KEY", "secret")
	t.Setenv("STABLERAIL_BLINDPAY_INSTANCE_ID", "in_test")
	t.Setenv("STABLERAIL_BLINDPAY_WEBHOOK_SECRET", "whsec_test")
	t.Setenv("STABLERAIL_BLINDPAY_NETWORK", "base")
	t.Setenv("STABLERAIL_BLINDPAY_TOKEN", "USDC")
	t.Setenv("STABLERAIL_BLINDPAY_MANAGED_WALLET_ID", "bl_test")
	t.Setenv("STABLERAIL_BLINDPAY_MANAGED_WALLET_ADDRESS", "0xabc")
	c, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.BlindPay.InstanceID != "in_test" || c.BlindPay.Token != "USDC" {
		t.Fatalf("unexpected BlindPay config: %+v", c.BlindPay)
	}
}
