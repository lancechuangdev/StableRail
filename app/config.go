package app

import (
	"errors"
	"os"
	"strings"
	"time"
)

type Config struct {
	HTTPAddress, DatabaseURL, SettlementProvider, OperatorToken string
	BlindPay                                                    BlindPayConfig
	KafkaBrokers                                                []string
	ShutdownTimeout, SagaPollInterval, ReconciliationInterval   time.Duration
}

type BlindPayConfig struct {
	APIKey, InstanceID, BaseURL           string
	WebhookSecret, Network, Token         string
	ManagedWalletID, ManagedWalletAddress string
}

func (c BlindPayConfig) Validate() error {
	if c.APIKey == "" || c.InstanceID == "" || c.WebhookSecret == "" || c.Network == "" || c.Token == "" || c.ManagedWalletID == "" || c.ManagedWalletAddress == "" {
		return errors.New("BlindPay API key, instance ID, webhook secret, network, token, managed wallet ID, and managed wallet address are required")
	}
	if !strings.HasPrefix(c.InstanceID, "in_") {
		return errors.New("STABLERAIL_BLINDPAY_INSTANCE_ID must start with in_")
	}
	if !strings.HasPrefix(c.ManagedWalletID, "bl_") {
		return errors.New("STABLERAIL_BLINDPAY_MANAGED_WALLET_ID must start with bl_")
	}
	return nil
}

func ConfigFromEnv() (Config, error) {
	c := Config{
		HTTPAddress:        env("STABLERAIL_HTTP_ADDRESS", ":8080"),
		DatabaseURL:        os.Getenv("STABLERAIL_DATABASE_URL"),
		SettlementProvider: env("STABLERAIL_SETTLEMENT_PROVIDER", "mock"),
		OperatorToken:      os.Getenv("STABLERAIL_OPERATOR_TOKEN"),
		BlindPay: BlindPayConfig{
			APIKey:               os.Getenv("STABLERAIL_BLINDPAY_API_KEY"),
			InstanceID:           os.Getenv("STABLERAIL_BLINDPAY_INSTANCE_ID"),
			BaseURL:              env("STABLERAIL_BLINDPAY_BASE_URL", "https://api.blindpay.com/v1"),
			WebhookSecret:        os.Getenv("STABLERAIL_BLINDPAY_WEBHOOK_SECRET"),
			Network:              os.Getenv("STABLERAIL_BLINDPAY_NETWORK"),
			Token:                os.Getenv("STABLERAIL_BLINDPAY_TOKEN"),
			ManagedWalletID:      os.Getenv("STABLERAIL_BLINDPAY_MANAGED_WALLET_ID"),
			ManagedWalletAddress: os.Getenv("STABLERAIL_BLINDPAY_MANAGED_WALLET_ADDRESS"),
		},
		KafkaBrokers:           strings.Split(env("STABLERAIL_KAFKA_BROKERS", "localhost:9092"), ","),
		ShutdownTimeout:        10 * time.Second,
		SagaPollInterval:       time.Second,
		ReconciliationInterval: time.Minute,
	}
	if c.DatabaseURL == "" {
		return Config{}, errors.New("STABLERAIL_DATABASE_URL is required")
	}
	if c.SettlementProvider != "mock" && c.SettlementProvider != "blindpay" {
		return Config{}, errors.New("STABLERAIL_SETTLEMENT_PROVIDER must be mock or blindpay")
	}
	if c.SettlementProvider == "blindpay" {
		if err := c.BlindPay.Validate(); err != nil {
			return Config{}, err
		}
	}
	if raw := os.Getenv("STABLERAIL_SHUTDOWN_TIMEOUT"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return Config{}, errors.New("invalid STABLERAIL_SHUTDOWN_TIMEOUT")
		}
		c.ShutdownTimeout = d
	}
	if raw := os.Getenv("STABLERAIL_SAGA_POLL_INTERVAL"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return Config{}, errors.New("invalid STABLERAIL_SAGA_POLL_INTERVAL")
		}
		c.SagaPollInterval = d
	}
	if raw := os.Getenv("STABLERAIL_RECONCILIATION_INTERVAL"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return Config{}, errors.New("invalid STABLERAIL_RECONCILIATION_INTERVAL")
		}
		c.ReconciliationInterval = d
	}
	for i := range c.KafkaBrokers {
		c.KafkaBrokers[i] = strings.TrimSpace(c.KafkaBrokers[i])
		if c.KafkaBrokers[i] == "" {
			return Config{}, errors.New("STABLERAIL_KAFKA_BROKERS contains an empty broker")
		}
	}
	return c, nil
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
