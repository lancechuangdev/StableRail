package app

import (
	"errors"
	"os"
	"strings"
	"time"
)

type Config struct {
	HTTPAddress, DatabaseURL, SettlementProvider, CircleAPIKey, CircleBaseURL, CircleSNSTopicARN string
	KafkaBrokers                                                                                 []string
	ShutdownTimeout, SagaPollInterval, ReconciliationInterval                                    time.Duration
}

func ConfigFromEnv() (Config, error) {
	c := Config{
		HTTPAddress:            env("STABLERAIL_HTTP_ADDRESS", ":8080"),
		DatabaseURL:            os.Getenv("STABLERAIL_DATABASE_URL"),
		SettlementProvider:     env("STABLERAIL_SETTLEMENT_PROVIDER", "mock"),
		CircleAPIKey:           os.Getenv("STABLERAIL_CIRCLE_API_KEY"),
		CircleBaseURL:          env("STABLERAIL_CIRCLE_BASE_URL", "https://api.circle.com"),
		CircleSNSTopicARN:      os.Getenv("STABLERAIL_CIRCLE_SNS_TOPIC_ARN"),
		KafkaBrokers:           strings.Split(env("STABLERAIL_KAFKA_BROKERS", "localhost:9092"), ","),
		ShutdownTimeout:        10 * time.Second,
		SagaPollInterval:       time.Second,
		ReconciliationInterval: time.Minute,
	}
	if c.DatabaseURL == "" {
		return Config{}, errors.New("STABLERAIL_DATABASE_URL is required")
	}
	if c.SettlementProvider != "mock" && c.SettlementProvider != "circle" {
		return Config{}, errors.New("STABLERAIL_SETTLEMENT_PROVIDER must be mock or circle")
	}
	if c.SettlementProvider == "circle" && c.CircleAPIKey == "" {
		return Config{}, errors.New("STABLERAIL_CIRCLE_API_KEY is required for Circle settlement")
	}
	if c.SettlementProvider == "circle" && c.CircleSNSTopicARN == "" {
		return Config{}, errors.New("STABLERAIL_CIRCLE_SNS_TOPIC_ARN is required for Circle settlement")
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
