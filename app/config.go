package app

import (
	"errors"
	"os"
	"strings"
	"time"
)

type Config struct {
	HTTPAddress, DatabaseURL                                  string
	KafkaBrokers                                              []string
	ShutdownTimeout, SagaPollInterval, ReconciliationInterval time.Duration
}

func ConfigFromEnv() (Config, error) {
	c := Config{
		HTTPAddress:            env("STABLERAIL_HTTP_ADDRESS", ":8080"),
		DatabaseURL:            os.Getenv("STABLERAIL_DATABASE_URL"),
		KafkaBrokers:           strings.Split(env("STABLERAIL_KAFKA_BROKERS", "localhost:9092"), ","),
		ShutdownTimeout:        10 * time.Second,
		SagaPollInterval:       time.Second,
		ReconciliationInterval: time.Minute,
	}
	if c.DatabaseURL == "" {
		return Config{}, errors.New("STABLERAIL_DATABASE_URL is required")
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
