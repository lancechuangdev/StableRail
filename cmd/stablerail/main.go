package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"stablerail/app"
	"stablerail/consumer"
	"stablerail/eventbus"
	"stablerail/inbox"
	"stablerail/ledger"
	"stablerail/notification"
	"stablerail/observability"
	"stablerail/outbox"
	"stablerail/paymentapi"
	"stablerail/paymentcore"
	"stablerail/policy"
	"stablerail/postgresdb"
	"stablerail/reconciliation"
	"stablerail/saga"
	"stablerail/settlement"
	"stablerail/settlement/blindpay"
	"stablerail/workers"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	logger := slog.New(slog.NewJSONHandler(log.Writer(), nil))
	slog.SetDefault(logger)
	config, err := app.ConfigFromEnv()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := postgresdb.Open(ctx, config.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()

	producer, err := eventbus.NewKafkaProducer(eventbus.KafkaConfig{Brokers: config.KafkaBrokers})
	if err != nil {
		return err
	}
	defer producer.Close()

	relay, err := outbox.NewRelay(db, producer, outbox.Config{})
	if err != nil {
		return err
	}

	coordinator, err := saga.NewCoordinator(db, saga.Config{})
	if err != nil {
		return err
	}

	inboxProcessor, err := inbox.NewProcessor(db)
	if err != nil {
		return err
	}

	sagaLoop := &consumer.Loop{
		Reader:    consumer.NewKafkaReader(config.KafkaBrokers, string(paymentcore.PaymentEventsTopic), "stablerail-saga"),
		Processor: inbox.BoundProcessor{Processor: inboxProcessor, Handler: workers.SagaHandler(coordinator)},
		Consumer:  "payment-saga",
	}

	webhookDispatcher, err := notification.NewDispatcher(db, nil, notification.Config{})
	if err != nil {
		return err
	}
	webhookLoop := &consumer.Loop{
		Reader:    consumer.NewKafkaReader(config.KafkaBrokers, string(paymentcore.PaymentEventsTopic), "stablerail-webhooks"),
		Processor: inbox.BoundProcessor{Processor: inboxProcessor, Handler: notification.EventHandler()},
		Consumer:  "payment-webhooks",
	}
	reconciler, err := reconciliation.New(db, reconciliation.Config{Interval: config.ReconciliationInterval}, logger)
	if err != nil {
		return err
	}

	var payoutQuotes paymentapi.BlindPayPayoutQuoteService
	var settlementProvider settlement.SettlementProvider = settlement.NewMockProvider(settlement.SettlementResult{})
	var blindPayWebhookHandler http.Handler
	webhookReconciler := func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	payoutRecovery := func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	if config.SettlementProvider == "blindpay" {
		client, err := blindpay.NewClient(blindpay.Config{
			APIKey:     config.BlindPay.APIKey,
			InstanceID: config.BlindPay.InstanceID,
			BaseURL:    config.BlindPay.BaseURL,
		})
		if err != nil {
			return err
		}
		references, err := blindpay.NewRepository(db)
		if err != nil {
			return err
		}
		payoutQuotes, err = blindpay.NewQuoteService(client, references, config.BlindPay.Network, config.BlindPay.Token)
		if err != nil {
			return err
		}
		payouts, err := blindpay.NewPayoutService(db, client)
		if err != nil {
			return err
		}
		payoutRecovery = func(ctx context.Context) error {
			return payouts.RunRecovery(ctx, config.ReconciliationInterval)
		}
		settlementProvider, err = blindpay.NewProvider(payouts)
		if err != nil {
			return err
		}
		verifier, err := blindpay.NewWebhookVerifier(config.BlindPay.WebhookSecret)
		if err != nil {
			return err
		}
		webhooks, err := blindpay.NewPayoutWebhookService(db)
		if err != nil {
			return err
		}
		blindPayWebhookHandler, err = blindpay.NewWebhookHandler(verifier, webhooks)
		if err != nil {
			return err
		}
		webhookReconciler = func(ctx context.Context) error {
			return webhooks.RunReconciler(ctx, config.ReconciliationInterval)
		}
	}
	commandLoop := &consumer.Loop{
		Reader:    consumer.NewKafkaReader(config.KafkaBrokers, string(saga.CommandTopic), "stablerail-core-workers"),
		Processor: inbox.BoundProcessor{Processor: inboxProcessor, Handler: workers.NewCommandHandler(policy.DeterministicEvaluator{}, ledger.NewPostgresService(), settlementProvider).Handle},
		Consumer:  "core-workers",
	}

	handler, err := paymentapi.NewHandler(paymentcore.NewPostgresService(db), db, payoutQuotes)
	if err != nil {
		return err
	}
	apiKeys, err := paymentapi.NewAPIKeyService(db)
	if err != nil {
		return err
	}
	handler = apiKeys.Middleware(handler)

	metrics := &observability.Metrics{}
	root := http.NewServeMux()
	root.Handle("/metrics", metrics.Handler())
	if config.OperatorToken != "" {
		operatorHandler, err := paymentapi.NewOperatorHandler(config.OperatorToken, coordinator)
		if err != nil {
			return err
		}
		root.Handle("POST /v1/operator/payments/{id}/manual-review", operatorHandler)
		apiKeyHandler, err := paymentapi.NewAPIKeyOperatorHandler(config.OperatorToken, apiKeys)
		if err != nil {
			return err
		}
		root.Handle("POST /v1/operator/tenants/{id}/api-keys", apiKeyHandler)
		apiKeyRevokeHandler, err := paymentapi.NewAPIKeyRevokeOperatorHandler(config.OperatorToken, apiKeys)
		if err != nil {
			return err
		}
		root.Handle("DELETE /v1/operator/api-keys/{id}", apiKeyRevokeHandler)
	}
	if blindPayWebhookHandler != nil {
		root.Handle("POST /v1/providers/blindpay/webhooks", blindPayWebhookHandler)
	}
	root.Handle("/", metrics.Middleware(handler, logger))
	server := &http.Server{
		Addr:              config.HTTPAddress,
		Handler:           root,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	err = app.Run(
		ctx,
		server,
		config.ShutdownTimeout,
		relay.Run,
		saga.TimeoutWorker(coordinator, config.SagaPollInterval),
		sagaLoop.Run,
		commandLoop.Run,
		webhookLoop.Run,
		webhookDispatcher.Run,
		reconciler.Run,
		webhookReconciler,
		payoutRecovery,
	)
	if errors.Is(err, context.Canceled) {
		return nil
	}

	return err
}
