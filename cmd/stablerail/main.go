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
	"stablerail/quote"
	"stablerail/reconciliation"
	"stablerail/saga"
	"stablerail/settlement"
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

	var settlementProvider settlement.SettlementProvider = settlement.NewMockProvider(settlement.SettlementResult{})
	var circleSNS *settlement.SNSReceiver
	if config.SettlementProvider == "circle" {
		settlementProvider, err = settlement.NewCircleProvider(settlement.CircleConfig{APIKey: config.CircleAPIKey, BaseURL: config.CircleBaseURL})
		if err != nil {
			return err
		}
		circleSNS, err = settlement.NewSNSReceiver(db, nil, config.CircleSNSTopicARN)
		if err != nil {
			return err
		}
	}
	commandLoop := &consumer.Loop{
		Reader:    consumer.NewKafkaReader(config.KafkaBrokers, string(saga.CommandTopic), "stablerail-core-workers"),
		Processor: inbox.BoundProcessor{Processor: inboxProcessor, Handler: workers.NewCommandHandler(policy.DeterministicEvaluator{}, ledger.NewPostgresService(), settlementProvider).Handle},
		Consumer:  "core-workers",
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

	quoteRepo, err := quote.NewPostgresRepository(db)
	if err != nil {
		return err
	}
	pricing := quote.DeterministicProvider{Quote: quote.Price{Rate: "1", FeeMinor: 0, ValidFor: time.Minute}}
	quoteService, err := quote.NewService(pricing, quoteRepo)
	if err != nil {
		return err
	}
	handler, err := paymentapi.NewHandler(paymentcore.NewPostgresService(db), quoteService, db)
	if err != nil {
		return err
	}

	metrics := &observability.Metrics{}
	root := http.NewServeMux()
	root.Handle("/metrics", metrics.Handler())
	if circleSNS != nil {
		root.Handle("POST /v1/providers/circle/notifications", circleSNS)
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
	)
	if errors.Is(err, context.Canceled) {
		return nil
	}

	return err
}
