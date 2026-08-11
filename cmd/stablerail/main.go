package main

import (
	"context"
	"errors"
	"log"
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
	"stablerail/outbox"
	"stablerail/paymentapi"
	"stablerail/paymentcore"
	"stablerail/policy"
	"stablerail/postgresdb"
	"stablerail/quote"
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

	commandLoop := &consumer.Loop{
		Reader: consumer.NewKafkaReader(config.KafkaBrokers, string(saga.CommandTopic), "stablerail-core-workers"),
		Processor: inbox.BoundProcessor{Processor: inboxProcessor, Handler: workers.NewCommandHandler(
			policy.DeterministicEvaluator{}, ledger.NewPostgresService(), settlement.NewMockProvider(settlement.SettlementResult{}),
		).Handle},
		Consumer: "core-workers",
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

	server := &http.Server{
		Addr:              config.HTTPAddress,
		Handler:           handler,
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
	)
	if errors.Is(err, context.Canceled) {
		return nil
	}

	return err
}
