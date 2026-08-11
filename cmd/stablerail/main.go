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
	"stablerail/outbox"
	"stablerail/paymentapi"
	"stablerail/paymentcore"
	"stablerail/policy"
	"stablerail/postgresdb"
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
		Processor: consumer.InboxProcessor{Inbox: inboxProcessor, Handler: workers.SagaHandler(coordinator)},
		Consumer:  "payment-saga",
	}

	commandLoop := &consumer.Loop{
		Reader: consumer.NewKafkaReader(config.KafkaBrokers, string(saga.CommandTopic), "stablerail-core-workers"),
		Processor: consumer.InboxProcessor{Inbox: inboxProcessor, Handler: workers.NewCommandHandler(
			policy.DeterministicEvaluator{}, ledger.NewPostgresService(), settlement.NewMockProvider(settlement.SettlementResult{}),
		).Handle},
		Consumer: "core-workers",
	}

	handler, err := paymentapi.NewHandler(paymentcore.NewPostgresService(db), db)
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
		app.SagaTimeoutWorker(coordinator, config.SagaPollInterval),
		sagaLoop.Run,
		commandLoop.Run,
	)
	if errors.Is(err, context.Canceled) {
		return nil
	}

	return err
}
