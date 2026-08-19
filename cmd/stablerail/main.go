package main

import (
	"context"
	"errors"
	"fmt"
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
	"stablerail/paymentcore/payin"
	"stablerail/paymentcore/payout"
	"stablerail/policy"
	"stablerail/postgresdb"
	"stablerail/reconciliation"
	"stablerail/settlement"
	"stablerail/settlement/blindpay"
	"stablerail/workers"
)

// directPayoutService keeps the deterministic mock lightweight. Production
// providers are wrapped by payout.Service so their attempts are durable.
type directPayoutService struct{ provider payout.ExecutionProvider }

func (s directPayoutService) Name() string { return s.provider.Name() }
func (s directPayoutService) CreatePayout(ctx context.Context, request payout.Request) (payout.Result, error) {
	return s.provider.ExecutePayout(ctx, request)
}

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

	payoutCoordinator, err := payout.NewSagaCoordinator(db, payout.SagaConfig{ComplianceTimeout: config.SagaComplianceTimeout})
	if err != nil {
		return err
	}
	payinCoordinator, err := payin.NewSagaCoordinator(db)
	if err != nil {
		return err
	}

	inboxProcessor, err := inbox.NewProcessor(db)
	if err != nil {
		return err
	}

	sagaLoop := &consumer.Loop{
		Reader:    consumer.NewKafkaReader(config.KafkaBrokers, string(eventbus.PayoutEventsTopic), "stablerail-saga"),
		Processor: inbox.BoundProcessor{Processor: inboxProcessor, Handler: workers.PayoutSagaHandler(payoutCoordinator)},
		Consumer:  "payment-saga",
	}

	webhookDispatcher, err := notification.NewDispatcher(db, nil, notification.Config{})
	if err != nil {
		return err
	}
	webhookLoop := &consumer.Loop{
		Reader:    consumer.NewKafkaReader(config.KafkaBrokers, string(eventbus.PayoutEventsTopic), "stablerail-webhooks"),
		Processor: inbox.BoundProcessor{Processor: inboxProcessor, Handler: notification.EventHandler()},
		Consumer:  "payment-webhooks",
	}
	reconciler, err := reconciliation.New(db, reconciliation.Config{Interval: config.ReconciliationInterval}, logger)
	if err != nil {
		return err
	}

	var provider settlement.SettlementProvider
	var payoutCreator interface {
		Name() string
		CreatePayout(context.Context, payout.Request) (payout.Result, error)
	}
	var payoutQuoteCreator interface {
		CreateQuote(context.Context, payout.QuoteRequest) (payout.QuoteResult, error)
	}
	var blindPayWebhookHandler http.Handler
	webhookReconciler := func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	payoutRecovery := func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	switch config.SettlementProvider {
	case "mock":
		mockProvider := settlement.NewMockProvider(payout.Result{})
		mockProvider.ResultsByAmount = map[int64]payout.Result{}
		if config.MockSettlementFailAmount > 0 {
			mockProvider.ResultsByAmount[config.MockSettlementFailAmount] = payout.Result{Status: payout.StatusFailed, FailureCode: "local_failure", FailureMessage: "local settlement failed"}
		}
		if config.MockSettlementHoldAmount > 0 {
			mockProvider.ResultsByAmount[config.MockSettlementHoldAmount] = payout.Result{Status: payout.StatusOnHold}
		}
		if config.MockSettlementPendingAmount > 0 {
			mockProvider.ResultsByAmount[config.MockSettlementPendingAmount] = payout.Result{Status: payout.StatusPending}
		}
		provider = mockProvider
		payoutCreator = directPayoutService{provider: mockProvider}
		payoutQuotes, err := payout.NewService(db, mockProvider)
		if err != nil {
			return err
		}
		payoutQuoteCreator = payoutQuotes
	case "blindpay":
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
		payoutQuotes, err := blindpay.NewQuoteService(client, references, config.BlindPay.Network, config.BlindPay.Token)
		if err != nil {
			return err
		}
		provider, err = blindpay.NewProvider(payoutQuotes, client, references)
		if err != nil {
			return err
		}
		payouts, err := payout.NewService(db, provider)
		if err != nil {
			return err
		}
		payoutRecovery = func(ctx context.Context) error {
			return payouts.RunRecovery(ctx, config.ReconciliationInterval)
		}
		payoutCreator = payouts
		payoutQuoteCreator = payouts
		verifier, err := blindpay.NewWebhookVerifier(config.BlindPay.WebhookSecret)
		if err != nil {
			return err
		}
		webhooks, err := blindpay.NewWebhookService(db)
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
	default:
		return fmt.Errorf("unsupported settlement provider %q", config.SettlementProvider)
	}
	apiKeys, err := paymentapi.NewAPIKeyService(db)
	if err != nil {
		return err
	}
	payins, err := payin.NewService(db, provider)
	if err != nil {
		return err
	}
	handler, err := paymentapi.NewHandler(paymentcore.NewPostgresService(db), db, payoutQuoteCreator, payins)
	if err != nil {
		return err
	}
	handler = apiKeys.Middleware(handler)
	payinSagaLoop := &consumer.Loop{
		Reader:    consumer.NewKafkaReader(config.KafkaBrokers, string(eventbus.PayinEventsTopic), "stablerail-payin-saga"),
		Processor: inbox.BoundProcessor{Processor: inboxProcessor, Handler: workers.PayinSagaHandler(payinCoordinator)},
		Consumer:  "payin-saga",
	}
	commandLoop := &consumer.Loop{
		Reader:    consumer.NewKafkaReader(config.KafkaBrokers, string(eventbus.SettlementCommandsTopic), "stablerail-core-workers"),
		Processor: inbox.BoundProcessor{Processor: inboxProcessor, Handler: workers.NewCommandHandler(policy.DeterministicEvaluator{RejectAmountMinor: config.MockPolicyRejectAmount}, ledger.NewPostgresService(), payoutCreator, payins).Handle},
		Consumer:  "core-workers",
	}
	webhookEndpoints, err := paymentapi.NewWebhookEndpointService(db)
	if err != nil {
		return err
	}
	if config.AllowPrivateWebhookURLs {
		webhookEndpoints.AllowPrivateURLs()
	}
	webhookEndpointHandler, err := paymentapi.NewWebhookEndpointHandler(webhookEndpoints)
	if err != nil {
		return err
	}

	metrics := &observability.Metrics{}
	root := http.NewServeMux()
	root.Handle("/metrics", metrics.Handler())
	if config.OperatorToken != "" {
		operatorHandler, err := paymentapi.NewOperatorHandler(config.OperatorToken, payoutCoordinator)
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
		if config.SettlementProvider == "mock" {
			control, err := paymentapi.NewLocalSettlementControl(db)
			if err != nil {
				return err
			}
			controlHandler, err := paymentapi.NewLocalSettlementHandler(config.OperatorToken, control)
			if err != nil {
				return err
			}
			root.Handle("POST /v1/operator/mock-settlements/{id}", controlHandler)
		}
	}
	if blindPayWebhookHandler != nil {
		root.Handle("POST /v1/providers/blindpay/webhooks", blindPayWebhookHandler)
	}
	root.Handle("POST /v1/webhook-endpoints", apiKeys.Middleware(webhookEndpointHandler))
	root.Handle("GET /v1/webhook-endpoints", apiKeys.Middleware(webhookEndpointHandler))
	root.Handle("DELETE /v1/webhook-endpoints/{id}", apiKeys.Middleware(webhookEndpointHandler))
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
		workers.TimeoutWorker(payoutCoordinator, config.SagaPollInterval),
		workers.TimeoutWorker(payinCoordinator, config.SagaPollInterval),
		sagaLoop.Run,
		payinSagaLoop.Run,
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
