# Integration guide

This guide shows the recommended path from an empty Go service to a
production-ready Conduit consumer.

## 1. Install Conduit and an adapter

```bash
go get github.com/b2balram/conduit
go get github.com/b2balram/conduit/kafka
```

## 2. Define a message and processor

```go
type OrderCreated struct {
	ID       string `json:"id"`
	Customer string `json:"customer"`
}

processor := conduit.HandlerFunc[OrderCreated](
	func(ctx context.Context, message conduit.Message[OrderCreated]) error {
		// Make this operation idempotent because delivery is at least once.
		return orders.CreateOnce(ctx, message.Value.ID, message.Value.Customer)
	},
)
```

`Message[T]` also exposes the transport key, topic, partition, offset,
timestamp, and headers. Avoid coupling business logic to transport metadata
unless it is genuinely part of the application contract.

## 3. Configure Kafka

```go
adapter, err := kafka.New(kafka.Config{
	Brokers:       []string{"kafka-1:9092", "kafka-2:9092"},
	GroupID:       "orders-service",
	Topics:        []string{"orders.created"},
	InitialOffset: kafka.OffsetOldest,
})
if err != nil {
	return err
}
```

Use a stable group ID per logical consumer. Starting at `OffsetOldest` affects
only a group without a committed offset.

For a production cluster, configure transport security and group behavior:

```go
adapter, err := kafka.New(kafka.Config{
	Brokers:  []string{"kafka-1:9093", "kafka-2:9093"},
	GroupID:  "orders-service",
	Topics:   []string{"orders.created"},
	ClientID: "orders-service",
	Version:  "3.8.0",
	TLS: kafka.TLSConfig{
		Enabled:  true,
		CAFile:   "/run/secrets/kafka/ca.pem",
		CertFile: "/run/secrets/kafka/client.pem", // Optional mTLS.
		KeyFile:  "/run/secrets/kafka/client-key.pem",
	},
	SASL: kafka.SASLConfig{
		Enabled:   true,
		Mechanism: kafka.SASLSCRAMSHA512,
		Username:  os.Getenv("KAFKA_USERNAME"),
		Password:  os.Getenv("KAFKA_PASSWORD"),
	},
	Timeouts: kafka.TimeoutConfig{
		Dial:      10 * time.Second,
		Read:      30 * time.Second,
		Write:     30 * time.Second,
		Session:   30 * time.Second,
		Heartbeat: 3 * time.Second,
		Rebalance: 60 * time.Second,
	},
	Rebalance: kafka.RebalanceConfig{
		Strategies:   []kafka.BalanceStrategy{kafka.BalanceCooperativeSticky},
		RetryMax:     6,
		RetryBackoff: 2 * time.Second,
		InstanceID:   hostname,
	},
})
```

Supported SASL mechanisms are PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512. Do not
use PLAIN without TLS. Cooperative sticky assignment requires Kafka 2.4+ and
all members of a group must use a compatible rollout strategy. Sarama recommends
a two-deployment migration: first offer cooperative sticky with the existing
eager strategy, then use cooperative sticky alone. `Config.Configure` is an
advanced escape hatch for Sarama options not represented by typed settings.

## 4. Bridge observability

Conduit deliberately does not import a metrics or logging SDK. Function adapters
make an integration small:

```go
metrics := conduit.MetricsFunc(func(ctx context.Context, metric conduit.Metric) {
	telemetry.Record(ctx, string(metric.Name), metric.Value, metric.Duration, metric.Attributes)
})

logger := conduit.LoggerFunc(func(ctx context.Context, level conduit.LogLevel, message string, fields ...conduit.Field) {
	attributes := make([]any, 0, len(fields)*2)
	for _, field := range fields {
		attributes = append(attributes, field.Key, field.Value)
	}
	slog.Default().Log(ctx, slogLevel(level), message, attributes...)
})
```

The application owns `slogLevel`, allowing it to choose how Conduit levels map
to its logging policy. Metrics implementations must support counter-like values
and duration observations. Both hooks can be omitted and must be safe for
concurrent calls when supplied.

Conduit never logs message payloads or keys. Metric attributes exclude keys,
offsets, and partitions to avoid sensitive data and unbounded time series.

## RabbitMQ

RabbitMQ uses manual acknowledgements. `Prefetch` limits unacknowledged messages;
when omitted in batch mode it defaults to the configured Conduit batch size.

```go
adapter, err := rabbitmq.New(rabbitmq.Config{
	URL:         os.Getenv("AMQP_URL"),
	Queue:       "orders.created",
	ConsumerTag: "orders-service",
	Prefetch:    250,
	Heartbeat:   30 * time.Second,
	TLS: rabbitmq.TLSConfig{
		CAFile:   "/run/secrets/rabbitmq/ca.pem",
		CertFile: "/run/secrets/rabbitmq/client.pem",
		KeyFile:  "/run/secrets/rabbitmq/client-key.pem",
	},
})
```

Declare exchanges, queues, bindings, and broker-side dead-letter exchanges
outside Conduit. A processing failure closes the consume run without
acknowledging the delivery, allowing RabbitMQ to requeue it when the channel
closes. Conduit's `DeadLetterHandler` is an application-level alternative when
the failed payload must be republished with custom metadata.

## NATS JetStream

The NATS adapter intentionally targets JetStream rather than Core NATS because
JetStream provides durable consumers, explicit acknowledgements, and
redelivery.

```go
adapter, err := nats.New(nats.Config{
	URLs:           []string{"nats://nats-1:4222", "nats://nats-2:4222"},
	Stream:         "ORDERS",
	Consumer:       "orders-service",
	FilterSubject:  "orders.created",
	ConnectionName: "orders-service",
	CredentialsFile: "/run/secrets/nats/orders.creds",
	AckWait:        30 * time.Second,
	MaxDeliver:     10,
	DoubleAck:      true,
	AckTimeout:     5 * time.Second,
})
```

The adapter creates or updates a durable pull consumer with explicit
acknowledgements. `DoubleAck` waits for the server to confirm the acknowledgement
and is recommended when avoiding acknowledgement loss is more important than
maximum throughput. JetStream's `MaxDeliver` limits broker redelivery; Conduit's
retry policy controls immediate in-process attempts before a message is left for
broker redelivery or passed to the application DLQ handler.

## 5. Build and run the consumer

```go
consumer, err := conduit.New(
	conduit.Config{Metrics: metrics, Logger: logger},
	adapter,
	conduit.JSON[OrderCreated]{},
	processor,
)
if err != nil {
	return err
}

ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

if err := consumer.Run(ctx); err != nil {
	return fmt.Errorf("run order consumer: %w", err)
}
```

Cancel the context during shutdown. The Kafka adapter stops accepting work,
closes its consumer group, and leaves unfinished messages available for replay.

## Batch processing

Use batching only when the destination can process a group efficiently:

```go
consumer, err := conduit.New(
	conduit.Config{
		BatchSize: 250,
		BatchWait: 500 * time.Millisecond,
		Metrics:   metrics,
		Logger:    logger,
	},
	adapter,
	conduit.JSON[OrderCreated]{},
	conduit.BatchHandlerFunc[OrderCreated](
		func(ctx context.Context, messages []conduit.Message[OrderCreated]) error {
			return warehouse.UpsertOrders(ctx, messages)
		},
	),
)
```

The adapter flushes when either limit is reached. A batch error leaves the batch
unacknowledged. Size the batch below downstream request and transaction limits,
and keep the wait short enough for the service's latency objective.

## Custom serialization

Implement `Serde[T]` for Protobuf, Avro, MessagePack, or an internal format:

```go
type EventSerde struct{}

func (EventSerde) Deserialize(data []byte) (Event, error) {
	return decodeEvent(data)
}

func (EventSerde) Serialize(event Event) ([]byte, error) {
	return encodeEvent(event)
}
```

Deserialization errors are classified as `conduit.StageDeserialize` and the
delivery is not acknowledged.

## Error handling

```go
err := consumer.Run(ctx)
if err != nil {
	var conduitErr *conduit.Error
	if errors.As(err, &conduitErr) {
		switch conduitErr.Stage {
		case conduit.StageTransport:
			// Usually terminate and let the supervisor restart the service.
		case conduit.StageDeserialize, conduit.StageProcess, conduit.StageAcknowledge:
			// Record policy-specific diagnostics; the record remains unacknowledged.
		case conduit.StageDeadLetter:
			// DLQ storage failed; the original record remains unacknowledged.
		}
	}
	return err
}
```

The underlying error is preserved for `errors.Is` and `errors.As`. Conduit does
not retry unless explicitly configured.

## Retry and dead-letter policy

```go
retry := conduit.RetryPolicy{
	MaxAttempts: 5, // Includes the initial attempt.
	Backoff: conduit.ExponentialBackoff{
		Initial: 100 * time.Millisecond,
		Max:     10 * time.Second,
		Jitter:  0.2,
	},
	Retryable: func(err error) bool {
		return !errors.Is(err, ErrInvalidOrder)
	},
}

deadLetter := conduit.DeadLetterFunc(func(ctx context.Context, failed conduit.FailedDelivery) error {
	return dlq.Publish(ctx, failed.Delivery, failed.Cause, failed.Attempts)
})
```

Retries apply to processor failures. Deserialization failures are deterministic
and go directly to the configured dead-letter handler. If no dead-letter handler
is configured, the final processing or deserialization error is returned and the
delivery remains unacknowledged. If dead-letter storage succeeds, Conduit
acknowledges the original. If it or the acknowledgement fails, redelivery can
repeat the dead-letter operation, so handlers must be idempotent.

## Production checklist

- Give each logical consumer a stable, unique group ID.
- Make processors idempotent and preserve a message or business identifier.
- Set timeouts on downstream calls through the supplied context.
- Emit and alert on failed-message and processing-duration metrics.
- Do not include payloads or credentials in application logs.
- Choose batch limits from downstream capacity and latency requirements.
- Cancel the run context and allow the process time to close cleanly.
- Test malformed payloads, processor failures, duplicate delivery, and shutdown.
- Bound retries, add jitter, classify permanent failures, and make dead-letter
  writes idempotent.

## Writing another adapter

An adapter implements one method:

```go
type Adapter interface {
	Consume(context.Context, conduit.BatchSettings, conduit.RawProcessor) error
}
```

It must:

1. Validate transport configuration before consumption starts.
2. Convert native records into independent `conduit.Delivery` values.
3. Supply an `Ack` callback without acknowledging early.
4. Respect batch size and maximum wait when batching is enabled.
5. Stop on context cancellation and close native resources.
6. Return connection, subscription, batching, and close failures.
7. Document native ordering, concurrency, acknowledgement, and redelivery rules.

Keep transport-specific authentication, TLS, and tuning in the adapter package;
do not add them to the core `conduit.Config`.

Implement `adaptertest.Harness` and run `adaptertest.Run` against a real broker
to verify the common contract before publishing an adapter.
