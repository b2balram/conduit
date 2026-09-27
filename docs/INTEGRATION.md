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
		}
	}
	return err
}
```

The underlying error is preserved for `errors.Is` and `errors.As`. Conduit does
not retry inside the core today. Use an idempotent processor and let the
transport redeliver, or implement an explicit retry/dead-letter policy at the
application or adapter boundary.

## Production checklist

- Give each logical consumer a stable, unique group ID.
- Make processors idempotent and preserve a message or business identifier.
- Set timeouts on downstream calls through the supplied context.
- Emit and alert on failed-message and processing-duration metrics.
- Do not include payloads or credentials in application logs.
- Choose batch limits from downstream capacity and latency requirements.
- Cancel the run context and allow the process time to close cleanly.
- Test malformed payloads, processor failures, duplicate delivery, and shutdown.

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
