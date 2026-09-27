# Conduit

**Conduit** is a small, typed Go library for consuming messages from any queue. Your application supplies a processor and a SerDe; an adapter connects it to a transport. Kafka is supported today, with RabbitMQ, Redis Streams, and other transports able to join through the same adapter contract.

## Documentation

- [Architecture](docs/ARCHITECTURE.md) — component boundaries, message flow, and delivery guarantees
- [Integration guide](docs/INTEGRATION.md) — production setup, batching, SerDes, metrics, logging, and shutdown
- [Roadmap](docs/ROADMAP.md) — prioritized improvements and contribution opportunities

## Install

Conduit requires Go 1.26 or newer.

```bash
go get github.com/b2balram/conduit
go get github.com/b2balram/conduit/kafka
go get github.com/b2balram/conduit/rabbitmq
go get github.com/b2balram/conduit/nats
```

## Kafka example

```go
adapter, err := kafka.New(kafka.Config{
	Brokers: []string{"localhost:9092"},
	GroupID: "billing",
	Topics:  []string{"orders.created"},
})
if err != nil { log.Fatal(err) }

consumer, err := conduit.New(conduit.Config{}, adapter,
	conduit.JSON[OrderCreated]{},
	conduit.HandlerFunc[OrderCreated](func(ctx context.Context, order conduit.Message[OrderCreated]) error {
		return charge(order.Value.ID)
	}),
)
if err != nil { log.Fatal(err) }
log.Fatal(consumer.Run(context.Background()))
```

## Batch processing

```go
consumer, err := conduit.New(conduit.Config{
	BatchSize: 100,
	BatchWait: 2 * time.Second,
}, adapter, conduit.JSON[Event]{},
	conduit.BatchHandlerFunc[Event](func(ctx context.Context, events []conduit.Message[Event]) error {
		return writeEvents(events)
	}),
)
```

Conduit acknowledges a delivery only after the processor returns successfully. Processors should therefore be idempotent: transports with at-least-once delivery may replay a successfully handled message after a crash or rebalance.

## Retries and dead letters

Processor retries are bounded, context-aware, and disabled by default. A
dead-letter handler is called only after attempts are exhausted; the original
delivery is acknowledged only after that handler succeeds.

```go
config := conduit.Config{
	Retry: conduit.RetryPolicy{
		MaxAttempts: 4,
		Backoff: conduit.ExponentialBackoff{
			Initial: 100 * time.Millisecond,
			Max:     5 * time.Second,
			Jitter:  0.2,
		},
	},
	DeadLetter: conduit.DeadLetterFunc(func(ctx context.Context, failed conduit.FailedDelivery) error {
		return deadLetters.Publish(ctx, failed.Delivery.Value, failed.Cause)
	}),
}
```

Use `RetryPolicy.Retryable` to prevent retries for permanent application errors.
Dead-letter handlers must be idempotent because an acknowledgement failure can
cause the original delivery—and therefore the dead-letter operation—to repeat.

## Metrics and logging

Observability is optional and vendor-neutral. Implement `conduit.Metrics` and
`conduit.Logger`, or use the function adapters, to bridge Conduit to Prometheus,
OpenTelemetry, `log/slog`, Zap, Zerolog, or another backend:

```go
config := conduit.Config{
	Metrics: conduit.MetricsFunc(func(ctx context.Context, metric conduit.Metric) {
		collector.Record(metric.Name, metric.Value, metric.Duration, metric.Attributes)
	}),
	Logger: conduit.LoggerFunc(func(ctx context.Context, level conduit.LogLevel, message string, fields ...conduit.Field) {
		applicationLogger.Log(ctx, level, message, fields)
	}),
}
```

Conduit reports received, successful, and failed message counts; observed batch
sizes; and deserialize, processor, and acknowledgement durations. Topic and
failure-stage attributes are bounded metadata; offsets and keys are deliberately
excluded from metric attributes to avoid high-cardinality series.

Logging records lifecycle events and failures. Conduit does not log message keys
or payloads, which may contain sensitive data.

## Errors

Processing failures are returned as `*conduit.Error` with a stable `Stage`
(`transport`, `deserialize`, `process`, `dead_letter`, or `acknowledge`) and
message location.
The original cause is retained, so callers can use `errors.Is` and `errors.As`.
An error prevents acknowledgement of the failing message; processors should be
idempotent because previously processed records can be delivered again.

## Adapter model

Adapters implement `conduit.Adapter`, converting their native messages into `conduit.Delivery` values. The core decodes them, invokes processors, and calls each delivery's `Ack` only on success. This keeps application processors and SerDes independent of Kafka or any future queue.

| Adapter | Status |
| --- | --- |
| `conduit/kafka` | Available, backed by IBM Sarama |
| `conduit/rabbitmq` | Available, backed by the official AMQP 0.9.1 client |
| `conduit/nats` | Available for durable NATS JetStream pull consumers |
| `conduit/redisstreams` | Planned |

Adapter maintainers can use [`adaptertest`](adaptertest/README.md) to verify the
shared acknowledgement, ordering, batching, cancellation, and error contract.

## SerDes

- `conduit.JSON[T]` for JSON
- `conduit.String` for UTF-8 text
- `conduit.Bytes` for raw bytes

Implement `conduit.Serde[T]` for Protobuf, Avro, MessagePack, or an internal format.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md), and report security issues under [SECURITY.md](SECURITY.md).

## License

MIT. See [LICENSE](LICENSE).
