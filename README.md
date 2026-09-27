# Conduit

**Conduit** is a small, typed Go library for consuming messages from any queue. Your application supplies a processor and a SerDe; an adapter connects it to a transport. Kafka is supported today, with RabbitMQ, Redis Streams, and other transports able to join through the same adapter contract.

## Install

```bash
go get github.com/b2balram/conduit
go get github.com/b2balram/conduit/kafka
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

## Adapter model

Adapters implement `conduit.Adapter`, converting their native messages into `conduit.Delivery` values. The core decodes them, invokes processors, and calls each delivery's `Ack` only on success. This keeps application processors and SerDes independent of Kafka or any future queue.

| Adapter | Status |
| --- | --- |
| `conduit/kafka` | Available, backed by IBM Sarama |
| `conduit/rabbitmq` | Planned |
| `conduit/redisstreams` | Planned |

## SerDes

- `conduit.JSON[T]` for JSON
- `conduit.String` for UTF-8 text
- `conduit.Bytes` for raw bytes

Implement `conduit.Serde[T]` for Protobuf, Avro, MessagePack, or an internal format.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md), and report security issues under [SECURITY.md](SECURITY.md).

## License

MIT. See [LICENSE](LICENSE).
