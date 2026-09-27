# Roadmap

Conduit is intentionally small. The following improvements would make it safer
and easier to operate without coupling the core to a specific vendor.

## Completed foundations

- Bounded retry and dead-letter policies with backoff and retry classification.
- Adapter conformance kit covering acknowledgement, cancellation, batching,
  ordering, and error propagation.
- Production Kafka configuration for TLS, mTLS, SASL, timeouts, static
  membership, cooperative rebalancing, and advanced Sarama settings.

## Near term

- Add a CI matrix for supported Go versions, race tests on Linux, static checks,
  code coverage, and dependency/security scanning.
- Publish API compatibility and semantic-versioning guarantees before `v1.0.0`.

## Next

- Provide optional OpenTelemetry and Prometheus bridge packages while retaining
  dependency-free core interfaces.
- Add tracing propagation and processor middleware for retries, recovery,
  instrumentation, and application cross-cutting concerns.
- Add health/readiness state that distinguishes configured, connected,
  consuming, rebalancing, and stopped states.
- Add first-party Protobuf SerDe support and documented recipes for Avro/schema
  registries.
- Add RabbitMQ and Redis Streams adapters with the conformance suite.

## Later

- Evaluate a transport-neutral producer API separately from the consumer API.
- Add benchmarks for allocation rate, individual throughput, and batch
  throughput.
- Add executable examples and integration tests using disposable brokers.

Community contributions should generally start with a proposal or issue for
public API changes. Adapter and documentation contributions can usually be kept
independent of the core.
