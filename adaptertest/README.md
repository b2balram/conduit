# Adapter conformance suite

Adapter maintainers can verify the common Conduit contract against a real test
transport by implementing `adaptertest.Harness` and calling:

```go
func TestConformance(t *testing.T) {
	adaptertest.Run(t, newHarness)
}
```

`newHarness` must create an isolated transport destination for each subtest.
The suite verifies success-only acknowledgement, ordered processing, size and
time-based batch flushing, cancellation, processor-error propagation,
acknowledgement-error propagation, and transport-error propagation.

The harness controls transport setup and message publication, so the same suite
can cover Kafka, RabbitMQ, Redis Streams, or another implementation without
embedding those clients in `adaptertest`.
