package conduit

import (
	"context"
	"fmt"
	"time"
)

// Adapter connects Conduit to a message transport. Implementations belong in
// transport packages, such as kafka, rabbitmq, or redisstreams.
//
// Consume must deliver batches that obey settings. It must not acknowledge a
// delivery itself: Conduit calls Delivery.Ack after processing succeeds.
type Adapter interface {
	Consume(context.Context, BatchSettings, RawProcessor) error
}

// BatchSettings let an Adapter batch at its natural transport boundary.
type BatchSettings struct {
	Size int
	Wait time.Duration
}

// RawProcessor receives deliveries before decoding. It is implemented by Consumer.
type RawProcessor interface {
	ProcessDeliveries(context.Context, []Delivery) error
}

// Consumer turns deliveries from any Adapter into typed messages.
type Consumer[T any] struct {
	config    Config
	adapter   Adapter
	serde     Serde[T]
	processor any
}

// New validates and creates a queue-neutral consumer. processor must implement
// Processor[T] or BatchProcessor[T].
func New[T any](config Config, adapter Adapter, serde Serde[T], processor any) (*Consumer[T], error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if adapter == nil {
		return nil, fmt.Errorf("conduit: adapter is required")
	}
	if serde == nil {
		return nil, fmt.Errorf("conduit: serde is required")
	}
	switch processor.(type) {
	case Processor[T]:
	case BatchProcessor[T]:
		if config.BatchSize == 0 {
			return nil, fmt.Errorf("conduit: batch size is required for a BatchProcessor")
		}
	default:
		return nil, fmt.Errorf("conduit: processor must implement Processor or BatchProcessor")
	}
	return &Consumer[T]{config: config, adapter: adapter, serde: serde, processor: processor}, nil
}

// Run starts the configured Adapter and blocks until ctx is canceled or it fails.
func (c *Consumer[T]) Run(ctx context.Context) error {
	return c.adapter.Consume(ctx, BatchSettings{Size: c.config.BatchSize, Wait: c.config.BatchWait}, c)
}

// ProcessDeliveries decodes, processes, then acknowledges a successful batch.
func (c *Consumer[T]) ProcessDeliveries(ctx context.Context, deliveries []Delivery) error {
	messages := make([]Message[T], len(deliveries))
	for i, delivery := range deliveries {
		value, err := c.serde.Deserialize(delivery.Value)
		if err != nil {
			return fmt.Errorf("conduit: deserialize %s/%d/%d: %w", delivery.Topic, delivery.Partition, delivery.Offset, err)
		}
		messages[i] = Message[T]{Value: value, Key: append([]byte(nil), delivery.Key...), Topic: delivery.Topic, Partition: delivery.Partition, Offset: delivery.Offset, Timestamp: delivery.Timestamp, Headers: delivery.Headers}
	}
	if processor, ok := c.processor.(BatchProcessor[T]); ok {
		if err := processor.ProcessBatch(ctx, messages); err != nil {
			return err
		}
	} else {
		processor := c.processor.(Processor[T])
		for _, message := range messages {
			if err := processor.Process(ctx, message); err != nil {
				return err
			}
		}
	}
	for _, delivery := range deliveries {
		if delivery.Ack != nil {
			if err := delivery.Ack(); err != nil {
				return fmt.Errorf("conduit: acknowledge: %w", err)
			}
		}
	}
	return nil
}
