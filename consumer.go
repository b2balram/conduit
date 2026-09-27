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
	if ctx == nil {
		return fmt.Errorf("conduit: context is required")
	}
	c.log(ctx, LogInfo, "consumer started")
	err := c.adapter.Consume(ctx, BatchSettings{Size: c.config.BatchSize, Wait: c.config.BatchWait}, c)
	if err == nil {
		c.log(ctx, LogInfo, "consumer stopped")
		return nil
	}
	wrapped := &Error{Stage: StageTransport, Err: err}
	c.logError(ctx, wrapped, Delivery{})
	return wrapped
}

// ProcessDeliveries decodes, processes, then acknowledges a successful batch.
func (c *Consumer[T]) ProcessDeliveries(ctx context.Context, deliveries []Delivery) error {
	if len(deliveries) == 0 {
		return nil
	}
	c.metric(ctx, Metric{Name: MetricMessagesReceived, Value: float64(len(deliveries))})
	c.metric(ctx, Metric{Name: MetricBatchSize, Value: float64(len(deliveries))})
	messages := make([]Message[T], len(deliveries))
	for i, delivery := range deliveries {
		started := time.Now()
		value, err := c.serde.Deserialize(delivery.Value)
		c.metric(ctx, Metric{Name: MetricDeserializeTime, Duration: time.Since(started), Attributes: deliveryAttributes(delivery)})
		if err != nil {
			return c.fail(ctx, StageDeserialize, delivery, 1, err)
		}
		messages[i] = Message[T]{Value: value, Key: append([]byte(nil), delivery.Key...), Topic: delivery.Topic, Partition: delivery.Partition, Offset: delivery.Offset, Timestamp: delivery.Timestamp, Headers: cloneHeaders(delivery.Headers)}
	}
	started := time.Now()
	if processor, ok := c.processor.(BatchProcessor[T]); ok {
		if err := processor.ProcessBatch(ctx, messages); err != nil {
			c.metric(ctx, Metric{Name: MetricProcessTime, Duration: time.Since(started)})
			return c.fail(ctx, StageProcess, deliveries[0], len(deliveries), err)
		}
	} else {
		processor := c.processor.(Processor[T])
		for i, message := range messages {
			if err := processor.Process(ctx, message); err != nil {
				c.metric(ctx, Metric{Name: MetricProcessTime, Duration: time.Since(started)})
				return c.fail(ctx, StageProcess, deliveries[i], 1, err)
			}
		}
	}
	c.metric(ctx, Metric{Name: MetricProcessTime, Duration: time.Since(started)})
	for _, delivery := range deliveries {
		if delivery.Ack != nil {
			started = time.Now()
			if err := delivery.Ack(); err != nil {
				c.metric(ctx, Metric{Name: MetricAcknowledgeTime, Duration: time.Since(started), Attributes: deliveryAttributes(delivery)})
				return c.fail(ctx, StageAcknowledge, delivery, 1, err)
			}
			c.metric(ctx, Metric{Name: MetricAcknowledgeTime, Duration: time.Since(started), Attributes: deliveryAttributes(delivery)})
		}
	}
	c.metric(ctx, Metric{Name: MetricMessagesSuccess, Value: float64(len(deliveries))})
	return nil
}

func (c *Consumer[T]) fail(ctx context.Context, stage ErrorStage, delivery Delivery, count int, cause error) error {
	err := &Error{Stage: stage, Topic: delivery.Topic, Partition: delivery.Partition, Offset: delivery.Offset, Err: cause}
	c.metric(ctx, Metric{Name: MetricMessagesFailed, Value: float64(count), Attributes: map[string]string{"stage": string(stage), "topic": delivery.Topic}})
	c.logError(ctx, err, delivery)
	return err
}

func (c *Consumer[T]) metric(ctx context.Context, metric Metric) {
	if c.config.Metrics != nil {
		c.config.Metrics.Record(ctx, metric)
	}
}

func (c *Consumer[T]) log(ctx context.Context, level LogLevel, message string, fields ...Field) {
	if c.config.Logger != nil {
		c.config.Logger.Log(ctx, level, message, fields...)
	}
}

func (c *Consumer[T]) logError(ctx context.Context, err error, delivery Delivery) {
	c.log(ctx, LogError, "conduit operation failed", Field{Key: "error", Value: err}, Field{Key: "topic", Value: delivery.Topic}, Field{Key: "partition", Value: delivery.Partition}, Field{Key: "offset", Value: delivery.Offset})
}

func deliveryAttributes(delivery Delivery) map[string]string {
	return map[string]string{"topic": delivery.Topic}
}

func cloneHeaders(headers []Header) []Header {
	cloned := make([]Header, len(headers))
	for i, header := range headers {
		cloned[i] = Header{Key: header.Key, Value: append([]byte(nil), header.Value...)}
	}
	return cloned
}
