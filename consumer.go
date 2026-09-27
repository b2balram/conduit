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
	messages := make([]Message[T], 0, len(deliveries))
	decoded := make([]Delivery, 0, len(deliveries))
	for _, delivery := range deliveries {
		started := time.Now()
		value, err := c.serde.Deserialize(delivery.Value)
		c.metric(ctx, Metric{Name: MetricDeserializeTime, Duration: time.Since(started), Attributes: deliveryAttributes(delivery)})
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if c.config.DeadLetter == nil {
				return c.fail(ctx, StageDeserialize, delivery, 1, err)
			}
			if err := c.deadLetter(ctx, delivery, err, 1); err != nil {
				return err
			}
			if err := c.ack(ctx, delivery); err != nil {
				return err
			}
			continue
		}
		messages = append(messages, Message[T]{Value: value, Key: append([]byte(nil), delivery.Key...), Topic: delivery.Topic, Partition: delivery.Partition, Offset: delivery.Offset, Timestamp: delivery.Timestamp, Headers: cloneHeaders(delivery.Headers)})
		decoded = append(decoded, delivery)
	}
	if len(messages) == 0 {
		return nil
	}
	if processor, ok := c.processor.(BatchProcessor[T]); ok {
		attempts, err := c.processWithRetry(ctx, decoded[0], len(decoded), func() error {
			return processor.ProcessBatch(ctx, messages)
		})
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if c.config.DeadLetter == nil {
				return c.fail(ctx, StageProcess, decoded[0], len(decoded), err)
			}
			for _, delivery := range decoded {
				if deadLetterErr := c.deadLetter(ctx, delivery, err, attempts); deadLetterErr != nil {
					return deadLetterErr
				}
			}
			for _, delivery := range decoded {
				if ackErr := c.ack(ctx, delivery); ackErr != nil {
					return ackErr
				}
			}
			return nil
		}
		for _, delivery := range decoded {
			if err := c.ack(ctx, delivery); err != nil {
				return err
			}
		}
		c.metric(ctx, Metric{Name: MetricMessagesSuccess, Value: float64(len(decoded))})
		return nil
	} else {
		processor := c.processor.(Processor[T])
		for i, message := range messages {
			attempts, err := c.processWithRetry(ctx, decoded[i], 1, func() error {
				return processor.Process(ctx, message)
			})
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if c.config.DeadLetter == nil {
					return c.fail(ctx, StageProcess, decoded[i], 1, err)
				}
				if err := c.deadLetter(ctx, decoded[i], err, attempts); err != nil {
					return err
				}
			} else {
				c.metric(ctx, Metric{Name: MetricMessagesSuccess, Value: 1})
			}
			if err := c.ack(ctx, decoded[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Consumer[T]) processWithRetry(ctx context.Context, delivery Delivery, count int, process func() error) (int, error) {
	maxAttempts := c.config.Retry.attempts()
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		started := time.Now()
		err := process()
		c.metric(ctx, Metric{Name: MetricProcessTime, Duration: time.Since(started), Attributes: deliveryAttributes(delivery)})
		if err == nil {
			return attempt, nil
		}
		if ctx.Err() != nil {
			return attempt, ctx.Err()
		}
		if attempt == maxAttempts || !c.config.Retry.shouldRetry(err) {
			return attempt, err
		}
		c.metric(ctx, Metric{Name: MetricMessagesRetried, Value: float64(count), Attributes: deliveryAttributes(delivery)})
		c.log(ctx, LogDebug, "retrying message processing", Field{Key: "topic", Value: delivery.Topic}, Field{Key: "partition", Value: delivery.Partition}, Field{Key: "offset", Value: delivery.Offset}, Field{Key: "attempt", Value: attempt + 1}, Field{Key: "error", Value: err})
		if err := c.config.Retry.wait(ctx, attempt); err != nil {
			return attempt, err
		}
	}
	return maxAttempts, nil
}

func (c *Consumer[T]) deadLetter(ctx context.Context, delivery Delivery, cause error, attempts int) error {
	deadLetterDelivery := delivery
	deadLetterDelivery.Value = append([]byte(nil), delivery.Value...)
	deadLetterDelivery.Key = append([]byte(nil), delivery.Key...)
	deadLetterDelivery.Headers = cloneHeaders(delivery.Headers)
	deadLetterDelivery.Ack = nil
	failed := FailedDelivery{Delivery: deadLetterDelivery, Cause: cause, Attempts: attempts}
	if err := c.config.DeadLetter.Handle(ctx, failed); err != nil {
		return c.fail(ctx, StageDeadLetter, delivery, 1, err)
	}
	c.metric(ctx, Metric{Name: MetricMessagesFailed, Value: 1, Attributes: map[string]string{"stage": "exhausted", "topic": delivery.Topic}})
	c.metric(ctx, Metric{Name: MetricMessagesDeadLettered, Value: 1, Attributes: deliveryAttributes(delivery)})
	c.log(ctx, LogInfo, "message sent to dead letter handler", Field{Key: "topic", Value: delivery.Topic}, Field{Key: "partition", Value: delivery.Partition}, Field{Key: "offset", Value: delivery.Offset}, Field{Key: "attempts", Value: attempts})
	return nil
}

func (c *Consumer[T]) ack(ctx context.Context, delivery Delivery) error {
	if delivery.Ack == nil {
		return nil
	}
	started := time.Now()
	err := delivery.Ack()
	c.metric(ctx, Metric{Name: MetricAcknowledgeTime, Duration: time.Since(started), Attributes: deliveryAttributes(delivery)})
	if err != nil {
		return c.fail(ctx, StageAcknowledge, delivery, 1, err)
	}
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
