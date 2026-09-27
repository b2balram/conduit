// Package kafka provides the Apache Kafka adapter for Conduit, using Sarama.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/b2balram/conduit"
)

// Adapter implements conduit.Adapter for Kafka consumer groups.
type Adapter struct{ config Config }

// New validates and creates a Kafka adapter.
func New(config Config) (*Adapter, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	return &Adapter{config: config}, nil
}

// Consume implements conduit.Adapter.
func (a *Adapter) Consume(ctx context.Context, settings conduit.BatchSettings, processor conduit.RawProcessor) (result error) {
	configuration, err := a.config.saramaConfig()
	if err != nil {
		return err
	}
	group, err := sarama.NewConsumerGroup(a.config.Brokers, a.config.GroupID, configuration)
	if err != nil {
		return fmt.Errorf("conduit/kafka: create consumer group: %w", err)
	}
	defer func() {
		if err := group.Close(); err != nil {
			result = errors.Join(result, fmt.Errorf("conduit/kafka: close consumer group: %w", err))
		}
	}()
	handler := groupHandler{settings: settings, processor: processor}
	for ctx.Err() == nil {
		if err := group.Consume(ctx, a.config.Topics, handler); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("conduit/kafka: consume: %w", err)
		}
	}
	return nil
}

type groupHandler struct {
	settings  conduit.BatchSettings
	processor conduit.RawProcessor
}

func (groupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (groupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h groupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	batcher := claimBatcher{settings: h.settings, session: session, processor: h.processor}
	defer batcher.stopTimer()
	for {
		var timer <-chan time.Time
		if batcher.timer != nil {
			timer = batcher.timer.C
		}
		select {
		case <-session.Context().Done():
			return nil
		case <-timer:
			if err := batcher.flush(); err != nil {
				return err
			}
		case record, ok := <-claim.Messages():
			if !ok {
				return batcher.flush()
			}
			if err := batcher.add(record); err != nil {
				return err
			}
		}
	}
}

type claimBatcher struct {
	settings   conduit.BatchSettings
	session    sarama.ConsumerGroupSession
	processor  conduit.RawProcessor
	deliveries []conduit.Delivery
	timer      *time.Timer
}

func (b *claimBatcher) add(record *sarama.ConsumerMessage) error {
	delivery := toDelivery(record, b.session)
	if b.settings.Size == 0 {
		return b.processor.ProcessDeliveries(b.session.Context(), []conduit.Delivery{delivery})
	}
	b.deliveries = append(b.deliveries, delivery)
	if b.timer == nil {
		b.timer = time.NewTimer(b.settings.Wait)
	}
	if len(b.deliveries) >= b.settings.Size {
		return b.flush()
	}
	return nil
}

func (b *claimBatcher) flush() error {
	if len(b.deliveries) == 0 {
		return nil
	}
	if err := b.processor.ProcessDeliveries(b.session.Context(), b.deliveries); err != nil {
		return err
	}
	b.deliveries = nil
	b.stopTimer()
	return nil
}

func (b *claimBatcher) stopTimer() {
	if b.timer == nil {
		return
	}
	if !b.timer.Stop() {
		select {
		case <-b.timer.C:
		default:
		}
	}
	b.timer = nil
}

func toDelivery(record *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) conduit.Delivery {
	headers := make([]conduit.Header, len(record.Headers))
	for i, header := range record.Headers {
		headers[i] = conduit.Header{Key: string(header.Key), Value: append([]byte(nil), header.Value...)}
	}
	return conduit.Delivery{
		Value: append([]byte(nil), record.Value...), Key: append([]byte(nil), record.Key...), Topic: record.Topic,
		Partition: record.Partition, Offset: record.Offset, Timestamp: record.Timestamp, Headers: headers,
		Ack: func() error {
			session.MarkMessage(record, "")
			return nil
		},
	}
}
