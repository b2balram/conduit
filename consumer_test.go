package conduit

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeAdapter struct{}

func (fakeAdapter) Consume(context.Context, BatchSettings, RawProcessor) error { return nil }

func TestNewRequiresBatchConfiguration(t *testing.T) {
	_, err := New(Config{}, fakeAdapter{}, String{}, BatchHandlerFunc[string](func(context.Context, []Message[string]) error { return nil }))
	if err == nil { t.Fatal("expected batch configuration error") }
}

func TestProcessAcknowledgesOnlyAfterSuccess(t *testing.T) {
	acked := false
	consumer, err := New(Config{}, fakeAdapter{}, String{}, HandlerFunc[string](func(context.Context, Message[string]) error { return nil }))
	if err != nil { t.Fatal(err) }
	err = consumer.ProcessDeliveries(context.Background(), []Delivery{{Value: []byte("hello"), Ack: func() error { acked = true; return nil }}})
	if err != nil { t.Fatal(err) }
	if !acked { t.Fatal("delivery was not acknowledged") }
}

func TestProcessDoesNotAcknowledgeOnFailure(t *testing.T) {
	acked := false
	consumer, err := New(Config{}, fakeAdapter{}, String{}, HandlerFunc[string](func(context.Context, Message[string]) error { return errors.New("fail") }))
	if err != nil { t.Fatal(err) }
	err = consumer.ProcessDeliveries(context.Background(), []Delivery{{Value: []byte("hello"), Ack: func() error { acked = true; return nil }}})
	if err == nil { t.Fatal("expected processor error") }
	if acked { t.Fatal("failed delivery was acknowledged") }
}

func TestNewAcceptsBatchProcessor(t *testing.T) {
	_, err := New(Config{BatchSize: 10, BatchWait: time.Second}, fakeAdapter{}, String{}, BatchHandlerFunc[string](func(context.Context, []Message[string]) error { return nil }))
	if err != nil { t.Fatal(err) }
}
