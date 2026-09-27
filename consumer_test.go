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
	if err == nil {
		t.Fatal("expected batch configuration error")
	}
}

func TestProcessAcknowledgesOnlyAfterSuccess(t *testing.T) {
	acked := false
	consumer, err := New(Config{}, fakeAdapter{}, String{}, HandlerFunc[string](func(context.Context, Message[string]) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	err = consumer.ProcessDeliveries(context.Background(), []Delivery{{Value: []byte("hello"), Ack: func() error { acked = true; return nil }}})
	if err != nil {
		t.Fatal(err)
	}
	if !acked {
		t.Fatal("delivery was not acknowledged")
	}
}

func TestProcessDoesNotAcknowledgeOnFailure(t *testing.T) {
	cause := errors.New("fail")
	acked := false
	consumer, err := New(Config{}, fakeAdapter{}, String{}, HandlerFunc[string](func(context.Context, Message[string]) error { return cause }))
	if err != nil {
		t.Fatal(err)
	}
	err = consumer.ProcessDeliveries(context.Background(), []Delivery{{Value: []byte("hello"), Ack: func() error { acked = true; return nil }}})
	if err == nil {
		t.Fatal("expected processor error")
	}
	if acked {
		t.Fatal("failed delivery was acknowledged")
	}
	if !errors.Is(err, cause) {
		t.Fatalf("error does not wrap processor cause: %v", err)
	}
	var conduitErr *Error
	if !errors.As(err, &conduitErr) || conduitErr.Stage != StageProcess {
		t.Fatalf("error stage = %v, want %v", conduitErr, StageProcess)
	}
}

func TestNewAcceptsBatchProcessor(t *testing.T) {
	_, err := New(Config{BatchSize: 10, BatchWait: time.Second}, fakeAdapter{}, String{}, BatchHandlerFunc[string](func(context.Context, []Message[string]) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
}

func TestProcessRecordsMetricsAndStructuredErrorLog(t *testing.T) {
	var metrics []Metric
	var loggedError error
	consumer, err := New(Config{
		Metrics: MetricsFunc(func(_ context.Context, metric Metric) { metrics = append(metrics, metric) }),
		Logger: LoggerFunc(func(_ context.Context, level LogLevel, _ string, fields ...Field) {
			if level != LogError {
				return
			}
			for _, field := range fields {
				if field.Key == "error" {
					loggedError, _ = field.Value.(error)
				}
			}
		}),
	}, fakeAdapter{}, String{}, HandlerFunc[string](func(context.Context, Message[string]) error {
		return errors.New("processor failed")
	}))
	if err != nil {
		t.Fatal(err)
	}

	err = consumer.ProcessDeliveries(context.Background(), []Delivery{{Value: []byte("hello"), Topic: "orders"}})
	if err == nil {
		t.Fatal("expected processor error")
	}
	if loggedError == nil {
		t.Fatal("expected structured error log")
	}
	want := map[MetricName]bool{
		MetricMessagesReceived: false,
		MetricBatchSize:        false,
		MetricDeserializeTime:  false,
		MetricProcessTime:      false,
		MetricMessagesFailed:   false,
	}
	for _, metric := range metrics {
		if _, ok := want[metric.Name]; ok {
			want[metric.Name] = true
		}
	}
	for name, seen := range want {
		if !seen {
			t.Errorf("metric %q was not recorded", name)
		}
	}
}

func TestProcessClonesHeaders(t *testing.T) {
	headerValue := []byte("original")
	consumer, err := New(Config{}, fakeAdapter{}, String{}, HandlerFunc[string](func(_ context.Context, message Message[string]) error {
		message.Headers[0].Value[0] = 'X'
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.ProcessDeliveries(context.Background(), []Delivery{{Value: []byte("hello"), Headers: []Header{{Key: "trace", Value: headerValue}}}}); err != nil {
		t.Fatal(err)
	}
	if string(headerValue) != "original" {
		t.Fatal("message headers alias delivery headers")
	}
}

func TestProcessRetriesThenAcknowledges(t *testing.T) {
	attempts := 0
	acked := false
	consumer, err := New(Config{Retry: RetryPolicy{MaxAttempts: 3, Backoff: FixedBackoff(0)}}, fakeAdapter{}, String{}, HandlerFunc[string](func(context.Context, Message[string]) error {
		attempts++
		if attempts < 3 {
			return errors.New("temporary")
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	err = consumer.ProcessDeliveries(context.Background(), []Delivery{{Value: []byte("hello"), Ack: func() error { acked = true; return nil }}})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	if !acked {
		t.Fatal("delivery was not acknowledged")
	}
}

func TestProcessDeadLettersAfterRetries(t *testing.T) {
	attempts := 0
	acked := false
	var failed FailedDelivery
	consumer, err := New(Config{
		Retry: RetryPolicy{MaxAttempts: 2},
		DeadLetter: DeadLetterFunc(func(_ context.Context, delivery FailedDelivery) error {
			failed = delivery
			return nil
		}),
	}, fakeAdapter{}, String{}, HandlerFunc[string](func(context.Context, Message[string]) error {
		attempts++
		return errors.New("permanent")
	}))
	if err != nil {
		t.Fatal(err)
	}
	err = consumer.ProcessDeliveries(context.Background(), []Delivery{standsForDelivery(&acked)})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || failed.Attempts != 2 {
		t.Fatalf("processor attempts = %d, dead-letter attempts = %d", attempts, failed.Attempts)
	}
	if failed.Delivery.Ack != nil {
		t.Fatal("dead-letter handler received acknowledgement callback")
	}
	if !acked {
		t.Fatal("dead-lettered delivery was not acknowledged")
	}
}

func TestDeadLetterFailureLeavesDeliveryUnacknowledged(t *testing.T) {
	acked := false
	deadLetterCause := errors.New("dead-letter unavailable")
	consumer, err := New(Config{DeadLetter: DeadLetterFunc(func(context.Context, FailedDelivery) error {
		return deadLetterCause
	})}, fakeAdapter{}, String{}, HandlerFunc[string](func(context.Context, Message[string]) error {
		return errors.New("permanent")
	}))
	if err != nil {
		t.Fatal(err)
	}
	err = consumer.ProcessDeliveries(context.Background(), []Delivery{standsForDelivery(&acked)})
	if !errors.Is(err, deadLetterCause) {
		t.Fatalf("error = %v, want dead-letter cause", err)
	}
	var conduitErr *Error
	if !errors.As(err, &conduitErr) || conduitErr.Stage != StageDeadLetter {
		t.Fatalf("error stage = %v, want %v", conduitErr, StageDeadLetter)
	}
	if acked {
		t.Fatal("delivery was acknowledged after dead-letter failure")
	}
}

func TestRetryableCanStopRetries(t *testing.T) {
	attempts := 0
	consumer, err := New(Config{Retry: RetryPolicy{
		MaxAttempts: 5,
		Retryable:   func(error) bool { return false },
	}}, fakeAdapter{}, String{}, HandlerFunc[string](func(context.Context, Message[string]) error {
		attempts++
		return errors.New("permanent")
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.ProcessDeliveries(context.Background(), []Delivery{{Value: []byte("hello")}}); err == nil {
		t.Fatal("expected processor error")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

func TestCancellationStopsRetriesWithoutDeadLettering(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	deadLettered := false
	consumer, err := New(Config{
		Retry: RetryPolicy{MaxAttempts: 5, Backoff: FixedBackoff(time.Second)},
		DeadLetter: DeadLetterFunc(func(context.Context, FailedDelivery) error {
			deadLettered = true
			return nil
		}),
	}, fakeAdapter{}, String{}, HandlerFunc[string](func(context.Context, Message[string]) error {
		attempts++
		cancel()
		return context.Canceled
	}))
	if err != nil {
		t.Fatal(err)
	}
	err = consumer.ProcessDeliveries(ctx, []Delivery{{Value: []byte("hello")}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context cancellation", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	if deadLettered {
		t.Fatal("canceled delivery was dead-lettered")
	}
}

func standsForDelivery(acked *bool) Delivery {
	return Delivery{
		Value: []byte("hello"),
		Topic: "orders",
		Ack: func() error {
			*acked = true
			return nil
		},
	}
}
