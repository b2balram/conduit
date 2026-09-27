// Package adaptertest provides a reusable behavioral contract for Conduit
// transport adapters. Adapter authors implement Harness and call Run from an
// integration test backed by their real transport.
package adaptertest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/b2balram/conduit"
)

const testTimeout = 10 * time.Second

// Message is a transport-neutral record used by the conformance suite. Harness
// implementations must publish messages from one Run subtest to the same
// ordered stream or partition, in the supplied order.
type Message struct {
	ID      string
	Value   []byte
	Key     []byte
	Headers []conduit.Header
}

// Harness connects the conformance suite to a transport-specific test fixture.
// Each Factory call must return a fresh, isolated harness.
type Harness interface {
	Adapter() conduit.Adapter
	WaitReady(context.Context) error
	Publish(context.Context, ...Message) error
	Acknowledged(id string) bool
	InjectAcknowledgeError(id string, err error) error
	InjectError(error) error
	Close() error
}

// Factory creates an isolated harness. Implementations should register any
// transport cleanup with t.Cleanup in addition to implementing Close.
type Factory func(testing.TB) Harness

// Run executes the Conduit adapter contract. It is intended to be called from
// a transport package's integration tests.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("acknowledges after success", func(t *testing.T) {
		h := factory(t)
		processed := make(chan struct{}, 1)
		run := start(t, h, conduit.BatchSettings{}, rawProcessorFunc(func(_ context.Context, deliveries []conduit.Delivery) error {
			processed <- struct{}{}
			return acknowledgeAll(deliveries)
		}))
		publish(t, h, Message{ID: "ack", Value: []byte("one")})
		awaitSignal(t, processed, "processor call")
		await(t, func() bool { return h.Acknowledged("ack") }, "acknowledgement")
		stop(t, h, run)
	})

	t.Run("preserves ordered stream", func(t *testing.T) {
		h := factory(t)
		var mu sync.Mutex
		var values []string
		done := make(chan struct{}, 1)
		run := start(t, h, conduit.BatchSettings{}, rawProcessorFunc(func(_ context.Context, deliveries []conduit.Delivery) error {
			mu.Lock()
			values = append(values, string(deliveries[0].Value))
			complete := len(values) == 3
			mu.Unlock()
			if complete {
				done <- struct{}{}
			}
			return deliveries[0].Ack()
		}))
		publish(t, h,
			Message{ID: "order-1", Value: []byte("one")},
			Message{ID: "order-2", Value: []byte("two")},
			Message{ID: "order-3", Value: []byte("three")},
		)
		awaitSignal(t, done, "ordered messages")
		mu.Lock()
		got := fmt.Sprint(values)
		mu.Unlock()
		if got != "[one two three]" {
			t.Fatalf("processing order = %s, want [one two three]", got)
		}
		stop(t, h, run)
	})

	t.Run("flushes at batch size", func(t *testing.T) {
		h := factory(t)
		sizes := make(chan int, 1)
		run := start(t, h, conduit.BatchSettings{Size: 3, Wait: time.Second}, rawProcessorFunc(func(_ context.Context, deliveries []conduit.Delivery) error {
			sizes <- len(deliveries)
			return acknowledgeAll(deliveries)
		}))
		publish(t, h,
			Message{ID: "size-1", Value: []byte("one")},
			Message{ID: "size-2", Value: []byte("two")},
			Message{ID: "size-3", Value: []byte("three")},
		)
		if size := awaitValue(t, sizes, "size batch"); size != 3 {
			t.Fatalf("batch size = %d, want 3", size)
		}
		stop(t, h, run)
	})

	t.Run("flushes at batch wait", func(t *testing.T) {
		h := factory(t)
		sizes := make(chan int, 1)
		run := start(t, h, conduit.BatchSettings{Size: 10, Wait: 50 * time.Millisecond}, rawProcessorFunc(func(_ context.Context, deliveries []conduit.Delivery) error {
			sizes <- len(deliveries)
			return acknowledgeAll(deliveries)
		}))
		publish(t, h, Message{ID: "wait", Value: []byte("one")})
		if size := awaitValue(t, sizes, "wait batch"); size != 1 {
			t.Fatalf("batch size = %d, want 1", size)
		}
		stop(t, h, run)
	})

	t.Run("does not acknowledge processor error", func(t *testing.T) {
		h := factory(t)
		processorErr := errors.New("processor failure")
		run := start(t, h, conduit.BatchSettings{}, rawProcessorFunc(func(context.Context, []conduit.Delivery) error {
			return processorErr
		}))
		publish(t, h, Message{ID: "failed", Value: []byte("one")})
		err := awaitValue(t, run.errors, "processor error")
		if !errors.Is(err, processorErr) {
			t.Fatalf("run error = %v, want processor failure", err)
		}
		if h.Acknowledged("failed") {
			t.Fatal("failed message was acknowledged")
		}
		closeHarness(t, h)
	})

	t.Run("returns acknowledgement error", func(t *testing.T) {
		h := factory(t)
		ackErr := errors.New("acknowledgement failure")
		if err := h.InjectAcknowledgeError("ack-failed", ackErr); err != nil {
			t.Fatalf("inject acknowledgement error: %v", err)
		}
		run := start(t, h, conduit.BatchSettings{}, rawProcessorFunc(func(_ context.Context, deliveries []conduit.Delivery) error {
			return acknowledgeAll(deliveries)
		}))
		publish(t, h, Message{ID: "ack-failed", Value: []byte("one")})
		err := awaitValue(t, run.errors, "acknowledgement error")
		if !errors.Is(err, ackErr) {
			t.Fatalf("run error = %v, want acknowledgement failure", err)
		}
		if h.Acknowledged("ack-failed") {
			t.Fatal("message was acknowledged after acknowledgement failure")
		}
		closeHarness(t, h)
	})

	t.Run("stops on cancellation", func(t *testing.T) {
		h := factory(t)
		run := start(t, h, conduit.BatchSettings{}, rawProcessorFunc(func(context.Context, []conduit.Delivery) error { return nil }))
		run.cancel()
		if err := awaitValue(t, run.errors, "cancellation"); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error = %v", err)
		}
		closeHarness(t, h)
	})

	t.Run("returns transport error", func(t *testing.T) {
		h := factory(t)
		run := start(t, h, conduit.BatchSettings{}, rawProcessorFunc(func(context.Context, []conduit.Delivery) error { return nil }))
		transportErr := errors.New("transport failure")
		if err := h.InjectError(transportErr); err != nil {
			t.Fatalf("inject transport error: %v", err)
		}
		err := awaitValue(t, run.errors, "transport error")
		if !errors.Is(err, transportErr) {
			t.Fatalf("run error = %v, want transport failure", err)
		}
		closeHarness(t, h)
	})
}

type rawProcessorFunc func(context.Context, []conduit.Delivery) error

func (f rawProcessorFunc) ProcessDeliveries(ctx context.Context, deliveries []conduit.Delivery) error {
	return f(ctx, deliveries)
}

type running struct {
	cancel context.CancelFunc
	errors chan error
}

func start(t *testing.T, h Harness, settings conduit.BatchSettings, processor conduit.RawProcessor) running {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errors := make(chan error, 1)
	go func() { errors <- h.Adapter().Consume(ctx, settings, processor) }()
	readyCtx, readyCancel := context.WithTimeout(ctx, testTimeout)
	defer readyCancel()
	if err := h.WaitReady(readyCtx); err != nil {
		cancel()
		closeHarness(t, h)
		t.Fatalf("wait for adapter readiness: %v", err)
	}
	return running{cancel: cancel, errors: errors}
}

func stop(t *testing.T, h Harness, run running) {
	t.Helper()
	run.cancel()
	if err := awaitValue(t, run.errors, "adapter stop"); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("stop adapter: %v", err)
	}
	closeHarness(t, h)
}

func closeHarness(t *testing.T, h Harness) {
	t.Helper()
	if err := h.Close(); err != nil {
		t.Errorf("close harness: %v", err)
	}
}

func publish(t *testing.T, h Harness, messages ...Message) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	if err := h.Publish(ctx, messages...); err != nil {
		t.Fatalf("publish test messages: %v", err)
	}
}

func acknowledgeAll(deliveries []conduit.Delivery) error {
	for _, delivery := range deliveries {
		if delivery.Ack != nil {
			if err := delivery.Ack(); err != nil {
				return err
			}
		}
	}
	return nil
}

func await(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func awaitSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	_ = awaitValue(t, signal, description)
}

func awaitValue[T any](t *testing.T, values <-chan T, description string) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(testTimeout):
		var zero T
		t.Fatalf("timed out waiting for %s", description)
		return zero
	}
}
