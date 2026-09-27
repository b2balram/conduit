package conduit

import "context"

// FailedDelivery describes a delivery sent to a dead-letter destination.
type FailedDelivery struct {
	Delivery Delivery
	Cause    error
	Attempts int
}

// DeadLetterHandler publishes or stores a delivery that cannot be processed.
// Returning nil tells Conduit it is safe to acknowledge the original delivery.
// Returning an error leaves the original delivery unacknowledged.
type DeadLetterHandler interface {
	Handle(context.Context, FailedDelivery) error
}

// DeadLetterFunc adapts a function into a DeadLetterHandler.
type DeadLetterFunc func(context.Context, FailedDelivery) error

func (f DeadLetterFunc) Handle(ctx context.Context, failed FailedDelivery) error {
	return f(ctx, failed)
}
