package conduit

import "context"

// Processor handles one decoded message.
type Processor[T any] interface { Process(context.Context, Message[T]) error }

// BatchProcessor handles a decoded batch. A successful return commits every record in the batch.
type BatchProcessor[T any] interface { ProcessBatch(context.Context, []Message[T]) error }

type HandlerFunc[T any] func(context.Context, Message[T]) error
func (f HandlerFunc[T]) Process(ctx context.Context, message Message[T]) error { return f(ctx, message) }

type BatchHandlerFunc[T any] func(context.Context, []Message[T]) error
func (f BatchHandlerFunc[T]) ProcessBatch(ctx context.Context, messages []Message[T]) error { return f(ctx, messages) }
