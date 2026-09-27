package conduit

import "time"

// Header is a transport message header.
type Header struct { Key string; Value []byte }

// Delivery is a raw message supplied by an Adapter. Ack is called only after a
// processor succeeds.
type Delivery struct {
	Value []byte
	Key []byte
	Topic string
	Partition int32
	Offset int64
	Timestamp time.Time
	Headers []Header
	Ack func() error
}

// Message is a decoded delivery passed to a processor.
type Message[T any] struct {
	Value T
	Key []byte
	Topic string
	Partition int32
	Offset int64
	Timestamp time.Time
	Headers []Header
}
