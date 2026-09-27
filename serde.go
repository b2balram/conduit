package conduit

import "encoding/json"

// Serde converts Kafka payload bytes to a typed value and back.
type Serde[T any] interface {
	Deserialize([]byte) (T, error)
	Serialize(T) ([]byte, error)
}

// JSON serializes values with encoding/json.
type JSON[T any] struct{}

func (JSON[T]) Deserialize(data []byte) (T, error) {
	var value T
	err := json.Unmarshal(data, &value)
	return value, err
}
func (JSON[T]) Serialize(value T) ([]byte, error) { return json.Marshal(value) }

// String serializes UTF-8 strings.
type String struct{}
func (String) Deserialize(data []byte) (string, error) { return string(data), nil }
func (String) Serialize(value string) ([]byte, error) { return []byte(value), nil }

// Bytes passes payload bytes through. Deserialize copies the supplied data.
type Bytes struct{}
func (Bytes) Deserialize(data []byte) ([]byte, error) { return append([]byte(nil), data...), nil }
func (Bytes) Serialize(value []byte) ([]byte, error) { return append([]byte(nil), value...), nil }
