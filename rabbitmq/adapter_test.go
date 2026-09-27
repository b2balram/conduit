package rabbitmq

import "testing"

func TestNewValidatesConfig(t *testing.T) {
	tests := []Config{
		{},
		{URL: "amqp://localhost", Queue: ""},
		{URL: "amqp://localhost", Queue: "orders", Prefetch: -1},
		{URL: "amqps://localhost", Queue: "orders", TLS: TLSConfig{CertFile: "client.pem"}},
	}
	for _, config := range tests {
		if _, err := New(config); err == nil {
			t.Fatalf("New(%+v) succeeded, want validation error", config)
		}
	}
}

func TestNewAcceptsMinimalConfig(t *testing.T) {
	if _, err := New(Config{URL: "amqp://localhost", Queue: "orders"}); err != nil {
		t.Fatal(err)
	}
}
