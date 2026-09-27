package nats

import "testing"

func TestNewValidatesConfig(t *testing.T) {
	tests := []Config{
		{},
		{URLs: []string{"nats://localhost:4222"}, Stream: "ORDERS", Consumer: ""},
		{URLs: []string{"nats://localhost:4222"}, Stream: "ORDERS", Consumer: "worker", Username: "u", Token: "t"},
		{URLs: []string{"nats://localhost:4222"}, Stream: "ORDERS", Consumer: "worker", TLS: TLSConfig{CertFile: "client.pem"}},
	}
	for _, config := range tests {
		if _, err := New(config); err == nil {
			t.Fatalf("New(%+v) succeeded, want validation error", config)
		}
	}
}

func TestNewAcceptsMinimalConfig(t *testing.T) {
	if _, err := New(Config{URLs: []string{"nats://localhost:4222"}, Stream: "ORDERS", Consumer: "worker"}); err != nil {
		t.Fatal(err)
	}
}
