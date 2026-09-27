package kafka

import (
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestConfigAppliesProductionSettings(t *testing.T) {
	config := Config{
		Brokers:  []string{"localhost:9092"},
		GroupID:  "orders",
		Topics:   []string{"orders.created"},
		ClientID: "orders-service",
		Version:  "2.4.0",
		SASL: SASLConfig{
			Enabled:   true,
			Mechanism: SASLSCRAMSHA256,
			Username:  "user",
			Password:  "secret",
		},
		Timeouts: TimeoutConfig{
			Dial:      time.Second,
			Read:      2 * time.Second,
			Write:     3 * time.Second,
			Session:   10 * time.Second,
			Heartbeat: 3 * time.Second,
			Rebalance: 30 * time.Second,
		},
		Rebalance: RebalanceConfig{
			Strategies:   []BalanceStrategy{BalanceCooperativeSticky},
			RetryMax:     7,
			RetryBackoff: time.Second,
			InstanceID:   "orders-1",
		},
	}

	got, err := config.saramaConfig()
	if err != nil {
		t.Fatal(err)
	}
	if got.ClientID != config.ClientID || got.Net.DialTimeout != config.Timeouts.Dial {
		t.Fatalf("client or timeout settings were not applied")
	}
	if got.Net.SASL.Mechanism != sarama.SASLTypeSCRAMSHA256 || got.Net.SASL.SCRAMClientGeneratorFunc == nil {
		t.Fatalf("SCRAM-SHA-256 was not configured")
	}
	if got.Consumer.Group.Rebalance.GroupStrategies[0].Name() != "cooperative-sticky" {
		t.Fatalf("strategy = %q, want cooperative-sticky", got.Consumer.Group.Rebalance.GroupStrategies[0].Name())
	}
	if got.Consumer.Group.InstanceId != config.Rebalance.InstanceID {
		t.Fatalf("instance ID = %q", got.Consumer.Group.InstanceId)
	}
}

func TestConfigRejectsInvalidSecuritySettings(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Config)
	}{
		{"TLS certificate without key", func(c *Config) { c.TLS.CertFile = "client.crt" }},
		{"SASL credentials missing", func(c *Config) { c.SASL.Enabled = true }},
		{"SASL mechanism unsupported", func(c *Config) {
			c.SASL = SASLConfig{Enabled: true, Mechanism: "UNKNOWN", Username: "user", Password: "password"}
		}},
		{"negative timeout", func(c *Config) { c.Timeouts.Dial = -time.Second }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := Config{Brokers: []string{"localhost:9092"}, GroupID: "orders", Topics: []string{"orders"}}
			test.change(&config)
			if _, err := New(config); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestCooperativeStrategyRequiresKafka24(t *testing.T) {
	config := Config{
		Brokers:   []string{"localhost:9092"},
		GroupID:   "orders",
		Topics:    []string{"orders"},
		Version:   "2.3.0",
		Rebalance: RebalanceConfig{Strategies: []BalanceStrategy{BalanceCooperativeSticky}},
	}
	if _, err := config.saramaConfig(); err == nil {
		t.Fatal("expected cooperative strategy version error")
	}
}
