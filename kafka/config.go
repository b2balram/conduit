package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/IBM/sarama"
)

// InitialOffset controls where a new Kafka consumer group starts.
type InitialOffset int

const (
	OffsetOldest InitialOffset = iota
	OffsetNewest
)

// SASLMechanism identifies a supported Kafka SASL mechanism.
type SASLMechanism string

const (
	SASLPlain       SASLMechanism = "PLAIN"
	SASLSCRAMSHA256 SASLMechanism = "SCRAM-SHA-256"
	SASLSCRAMSHA512 SASLMechanism = "SCRAM-SHA-512"
)

// BalanceStrategy identifies a consumer-group partition assignment strategy.
type BalanceStrategy string

const (
	BalanceRange             BalanceStrategy = "range"
	BalanceRoundRobin        BalanceStrategy = "round_robin"
	BalanceSticky            BalanceStrategy = "sticky"
	BalanceCooperativeSticky BalanceStrategy = "cooperative_sticky"
)

// TLSConfig configures encrypted broker connections. CAFile is optional and,
// when omitted, the system trust store is used. CertFile and KeyFile must be
// provided together for mutual TLS.
type TLSConfig struct {
	Enabled            bool
	CAFile             string
	CertFile           string
	KeyFile            string
	ServerName         string
	InsecureSkipVerify bool
}

// SASLConfig configures broker authentication.
type SASLConfig struct {
	Enabled   bool
	Mechanism SASLMechanism
	Username  string
	Password  string
}

// TimeoutConfig configures network and consumer-group timeouts. Zero values
// retain Sarama defaults.
type TimeoutConfig struct {
	Dial      time.Duration
	Read      time.Duration
	Write     time.Duration
	Session   time.Duration
	Heartbeat time.Duration
	Rebalance time.Duration
}

// RebalanceConfig configures group assignment and rebalance retries. Strategies
// are offered in order. Cooperative sticky requires Kafka 2.4 or newer.
type RebalanceConfig struct {
	Strategies   []BalanceStrategy
	RetryMax     int
	RetryBackoff time.Duration
	InstanceID   string
}

// Config contains Kafka-specific connection and subscription settings.
type Config struct {
	Brokers       []string
	GroupID       string
	Topics        []string
	ClientID      string
	Version       string
	InitialOffset InitialOffset
	TLS           TLSConfig
	SASL          SASLConfig
	Timeouts      TimeoutConfig
	Rebalance     RebalanceConfig

	// Configure is an advanced escape hatch applied after typed options and
	// before validation. Avoid replacing Conduit-managed offset behavior.
	Configure func(*sarama.Config) error
}

func (c Config) validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("conduit/kafka: at least one broker is required")
	}
	if c.GroupID == "" {
		return errors.New("conduit/kafka: group ID is required")
	}
	if len(c.Topics) == 0 {
		return errors.New("conduit/kafka: at least one topic is required")
	}
	if c.TLS.CertFile == "" != (c.TLS.KeyFile == "") {
		return errors.New("conduit/kafka: TLS certificate and key must be provided together")
	}
	if c.SASL.Enabled {
		if c.SASL.Username == "" || c.SASL.Password == "" {
			return errors.New("conduit/kafka: SASL username and password are required")
		}
		switch c.SASL.Mechanism {
		case "", SASLPlain, SASLSCRAMSHA256, SASLSCRAMSHA512:
		default:
			return fmt.Errorf("conduit/kafka: unsupported SASL mechanism %q", c.SASL.Mechanism)
		}
	}
	if c.Rebalance.RetryMax < 0 || c.Rebalance.RetryBackoff < 0 {
		return errors.New("conduit/kafka: rebalance retry settings must not be negative")
	}
	if c.Timeouts.Dial < 0 || c.Timeouts.Read < 0 || c.Timeouts.Write < 0 || c.Timeouts.Session < 0 || c.Timeouts.Heartbeat < 0 || c.Timeouts.Rebalance < 0 {
		return errors.New("conduit/kafka: timeouts must not be negative")
	}
	return nil
}

func (c Config) saramaConfig() (*sarama.Config, error) {
	configuration := sarama.NewConfig()
	configuration.Consumer.Offsets.Initial = sarama.OffsetOldest
	if c.InitialOffset == OffsetNewest {
		configuration.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
	if c.ClientID != "" {
		configuration.ClientID = c.ClientID
	}
	if c.Version != "" {
		version, err := sarama.ParseKafkaVersion(c.Version)
		if err != nil {
			return nil, fmt.Errorf("conduit/kafka: invalid Kafka version: %w", err)
		}
		configuration.Version = version
	}
	if err := applyTLS(configuration, c.TLS); err != nil {
		return nil, err
	}
	applySASL(configuration, c.SASL)
	applyTimeouts(configuration, c.Timeouts)
	if err := applyRebalance(configuration, c.Rebalance); err != nil {
		return nil, err
	}
	if c.Configure != nil {
		if err := c.Configure(configuration); err != nil {
			return nil, fmt.Errorf("conduit/kafka: configure Sarama: %w", err)
		}
	}
	if err := configuration.Validate(); err != nil {
		return nil, fmt.Errorf("conduit/kafka: invalid configuration: %w", err)
	}
	return configuration, nil
}

func applyTLS(configuration *sarama.Config, settings TLSConfig) error {
	if !settings.Enabled {
		return nil
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: settings.ServerName, InsecureSkipVerify: settings.InsecureSkipVerify} //nolint:gosec // explicitly requested escape hatch
	if settings.CAFile != "" {
		pem, err := os.ReadFile(settings.CAFile)
		if err != nil {
			return fmt.Errorf("conduit/kafka: read TLS CA: %w", err)
		}
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return errors.New("conduit/kafka: TLS CA file contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	if settings.CertFile != "" {
		certificate, err := tls.LoadX509KeyPair(settings.CertFile, settings.KeyFile)
		if err != nil {
			return fmt.Errorf("conduit/kafka: load TLS client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	configuration.Net.TLS.Enable = true
	configuration.Net.TLS.Config = tlsConfig
	return nil
}

func applySASL(configuration *sarama.Config, settings SASLConfig) {
	if !settings.Enabled {
		return
	}
	configuration.Net.SASL.Enable = true
	configuration.Net.SASL.User = settings.Username
	configuration.Net.SASL.Password = settings.Password
	mechanism := settings.Mechanism
	if mechanism == "" {
		mechanism = SASLPlain
	}
	configuration.Net.SASL.Mechanism = sarama.SASLMechanism(mechanism)
	if mechanism == SASLSCRAMSHA256 {
		configuration.Net.SASL.SCRAMClientGeneratorFunc = newSCRAMSHA256Client
	}
	if mechanism == SASLSCRAMSHA512 {
		configuration.Net.SASL.SCRAMClientGeneratorFunc = newSCRAMSHA512Client
	}
}

func applyTimeouts(configuration *sarama.Config, settings TimeoutConfig) {
	if settings.Dial > 0 {
		configuration.Net.DialTimeout = settings.Dial
	}
	if settings.Read > 0 {
		configuration.Net.ReadTimeout = settings.Read
	}
	if settings.Write > 0 {
		configuration.Net.WriteTimeout = settings.Write
	}
	if settings.Session > 0 {
		configuration.Consumer.Group.Session.Timeout = settings.Session
	}
	if settings.Heartbeat > 0 {
		configuration.Consumer.Group.Heartbeat.Interval = settings.Heartbeat
	}
	if settings.Rebalance > 0 {
		configuration.Consumer.Group.Rebalance.Timeout = settings.Rebalance
	}
}

func applyRebalance(configuration *sarama.Config, settings RebalanceConfig) error {
	if len(settings.Strategies) > 0 {
		strategies := make([]sarama.BalanceStrategy, 0, len(settings.Strategies))
		for _, strategy := range settings.Strategies {
			switch strategy {
			case BalanceRange:
				strategies = append(strategies, sarama.NewBalanceStrategyRange())
			case BalanceRoundRobin:
				strategies = append(strategies, sarama.NewBalanceStrategyRoundRobin())
			case BalanceSticky:
				strategies = append(strategies, sarama.NewBalanceStrategySticky())
			case BalanceCooperativeSticky:
				strategies = append(strategies, sarama.NewBalanceStrategyCooperativeSticky())
			default:
				return fmt.Errorf("conduit/kafka: unsupported balance strategy %q", strategy)
			}
		}
		configuration.Consumer.Group.Rebalance.GroupStrategies = strategies
	}
	if settings.RetryMax > 0 {
		configuration.Consumer.Group.Rebalance.Retry.Max = settings.RetryMax
	}
	if settings.RetryBackoff > 0 {
		configuration.Consumer.Group.Rebalance.Retry.Backoff = settings.RetryBackoff
	}
	configuration.Consumer.Group.InstanceId = settings.InstanceID
	return nil
}
