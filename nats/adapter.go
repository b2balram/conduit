// Package nats provides a NATS JetStream pull-consumer adapter for Conduit.
package nats

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/b2balram/conduit"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type TLSConfig struct {
	CAFile, CertFile, KeyFile, ServerName string
	InsecureSkipVerify                    bool
}
type Config struct {
	URLs                                            []string
	Stream, Consumer, FilterSubject, ConnectionName string
	Username, Password, Token, CredentialsFile      string
	AckWait                                         time.Duration
	MaxDeliver                                      int
	DoubleAck                                       bool
	AckTimeout                                      time.Duration
	TLS                                             TLSConfig
}

type Adapter struct{ config Config }

func New(config Config) (*Adapter, error) {
	if len(config.URLs) == 0 || config.Stream == "" || config.Consumer == "" {
		return nil, errors.New("conduit/nats: URLs, stream, and consumer are required")
	}
	authMethods := 0
	if config.Username != "" {
		authMethods++
	}
	if config.Token != "" {
		authMethods++
	}
	if config.CredentialsFile != "" {
		authMethods++
	}
	if authMethods > 1 {
		return nil, errors.New("conduit/nats: authentication methods are mutually exclusive")
	}
	if config.Password != "" && config.Username == "" {
		return nil, errors.New("conduit/nats: username is required with password")
	}
	if (config.TLS.CertFile == "") != (config.TLS.KeyFile == "") {
		return nil, errors.New("conduit/nats: TLS certificate and key must be provided together")
	}
	if config.AckWait < 0 || config.MaxDeliver < 0 || config.AckTimeout < 0 {
		return nil, errors.New("conduit/nats: acknowledgement settings must not be negative")
	}
	return &Adapter{config: config}, nil
}

func (a *Adapter) Consume(ctx context.Context, settings conduit.BatchSettings, processor conduit.RawProcessor) error {
	options, err := a.options()
	if err != nil {
		return err
	}
	connection, err := natsgo.Connect(joinURLs(a.config.URLs), options...)
	if err != nil {
		return fmt.Errorf("conduit/nats: connect: %w", err)
	}
	defer connection.Close()
	js, err := jetstream.New(connection)
	if err != nil {
		return fmt.Errorf("conduit/nats: create JetStream context: %w", err)
	}
	consumer, err := js.CreateOrUpdateConsumer(ctx, a.config.Stream, jetstream.ConsumerConfig{Name: a.config.Consumer, Durable: a.config.Consumer, FilterSubject: a.config.FilterSubject, AckPolicy: jetstream.AckExplicitPolicy, AckWait: a.config.AckWait, MaxDeliver: a.config.MaxDeliver})
	if err != nil {
		return fmt.Errorf("conduit/nats: create consumer: %w", err)
	}
	for ctx.Err() == nil {
		size, wait := settings.Size, settings.Wait
		if size == 0 {
			size, wait = 1, time.Second
		}
		batch, err := consumer.Fetch(size, jetstream.FetchMaxWait(wait))
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("conduit/nats: fetch: %w", err)
		}
		deliveries := make([]conduit.Delivery, 0, size)
		for message := range batch.Messages() {
			delivery, err := a.convert(message)
			if err != nil {
				return err
			}
			deliveries = append(deliveries, delivery)
		}
		if err := batch.Error(); err != nil {
			return fmt.Errorf("conduit/nats: fetch batch: %w", err)
		}
		if len(deliveries) > 0 {
			if err := processor.ProcessDeliveries(ctx, deliveries); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *Adapter) convert(message jetstream.Msg) (conduit.Delivery, error) {
	metadata, err := message.Metadata()
	if err != nil {
		return conduit.Delivery{}, fmt.Errorf("conduit/nats: message metadata: %w", err)
	}
	headers := make([]conduit.Header, 0, len(message.Headers()))
	for key, values := range message.Headers() {
		for _, value := range values {
			headers = append(headers, conduit.Header{Key: key, Value: []byte(value)})
		}
	}
	ack := func() error {
		if !a.config.DoubleAck {
			return message.Ack()
		}
		timeout := a.config.AckTimeout
		if timeout == 0 {
			timeout = 5 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return message.DoubleAck(ctx)
	}
	return conduit.Delivery{Value: append([]byte(nil), message.Data()...), Topic: message.Subject(), Offset: int64(metadata.Sequence.Stream), Timestamp: metadata.Timestamp, Headers: headers, Ack: ack}, nil
}

func (a *Adapter) options() ([]natsgo.Option, error) {
	var options []natsgo.Option
	if a.config.ConnectionName != "" {
		options = append(options, natsgo.Name(a.config.ConnectionName))
	}
	if a.config.Username != "" {
		options = append(options, natsgo.UserInfo(a.config.Username, a.config.Password))
	}
	if a.config.Token != "" {
		options = append(options, natsgo.Token(a.config.Token))
	}
	if a.config.CredentialsFile != "" {
		options = append(options, natsgo.UserCredentials(a.config.CredentialsFile))
	}
	if a.config.TLS != (TLSConfig{}) {
		tlsConfig, err := natsTLS(a.config.TLS)
		if err != nil {
			return nil, err
		}
		options = append(options, natsgo.Secure(tlsConfig))
	}
	return options, nil
}

func natsTLS(settings TLSConfig) (*tls.Config, error) {
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: settings.ServerName, InsecureSkipVerify: settings.InsecureSkipVerify} //nolint:gosec
	if settings.CAFile != "" {
		pem, err := os.ReadFile(settings.CAFile)
		if err != nil {
			return nil, fmt.Errorf("conduit/nats: read TLS CA: %w", err)
		}
		roots, _ := x509.SystemCertPool()
		if roots == nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("conduit/nats: TLS CA file contains no certificates")
		}
		config.RootCAs = roots
	}
	if settings.CertFile != "" {
		certificate, err := tls.LoadX509KeyPair(settings.CertFile, settings.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("conduit/nats: load TLS client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}

func joinURLs(urls []string) string {
	result := ""
	for i, url := range urls {
		if i > 0 {
			result += ","
		}
		result += url
	}
	return result
}
