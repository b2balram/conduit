// Package rabbitmq provides the RabbitMQ AMQP 0.9.1 adapter for Conduit.
package rabbitmq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/b2balram/conduit"
	amqp "github.com/rabbitmq/amqp091-go"
)

type TLSConfig struct {
	CAFile, CertFile, KeyFile, ServerName string
	InsecureSkipVerify                    bool
}

type Config struct {
	URL, Queue, ConsumerTag string
	Prefetch                int
	Heartbeat               time.Duration
	TLS                     TLSConfig
}

type Adapter struct{ config Config }

func New(config Config) (*Adapter, error) {
	if config.URL == "" || config.Queue == "" {
		return nil, errors.New("conduit/rabbitmq: URL and queue are required")
	}
	if config.Prefetch < 0 || config.Heartbeat < 0 {
		return nil, errors.New("conduit/rabbitmq: prefetch and heartbeat must not be negative")
	}
	if (config.TLS.CertFile == "") != (config.TLS.KeyFile == "") {
		return nil, errors.New("conduit/rabbitmq: TLS certificate and key must be provided together")
	}
	return &Adapter{config: config}, nil
}

func (a *Adapter) Consume(ctx context.Context, settings conduit.BatchSettings, processor conduit.RawProcessor) (result error) {
	connectionConfig := amqp.Config{Heartbeat: a.config.Heartbeat}
	if a.config.TLS != (TLSConfig{}) {
		tlsConfig, err := buildTLS(a.config.TLS)
		if err != nil {
			return err
		}
		connectionConfig.TLSClientConfig = tlsConfig
	}
	connection, err := amqp.DialConfig(a.config.URL, connectionConfig)
	if err != nil {
		return fmt.Errorf("conduit/rabbitmq: connect: %w", err)
	}
	defer func() { result = errors.Join(result, ignoreClosed(connection.Close())) }()
	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("conduit/rabbitmq: open channel: %w", err)
	}
	defer func() { result = errors.Join(result, ignoreClosed(channel.Close())) }()
	prefetch := a.config.Prefetch
	if prefetch == 0 && settings.Size > 0 {
		prefetch = settings.Size
	}
	if err := channel.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("conduit/rabbitmq: configure QoS: %w", err)
	}
	deliveries, err := channel.ConsumeWithContext(ctx, a.config.Queue, a.config.ConsumerTag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("conduit/rabbitmq: consume: %w", err)
	}
	closed := channel.NotifyClose(make(chan *amqp.Error, 1))
	batch := make([]conduit.Delivery, 0, max(settings.Size, 1))
	var timer *time.Timer
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := processor.ProcessDeliveries(ctx, batch); err != nil {
			return err
		}
		batch = batch[:0]
		if timer != nil {
			timer.Stop()
			timer = nil
		}
		return nil
	}
	for {
		var timeout <-chan time.Time
		if timer != nil {
			timeout = timer.C
		}
		select {
		case <-ctx.Done():
			return nil
		case closeErr := <-closed:
			if closeErr == nil {
				return nil
			}
			return fmt.Errorf("conduit/rabbitmq: channel closed: %w", closeErr)
		case <-timeout:
			if err := flush(); err != nil {
				return err
			}
		case delivery, ok := <-deliveries:
			if !ok {
				return flush()
			}
			converted := convert(delivery)
			if settings.Size == 0 {
				if err := processor.ProcessDeliveries(ctx, []conduit.Delivery{converted}); err != nil {
					return err
				}
				continue
			}
			batch = append(batch, converted)
			if timer == nil {
				timer = time.NewTimer(settings.Wait)
			}
			if len(batch) >= settings.Size {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

func convert(delivery amqp.Delivery) conduit.Delivery {
	headers := make([]conduit.Header, 0, len(delivery.Headers))
	for key, value := range delivery.Headers {
		headers = append(headers, conduit.Header{Key: key, Value: []byte(fmt.Sprint(value))})
	}
	return conduit.Delivery{Value: append([]byte(nil), delivery.Body...), Key: []byte(delivery.MessageId), Topic: delivery.RoutingKey, Offset: int64(delivery.DeliveryTag), Timestamp: delivery.Timestamp, Headers: headers, Ack: func() error { return delivery.Ack(false) }}
}

func buildTLS(settings TLSConfig) (*tls.Config, error) {
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: settings.ServerName, InsecureSkipVerify: settings.InsecureSkipVerify} //nolint:gosec
	if settings.CAFile != "" {
		pem, err := os.ReadFile(settings.CAFile)
		if err != nil {
			return nil, fmt.Errorf("conduit/rabbitmq: read TLS CA: %w", err)
		}
		roots, _ := x509.SystemCertPool()
		if roots == nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("conduit/rabbitmq: TLS CA file contains no certificates")
		}
		config.RootCAs = roots
	}
	if settings.CertFile != "" {
		certificate, err := tls.LoadX509KeyPair(settings.CertFile, settings.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("conduit/rabbitmq: load TLS client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}

func ignoreClosed(err error) error {
	if errors.Is(err, amqp.ErrClosed) {
		return nil
	}
	return err
}
