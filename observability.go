package conduit

import (
	"context"
	"time"
)

// MetricName identifies a stable Conduit metric. Collectors can translate
// these names into the naming conventions of Prometheus, OpenTelemetry, or
// another metrics backend.
type MetricName string

const (
	MetricMessagesReceived MetricName = "conduit.messages.received"
	MetricMessagesSuccess  MetricName = "conduit.messages.success"
	MetricMessagesFailed   MetricName = "conduit.messages.failed"
	MetricBatchSize        MetricName = "conduit.batch.size"
	MetricDeserializeTime  MetricName = "conduit.deserialize.duration"
	MetricProcessTime      MetricName = "conduit.process.duration"
	MetricAcknowledgeTime  MetricName = "conduit.acknowledge.duration"
)

// Metric is a counter increment or duration observation. Duration is set for
// timing metrics; Value is used for counters and batch sizes.
type Metric struct {
	Name       MetricName
	Value      float64
	Duration   time.Duration
	Attributes map[string]string
}

// Metrics receives Conduit metric observations. Implementations must be safe
// for concurrent use and should return quickly. A nil Metrics disables metrics.
type Metrics interface {
	Record(context.Context, Metric)
}

// MetricsFunc adapts a function into a Metrics implementation.
type MetricsFunc func(context.Context, Metric)

func (f MetricsFunc) Record(ctx context.Context, metric Metric) { f(ctx, metric) }

// LogLevel is the severity of a structured log event.
type LogLevel string

const (
	LogDebug LogLevel = "debug"
	LogInfo  LogLevel = "info"
	LogError LogLevel = "error"
)

// Field is a structured logging field.
type Field struct {
	Key   string
	Value any
}

// Logger receives structured Conduit logs. Implementations must be safe for
// concurrent use. A nil Logger disables library logging.
type Logger interface {
	Log(context.Context, LogLevel, string, ...Field)
}

// LoggerFunc adapts a function into a Logger implementation.
type LoggerFunc func(context.Context, LogLevel, string, ...Field)

func (f LoggerFunc) Log(ctx context.Context, level LogLevel, message string, fields ...Field) {
	f(ctx, level, message, fields...)
}
