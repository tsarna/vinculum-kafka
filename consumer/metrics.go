package consumer

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ConsumerMetrics holds the OTel instruments for a KafkaConsumer.
// A nil *ConsumerMetrics is valid and results in no-op recording.
type ConsumerMetrics struct {
	recordsReceived metric.Int64Counter    // messaging.client.consumed.messages
	errors          metric.Int64Counter    // kafka.consumer.errors
	lag             metric.Float64Gauge    // kafka.consumer.lag
	processDuration metric.Float64Histogram // messaging.process.duration
	commits         metric.Int64Counter    // kafka.consumer.commits
	clientTag       attribute.KeyValue
}

// NewConsumerMetrics creates a ConsumerMetrics using the given Meter.
// Returns nil if meter is nil, which is safe to call all methods on.
func NewConsumerMetrics(clientName string, meter metric.Meter) *ConsumerMetrics {
	if meter == nil {
		return nil
	}
	rr, _ := meter.Int64Counter("messaging.client.consumed.messages",
		metric.WithUnit("{message}"),
		metric.WithDescription("Records consumed by the Kafka consumer"),
	)
	e, _ := meter.Int64Counter("kafka.consumer.errors",
		metric.WithUnit("{error}"),
		metric.WithDescription("Errors encountered during Kafka consumption"),
	)
	l, _ := meter.Float64Gauge("kafka.consumer.lag",
		metric.WithUnit("{message}"),
		metric.WithDescription("Consumer lag per topic/partition"),
	)
	pd, _ := meter.Float64Histogram("messaging.process.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of message processing"),
	)
	c, _ := meter.Int64Counter("kafka.consumer.commits",
		metric.WithUnit("{operation}"),
		metric.WithDescription("Offset commits performed"),
	)
	return &ConsumerMetrics{
		recordsReceived: rr,
		errors:          e,
		lag:             l,
		processDuration: pd,
		commits:         c,
		clientTag:       attribute.String("vinculum.client.name", clientName),
	}
}

// topicAttr returns standard messaging attributes for a Kafka topic.
func topicAttr(topic string) metric.MeasurementOption {
	return metric.WithAttributes(
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.destination.name", topic),
	)
}

// RecordReceived increments the records-received counter for topic.
func (m *ConsumerMetrics) RecordReceived(ctx context.Context, topic string) {
	if m == nil {
		return
	}
	m.recordsReceived.Add(ctx, 1, topicAttr(topic), metric.WithAttributes(m.clientTag))
}

// RecordError increments the error counter for topic.
func (m *ConsumerMetrics) RecordError(ctx context.Context, topic string) {
	if m == nil {
		return
	}
	m.errors.Add(ctx, 1, topicAttr(topic), metric.WithAttributes(m.clientTag))
}

// UpdateLag sets the lag gauge for a topic/partition.
func (m *ConsumerMetrics) UpdateLag(ctx context.Context, topic string, partition int32, lag int64) {
	if m == nil {
		return
	}
	m.lag.Record(ctx, float64(lag),
		topicAttr(topic),
		metric.WithAttributes(attribute.String("messaging.destination.partition.id", fmt.Sprintf("%d", partition))),
		metric.WithAttributes(m.clientTag),
	)
}

// RecordProcessDuration records how long subscriber.OnEvent took for topic.
func (m *ConsumerMetrics) RecordProcessDuration(ctx context.Context, topic string, d time.Duration) {
	if m == nil {
		return
	}
	m.processDuration.Record(ctx, d.Seconds(), topicAttr(topic), metric.WithAttributes(m.clientTag))
}

// RecordCommit increments the commits counter.
func (m *ConsumerMetrics) RecordCommit(ctx context.Context) {
	if m == nil {
		return
	}
	m.commits.Add(ctx, 1, metric.WithAttributes(attribute.String("messaging.system", "kafka"), m.clientTag))
}
