package producer

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ProducerMetrics holds the OTel instruments for a KafkaProducer.
// A nil *ProducerMetrics is valid and results in no-op recording.
type ProducerMetrics struct {
	recordsSent     metric.Int64Counter    // messaging.client.sent.messages
	errors          metric.Int64Counter    // kafka.producer.errors
	produceDuration metric.Float64Histogram // messaging.client.operation.duration
}

// NewProducerMetrics creates a ProducerMetrics using the given Meter.
// Returns nil if meter is nil, which is safe to call all methods on.
func NewProducerMetrics(meter metric.Meter) *ProducerMetrics {
	if meter == nil {
		return nil
	}
	rs, _ := meter.Int64Counter("messaging.client.sent.messages",
		metric.WithUnit("{message}"),
		metric.WithDescription("Records sent by the Kafka producer"),
	)
	e, _ := meter.Int64Counter("kafka.producer.errors",
		metric.WithUnit("{error}"),
		metric.WithDescription("Errors encountered during Kafka produce"),
	)
	pd, _ := meter.Float64Histogram("messaging.client.operation.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of Kafka produce operations"),
	)
	return &ProducerMetrics{
		recordsSent:     rs,
		errors:          e,
		produceDuration: pd,
	}
}

// topicAttr returns standard messaging attributes for a Kafka topic.
func topicAttr(topic string) metric.MeasurementOption {
	return metric.WithAttributes(
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.destination.name", topic),
	)
}

// RecordSent increments the sent counter for the given Kafka topic.
func (m *ProducerMetrics) RecordSent(ctx context.Context, topic string) {
	if m == nil {
		return
	}
	m.recordsSent.Add(ctx, 1, topicAttr(topic),
		metric.WithAttributes(attribute.String("messaging.operation.name", "send")),
	)
}

// RecordError increments the error counter for the given Kafka topic.
func (m *ProducerMetrics) RecordError(ctx context.Context, topic string) {
	if m == nil {
		return
	}
	m.errors.Add(ctx, 1, topicAttr(topic))
}

// RecordProduceDuration records how long ProduceSync took for the given Kafka topic.
func (m *ProducerMetrics) RecordProduceDuration(ctx context.Context, topic string, d time.Duration) {
	if m == nil {
		return
	}
	m.produceDuration.Record(ctx, d.Seconds(), topicAttr(topic),
		metric.WithAttributes(attribute.String("messaging.operation.name", "send")),
	)
}
