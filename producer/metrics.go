package producer

import (
	"context"
	"time"

	"github.com/tsarna/vinculum-bus/o11y"
)

// ProducerMetrics holds the o11y instruments for a KafkaProducer.
// A nil *ProducerMetrics is valid and results in no-op recording.
type ProducerMetrics struct {
	recordsSent     o11y.Counter   // kafka_producer_records_sent_total          (label: topic)
	errors          o11y.Counter   // kafka_producer_errors_total                 (label: topic)
	produceDuration o11y.Histogram // kafka_producer_produce_duration_seconds     (label: topic)
}

// NewProducerMetrics creates a ProducerMetrics using the given provider.
// Returns nil if provider is nil, which is safe to call all methods on.
func NewProducerMetrics(provider o11y.MetricsProvider) *ProducerMetrics {
	if provider == nil {
		return nil
	}
	return &ProducerMetrics{
		recordsSent:     provider.Counter("kafka_producer_records_sent_total"),
		errors:          provider.Counter("kafka_producer_errors_total"),
		produceDuration: provider.Histogram("kafka_producer_produce_duration_seconds"),
	}
}

// RecordSent increments the sent counter for the given Kafka topic.
func (m *ProducerMetrics) RecordSent(ctx context.Context, topic string) {
	if m == nil {
		return
	}
	m.recordsSent.Add(ctx, 1, o11y.Label{Key: "topic", Value: topic})
}

// RecordError increments the error counter for the given Kafka topic.
func (m *ProducerMetrics) RecordError(ctx context.Context, topic string) {
	if m == nil {
		return
	}
	m.errors.Add(ctx, 1, o11y.Label{Key: "topic", Value: topic})
}

// RecordProduceDuration records how long ProduceSync took for the given Kafka topic.
func (m *ProducerMetrics) RecordProduceDuration(ctx context.Context, topic string, d time.Duration) {
	if m == nil {
		return
	}
	m.produceDuration.Record(ctx, d.Seconds(), o11y.Label{Key: "topic", Value: topic})
}
