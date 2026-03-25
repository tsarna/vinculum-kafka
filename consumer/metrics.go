package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/tsarna/vinculum-bus/o11y"
)

// ConsumerMetrics holds the o11y instruments for a KafkaConsumer.
// A nil *ConsumerMetrics is valid and results in no-op recording.
type ConsumerMetrics struct {
	recordsReceived o11y.Counter   // kafka_consumer_records_received_total (label: topic)
	errors          o11y.Counter   // kafka_consumer_errors_total            (label: topic)
	lag             o11y.Gauge     // kafka_consumer_lag                     (labels: topic, partition)
	processDuration o11y.Histogram // kafka_consumer_process_duration_seconds (label: topic)
	commits         o11y.Counter   // kafka_consumer_commits_total
}

// NewConsumerMetrics creates a ConsumerMetrics using the given provider.
// Returns nil if provider is nil, which is safe to call all methods on.
func NewConsumerMetrics(provider o11y.MetricsProvider) *ConsumerMetrics {
	if provider == nil {
		return nil
	}
	return &ConsumerMetrics{
		recordsReceived: provider.Counter("kafka_consumer_records_received_total"),
		errors:          provider.Counter("kafka_consumer_errors_total"),
		lag:             provider.Gauge("kafka_consumer_lag"),
		processDuration: provider.Histogram("kafka_consumer_process_duration_seconds"),
		commits:         provider.Counter("kafka_consumer_commits_total"),
	}
}

// RecordReceived increments the records-received counter for topic.
func (m *ConsumerMetrics) RecordReceived(ctx context.Context, topic string) {
	if m == nil {
		return
	}
	m.recordsReceived.Add(ctx, 1, o11y.Label{Key: "topic", Value: topic})
}

// RecordError increments the error counter for topic.
func (m *ConsumerMetrics) RecordError(ctx context.Context, topic string) {
	if m == nil {
		return
	}
	m.errors.Add(ctx, 1, o11y.Label{Key: "topic", Value: topic})
}

// UpdateLag sets the lag gauge for a topic/partition.
func (m *ConsumerMetrics) UpdateLag(ctx context.Context, topic string, partition int32, lag int64) {
	if m == nil {
		return
	}
	m.lag.Set(ctx, float64(lag),
		o11y.Label{Key: "topic", Value: topic},
		o11y.Label{Key: "partition", Value: fmt.Sprintf("%d", partition)},
	)
}

// RecordProcessDuration records how long subscriber.OnEvent took for topic.
func (m *ConsumerMetrics) RecordProcessDuration(ctx context.Context, topic string, d time.Duration) {
	if m == nil {
		return
	}
	m.processDuration.Record(ctx, d.Seconds(), o11y.Label{Key: "topic", Value: topic})
}

// RecordCommit increments the commits counter.
func (m *ConsumerMetrics) RecordCommit(ctx context.Context) {
	if m == nil {
		return
	}
	m.commits.Add(ctx, 1)
}
