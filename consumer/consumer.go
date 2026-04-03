package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	bus "github.com/tsarna/vinculum-bus"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.uber.org/zap"
)

// CommitMode controls when Kafka offsets are committed.
type CommitMode int

const (
	// CommitAfterProcess commits the offset after subscriber.OnEvent returns
	// without error. This is the default. Provides at-least-once delivery.
	CommitAfterProcess CommitMode = iota

	// CommitPeriodic delegates offset management to franz-go's auto-commit.
	CommitPeriodic

	// CommitManual reserves the mode for future caller-controlled commits.
	CommitManual
)

// VinculumTopicFunc resolves the vinculum topic for an inbound Kafka record.
// It is called per message. kafkaTopic is the source Kafka topic; key is the
// record key decoded as UTF-8, or nil if the record has no key; fields are
// populated from record headers; msg is the deserialized payload.
//
// Constructed by the config layer to avoid a circular import dependency.
type VinculumTopicFunc func(kafkaTopic string, key *string, fields map[string]string, msg any) (string, error)

// TopicSubscription maps one Kafka topic to a vinculum topic resolver.
type TopicSubscription struct {
	// KafkaTopic is the exact Kafka topic name (no wildcards).
	KafkaTopic        string
	VinculumTopicFunc VinculumTopicFunc
}

// KafkaConsumer runs a poll loop that reads records from Kafka and publishes
// them to a bus.Subscriber. Create via NewConsumer().Build().
//
// KafkaConsumer is a source, not a sink — it does NOT implement bus.Subscriber.
type KafkaConsumer struct {
	client        *kgo.Client
	subscriptions []TopicSubscription
	subscriber    bus.Subscriber
	commitMode    CommitMode
	dlqTopic      string // optional; if non-empty, failed records are produced here
	logger        *zap.Logger
	metrics       *ConsumerMetrics

	cancel context.CancelFunc
	done   chan struct{}
}

// Start launches the poll goroutine and returns immediately. The goroutine
// runs until ctx is cancelled or Stop is called.
func (c *KafkaConsumer) Start(ctx context.Context) error {
	pollCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.done = make(chan struct{})
	go c.pollLoop(pollCtx)
	return nil
}

// Stop signals the poll goroutine to exit, waits for it to finish, then
// closes the underlying kgo.Client.
func (c *KafkaConsumer) Stop() error {
	if c.cancel != nil {
		c.cancel()
	}
	if c.done != nil {
		<-c.done
	}
	if c.client != nil {
		c.client.Close()
	}
	return nil
}

func (c *KafkaConsumer) pollLoop(ctx context.Context) {
	defer close(c.done)

	for {
		fetches := c.client.PollFetches(ctx)

		if ctx.Err() != nil {
			return // normal shutdown
		}

		if err := fetches.Err(); err != nil {
			c.logger.Error("kafka consumer: poll error", zap.Error(err))
			// continue — transient errors should not kill the loop
		}

		var toCommit []*kgo.Record

		fetches.EachRecord(func(r *kgo.Record) {
			if err := c.processRecord(ctx, r); err != nil {
				c.logger.Error("kafka consumer: process record failed",
					zap.String("topic", r.Topic),
					zap.Error(err))
				if c.dlqTopic != "" {
					if dlqErr := c.sendToDLQ(ctx, r, err); dlqErr != nil {
						c.logger.Error("kafka consumer: DLQ send failed",
							zap.String("topic", r.Topic),
							zap.Error(dlqErr))
						return // don't commit — DLQ itself failed
					}
					// DLQ send succeeded — fall through to commit
				} else {
					return // no DLQ; skip commit
				}
			}
			if c.commitMode == CommitAfterProcess {
				toCommit = append(toCommit, r)
			}
		})

		if len(toCommit) > 0 {
			if err := c.client.CommitRecords(ctx, toCommit...); err != nil {
				if ctx.Err() == nil {
					c.logger.Error("kafka consumer: commit failed", zap.Error(err))
				}
			} else {
				c.metrics.RecordCommit(ctx)
			}
		}

		c.updateLag(ctx, fetches)
		c.client.AllowRebalance()
	}
}

// updateLag calculates consumer lag from the fetch result and updates the lag gauge.
// kgo.Client.Lag() is not available in v1.18.1, so lag is derived from HighWatermark
// and the offset of the last record in each fetched partition.
// For caught-up partitions (no records returned), lag is reported as 0.
func (c *KafkaConsumer) updateLag(ctx context.Context, fetches kgo.Fetches) {
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		var lag int64
		if len(p.Records) > 0 {
			lastOffset := p.Records[len(p.Records)-1].Offset
			if lag = p.HighWatermark - (lastOffset + 1); lag < 0 {
				lag = 0
			}
		}
		c.metrics.UpdateLag(ctx, p.Topic, p.Partition, lag)
	})
}

func (c *KafkaConsumer) processRecord(ctx context.Context, r *kgo.Record) error {
	fields := headersToFields(r.Headers)
	var key *string
	if r.Key != nil {
		s := string(r.Key)
		key = &s
	}
	msg := deserializePayload(r.Value)

	sub, err := c.findSubscription(r.Topic)
	if err != nil {
		c.metrics.RecordError(ctx, r.Topic)
		return err
	}

	vinculumTopic, err := sub.VinculumTopicFunc(r.Topic, key, fields, msg)
	if err != nil {
		c.metrics.RecordError(ctx, r.Topic)
		return fmt.Errorf("kafka consumer: resolve vinculum topic for %q: %w", r.Topic, err)
	}

	// Use the record context as the parent — kotel has already extracted the
	// remote trace context from the record headers, created a new root span
	// linked to the producer span, and stored it in r.Context. This makes the
	// vinculum processing span a child of that new root (i.e. a separate trace
	// from the producer, linked rather than parented). Fall back to the poll
	// context if r.Context is nil.
	recCtx := r.Context
	if recCtx == nil {
		recCtx = ctx
	}

	// Create a span covering the full vinculum processing time (topic resolution,
	// deserialization, and subscriber.OnEvent including action evaluation).
	tracer := otel.GetTracerProvider().Tracer("vinculum-kafka/consumer")
	recCtx, span := tracer.Start(recCtx, "vinculum.process "+vinculumTopic)
	defer span.End()

	start := time.Now()
	err = c.subscriber.OnEvent(recCtx, vinculumTopic, msg, fields)
	c.metrics.RecordProcessDuration(ctx, r.Topic, time.Since(start))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		c.metrics.RecordError(ctx, r.Topic)
		return err
	}
	c.metrics.RecordReceived(ctx, r.Topic)
	return nil
}

func (c *KafkaConsumer) sendToDLQ(ctx context.Context, r *kgo.Record, procErr error) error {
	return c.client.ProduceSync(ctx, c.buildDLQRecord(r, procErr)).FirstErr()
}

// buildDLQRecord constructs the record to be produced to the DLQ topic.
// Kept separate from sendToDLQ so the record structure can be tested without a kgo.Client.
func (c *KafkaConsumer) buildDLQRecord(r *kgo.Record, procErr error) *kgo.Record {
	extra := []kgo.RecordHeader{
		{Key: "vinculum-error", Value: []byte(procErr.Error())},
		{Key: "vinculum-original-topic", Value: []byte(r.Topic)},
		{Key: "vinculum-timestamp", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	}
	headers := make([]kgo.RecordHeader, 0, len(r.Headers)+len(extra))
	headers = append(headers, r.Headers...)
	headers = append(headers, extra...)

	return &kgo.Record{
		Topic:   c.dlqTopic,
		Key:     r.Key,
		Value:   r.Value,
		Headers: headers,
	}
}

func (c *KafkaConsumer) findSubscription(kafkaTopic string) (*TopicSubscription, error) {
	for i := range c.subscriptions {
		if c.subscriptions[i].KafkaTopic == kafkaTopic {
			return &c.subscriptions[i], nil
		}
	}
	return nil, fmt.Errorf("kafka consumer: no subscription found for topic %q", kafkaTopic)
}

// deserializePayload converts a Kafka record value to a Go value.
// Valid JSON is unmarshalled to any (map/slice/scalar).
// Invalid JSON is returned as []byte.
func deserializePayload(value []byte) any {
	if value == nil {
		return nil
	}
	var v any
	if err := json.Unmarshal(value, &v); err != nil {
		return value
	}
	return v
}

// traceHeaders is the set of W3C trace context header keys injected by kotel.
// These are filtered from the fields map to keep business metadata clean.
var traceHeaders = map[string]struct{}{
	"traceparent": {},
	"tracestate":  {},
	"baggage":     {},
}

// headersToFields converts Kafka record headers to a string map, filtering out
// W3C trace context headers (traceparent, tracestate, baggage) since those are
// extracted into the Go context by kotel and should not appear as business fields.
// Returns nil (not an empty map) when there are no non-trace headers.
func headersToFields(headers []kgo.RecordHeader) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	m := make(map[string]string, len(headers))
	for _, h := range headers {
		if _, isTrace := traceHeaders[h.Key]; !isTrace {
			m[h.Key] = string(h.Value)
		}
	}
	if len(m) == 0 {
		return nil
	}
	return m
}
