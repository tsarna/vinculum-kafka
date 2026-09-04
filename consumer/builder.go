package consumer

import (
	"context"
	"errors"
	"fmt"

	bus "github.com/tsarna/vinculum-bus"
	wire "github.com/tsarna/vinculum-wire"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// ConsumerBuilder constructs a KafkaConsumer with a fluent API.
type ConsumerBuilder struct {
	baseOpts      []kgo.Opt
	clientName    string
	groupID       string
	startOffset   kgo.Offset
	subscriptions []TopicSubscription
	subscriber    bus.Subscriber
	ackMode       AckMode
	maxInFlight   int
	dlqTopic      string
	wireFormat    wire.WireFormat
	onDecodeError wire.DecodeErrorHook
	logger        *zap.Logger
	meterProvider metric.MeterProvider
}

// NewConsumer returns a ConsumerBuilder with default settings:
// ack=after_handling, start_offset=stored.
func NewConsumer() *ConsumerBuilder {
	return &ConsumerBuilder{
		startOffset: kgo.NewOffset(),
		ackMode:     AckAfterHandling,
		logger:      zap.NewNop(),
	}
}

// WithBaseOpts sets the connection-level kgo options (brokers, TLS, SASL,
// timeouts) shared with the producer client.
func (b *ConsumerBuilder) WithBaseOpts(opts []kgo.Opt) *ConsumerBuilder {
	b.baseOpts = opts
	return b
}

// WithGroupID sets the Kafka consumer group ID (required).
func (b *ConsumerBuilder) WithGroupID(id string) *ConsumerBuilder {
	b.groupID = id
	return b
}

// WithStartOffset sets the offset to use when no committed offset exists for
// a partition. Use kgo.NewOffset().AtStart() for earliest,
// kgo.NewOffset().AtEnd() for latest, or kgo.NewOffset() for stored (default).
func (b *ConsumerBuilder) WithStartOffset(o kgo.Offset) *ConsumerBuilder {
	b.startOffset = o
	return b
}

// WithSubscription appends a topic subscription.
func (b *ConsumerBuilder) WithSubscription(sub TopicSubscription) *ConsumerBuilder {
	b.subscriptions = append(b.subscriptions, sub)
	return b
}

// WithSubscriber sets the subscriber that receives deserialized messages (required).
func (b *ConsumerBuilder) WithSubscriber(t bus.Subscriber) *ConsumerBuilder {
	b.subscriber = t
	return b
}

// WithAckMode sets who settles a record with Kafka, and when.
func (b *ConsumerBuilder) WithAckMode(m AckMode) *ConsumerBuilder {
	b.ackMode = m
	return b
}

// WithMaxInFlight bounds how far a partition's completions may run behind the
// records handed out before the partition stops being fetched. Zero or less
// means DefaultMaxInFlight.
//
// It is a bound on memory and on blast radius, not a throughput knob: the
// records between a partition's low-water mark and its highest dispatched
// offset are all reprocessed if this member loses the partition, so a very
// large window trades duplicate work at a rebalance for nothing in particular.
func (b *ConsumerBuilder) WithMaxInFlight(n int) *ConsumerBuilder {
	b.maxInFlight = n
	return b
}

// WithDLQTopic sets the Kafka topic to which failed records are forwarded.
// If empty (default), failed records are logged and skipped without committing.
func (b *ConsumerBuilder) WithDLQTopic(topic string) *ConsumerBuilder {
	b.dlqTopic = topic
	return b
}

// WithWireFormat sets the wire format used to deserialize inbound payloads.
func (b *ConsumerBuilder) WithWireFormat(f wire.WireFormat) *ConsumerBuilder {
	b.wireFormat = f
	return b
}

// WithWireFormatName sets the wire format by name (e.g. "json", "auto").
// Build returns an error if the name is not recognized.
func (b *ConsumerBuilder) WithWireFormatName(name string) *ConsumerBuilder {
	b.wireFormat = wire.ByName(name)
	return b
}

// WithDecodeErrorHook sets an observer invoked when an inbound record value
// fails to deserialize. The hook cannot suppress the failure: the record is
// treated as failed either way (routed to dlq_topic when configured, or left
// uncommitted). nil (the default) means no observer.
func (b *ConsumerBuilder) WithDecodeErrorHook(h wire.DecodeErrorHook) *ConsumerBuilder {
	b.onDecodeError = h
	return b
}

// WithClientName sets the vinculum client name used in metric attributes.
func (b *ConsumerBuilder) WithClientName(name string) *ConsumerBuilder {
	b.clientName = name
	return b
}

// WithMeterProvider sets the OTel MeterProvider used to instrument the consumer.
// If nil (the default), no metrics are collected.
func (b *ConsumerBuilder) WithMeterProvider(p metric.MeterProvider) *ConsumerBuilder {
	b.meterProvider = p
	return b
}

// WithLogger sets the logger used for poll and processing errors.
func (b *ConsumerBuilder) WithLogger(l *zap.Logger) *ConsumerBuilder {
	if l != nil {
		b.logger = l
	}
	return b
}

// Build validates configuration, creates the kgo.Client, and returns a
// KafkaConsumer ready to be started.
func (b *ConsumerBuilder) Build() (*KafkaConsumer, error) {
	if b.groupID == "" {
		return nil, errors.New("kafka consumer: group_id is required")
	}
	if b.subscriber == nil {
		return nil, errors.New("kafka consumer: subscriber is required")
	}
	if len(b.subscriptions) == 0 {
		return nil, errors.New("kafka consumer: at least one topic_subscription is required")
	}

	topics := make([]string, len(b.subscriptions))
	for i, sub := range b.subscriptions {
		topics[i] = sub.KafkaTopic
	}

	var meter metric.Meter
	if b.meterProvider != nil {
		meter = b.meterProvider.Meter("github.com/tsarna/vinculum-kafka/consumer")
	}

	wf := b.wireFormat
	if wf == nil {
		wf = wire.Auto
	}

	c := &KafkaConsumer{
		subscriptions: b.subscriptions,
		subscriber:    b.subscriber,
		ackMode:       b.ackMode,
		dlqTopic:      b.dlqTopic,
		wireFormat:    wf,
		onDecodeError: b.onDecodeError,
		logger:        b.logger,
		metrics:       NewConsumerMetrics(b.clientName, meter),
	}

	consumerOpts := make([]kgo.Opt, 0, len(b.baseOpts)+7)
	consumerOpts = append(consumerOpts, b.baseOpts...)
	consumerOpts = append(consumerOpts,
		kgo.ConsumerGroup(b.groupID),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(b.startOffset),
	)

	// Under AckPeriodic offsets are franz-go's timer's business and there is
	// nothing per-record to settle. Every other mode tracks its own low-water
	// marks, so autocommit has to be off — leaving it on is what made the old
	// commit_mode = "manual" an alias for periodic.
	if b.ackMode != AckPeriodic {
		c.tracker = newTracker(b.maxInFlight)

		consumerOpts = append(consumerOpts,
			kgo.DisableAutoCommit(),

			// A rebalance in the middle of a fetch would hand a partition away
			// while records from it were still being dispatched, so their
			// settlers would be registered against an assignment that no longer
			// exists. Blocking confines rebalances to the gap between polls, by
			// which time every record of a fetch has a settler and the marks
			// have been committed.
			kgo.BlockRebalanceOnPoll(),

			// A revoke is an orderly hand-off: commit what has completed, then
			// forget the partitions so a settle still in flight for one reports
			// that it was reassigned rather than moving a mark another member
			// now owns.
			kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
				c.commitMarks(ctx)
				c.dropPartitions(cl, revoked)
			}),

			// A loss is the same hand-off without the commit: the group has
			// already moved on, so an offset written now would be for a
			// partition someone else is consuming.
			kgo.OnPartitionsLost(func(_ context.Context, cl *kgo.Client, lost map[string][]int32) {
				c.dropPartitions(cl, lost)
			}),
		)
	}

	client, err := kgo.NewClient(consumerOpts...)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer %q: create client: %w", b.groupID, err)
	}
	c.client = client

	return c, nil
}
