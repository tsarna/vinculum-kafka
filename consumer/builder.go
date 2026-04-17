package consumer

import (
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
	groupID       string
	startOffset   kgo.Offset
	subscriptions []TopicSubscription
	subscriber    bus.Subscriber
	commitMode    CommitMode
	dlqTopic      string
	wireFormat    wire.WireFormat
	logger        *zap.Logger
	meterProvider metric.MeterProvider
}

// NewConsumer returns a ConsumerBuilder with default settings:
// commit_mode=after_process, start_offset=stored.
func NewConsumer() *ConsumerBuilder {
	return &ConsumerBuilder{
		startOffset: kgo.NewOffset(),
		commitMode:  CommitAfterProcess,
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

// WithCommitMode sets the offset commit strategy.
func (b *ConsumerBuilder) WithCommitMode(m CommitMode) *ConsumerBuilder {
	b.commitMode = m
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

	consumerOpts := make([]kgo.Opt, 0, len(b.baseOpts)+4)
	consumerOpts = append(consumerOpts, b.baseOpts...)
	consumerOpts = append(consumerOpts,
		kgo.ConsumerGroup(b.groupID),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(b.startOffset),
	)

	// In CommitAfterProcess mode we call CommitRecords explicitly after each
	// successfully processed record — disable franz-go's auto-commit.
	if b.commitMode == CommitAfterProcess {
		consumerOpts = append(consumerOpts, kgo.DisableAutoCommit())
	}

	client, err := kgo.NewClient(consumerOpts...)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer %q: create client: %w", b.groupID, err)
	}

	var meter metric.Meter
	if b.meterProvider != nil {
		meter = b.meterProvider.Meter("github.com/tsarna/vinculum-kafka/consumer")
	}

	wf := b.wireFormat
	if wf == nil {
		wf = wire.Auto
	}

	return &KafkaConsumer{
		client:        client,
		subscriptions: b.subscriptions,
		subscriber:    b.subscriber,
		commitMode:    b.commitMode,
		dlqTopic:      b.dlqTopic,
		wireFormat:    wf,
		logger:        b.logger,
		metrics:       NewConsumerMetrics(meter),
	}, nil
}
