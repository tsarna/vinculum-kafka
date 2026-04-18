package producer

import (
	"errors"
	"fmt"

	wire "github.com/tsarna/vinculum-wire"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// ProducerBuilder constructs a KafkaProducer with a fluent API.
type ProducerBuilder struct {
	client           *kgo.Client
	clientName       string
	topicMappings    []TopicMapping
	defaultTransform DefaultTopicTransform
	produceMode      ProduceMode
	wireFormat       wire.WireFormat
	logger           *zap.Logger
	meterProvider    metric.MeterProvider
}

// NewProducer returns a ProducerBuilder with default settings:
// produce_mode=sync, default_topic_transform=error.
func NewProducer() *ProducerBuilder {
	return &ProducerBuilder{
		defaultTransform: DefaultTopicError,
		produceMode:      ProduceModeSync,
		logger:           zap.NewNop(),
	}
}

// WithClient sets the shared franz-go client (required).
func (b *ProducerBuilder) WithClient(c *kgo.Client) *ProducerBuilder {
	b.client = c
	return b
}

// WithTopicMapping appends a topic mapping. Mappings are evaluated in order;
// first match wins.
func (b *ProducerBuilder) WithTopicMapping(tm TopicMapping) *ProducerBuilder {
	b.topicMappings = append(b.topicMappings, tm)
	return b
}

// WithDefaultTransform sets what happens when no topic_mapping matches.
func (b *ProducerBuilder) WithDefaultTransform(t DefaultTopicTransform) *ProducerBuilder {
	b.defaultTransform = t
	return b
}

// WithProduceMode sets sync vs async produce mode.
func (b *ProducerBuilder) WithProduceMode(m ProduceMode) *ProducerBuilder {
	b.produceMode = m
	return b
}

// WithMeterProvider sets the OTel MeterProvider used to instrument the producer.
// If nil (the default), no metrics are collected.
func (b *ProducerBuilder) WithMeterProvider(p metric.MeterProvider) *ProducerBuilder {
	b.meterProvider = p
	return b
}

// WithWireFormat sets the wire format used to serialize outbound payloads.
func (b *ProducerBuilder) WithWireFormat(f wire.WireFormat) *ProducerBuilder {
	b.wireFormat = f
	return b
}

// WithWireFormatName sets the wire format by name (e.g. "json", "auto").
// Build returns an error if the name is not recognized.
func (b *ProducerBuilder) WithWireFormatName(name string) *ProducerBuilder {
	b.wireFormat = wire.ByName(name)
	return b
}

// WithClientName sets the vinculum client name used in metric attributes.
func (b *ProducerBuilder) WithClientName(name string) *ProducerBuilder {
	b.clientName = name
	return b
}

// WithLogger sets the logger used for async produce errors.
func (b *ProducerBuilder) WithLogger(l *zap.Logger) *ProducerBuilder {
	if l != nil {
		b.logger = l
	}
	return b
}

// Build validates configuration and returns a KafkaProducer.
func (b *ProducerBuilder) Build() (*KafkaProducer, error) {
	if b.client == nil {
		return nil, errors.New("kafka producer: franz-go client is required")
	}
	wf := b.wireFormat
	if wf == nil {
		wf = wire.Auto
	}
	if wf.Name() == "" {
		return nil, fmt.Errorf("kafka producer: unknown wire format name")
	}
	var meter metric.Meter
	if b.meterProvider != nil {
		meter = b.meterProvider.Meter("github.com/tsarna/vinculum-kafka/producer")
	}
	return &KafkaProducer{
		client:           b.client,
		topicMappings:    b.topicMappings,
		defaultTransform: b.defaultTransform,
		produceMode:      b.produceMode,
		wireFormat:       wf,
		logger:           b.logger,
		metrics:          NewProducerMetrics(b.clientName, meter),
	}, nil
}
