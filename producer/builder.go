package producer

import (
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// ProducerBuilder constructs a KafkaProducer with a fluent API.
type ProducerBuilder struct {
	client           *kgo.Client
	topicMappings    []TopicMapping
	defaultTransform DefaultTopicTransform
	produceMode      ProduceMode
	logger           *zap.Logger
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
	return &KafkaProducer{
		client:           b.client,
		topicMappings:    b.topicMappings,
		defaultTransform: b.defaultTransform,
		produceMode:      b.produceMode,
		logger:           b.logger,
	}, nil
}
