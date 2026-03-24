package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/amir-yaghoubi/mqttpattern"
	"github.com/tsarna/go2cty2go"
	bus "github.com/tsarna/vinculum-bus"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zclconf/go-cty/cty"
	"go.uber.org/zap"
)

// ProduceMode controls whether OnEvent waits for a broker acknowledgement.
type ProduceMode int

const (
	// ProduceModeSync calls ProduceSync and returns after the broker acks.
	// This is the default and provides at-least-once delivery with backpressure.
	ProduceModeSync ProduceMode = iota

	// ProduceModeAsync calls Produce and returns immediately. Errors are
	// logged. Use for high-throughput, at-most-once pipelines.
	ProduceModeAsync
)

// DefaultTopicTransform controls fallback behaviour when no topic_mapping matches.
type DefaultTopicTransform int

const (
	// DefaultTopicError returns an error for unmatched topics (default).
	DefaultTopicError DefaultTopicTransform = iota

	// DefaultTopicSlashToDot replaces "/" with "." in the vinculum topic to
	// derive the Kafka topic (e.g. "sensor/temp/reading" → "sensor.temp.reading").
	DefaultTopicSlashToDot

	// DefaultTopicIgnore silently succeeds without producing anything when no
	// topic_mapping matches. Useful when a producer owns only a subset of topics
	// and multiple producers share a client.
	DefaultTopicIgnore
)

// KeyFunc resolves the Kafka partition key for a message. Returns nil to omit
// the key (Kafka will round-robin across partitions).
type KeyFunc func(topic string, msg any, fields map[string]string) (key []byte, err error)

// TopicMapping maps a vinculum MQTT-style topic pattern to a Kafka topic and
// optional partition key resolver.
type TopicMapping struct {
	// Pattern is an MQTT-style topic pattern (supports + and # wildcards).
	Pattern string

	// KafkaTopic is the destination Kafka topic name.
	KafkaTopic string

	// KeyFunc resolves the record key per message. nil means no key.
	KeyFunc KeyFunc
}

// KafkaProducer implements bus.Subscriber and produces received vinculum
// events to Kafka. Create via NewProducer().Build().
type KafkaProducer struct {
	bus.BaseSubscriber
	client           *kgo.Client
	topicMappings    []TopicMapping
	defaultTransform DefaultTopicTransform
	produceMode      ProduceMode
	logger           *zap.Logger
}

// OnEvent implements bus.Subscriber. It maps the vinculum topic to a Kafka
// topic, serializes the payload to JSON, converts fields to record headers,
// and produces the record according to the configured produce_mode.
func (p *KafkaProducer) OnEvent(ctx context.Context, topic string, msg any, fields map[string]string) error {
	kafkaTopic, key, err := p.resolveTopicAndKey(topic, msg, fields)
	if err != nil {
		return err
	}
	if kafkaTopic == "" {
		return nil // DefaultTopicIgnore — no match, silently skip
	}

	value, err := serializePayload(msg)
	if err != nil {
		return fmt.Errorf("kafka producer: serialize payload: %w", err)
	}

	headers := make([]kgo.RecordHeader, 0, len(fields))
	for k, v := range fields {
		headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}

	record := &kgo.Record{
		Topic:   kafkaTopic,
		Key:     key,
		Value:   value,
		Headers: headers,
	}

	switch p.produceMode {
	case ProduceModeSync:
		results := p.client.ProduceSync(ctx, record)
		if err := results.FirstErr(); err != nil {
			return fmt.Errorf("kafka producer: produce: %w", err)
		}
	case ProduceModeAsync:
		p.client.Produce(ctx, record, func(r *kgo.Record, err error) {
			if err != nil {
				p.logger.Error("kafka async produce error",
					zap.String("topic", r.Topic),
					zap.Error(err))
			}
		})
	}

	return nil
}

// resolveTopicAndKey iterates topic_mappings in order (first match wins)
// and returns the Kafka topic and partition key. Falls back to
// defaultTransform when no mapping matches.
func (p *KafkaProducer) resolveTopicAndKey(topic string, msg any, fields map[string]string) (kafkaTopic string, key []byte, err error) {
	for _, m := range p.topicMappings {
		if !mqttpattern.Matches(m.Pattern, topic) {
			continue
		}

		// Merge pattern-extracted fields with provided fields.
		// Pattern-extracted values take precedence (they are derived from the topic).
		extracted := mqttpattern.Extract(m.Pattern, topic)
		var mergedFields map[string]string
		if len(extracted) > 0 {
			mergedFields = make(map[string]string, len(fields)+len(extracted))
			for k, v := range fields {
				mergedFields[k] = v
			}
			for k, v := range extracted {
				mergedFields[k] = v
			}
		} else {
			mergedFields = fields
		}

		kafkaTopic = m.KafkaTopic
		if m.KeyFunc != nil {
			key, err = m.KeyFunc(topic, msg, mergedFields)
			if err != nil {
				return "", nil, fmt.Errorf("kafka producer: resolve key for %q: %w", topic, err)
			}
		}
		return kafkaTopic, key, nil
	}

	// No mapping matched.
	switch p.defaultTransform {
	case DefaultTopicSlashToDot:
		return strings.ReplaceAll(topic, "/", "."), nil, nil
	case DefaultTopicIgnore:
		return "", nil, nil
	default:
		return "", nil, fmt.Errorf("kafka producer: no topic mapping matched for topic %q and default_topic_transform is error", topic)
	}
}

// serializePayload converts a vinculum message payload to []byte for the
// Kafka record value.
//
//   - cty.Value  → go2cty2go.CtyToAny() → json.Marshal
//   - []byte     → pass through unchanged
//   - anything else → json.Marshal
func serializePayload(msg any) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}

	// Convert cty.Value to native Go type before JSON marshalling.
	if val, ok := msg.(cty.Value); ok {
		var err error
		msg, err = go2cty2go.CtyToAny(val)
		if err != nil {
			return nil, fmt.Errorf("cty conversion: %w", err)
		}
	}

	// Pass raw bytes through unchanged.
	if b, ok := msg.([]byte); ok {
		return b, nil
	}

	return json.Marshal(msg)
}
