package consumer

import (
	"context"
	"encoding/json"
	"fmt"

	bus "github.com/tsarna/vinculum-bus"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// CommitMode controls when Kafka offsets are committed.
type CommitMode int

const (
	// CommitAfterProcess commits the offset after target.OnEvent returns
	// without error. This is the default. Provides at-least-once delivery.
	CommitAfterProcess CommitMode = iota

	// CommitPeriodic delegates offset management to franz-go's auto-commit.
	CommitPeriodic

	// CommitManual reserves the mode for future caller-controlled commits.
	CommitManual
)

// VinculumTopicFunc resolves the vinculum topic for an inbound Kafka record.
// It is called per message. kafkaTopic is the source Kafka topic; key is the
// record key decoded as UTF-8 (empty string if no key); fields are populated
// from record headers; msg is the deserialized payload.
//
// Constructed by the config layer to avoid a circular import dependency.
type VinculumTopicFunc func(kafkaTopic, key string, fields map[string]string, msg any) (string, error)

// TopicSubscription maps one Kafka topic to a vinculum topic resolver.
type TopicSubscription struct {
	// KafkaTopic is the exact Kafka topic name (no wildcards).
	KafkaTopic        string
	VinculumTopicFunc VinculumTopicFunc
}

// KafkaConsumer runs a poll loop that reads records from Kafka and publishes
// them to a target bus.Subscriber. Create via NewConsumer().Build().
//
// KafkaConsumer is a source, not a sink — it does NOT implement bus.Subscriber.
type KafkaConsumer struct {
	client        *kgo.Client
	subscriptions []TopicSubscription
	target        bus.Subscriber
	commitMode    CommitMode
	logger        *zap.Logger

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
				return
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
			}
		}

		c.client.AllowRebalance()
	}
}

func (c *KafkaConsumer) processRecord(ctx context.Context, r *kgo.Record) error {
	fields := headersToFields(r.Headers)
	key := string(r.Key)
	msg := deserializePayload(r.Value)

	sub, err := c.findSubscription(r.Topic)
	if err != nil {
		return err
	}

	vinculumTopic, err := sub.VinculumTopicFunc(r.Topic, key, fields, msg)
	if err != nil {
		return fmt.Errorf("kafka consumer: resolve vinculum topic for %q: %w", r.Topic, err)
	}

	return c.target.OnEvent(ctx, vinculumTopic, msg, fields)
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

// headersToFields converts Kafka record headers to a string map.
// Returns nil (not an empty map) when there are no headers.
func headersToFields(headers []kgo.RecordHeader) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	m := make(map[string]string, len(headers))
	for _, h := range headers {
		m[h.Key] = string(h.Value)
	}
	return m
}
