package consumer

import (
	"context"
	"errors"
	"testing"

	bus "github.com/tsarna/vinculum-bus"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// captureSubscriber records the arguments passed to OnEvent.
type captureSubscriber struct {
	bus.BaseSubscriber
	topic  string
	msg    any
	fields map[string]string
	err    error // if non-nil, OnEvent returns this error
}

func (s *captureSubscriber) OnEvent(_ context.Context, topic string, msg any, fields map[string]string) error {
	s.topic = topic
	s.msg = msg
	s.fields = fields
	return s.err
}

// makeConsumer builds a KafkaConsumer without a kgo.Client for unit tests.
func makeConsumer(subs []TopicSubscription, target bus.Subscriber) *KafkaConsumer {
	return &KafkaConsumer{
		subscriptions: subs,
		target:        target,
		commitMode:    CommitAfterProcess,
		logger:        zap.NewNop(),
	}
}

// staticTopicFunc always returns the given vinculum topic.
func staticTopicFunc(topic string) VinculumTopicFunc {
	return func(_, _ string, _ map[string]string, _ any) (string, error) {
		return topic, nil
	}
}

// ── deserializePayload ────────────────────────────────────────────────────────

func TestDeserializePayload_Nil(t *testing.T) {
	assert.Nil(t, deserializePayload(nil))
}

func TestDeserializePayload_ValidJSONObject(t *testing.T) {
	result := deserializePayload([]byte(`{"a":1}`))
	m, ok := result.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(1), m["a"])
}

func TestDeserializePayload_ValidJSONArray(t *testing.T) {
	result := deserializePayload([]byte(`[1,2,3]`))
	arr, ok := result.([]any)
	require.True(t, ok)
	assert.Len(t, arr, 3)
}

func TestDeserializePayload_ValidJSONString(t *testing.T) {
	result := deserializePayload([]byte(`"hello"`))
	assert.Equal(t, "hello", result)
}

func TestDeserializePayload_InvalidJSON(t *testing.T) {
	raw := []byte("not-json")
	result := deserializePayload(raw)
	b, ok := result.([]byte)
	require.True(t, ok)
	assert.Equal(t, raw, b)
}

// ── headersToFields ───────────────────────────────────────────────────────────

func TestHeadersToFields_Empty(t *testing.T) {
	assert.Nil(t, headersToFields(nil))
	assert.Nil(t, headersToFields([]kgo.RecordHeader{}))
}

func TestHeadersToFields_Multiple(t *testing.T) {
	headers := []kgo.RecordHeader{
		{Key: "k1", Value: []byte("v1")},
		{Key: "k2", Value: []byte("v2")},
	}
	result := headersToFields(headers)
	assert.Equal(t, map[string]string{"k1": "v1", "k2": "v2"}, result)
}

// ── findSubscription ──────────────────────────────────────────────────────────

func TestFindSubscription_Found(t *testing.T) {
	subs := []TopicSubscription{
		{KafkaTopic: "foo", VinculumTopicFunc: staticTopicFunc("a/b")},
		{KafkaTopic: "bar", VinculumTopicFunc: staticTopicFunc("c/d")},
	}
	c := makeConsumer(subs, &captureSubscriber{})

	sub, err := c.findSubscription("bar")
	require.NoError(t, err)
	assert.Equal(t, "bar", sub.KafkaTopic)
}

func TestFindSubscription_NotFound(t *testing.T) {
	c := makeConsumer([]TopicSubscription{
		{KafkaTopic: "foo", VinculumTopicFunc: staticTopicFunc("a/b")},
	}, &captureSubscriber{})

	_, err := c.findSubscription("unknown")
	assert.Error(t, err)
}

// ── processRecord ─────────────────────────────────────────────────────────────

func TestProcessRecord_CorrectArgs(t *testing.T) {
	target := &captureSubscriber{}
	c := makeConsumer([]TopicSubscription{
		{
			KafkaTopic: "sensor.readings",
			VinculumTopicFunc: func(kafkaTopic, key string, fields map[string]string, msg any) (string, error) {
				assert.Equal(t, "sensor.readings", kafkaTopic)
				assert.Equal(t, "device42", key)
				assert.Equal(t, "eu-west", fields["region"])
				return "sensor/" + key + "/reading", nil
			},
		},
	}, target)

	r := &kgo.Record{
		Topic: "sensor.readings",
		Key:   []byte("device42"),
		Value: []byte(`{"temp":22.5}`),
		Headers: []kgo.RecordHeader{
			{Key: "region", Value: []byte("eu-west")},
		},
	}

	err := c.processRecord(context.Background(), r)
	require.NoError(t, err)
	assert.Equal(t, "sensor/device42/reading", target.topic)
	assert.Equal(t, map[string]any{"temp": 22.5}, target.msg)
	assert.Equal(t, map[string]string{"region": "eu-west"}, target.fields)
}

func TestProcessRecord_VinculumTopicFuncError(t *testing.T) {
	target := &captureSubscriber{}
	c := makeConsumer([]TopicSubscription{
		{
			KafkaTopic: "foo",
			VinculumTopicFunc: func(_, _ string, _ map[string]string, _ any) (string, error) {
				return "", errors.New("bad expression")
			},
		},
	}, target)

	err := c.processRecord(context.Background(), &kgo.Record{Topic: "foo", Value: []byte(`{}`)})
	assert.Error(t, err)
	assert.Empty(t, target.topic) // target should not have been called
}

func TestProcessRecord_TargetError(t *testing.T) {
	target := &captureSubscriber{err: errors.New("downstream failure")}
	c := makeConsumer([]TopicSubscription{
		{KafkaTopic: "foo", VinculumTopicFunc: staticTopicFunc("a/b")},
	}, target)

	err := c.processRecord(context.Background(), &kgo.Record{Topic: "foo", Value: []byte(`{}`)})
	assert.ErrorContains(t, err, "downstream failure")
}

func TestProcessRecord_NoKeyNoHeaders(t *testing.T) {
	target := &captureSubscriber{}
	c := makeConsumer([]TopicSubscription{
		{KafkaTopic: "foo", VinculumTopicFunc: staticTopicFunc("a/b")},
	}, target)

	err := c.processRecord(context.Background(), &kgo.Record{Topic: "foo", Value: []byte(`"hello"`)})
	require.NoError(t, err)
	assert.Equal(t, "a/b", target.topic)
	assert.Equal(t, "hello", target.msg)
	assert.Nil(t, target.fields)
}
