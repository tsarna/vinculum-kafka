package consumer

import (
	"context"
	"errors"
	"testing"

	bus "github.com/tsarna/vinculum-bus"
	wire "github.com/tsarna/vinculum-wire"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/zap"
)

// dlqHeaderMap extracts named headers from a kgo.Record into a map.
func dlqHeaderMap(r *kgo.Record) map[string]string {
	m := make(map[string]string, len(r.Headers))
	for _, h := range r.Headers {
		m[h.Key] = string(h.Value)
	}
	return m
}

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
		subscriber:    target,
		commitMode:    CommitAfterProcess,
		wireFormat:    wire.Auto,
		logger:        zap.NewNop(),
	}
}

// staticTopicFunc always returns the given vinculum topic.
func staticTopicFunc(topic string) VinculumTopicFunc {
	return func(_ string, _ *string, _ map[string]string, _ any) (string, error) {
		return topic, nil
	}
}

// ── deserialize (via wire format) ─────────────────────────────────────────────

func TestDeserialize_Nil(t *testing.T) {
	// nil payloads are handled before calling Deserialize in processRecord
	result, err := wire.Auto.Deserialize([]byte{})
	require.NoError(t, err)
	assert.Equal(t, "", result)
}

func TestDeserialize_ValidJSONObject(t *testing.T) {
	result, err := wire.Auto.Deserialize([]byte(`{"a":1}`))
	require.NoError(t, err)
	m, ok := result.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(1), m["a"])
}

func TestDeserialize_ValidJSONArray(t *testing.T) {
	result, err := wire.Auto.Deserialize([]byte(`[1,2,3]`))
	require.NoError(t, err)
	arr, ok := result.([]any)
	require.True(t, ok)
	assert.Len(t, arr, 3)
}

func TestDeserialize_ValidJSONString(t *testing.T) {
	result, err := wire.Auto.Deserialize([]byte(`"hello"`))
	require.NoError(t, err)
	assert.Equal(t, "hello", result)
}

func TestDeserialize_InvalidJSON(t *testing.T) {
	result, err := wire.Auto.Deserialize([]byte("not-json"))
	require.NoError(t, err)
	// auto format falls back to string (not []byte) per spec
	assert.Equal(t, "not-json", result)
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

func TestHeadersToFields_FiltersTraceHeaders(t *testing.T) {
	headers := []kgo.RecordHeader{
		{Key: "region", Value: []byte("eu-west")},
		{Key: "traceparent", Value: []byte("00-80e1afed08e019fc1110464cfa66635c-7a085853722dc6d2-01")},
		{Key: "tracestate", Value: []byte("vendor=abc")},
		{Key: "baggage", Value: []byte("userId=42")},
	}
	result := headersToFields(headers)
	assert.Equal(t, map[string]string{"region": "eu-west"}, result,
		"trace headers should be filtered, business headers should remain")
}

func TestHeadersToFields_OnlyTraceHeaders(t *testing.T) {
	headers := []kgo.RecordHeader{
		{Key: "traceparent", Value: []byte("00-abc-def-01")},
		{Key: "tracestate", Value: []byte("k=v")},
	}
	assert.Nil(t, headersToFields(headers),
		"should return nil when only trace headers are present")
}

// ── processRecord tracing ─────────────────────────────────────────────────────

func setupTestTracer(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	t.Cleanup(func() {
		tp.Shutdown(context.Background()) //nolint:errcheck
		otel.SetTracerProvider(otel.GetTracerProvider())
	})
	return exporter
}

func TestProcessRecord_CreatesVinculumSpan(t *testing.T) {
	exporter := setupTestTracer(t)

	target := &captureSubscriber{}
	c := makeConsumer([]TopicSubscription{
		{KafkaTopic: "foo", VinculumTopicFunc: staticTopicFunc("a/b")},
	}, target)

	err := c.processRecord(context.Background(), &kgo.Record{Topic: "foo", Value: []byte(`{}`)})
	require.NoError(t, err)

	spans := exporter.GetSpans()
	require.NotEmpty(t, spans)
	assert.Equal(t, "vinculum.process a/b", spans[0].Name)
}

func TestProcessRecord_UsesRecordContextAsParent(t *testing.T) {
	exporter := setupTestTracer(t)

	target := &captureSubscriber{}
	c := makeConsumer([]TopicSubscription{
		{KafkaTopic: "foo", VinculumTopicFunc: staticTopicFunc("a/b")},
	}, target)

	// Simulate what kotel does with LinkSpans enabled: it creates a new root
	// span (linked to the producer span) and stores it in r.Context. The
	// vinculum.process span should become a child of that new root span, NOT
	// of the original remote trace.
	tp := otel.GetTracerProvider()
	rCtx, rootSpan := tp.Tracer("test").Start(context.Background(), "kotel.receive")
	rootTraceID := rootSpan.SpanContext().TraceID()
	rootSpan.End()

	r := &kgo.Record{Topic: "foo", Value: []byte(`{}`), Context: rCtx}
	err := c.processRecord(context.Background(), r)
	require.NoError(t, err)

	spans := exporter.GetSpans()
	require.NotEmpty(t, spans)
	assert.Equal(t, rootTraceID, spans[0].SpanContext.TraceID(),
		"vinculum processing span should be a child of the span in r.Context")
}

func TestProcessRecord_SpanRecordsError(t *testing.T) {
	exporter := setupTestTracer(t)

	target := &captureSubscriber{err: errors.New("downstream failure")}
	c := makeConsumer([]TopicSubscription{
		{KafkaTopic: "foo", VinculumTopicFunc: staticTopicFunc("a/b")},
	}, target)

	err := c.processRecord(context.Background(), &kgo.Record{Topic: "foo", Value: []byte(`{}`)})
	assert.Error(t, err)

	spans := exporter.GetSpans()
	require.NotEmpty(t, spans)
	assert.Len(t, spans[0].Events, 1, "expected one error event on the span")
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
			VinculumTopicFunc: func(kafkaTopic string, key *string, fields map[string]string, msg any) (string, error) {
				assert.Equal(t, "sensor.readings", kafkaTopic)
				require.NotNil(t, key)
				assert.Equal(t, "device42", *key)
				assert.Equal(t, "eu-west", fields["region"])
				return "sensor/" + *key + "/reading", nil
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
			VinculumTopicFunc: func(_ string, _ *string, _ map[string]string, _ any) (string, error) {
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

// ── DLQ record construction ───────────────────────────────────────────────────

func TestBuildDLQRecord_HeadersAndMetadata(t *testing.T) {
	c := &KafkaConsumer{dlqTopic: "my.dlq", logger: zap.NewNop()}
	original := &kgo.Record{
		Topic: "source.topic",
		Key:   []byte("k1"),
		Value: []byte(`{"x":1}`),
		Headers: []kgo.RecordHeader{
			{Key: "region", Value: []byte("us-east")},
		},
	}
	procErr := errors.New("downstream failed")

	dlq := c.buildDLQRecord(original, procErr)

	assert.Equal(t, "my.dlq", dlq.Topic)
	assert.Equal(t, original.Key, dlq.Key)
	assert.Equal(t, original.Value, dlq.Value)

	hm := dlqHeaderMap(dlq)
	assert.Equal(t, "us-east", hm["region"])
	assert.Equal(t, "downstream failed", hm["vinculum-error"])
	assert.Equal(t, "source.topic", hm["vinculum-original-topic"])
	assert.NotEmpty(t, hm["vinculum-timestamp"])
}

func TestBuildDLQRecord_NoOriginalHeaders(t *testing.T) {
	c := &KafkaConsumer{dlqTopic: "dlq", logger: zap.NewNop()}
	original := &kgo.Record{Topic: "t", Value: []byte(`{}`)}

	dlq := c.buildDLQRecord(original, errors.New("oops"))
	hm := dlqHeaderMap(dlq)
	assert.Equal(t, "oops", hm["vinculum-error"])
	assert.Equal(t, "t", hm["vinculum-original-topic"])
	assert.NotEmpty(t, hm["vinculum-timestamp"])
	assert.Len(t, dlq.Headers, 3) // only the 3 metadata headers
}

func TestProcessRecord_NilKeyIsNilPointer(t *testing.T) {
	var gotKey *string
	c := makeConsumer([]TopicSubscription{
		{
			KafkaTopic: "foo",
			VinculumTopicFunc: func(_ string, key *string, _ map[string]string, _ any) (string, error) {
				gotKey = key
				return "a/b", nil
			},
		},
	}, &captureSubscriber{})

	// Record with no key (nil slice)
	err := c.processRecord(context.Background(), &kgo.Record{Topic: "foo", Value: []byte(`{}`)})
	require.NoError(t, err)
	assert.Nil(t, gotKey)

	// Record with an explicit empty key
	err = c.processRecord(context.Background(), &kgo.Record{Topic: "foo", Key: []byte{}, Value: []byte(`{}`)})
	require.NoError(t, err)
	require.NotNil(t, gotKey)
	assert.Equal(t, "", *gotKey)
}
