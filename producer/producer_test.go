package producer

import (
	"errors"
	"testing"

	"github.com/tsarna/go2cty2go"
	wire "github.com/tsarna/vinculum-wire"
	"github.com/zclconf/go-cty/cty"
)

// makeProducer builds a KafkaProducer without a kgo.Client for unit-testing
// methods that don't touch the broker (resolveTopicAndKey, serializePayload).
func makeProducer(mappings []TopicMapping, defaultTransform DefaultTopicTransform) *KafkaProducer {
	return &KafkaProducer{
		topicMappings:    mappings,
		defaultTransform: defaultTransform,
	}
}

// staticKey returns a KeyFunc that always returns the given key bytes.
func staticKey(k []byte) KeyFunc {
	return func(_ string, _ any, _ map[string]string) ([]byte, error) {
		return k, nil
	}
}

// fieldKey returns a KeyFunc that returns fields[name] as the key.
func fieldKey(name string) KeyFunc {
	return func(_ string, _ any, fields map[string]string) ([]byte, error) {
		return []byte(fields[name]), nil
	}
}

// errorKey returns a KeyFunc that always returns an error.
func errorKey(msg string) KeyFunc {
	return func(_ string, _ any, _ map[string]string) ([]byte, error) {
		return nil, errors.New(msg)
	}
}

// --- resolveTopicAndKey ---

func TestResolveTopicAndKey_ExactMatch(t *testing.T) {
	p := makeProducer([]TopicMapping{
		{Pattern: "sensor/temp", KafkaTopic: "sensors", KeyFunc: staticKey([]byte("mykey"))},
	}, DefaultTopicError)

	topic, key, err := p.resolveTopicAndKey("sensor/temp", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if topic != "sensors" {
		t.Errorf("kafka topic: got %q, want %q", topic, "sensors")
	}
	if string(key) != "mykey" {
		t.Errorf("key: got %q, want %q", key, "mykey")
	}
}

func TestResolveTopicAndKey_FirstMatchWins(t *testing.T) {
	p := makeProducer([]TopicMapping{
		{Pattern: "sensor/#", KafkaTopic: "first"},
		{Pattern: "sensor/temp", KafkaTopic: "second"},
	}, DefaultTopicError)

	topic, _, err := p.resolveTopicAndKey("sensor/temp", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if topic != "first" {
		t.Errorf("expected first match to win, got %q", topic)
	}
}

func TestResolveTopicAndKey_WildcardExtractsFields(t *testing.T) {
	p := makeProducer([]TopicMapping{
		{Pattern: "sensor/+deviceId/reading", KafkaTopic: "sensors", KeyFunc: fieldKey("deviceId")},
	}, DefaultTopicError)

	topic, key, err := p.resolveTopicAndKey("sensor/abc123/reading", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if topic != "sensors" {
		t.Errorf("kafka topic: got %q, want %q", topic, "sensors")
	}
	if string(key) != "abc123" {
		t.Errorf("key: got %q, want %q", key, "abc123")
	}
}

func TestResolveTopicAndKey_ExtractedFieldsMergedWithProvided(t *testing.T) {
	// Pattern-extracted fields should be merged with provided fields, with
	// pattern-extracted values taking precedence on conflicts.
	var capturedFields map[string]string
	capturingKey := func(_ string, _ any, fields map[string]string) ([]byte, error) {
		capturedFields = fields
		return nil, nil
	}

	p := makeProducer([]TopicMapping{
		{Pattern: "sensor/+deviceId/reading", KafkaTopic: "sensors", KeyFunc: capturingKey},
	}, DefaultTopicError)

	provided := map[string]string{"region": "us-east", "deviceId": "should-be-overridden"}
	_, _, err := p.resolveTopicAndKey("sensor/abc123/reading", nil, provided)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedFields["deviceId"] != "abc123" {
		t.Errorf("pattern-extracted field should override provided: got %q", capturedFields["deviceId"])
	}
	if capturedFields["region"] != "us-east" {
		t.Errorf("provided field should be preserved: got %q", capturedFields["region"])
	}
}

func TestResolveTopicAndKey_NilKeyFunc(t *testing.T) {
	p := makeProducer([]TopicMapping{
		{Pattern: "alerts/#", KafkaTopic: "alerts", KeyFunc: nil},
	}, DefaultTopicError)

	_, key, err := p.resolveTopicAndKey("alerts/critical", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key != nil {
		t.Errorf("expected nil key, got %v", key)
	}
}

func TestResolveTopicAndKey_KeyFuncError(t *testing.T) {
	p := makeProducer([]TopicMapping{
		{Pattern: "sensor/#", KafkaTopic: "sensors", KeyFunc: errorKey("key resolution failed")},
	}, DefaultTopicError)

	_, _, err := p.resolveTopicAndKey("sensor/temp", nil, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestResolveTopicAndKey_NoMatchError(t *testing.T) {
	p := makeProducer([]TopicMapping{
		{Pattern: "sensor/#", KafkaTopic: "sensors"},
	}, DefaultTopicError)

	_, _, err := p.resolveTopicAndKey("alerts/critical", nil, nil)
	if err == nil {
		t.Fatal("expected error for unmatched topic with DefaultTopicError")
	}
}

func TestResolveTopicAndKey_NoMatchSlashToDot(t *testing.T) {
	p := makeProducer(nil, DefaultTopicSlashToDot)

	topic, key, err := p.resolveTopicAndKey("a/b/c", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if topic != "a.b.c" {
		t.Errorf("got %q, want %q", topic, "a.b.c")
	}
	if key != nil {
		t.Errorf("expected nil key, got %v", key)
	}
}

func TestResolveTopicAndKey_NoMatchIgnore(t *testing.T) {
	p := makeProducer([]TopicMapping{
		{Pattern: "sensor/#", KafkaTopic: "sensors"},
	}, DefaultTopicIgnore)

	topic, key, err := p.resolveTopicAndKey("alerts/critical", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if topic != "" {
		t.Errorf("expected empty topic for ignore, got %q", topic)
	}
	if key != nil {
		t.Errorf("expected nil key for ignore, got %v", key)
	}
}

func TestResolveTopicAndKey_NoMappingsIgnore(t *testing.T) {
	p := makeProducer(nil, DefaultTopicIgnore)

	topic, _, err := p.resolveTopicAndKey("anything/at/all", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if topic != "" {
		t.Errorf("expected empty topic, got %q", topic)
	}
}

// --- serialize (via wire format + cty shim) ---

// serializeWithCtyShim mimics the OnEvent cty conversion + wire format path.
func serializeWithCtyShim(wf wire.WireFormat, msg any) ([]byte, error) {
	if val, ok := msg.(cty.Value); ok {
		native, err := go2cty2go.CtyToAny(val)
		if err != nil {
			return nil, err
		}
		msg = native
	}
	return wf.Serialize(msg)
}

func TestSerialize_Nil(t *testing.T) {
	b, err := wire.Auto.Serialize(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b != nil {
		t.Errorf("expected nil, got %v", b)
	}
}

func TestSerialize_BytesPassthrough(t *testing.T) {
	raw := []byte(`{"already":"encoded"}`)
	b, err := wire.Auto.Serialize(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(b) != string(raw) {
		t.Errorf("got %q, want %q", b, raw)
	}
}

func TestSerialize_GoValue(t *testing.T) {
	b, err := wire.Auto.Serialize(map[string]any{"hello": "world"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(b) != `{"hello":"world"}` {
		t.Errorf("got %q, want %q", b, `{"hello":"world"}`)
	}
}

func TestSerialize_CtyString(t *testing.T) {
	b, err := serializeWithCtyShim(wire.Auto, cty.StringVal("hello"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// auto format passes strings through verbatim (not JSON-encoded)
	if string(b) != `hello` {
		t.Errorf("got %q, want %q", b, `hello`)
	}
}

func TestSerialize_CtyStringJSON(t *testing.T) {
	b, err := serializeWithCtyShim(wire.JSON, cty.StringVal("hello"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(b) != `"hello"` {
		t.Errorf("got %q, want %q", b, `"hello"`)
	}
}

func TestSerialize_CtyObject(t *testing.T) {
	val := cty.ObjectVal(map[string]cty.Value{
		"count": cty.NumberIntVal(42),
	})
	b, err := serializeWithCtyShim(wire.Auto, val)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(b) != `{"count":42}` {
		t.Errorf("got %q, want %q", b, `{"count":42}`)
	}
}

func TestSerialize_CtyNumber(t *testing.T) {
	b, err := serializeWithCtyShim(wire.Auto, cty.NumberIntVal(99))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(b) != `99` {
		t.Errorf("got %q, want %q", b, `99`)
	}
}
