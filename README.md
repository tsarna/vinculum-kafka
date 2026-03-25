# vinculum-kafka

Kafka client integration for [Vinculum](https://github.com/tsarna/vinculum), built on [franz-go](https://github.com/twmb/franz-go).

Provides a `KafkaConsumer` (source) and `KafkaProducer` (sink) that integrate with the `vinculum-bus` event bus. The VCL configuration wiring lives in the main vinculum repo (`config/kafka.go`) to avoid circular imports — this module only depends on `vinculum-bus`.

---

## Packages

### `consumer` — KafkaConsumer

Runs a poll loop that reads records from Kafka and publishes them to a `bus.Subscriber`. Each consumer runs its own goroutine, belongs to a consumer group, and supports multiple topic subscriptions with per-message vinculum topic resolution.

**Features:**
- Consumer group with configurable start offset (`stored` / `earliest` / `latest`)
- Three commit modes: `after_process` (at-least-once, default), `periodic`, `manual`
- Dead-letter queue: failed records forwarded to a configurable DLQ topic with error metadata headers
- Flexible topic mapping: per-subscription `VinculumTopicFunc` resolves the vinculum topic from the Kafka topic, record key, headers, and payload
- Optional `MetricsProvider` instrumentation

**Builder example:**

```go
consumer, err := consumer.NewConsumer().
    WithBaseOpts(kgoOpts).          // shared TLS/SASL/broker opts
    WithGroupID("my-group").
    WithTarget(myBus).
    WithSubscription(consumer.TopicSubscription{
        KafkaTopic: "sensor.readings",
        VinculumTopicFunc: func(kafkaTopic string, key *string, fields map[string]string, msg any) (string, error) {
            return "sensor/readings", nil
        },
    }).
    WithCommitMode(consumer.CommitAfterProcess).
    WithDLQTopic("my-app.dlq").
    WithMetricsProvider(metricsProvider).
    WithLogger(logger).
    Build()

if err := consumer.Start(ctx); err != nil { ... }
defer consumer.Stop()
```

### `producer` — KafkaProducer

Implements `bus.Subscriber`. `OnEvent` maps vinculum topics to Kafka topics via MQTT-style pattern matching, serializes the payload to JSON (or passes `[]byte` through unchanged), converts fields to record headers, and produces the record.

**Features:**
- MQTT-style topic mapping with `+` and `#` wildcards (first match wins)
- Per-mapping partition key resolution via `KeyFunc`
- Two produce modes: `sync` (ProduceSync, default) and `async` (fire-and-forget)
- Fallback transforms for unmatched topics: `error` (default), `slash_to_dot`, `ignore`
- `cty.Value` payloads converted via `go2cty2go.CtyToAny()` before JSON marshalling
- Optional `MetricsProvider` instrumentation

**Builder example:**

```go
producer, err := producer.NewProducer().
    WithClient(kgoClient).
    WithTopicMapping(producer.TopicMapping{
        Pattern:    "sensor/+deviceId/reading",
        KafkaTopic: "sensor.readings",
        KeyFunc: func(topic string, msg any, fields map[string]string) ([]byte, error) {
            return []byte(fields["deviceId"]), nil
        },
    }).
    WithDefaultTransform(producer.DefaultTopicSlashToDot).
    WithProduceMode(producer.ProduceModeSync).
    WithMetricsProvider(metricsProvider).
    WithLogger(logger).
    Build()
```

---

## Metrics

Both packages expose instrumentation via the `o11y.MetricsProvider` interface from `vinculum-bus/o11y`. Pass `nil` to disable metrics (all methods are nil-safe).

### Consumer metrics

| Metric | Type | Labels |
|--------|------|--------|
| `kafka_consumer_records_received_total` | Counter | `topic` |
| `kafka_consumer_errors_total` | Counter | `topic` |
| `kafka_consumer_lag` | Gauge | `topic`, `partition` |
| `kafka_consumer_process_duration_seconds` | Histogram | `topic` |
| `kafka_consumer_commits_total` | Counter | — |

Consumer lag is derived from `FetchPartition.HighWatermark` at the end of each poll cycle. Caught-up partitions report lag = 0.

### Producer metrics

| Metric | Type | Labels |
|--------|------|--------|
| `kafka_producer_records_sent_total` | Counter | `topic` |
| `kafka_producer_errors_total` | Counter | `topic` |
| `kafka_producer_produce_duration_seconds` | Histogram | `topic` |

`produce_duration` is only recorded in `sync` mode (async mode returns before the broker ack).

---

## VCL configuration

When used via vinculum, the full `client "kafka"` block is available:

```hcl
server "metrics" "metrics" {
  listen = ":9090"
}

client "kafka" "events" {
  brokers = ["broker1:9092", "broker2:9092"]

  tls { enabled = true }

  sasl {
    mechanism = "SCRAM-SHA-256"
    username  = "vinculum"
    password  = env.KAFKA_PASSWORD
  }

  producer "main" {
    default_topic_transform = "slash_to_dot"
  }

  consumer "main" {
    group_id = "my-app"
    subscriber = bus.main

    topic_subscription {
      kafka_topic    = "sensor.readings"
      vinculum_topic = "sensor/${fields.deviceId}/reading"
    }
  }
}
```

See the [Kafka client documentation](https://github.com/tsarna/vinculum/blob/main/doc/client-kafka.md) in the main vinculum repo for the full VCL reference.

---

## Dependencies

- [`github.com/twmb/franz-go`](https://github.com/twmb/franz-go) — Kafka client (no CGo, static binary)
- [`github.com/tsarna/vinculum-bus`](https://github.com/tsarna/vinculum-bus) — `Subscriber`, `EventBus`, `o11y` interfaces
- [`github.com/zclconf/go-cty`](https://github.com/zclconf/go-cty) — cty value support in producer payloads
- [`go.uber.org/zap`](https://pkg.go.dev/go.uber.org/zap) — structured logging

## License

MIT — see [LICENSE](LICENSE).
