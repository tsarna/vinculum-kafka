# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.11.0] - 2026-07-19

### Changed

- **BREAKING: deserialize failures are no longer swallowed.** `KafkaConsumer.processRecord`
  used to log a warning and pass the **raw bytes** through as the message payload when the
  configured wire format failed to decode. A decode failure is now fatal to the record:
  `processRecord` returns an error and the record is not delivered.

  **Configure `WithDLQTopic`.** A failed record's offset is not committed. With a DLQ topic
  set, the record is routed there and the offset advances; without one the consumer
  re-fetches the same record forever and the partition never makes progress — every later
  message on it is starved.

  Callers wanting best-effort decoding should use `wire.Auto`, which never fails (it yields
  a `string` for anything it can't parse as JSON). Note that is not an exact replacement:
  the old fallback produced `[]byte`, so a subscriber that type-switches on `[]byte` must
  be adjusted.

- **BREAKING: `ConsumerMetrics.RecordError` takes an `errType` argument** —
  `RecordError(ctx, topic, errType)` — recorded as the `error.type` attribute. Existing
  call sites pass `"subscription"`, `"vinculum_topic"`, or `"subscriber"`.

- Requires `github.com/tsarna/vinculum-wire` v0.3.0 for the `DecodeError` /
  `DecodeErrorHook` types.

### Added

- `WithDecodeErrorHook(wire.DecodeErrorHook)` on the consumer builder. The hook observes a
  decode failure — it receives the raw bytes, the error, the format name, and the record's
  topic, partition, offset, and key — but cannot suppress it: the record is treated as
  failed either way. nil (the default) means no observer.

- Deserialize failures are recorded on the error counter with
  `error.type = "deserialize"`.

## [0.10.0] - 2026-05-27

### Changed

- Changed license to Apache-2.0

## [0.9.3] - 2026-04-23

### Changed

- **Topic matching routes through `vinculum-bus/topicmatch`** — producer pattern routing now honors MQTT 5.0 §4.7.2: filters starting with `+` or `#` no longer match reserved `$`-prefixed topics. Exact and `$`-prefixed patterns are unaffected. Requires vinculum-bus v0.12.0.

## [0.9.2] - 2026-04-22

### Fixed

- **Async producer context cancellation** — async mode now uses `context.WithoutCancel` so a canceled caller context (e.g. completed HTTP request) does not cause franz-go to fail the entire produce batch. Trace context values are preserved. Sync mode is unchanged — it correctly uses the caller's context for cancellation.

## [0.9.1] - 2026-04-19

### Added

- **`vinculum.client.name` metric attribute** — all producer and consumer metrics now carry a `vinculum.client.name` attribute identifying the vinculum client block. Builders accept `WithClientName(name)` to set it.

## [0.9.0] - 2026-04-17

### Added

- **Pluggable wire format support** — producer and consumer builders now accept `WithWireFormat(wire.WireFormat)` or `WithWireFormatName(name)` to control payload serialization/deserialization. Built-in formats: `auto` (default), `json`, `string`, `bytes`. The default `auto` preserves backward compatibility. Depends on `github.com/tsarna/vinculum-wire` v0.1.0.

### Changed

- **Strings serialize verbatim in auto mode** — the `auto` wire format passes strings through unchanged (not JSON-encoded). Previously, strings were JSON-encoded with quotes. Use `wire_format = "json"` for the old behavior.

### Removed

- **Inline `serializePayload` / `deserializePayload` functions** — replaced by the shared `vinculum-wire` module.
- **`go2cty2go` and `go-cty` dependencies** — cty conversion now handled by vinculum's `CtyWireFormat` decorator at the config layer.

## [0.8.0] - 2026-04-08

### Changed

- **OTel metrics replaces o11y.MetricsProvider abstraction** — producer and consumer now accept `metric.MeterProvider` directly via `WithMeterProvider()` (replacing `WithMetricsProvider(o11y.MetricsProvider)`). Metric names follow OTel semantic conventions: `messaging.client.sent.messages`, `messaging.client.consumed.messages`, `messaging.client.operation.duration`, `messaging.process.duration` where applicable; `kafka.producer.errors`, `kafka.consumer.errors`, `kafka.consumer.lag`, `kafka.consumer.commits` for Kafka-specific metrics. All metrics carry `messaging.system=kafka` and `messaging.destination.name` attributes. Requires vinculum-bus v0.11.0.

## [0.7.0] - 2026-04-03

### Changed

- **OTel span links for Kafka consumer traces** — the kotel tracer is now configured with `kotel.LinkSpans()`, following the [OTel messaging semantic conventions](https://opentelemetry.io/docs/specs/otel/trace/semantic_conventions/messaging/) recommendation for pub/sub systems. Consumer spans are now new trace roots linked to the producer span rather than children of it, correctly representing the asynchronous boundary.

## [0.6.0] - 2026-04-02

### Added

- **Distributed tracing via Kotel** — bidirectional W3C TraceContext propagation over Kafka record headers using the official `kotel` plugin (`github.com/twmb/franz-go/plugin/kotel`). When a `trace.TracerProvider` is configured on the `KafkaClient`, kotel attaches to both the producer and consumer `kgo.Client` instances as a hook:
  - **Consumer**: extracts `traceparent`/`tracestate` from inbound record headers into `r.Context`; a `vinculum.process <topic>` child span wraps the full vinculum processing time (deserialization, topic resolution, `subscriber.OnEvent`) so action evaluation time is captured in the trace.
  - **Producer**: injects `traceparent`/`tracestate` into outbound record headers from the current context span; `record.Context` is set so the kotel hook can find the parent span.
  - W3C trace headers (`traceparent`, `tracestate`, `baggage`) are filtered from the `fields` map delivered to subscribers, keeping business metadata clean.
