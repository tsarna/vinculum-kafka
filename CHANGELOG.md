# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
