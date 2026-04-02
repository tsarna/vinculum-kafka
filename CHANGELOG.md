# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Distributed tracing via Kotel** — bidirectional W3C TraceContext propagation over Kafka record headers using the official `kotel` plugin (`github.com/twmb/franz-go/plugin/kotel`). When a `trace.TracerProvider` is configured on the `KafkaClient`, kotel attaches to both the producer and consumer `kgo.Client` instances as a hook:
  - **Consumer**: extracts `traceparent`/`tracestate` from inbound record headers into `r.Context`; a `vinculum.process <topic>` child span wraps the full vinculum processing time (deserialization, topic resolution, `subscriber.OnEvent`) so action evaluation time is captured in the trace.
  - **Producer**: injects `traceparent`/`tracestate` into outbound record headers from the current context span; `record.Context` is set so the kotel hook can find the parent span.
  - W3C trace headers (`traceparent`, `tracestate`, `baggage`) are filtered from the `fields` map delivered to subscribers, keeping business metadata clean.
