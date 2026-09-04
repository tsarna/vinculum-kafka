# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.14.0] - 2026-09-04

### Added

- **A per-record settler, on a per-partition low-water-mark commit tracker.**
  A Kafka commit is an assertion about a *prefix* — committing offset N says
  everything below N is done — which is why this consumer was the only one that
  could not settle a single delivery. It now tracks, per partition, the offset
  of the oldest record that has not completed, and commits only that.

  Each record carries a `bus.Settler` on its context, so the acknowledgement
  follows the work rather than the call that handed it on: behind an async
  queue, across a bus, or wherever a caller settles it explicitly. A settle
  marks the record complete; the mark then advances over every contiguous
  completion above it. An offset therefore never passes a record still in
  flight, whatever order completions arrive in.

  What follows from that:

  - A record nothing settles stops its partition's committed offset. Records
    after it keep being processed, and everything from it onwards is handled
    again by whoever next owns the partition.
  - A partition whose unsettled window reaches `WithMaxInFlight` (default 1024)
    stops being fetched until half of it has drained, rather than growing
    without bound. Nothing is dropped, and the pause is logged with the topic,
    partition and the mark it stopped at.
  - A settle for a partition that has been revoked reports `bus.StaleError`
    with "partition reassigned", instead of moving a mark another member of the
    group now owns.
  - Where completions arrive in order the mark is exact — the last settled
    record plus one — and a restart replays only what settled since the last
    commit. Where they do not, the mark is pinned at the oldest incomplete
    record, so every record settled above it is delivered again after a restart
    or a rebalance. An offset cannot record a hole, only where the finished
    prefix ends, and committing past the hole would lose the record in it.
    Bounded by `WithMaxInFlight`; work that settles out of order has to be
    idempotent.

  Only the mark is ever sent to Kafka, and it is sent with `CommitOffsetsSync`
  rather than `CommitRecords`, whose "commit the records I just finished" shape
  is the thing a mark replaces. That callback reports the call's own error but
  not the per-partition error codes, so a rejection for one partition — a stale
  generation, a fenced leader epoch — arrives looking like a success. It is read
  here, because a mark wrongly recorded as committed is never offered again and
  the loss would be permanent.

- `WithMaxInFlight`, bounding how far a partition's completions may run behind
  the records handed out. It is a bound on memory and on how much is reprocessed
  at a rebalance, not a throughput knob.

### Changed

- **`WithAckMode` replaces `WithCommitMode`, which is removed along with the
  `CommitMode` type and its constants.** A source-breaking rename: callers pass
  `AckAfterHandling` where they passed `CommitAfterProcess`, and `AckPeriodic`
  where they passed `CommitPeriodic`.

  `CommitManual` has no direct replacement, because it never did what it said.
  It reserved a mode nothing implemented and left franz-go's autocommit enabled,
  so it behaved as `CommitPeriodic` — pass `AckPeriodic` to keep that, or
  `AckManual` for the caller-controlled settle the name always promised.

- **Dead-lettering moved from the poll loop into the nack.** A record refused
  anywhere — including several hops downstream, by a caller that settles
  explicitly — now reaches `dlq_topic`, where before only a failure the poll
  loop saw could. The mark advances past a record only once the dead-letter
  send has succeeded, so a failure there is handled again rather than lost.

- The consumer sets `BlockRebalanceOnPoll` and handles `OnPartitionsRevoked` /
  `OnPartitionsLost`, so a rebalance can only happen between polls, a revoke
  commits what has completed before handing the partitions back, and a loss
  forgets them without committing offsets the group has already moved past.

- Each poll is bounded, so the loop commits marks that completed after the
  poll returned even on a quiet topic. `Stop` commits once more on the way out,
  which is what keeps an orderly shutdown from replaying finished work.

## [0.13.0] - 2026-09-01

### Fixed

- **`produce_mode = "async"` no longer acknowledges the inbound message before
  the broker has taken the outbound one.** The async path hands the record to
  franz-go and returns immediately, so a bridge — a receiver on one transport
  feeding a producer on Kafka — settled its inbound delivery at that moment.
  A produce that then failed had nothing left to redeliver, in exactly the
  situation where at-least-once matters most.

  The producer now reports its deliveries as `bus.Deferred` when its mode is
  async, and the produce callback settles once the broker has answered. The
  record already carried the detached context, so the callback already held the
  settler.

  The default is `sync`, which made this worse rather than better: an author
  flipping a throughput knob gave up the delivery guarantee, with nothing in
  the configuration mentioning acknowledgement. Under `sync` the produce has
  completed by the time `OnEvent` returns and its error is the outcome, so
  nothing there changes.

### Changed

- The async produce callback records its metrics against the detached context
  rather than the caller's, which by then may have been canceled — the same
  reason the record itself carries the detached one.

## [0.12.0] - 2026-08-02

### Changed

- **BREAKING: the decode-error hook's Kafka topic is keyed `kafka_topic`, not `topic`.**
  `topic` is reserved by `wire.DecodeError`'s own `Topic` field, and a consumer is
  expected to drop a colliding `Attrs` key rather than let a receiver shadow a fixed
  field. Vinculum does exactly that, so this key never reached a config at all; a
  consumer reading `e.Attrs` directly did see it, which is what makes the rename
  breaking for them.

  No information was lost either way — the dropped value duplicated `Topic` — but the
  key was unusable through any consumer that honours the reserved set, and every other
  receiver names its transport identifier after the transport (`routing_key`, `stream`,
  `entry_id`, and `mqtt_topic` since vinculum-mqtt v0.10.0).

  Consumers reading `e.Attrs["topic"]` should read `e.Attrs["kafka_topic"]`, or
  `e.Topic`, which carries the same value.

- Requires `vinculum-wire` v0.5.0 for `wire.IsReservedAttr`, which the consumer's tests
  now assert every `Attrs` key against — so a key that would be dropped by a consumer
  fails here instead of vanishing silently downstream.

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
