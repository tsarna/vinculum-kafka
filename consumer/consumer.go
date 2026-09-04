package consumer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	bus "github.com/tsarna/vinculum-bus"
	wire "github.com/tsarna/vinculum-wire"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.uber.org/zap"
)

// commitInterval bounds how long a low-water mark may sit uncommitted while
// nothing is arriving. Records settle wherever the work finishes, so a
// completion routinely lands after the poll that produced it has returned; on a
// quiet topic the next poll may be a long way off, and the mark would wait for
// it. So a poll is given this long to produce something before the loop goes
// round to commit what has completed in the meantime.
const commitInterval = 5 * time.Second

// shutdownCommitTimeout bounds the final commit Stop makes on its way out.
//
// Equal to commitInterval by coincidence rather than by construction: this one
// is how long a shutdown is willing to wait for a broker that has stopped
// answering, and it wants to be short for the same reason any teardown does.
// They are separate constants so that changing the heartbeat does not silently
// change how long a process takes to exit.
const shutdownCommitTimeout = 5 * time.Second

// VinculumTopicFunc resolves the vinculum topic for an inbound Kafka record.
// It is called per message. kafkaTopic is the source Kafka topic; key is the
// record key decoded as UTF-8, or nil if the record has no key; fields are
// populated from record headers; msg is the deserialized payload.
//
// Constructed by the config layer to avoid a circular import dependency.
type VinculumTopicFunc func(kafkaTopic string, key *string, fields map[string]string, msg any) (string, error)

// TopicSubscription maps one Kafka topic to a vinculum topic resolver.
type TopicSubscription struct {
	// KafkaTopic is the exact Kafka topic name (no wildcards).
	KafkaTopic        string
	VinculumTopicFunc VinculumTopicFunc
}

// KafkaConsumer runs a poll loop that reads records from Kafka and publishes
// them to a bus.Subscriber. Create via NewConsumer().Build().
//
// KafkaConsumer is a source, not a sink — it does NOT implement bus.Subscriber.
type KafkaConsumer struct {
	client        *kgo.Client
	subscriptions []TopicSubscription
	subscriber    bus.Subscriber
	ackMode       AckMode
	tracker       *tracker
	dlqTopic      string // optional; if non-empty, failed records are produced here
	wireFormat    wire.WireFormat
	onDecodeError wire.DecodeErrorHook
	logger        *zap.Logger
	metrics       *ConsumerMetrics

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

// Stop signals the poll goroutine to exit, waits for it to finish, commits
// whatever completed while it was winding down, then closes the underlying
// kgo.Client.
//
// The final commit is what keeps an orderly shutdown from replaying work. By
// the time a receiver is stopped the pipeline in front of it has been drained,
// so completions have landed that the loop exited before committing — and every
// mark left behind is a record the next process handles a second time.
func (c *KafkaConsumer) Stop() error {
	if c.cancel != nil {
		c.cancel()
	}
	if c.done != nil {
		<-c.done
	}

	if c.client != nil {
		// A context of its own: the poll context is cancelled by now, and this
		// commit is the reason for waiting rather than something the wait
		// should skip.
		ctx, cancel := context.WithTimeout(context.Background(), shutdownCommitTimeout)
		c.commitMarks(ctx)
		cancel()

		c.client.Close()
	}
	return nil
}

func (c *KafkaConsumer) pollLoop(ctx context.Context) {
	defer close(c.done)

	for {
		// Bounding the poll is what gives the loop a heartbeat. A completion
		// that lands after its poll returned has moved a mark nothing else will
		// notice, and on a quiet topic an unbounded poll would sit on it until
		// the next record arrived. Everything below runs either way.
		pollCtx, cancel := context.WithTimeout(ctx, commitInterval)
		fetches := c.client.PollFetches(pollCtx)
		cancel()

		if ctx.Err() != nil {
			// Under BlockRebalanceOnPoll a poll registers this goroutine as a
			// poller and nothing in the group may move until it says otherwise
			// — including leaving the group. Returning without this deadlocks
			// the shutdown that asked for it.
			c.client.AllowRebalance()
			return
		}

		// A deadline is this loop's own heartbeat, not a fetch failure: franz-go
		// injects the context error as a fake fetch, and reporting it would log
		// once every commitInterval on an idle topic.
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			c.logger.Error("kafka consumer: poll error", zap.Error(err))
			// continue — transient errors should not kill the loop
		}

		// The error is not logged here. Every failure a record can have ends in
		// a nack, and the nack names the record, the reason and what became of
		// it — from wherever the refusal came, including hops this loop cannot
		// see. Reporting again from here would say the same thing twice about
		// the subset that happened to fail in front of it.
		fetches.EachRecord(func(r *kgo.Record) {
			_ = c.processRecord(ctx, r)
		})

		c.commitMarks(ctx)
		c.applyPressure()
		c.updateLag(ctx, fetches)

		// Paired with BlockRebalanceOnPoll: a rebalance may only happen between
		// polls, so every record from this fetch has been dispatched and its
		// settler registered before the group can take a partition away.
		c.client.AllowRebalance()
	}
}

// commitTo is the one place this consumer advances offsets in Kafka. Nothing
// else calls the client's commit methods, so making commits transactional means
// replacing this body with sendOffsetsToTransaction rather than finding every
// place a record happens to finish. Exactly-once would need a transactional id
// per consumer instance, which is a decision this library has not taken; the
// seam is the half of it that was worth taking up front.
//
// It returns the marks Kafka accepted, which is not always all of them, and an
// error describing every one it refused.
func (c *KafkaConsumer) commitTo(ctx context.Context, offsets map[string]map[int32]kgo.EpochOffset) (map[string]map[int32]kgo.EpochOffset, error) {
	// Sync rather than async, and franz-go is emphatic about why: an async
	// commit issued around a rebalance proceeds after the rebalance has, so it
	// writes offsets for partitions another member now owns. The loop is
	// between polls here and has nothing else to do.
	var (
		accepted map[string]map[int32]kgo.EpochOffset
		refused  []error
	)
	c.client.CommitOffsetsSync(ctx, offsets,
		func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			accepted, refused = commitResult(offsets, resp, err)
		})
	return accepted, errors.Join(refused...)
}

// commitResult reads one commit response, returning the marks Kafka accepted
// and one error per mark it refused.
//
// It is separate from commitTo, and pure, because it is the part that can be
// wrong in a way nothing would notice: a mark wrongly recorded as accepted is
// never offered again, so the loss is permanent and silent. Given a response it
// can be checked directly, without a broker to produce one.
//
// The per-partition error codes have to be read as well as the call's own
// error: a commit can be refused for one partition — a stale generation, a
// fenced leader epoch — while the request itself succeeded, and
// CommitOffsetsSync's callback reports only the latter.
//
// A refusal is per partition, so the answer is too. Reporting one refusal as a
// whole failed commit would leave every mark in the same request outstanding,
// and the loop would offer them all again on every pass for as long as the one
// bad partition kept failing.
func commitResult(
	offsets map[string]map[int32]kgo.EpochOffset,
	resp *kmsg.OffsetCommitResponse,
	err error,
) (accepted map[string]map[int32]kgo.EpochOffset, refused []error) {
	if err != nil {
		// The request itself did not land, so nothing in it was accepted.
		return nil, []error{err}
	}
	if resp == nil {
		return nil, nil
	}

	for _, topic := range resp.Topics {
		for _, partition := range topic.Partitions {
			if perr := kerr.ErrorForCode(partition.ErrorCode); perr != nil {
				refused = append(refused, fmt.Errorf("commit %s[%d]: %w",
					topic.Topic, partition.Partition, perr))
				continue
			}
			eo, ok := offsets[topic.Topic][partition.Partition]
			if !ok {
				continue // not a partition this commit asked about
			}
			if accepted == nil {
				accepted = make(map[string]map[int32]kgo.EpochOffset)
			}
			byPartition := accepted[topic.Topic]
			if byPartition == nil {
				byPartition = make(map[int32]kgo.EpochOffset)
				accepted[topic.Topic] = byPartition
			}
			byPartition[partition.Partition] = eo
		}
	}
	return accepted, refused
}

// commitMarks commits every partition's low-water mark that has moved since
// Kafka last accepted it.
//
// A mark Kafka refused is not recorded, so the next pass offers it again, and a
// partition whose commit failed does not hold back one whose commit landed.
// Nothing is lost by a commit that never lands: the records below the mark have
// been handled, and the worst case is that they are handled a second time.
func (c *KafkaConsumer) commitMarks(ctx context.Context) {
	if c.tracker == nil {
		return // AckPeriodic: franz-go's own timer owns the offsets
	}

	offsets := c.tracker.commitReady()
	if len(offsets) == 0 {
		return
	}

	accepted, err := c.commitTo(ctx, offsets)
	if len(accepted) > 0 {
		c.tracker.noteCommitted(accepted)
		c.metrics.RecordCommit(ctx)
	}
	if err != nil && ctx.Err() == nil {
		c.logger.Error("kafka consumer: commit failed", zap.Error(err))
	}
}

// applyPressure stops fetching partitions whose completions have run too far
// behind the records handed out, and starts again once they catch up.
func (c *KafkaConsumer) applyPressure() {
	if c.tracker == nil {
		return
	}

	pause, resume := c.tracker.pressure()
	if len(pause) > 0 {
		// Each partition is named with the mark it stopped at, which is the
		// record everything behind the bound is waiting on.
		c.logger.Warn("kafka consumer: pausing fetches; unsettled records have "+
			"reached the in-flight bound",
			zap.Array("partitions", pause))
		c.client.PauseFetchPartitions(pause.partitions())
	}
	if len(resume) > 0 {
		// Debug rather than Info: a resume is the bound working, and it is the
		// half of the pair that repeats — a partition that keeps filling and
		// draining crosses back every time it recovers.
		c.logger.Debug("kafka consumer: resuming fetches",
			zap.Any("partitions", resume))
		c.client.ResumeFetchPartitions(resume)
	}
}

// dropPartitions forgets partitions this member no longer holds, resuming any
// the tracker had paused.
//
// A pause is client state that no rebalance clears, so one left behind by a
// dropped partition would still be in force if the group handed the partition
// back — and with no entry to notice, nothing would ever ask for it to be
// resumed. Resuming a partition this member no longer holds costs nothing: it
// only takes the partition off the client's paused list.
func (c *KafkaConsumer) dropPartitions(cl *kgo.Client, partitions map[string][]int32) {
	resume := c.tracker.drop(partitions)
	if len(resume) == 0 {
		return
	}
	c.logger.Info("kafka consumer: resuming fetches for partitions dropped while paused",
		zap.Any("partitions", resume))
	cl.ResumeFetchPartitions(resume)
}

// updateLag calculates consumer lag from the fetch result and updates the lag gauge.
// kgo.Client.Lag() is not available in v1.18.1, so lag is derived from HighWatermark
// and the offset of the last record in each fetched partition.
// For caught-up partitions (no records returned), lag is reported as 0.
func (c *KafkaConsumer) updateLag(ctx context.Context, fetches kgo.Fetches) {
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		// franz-go reports a poll's own error as a fake fetch with no topic and
		// a partition of -1. The bounded poll produces one of those every
		// commitInterval on a quiet topic, and recording it would report lag
		// for a partition that does not exist — forever, since a gauge series
		// outlives the sample that created it.
		if p.Partition < 0 {
			return
		}

		var lag int64
		if len(p.Records) > 0 {
			lastOffset := p.Records[len(p.Records)-1].Offset
			if lag = p.HighWatermark - (lastOffset + 1); lag < 0 {
				lag = 0
			}
		}
		c.metrics.UpdateLag(ctx, p.Topic, p.Partition, lag)
	})
}

func (c *KafkaConsumer) processRecord(ctx context.Context, r *kgo.Record) error {
	// Registered before anything can fail, so every path below has something to
	// settle. A record rejected here never reaches the configuration, so the
	// configuration cannot be what answers for it.
	settler := c.newSettler(r)

	fields := headersToFields(r.Headers)
	var key *string
	if r.Key != nil {
		s := string(r.Key)
		key = &s
	}
	// A decode failure is fatal to the record: the configured wire format is
	// a contract, so a value that doesn't satisfy it is rejected rather than
	// delivered as raw bytes. Use wire format "auto" for best-effort decoding.
	// The record is nacked, which dead-letters it when dlq_topic is set and
	// otherwise stops the partition's committed offset at it.
	var msg any
	if r.Value != nil {
		var deserErr error
		msg, deserErr = c.wireFormat.Deserialize(r.Value)
		if deserErr != nil {
			c.logger.Error("kafka consumer: deserialize failed",
				zap.String("topic", r.Topic),
				zap.String("wire_format", c.wireFormat.Name()),
				zap.Error(deserErr))
			c.metrics.RecordError(ctx, r.Topic, "deserialize")
			if c.onDecodeError != nil {
				attrs := map[string]string{
					// Named for the transport, not for the concept: "topic"
					// is reserved by DecodeError's own Topic field and would
					// be dropped by a consumer honouring the reserved set.
					"kafka_topic": r.Topic,
					"partition":   strconv.FormatInt(int64(r.Partition), 10),
					"offset":      strconv.FormatInt(r.Offset, 10),
				}
				if key != nil {
					attrs["key"] = *key
				}
				c.onDecodeError(ctx, wire.DecodeError{
					Raw:    r.Value,
					Err:    deserErr,
					Format: c.wireFormat.Name(),
					Topic:  r.Topic,
					Fields: fields,
					Attrs:  attrs,
				})
			}
			err := fmt.Errorf("kafka consumer: deserialize value for %q: %w", r.Topic, deserErr)
			c.nackOwn(ctx, r, settler, err.Error())
			return err
		}
	}

	sub, err := c.findSubscription(r.Topic)
	if err != nil {
		c.metrics.RecordError(ctx, r.Topic, "subscription")
		c.nackOwn(ctx, r, settler, err.Error())
		return err
	}

	vinculumTopic, err := sub.VinculumTopicFunc(r.Topic, key, fields, msg)
	if err != nil {
		c.metrics.RecordError(ctx, r.Topic, "vinculum_topic")
		err = fmt.Errorf("kafka consumer: resolve vinculum topic for %q: %w", r.Topic, err)
		c.nackOwn(ctx, r, settler, err.Error())
		return err
	}

	// Use the record context as the parent — kotel has already extracted the
	// remote trace context from the record headers, created a new root span
	// linked to the producer span, and stored it in r.Context. This makes the
	// vinculum processing span a child of that new root (i.e. a separate trace
	// from the producer, linked rather than parented). Fall back to the poll
	// context if r.Context is nil.
	recCtx := r.Context
	if recCtx == nil {
		recCtx = ctx
	}

	// Create a span covering the full vinculum processing time (topic resolution,
	// deserialization, and subscriber.OnEvent including action evaluation).
	tracer := otel.GetTracerProvider().Tracer("vinculum-kafka/consumer")
	recCtx, span := tracer.Start(recCtx, "vinculum.process "+vinculumTopic)
	defer span.End()

	// A record's acknowledgement is a property of the record, and `fields`
	// cannot carry it — the bus rewrites those per subscription. The context
	// can, and the async queue preserves it across its goroutine hop, so this is
	// what lets a subscription several hops downstream settle the record it
	// handled.
	recCtx = bus.WithSettler(recCtx, settler)

	start := time.Now()
	err = c.subscriber.OnEvent(recCtx, vinculumTopic, msg, fields)
	c.metrics.RecordProcessDuration(ctx, r.Topic, time.Since(start))

	// The settle point. Under AckAfterHandling this completes a subscriber that
	// handled the record and leaves one that only queued it to settle at its own
	// completion; under AckManual it nacks a failure and otherwise waits for the
	// configuration. Under AckPeriodic there is no settler and this does
	// nothing — offsets move on franz-go's timer — so the dead letter that mode
	// still owes is issued below.
	bus.SettleOnReturn(recCtx, c.subscriber, err)

	if err != nil {
		// The nack above reports the failure, so nothing is logged here. The
		// one case it does not cover is a delivery something had already
		// settled — an explicit ack followed by a failure, which is a decision
		// rather than an accident — and the span and the error counter below
		// record it either way.
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		c.metrics.RecordError(ctx, r.Topic, "subscriber")
		if settler == nil {
			c.nackOwn(recCtx, r, nil, err.Error())
		}
		return err
	}
	c.metrics.RecordReceived(ctx, r.Topic)
	return nil
}

// deadLetter reproduces a record to dlq_topic, annotated with why it could not
// be handled. Reached from a nack, so a record refused three bus hops from here
// is dead-lettered the same way as one this receiver refused itself.
func (c *KafkaConsumer) deadLetter(ctx context.Context, r *kgo.Record, reason string) error {
	return c.client.ProduceSync(ctx, c.buildDLQRecord(r, reason)).FirstErr()
}

// buildDLQRecord constructs the record to be produced to the DLQ topic.
// Kept separate from deadLetter so the record structure can be tested without a
// kgo.Client.
//
// reason is a string rather than an error because a nack carries one: the
// settle vocabulary describes why a record was refused, and that description
// reaches here from wherever the refusal happened.
func (c *KafkaConsumer) buildDLQRecord(r *kgo.Record, reason string) *kgo.Record {
	extra := []kgo.RecordHeader{
		{Key: "vinculum-error", Value: []byte(reason)},
		{Key: "vinculum-original-topic", Value: []byte(r.Topic)},
		{Key: "vinculum-timestamp", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	}
	headers := make([]kgo.RecordHeader, 0, len(r.Headers)+len(extra))
	headers = append(headers, r.Headers...)
	headers = append(headers, extra...)

	return &kgo.Record{
		Topic:   c.dlqTopic,
		Key:     r.Key,
		Value:   r.Value,
		Headers: headers,
	}
}

func (c *KafkaConsumer) findSubscription(kafkaTopic string) (*TopicSubscription, error) {
	for i := range c.subscriptions {
		if c.subscriptions[i].KafkaTopic == kafkaTopic {
			return &c.subscriptions[i], nil
		}
	}
	return nil, fmt.Errorf("kafka consumer: no subscription found for topic %q", kafkaTopic)
}

// traceHeaders is the set of W3C trace context header keys injected by kotel.
// These are filtered from the fields map to keep business metadata clean.
var traceHeaders = map[string]struct{}{
	"traceparent": {},
	"tracestate":  {},
	"baggage":     {},
}

// headersToFields converts Kafka record headers to a string map, filtering out
// W3C trace context headers (traceparent, tracestate, baggage) since those are
// extracted into the Go context by kotel and should not appear as business fields.
// Returns nil (not an empty map) when there are no non-trace headers.
func headersToFields(headers []kgo.RecordHeader) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	m := make(map[string]string, len(headers))
	for _, h := range headers {
		if _, isTrace := traceHeaders[h.Key]; !isTrace {
			m[h.Key] = string(h.Value)
		}
	}
	if len(m) == 0 {
		return nil
	}
	return m
}
