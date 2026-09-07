package consumer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
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
	// groupID names this consumer in everything it reports. A process runs one
	// per `receiver` block and a teardown phase logs whichever failed, so a
	// message that does not say which one leaves an operator with nothing to
	// act on — the other receivers in this family all name their queue or
	// stream, and this is the equivalent.
	groupID       string
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

	// Two cancels, because stopping is two things and a graceful shutdown wants
	// them apart. stopRead ends the poll loop; stopWork cancels the context
	// every record and every settle rides on, and so is the one that ends the
	// consumer. stopRead's context is derived from stopWork's, so cancelling
	// work ends polling too.
	mu       sync.Mutex
	stopRead context.CancelFunc
	stopWork context.CancelFunc
	done     chan struct{}

	// drained says this consumer has been told to stop consuming, and is the
	// reason Start refuses afterwards. Set under mu, which is what makes it a
	// guarantee rather than a hint.
	drained bool

	// unsettled counts records handed out and not yet settled. See Unsettled.
	unsettled atomic.Int64

	// stillDelivering is the channel a timed-out Drain was waiting on, closed
	// when the loop finally finishes. Nil until the first drain, and set by
	// every drain rather than only by one that gives up — what makes it answer
	// "no" is the channel being closed, not the field being absent.
	stillDelivering atomic.Pointer[chan struct{}]
}

// Unsettled reports how many records this consumer has handed out that nothing
// has settled yet.
//
// It is not the tracker's in-flight window, which is `highest - base` and stays
// wide while a refused record pins a partition's mark — deliberately, and
// possibly forever. That number is about Kafka's offsets; this one is about
// whether a settle is still coming, which is the narrower thing a shutdown can
// usefully wait for. Under `ack = "periodic"` there is no settler and nothing
// to count: franz-go's own timer owns the offsets.
func (c *KafkaConsumer) Unsettled() int { return int(c.unsettled.Load()) }

// stillRunning reports whether a delivery a drain gave up on is running *now*,
// rather than whether one ever was.
func (c *KafkaConsumer) stillRunning() bool {
	ch := c.stillDelivering.Load()
	if ch == nil {
		return false
	}
	select {
	case <-*ch:
		return false
	default:
		return true
	}
}

// Start launches the poll goroutine and returns immediately. The goroutine
// runs until ctx is cancelled or Stop is called.
func (c *KafkaConsumer) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Refused after a drain, whether or not this consumer had started when the
	// drain reached it. Taking the decision and the registration under one lock
	// is what makes it a guarantee rather than advice — a drain landing between
	// a caller's check and its call would otherwise put the process back to
	// consuming during the phase that waits for it to have finished.
	//
	// It also covers the loop a timed-out Stop abandoned: that loop still owns
	// `done`, and a second one over the top would leave the next Stop waiting
	// forever on it. There is no way to reach that state except through a
	// Drain, so this one flag answers for both.
	//
	// Checked before "already started", because after a drain it is the more
	// specific and more useful answer: a drained consumer is still started
	// until Stop runs, and reporting that would send a caller looking for a
	// double-Start that did not happen.
	if c.drained {
		return fmt.Errorf("kafka consumer %q: drained, not restarting", c.groupID)
	}
	if c.stopWork != nil {
		return fmt.Errorf("kafka consumer %q: already started", c.groupID)
	}

	workCtx, stopWork := context.WithCancel(ctx)
	readCtx, stopRead := context.WithCancel(workCtx)
	c.stopWork = stopWork
	c.stopRead = stopRead
	c.done = make(chan struct{})
	go c.pollLoop(readCtx, workCtx, c.done)
	return nil
}

// Drain stops polling for new records and waits for the loop to finish and
// commit what it is holding. It leaves everything else alone: the client stays
// open, the group session stays alive, and every settler already handed out
// stays valid — so a record still travelling through a queue downstream settles
// normally when the work lands, and the mark it moves is still committable.
//
// That is the whole difference between draining and stopping. Closing the
// client leaves the group, which hands this consumer's partitions to somebody
// else; the offsets not yet committed are then replayed there. Draining is what
// lets the work in flight finish and be committed first.
//
// Bounded by ctx, which the caller sizes: delivery runs user-supplied work.
//
// Terminal for the consumer: Start refuses afterwards. Draining is a shutdown,
// not a pause.
//
// Safe to call before Start, after Stop, or twice — though a second call after
// one that timed out reports the timeout again rather than a clean drain, since
// the record it gave up on is still being handled.
func (c *KafkaConsumer) Drain(ctx context.Context) error {
	c.mu.Lock()
	c.drained = true

	stopRead := c.stopRead
	if stopRead == nil {
		c.mu.Unlock()
		if c.stillRunning() {
			return fmt.Errorf("kafka consumer %q: still delivering", c.groupID)
		}
		return nil
	}
	c.stopRead = nil
	stopRead()

	done := c.done
	// Published under the same lock that cleared stopRead, because the two
	// together are what a concurrent second Drain reads. Between them it would
	// see the field already taken and no waiter yet, and report a clean drain
	// that has not happened.
	if done != nil {
		c.stillDelivering.Store(&done)
	}
	c.mu.Unlock()

	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("kafka consumer %q: drain: %w", c.groupID, ctx.Err())
	}
}

// Stop signals the poll goroutine to exit, waits for it to finish, commits
// whatever completed while it was winding down, then closes the underlying
// kgo.Client.
//
// The final commit is what keeps an orderly shutdown from replaying work. By
// the time a receiver is stopped the pipeline in front of it has been drained,
// so completions have landed that the loop exited before committing — and every
// mark left behind is a record the next process handles a second time.
// It waits for the loop, so a record still being handled finishes and settles
// normally — with one exception. A Drain that timed out has already given that
// record a bounded chance to finish, and it did not take it; waiting here would
// hand the same expression a second wait with no bound at all. So Stop cancels
// and reports rather than blocking, because the one thing a stuck action must
// never be able to do is stop the process from exiting. Whether it is *still*
// running is checked here rather than remembered from the drain, a whole phase
// earlier.
func (c *KafkaConsumer) Stop() error {
	c.mu.Lock()
	stopWork := c.stopWork
	done := c.done
	c.stopRead, c.stopWork = nil, nil
	c.mu.Unlock()

	if stopWork != nil {
		// Cancelling work cancels polling with it: the read context is derived
		// from this one, so a Stop that was not preceded by a Drain still ends
		// the loop.
		stopWork()
	}

	abandoned := c.stillRunning()
	if !abandoned && done != nil {
		<-done
	}

	if abandoned && c.client != nil {
		// The abandoned goroutine is not in a poll — it is in the dispatch
		// between one, holding the poller registration a poll takes and the
		// AllowRebalance at the end of the loop gives back. Under
		// BlockRebalanceOnPoll that registration is what LeaveGroup waits on,
		// so closing the client below would block on the same expression the
		// drain already gave up on, and the bound would have bought nothing.
		// Releasing it here is what lets the process leave.
		//
		// Calling it from here rather than from the poller is a contract
		// violation by franz-go's own description, and it is tolerated rather
		// than blessed: allowRebalance masks the poller count and broadcasts
		// with no goroutine affinity, and unaddPoller guards the underflow when
		// the abandoned loop eventually calls it too. The alternative is a
		// process that cannot exit.
		//
		// A no-op under `ack = "periodic"`, which does not set
		// BlockRebalanceOnPoll — so this needs no mode guard.
		c.client.AllowRebalance()
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

	// Reported after the commit and the close, not instead of them. A record
	// nobody finished still leaves marks worth committing and a client worth
	// closing; what the caller needs to know is that the process left with work
	// outstanding, which on Kafka means those offsets are replayed wherever the
	// partitions land next.
	//
	// Worth naming, because it is new: on this path the abandoned loop is still
	// live, so once its action finally returns it runs its own commit, its own
	// lag update and one more poll — concurrently with, or after, the two above.
	// All of that is safe rather than lucky: franz-go serialises commits behind
	// its own mutex, and a poll on a closed client returns ErrClientClosed
	// instead of touching anything. Every other path waits, so this is the one
	// place two goroutines are in the client at once.
	if abandoned {
		return fmt.Errorf("kafka consumer %q: stopped with a record still being handled", c.groupID)
	}
	return nil
}

// pollLoop reads records and dispatches them until reading is stopped.
//
// The two contexts are the same lifetime until a drain separates them. readCtx
// bounds the poll and decides when the loop exits; workCtx is what every record
// and every settle runs on, and outlives readCtx by the length of the shutdown.
// Passing readCtx to a record would mean draining cancelled the work it was
// waiting for, and cancelled the commit that work was about to earn.
func (c *KafkaConsumer) pollLoop(readCtx, workCtx context.Context, done chan struct{}) {
	defer close(done)

	for {
		// Bounding the poll is what gives the loop a heartbeat. A completion
		// that lands after its poll returned has moved a mark nothing else will
		// notice, and on a quiet topic an unbounded poll would sit on it until
		// the next record arrived. Everything below runs either way.
		pollCtx, cancel := context.WithTimeout(readCtx, commitInterval)
		fetches := c.client.PollFetches(pollCtx)
		cancel()

		if workCtx.Err() != nil {
			// A hard stop, and the one case where records this poll returned
			// are abandoned: the context they would be handled on is already
			// cancelled, so handling them would fail and settling them would
			// fail after that. They are uncommitted, so they are replayed.
			//
			// Under BlockRebalanceOnPoll a poll registers this goroutine as a
			// poller and nothing in the group may move until it says otherwise
			// — including leaving the group. Returning without this deadlocks
			// the shutdown that asked for it.
			c.client.AllowRebalance()
			return
		}

		// A deadline is this loop's own heartbeat, not a fetch failure: franz-go
		// injects the context error as a fake fetch, and reporting it would log
		// once every commitInterval on an idle topic. A cancellation is the
		// same kind of non-event, and is how a drain ends the poll it
		// interrupted.
		if err := fetches.Err(); err != nil &&
			!errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			c.logger.Error("kafka consumer: poll error", zap.Error(err))
			// continue — transient errors should not kill the loop
		}

		// The error is not logged here. Every failure a record can have ends in
		// a nack, and the nack names the record, the reason and what became of
		// it — from wherever the refusal came, including hops this loop cannot
		// see. Reporting again from here would say the same thing twice about
		// the subset that happened to fail in front of it.
		fetches.EachRecord(func(r *kgo.Record) {
			_ = c.processRecord(workCtx, r)
		})

		c.commitMarks(workCtx)
		c.applyPressure()
		c.updateLag(workCtx, fetches)

		// Paired with BlockRebalanceOnPoll: a rebalance may only happen between
		// polls, so every record from this fetch has been dispatched and its
		// settler registered before the group can take a partition away.
		c.client.AllowRebalance()

		// Checked here rather than beside the hard stop above, and that
		// placement is the whole of what draining adds. A drain cancels the
		// read context while a poll is in flight, so the poll returns whatever
		// it had — and testing for it before dispatch would throw those records
		// away. They are dispatched, they settle, and the marks they move are
		// committed by the call above; only then does the loop leave.
		//
		// It leaves only on a poll that produced nothing, which is not
		// impatience but franz-go's bookkeeping. Under default autocommit —
		// `ack = "periodic"`, the one mode that uses it — a poll's offsets are
		// recorded as *dirty* and promoted to committable at the start of the
		// *next* poll. That one-poll lag is deliberate on franz-go's part and
		// is what makes its autocommit at-least-once. Leaving straight after
		// dispatching a fetch would strand that fetch's offsets dirty, and the
		// records would be handled a second time by the next process — a
		// shutdown quietly reintroducing the duplicates it exists to avoid.
		//
		// One further round is all it ever takes. A poll on a cancelled context
		// returns before franz-go fills it, so it can only come back empty —
		// but it promotes first, which is the whole reason for making it. The
		// records franz-go still holds buffered are neither dirty nor head, so
		// they are simply replayed.
		if readCtx.Err() != nil && fetches.NumRecords() == 0 {
			return
		}
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
	settler, ops := c.newSettler(r)

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

	// An observing subscriber settles nothing and defers to nobody — it saw the
	// record go past. SettleOnReturn returns without acting, so no settle is
	// coming from anywhere and this record has to be released by hand or the
	// count never comes back down. It is the only path through here that
	// reaches no settler at all: every failure above nacks, and a nack
	// releases.
	if ops != nil && bus.DispositionOf(c.subscriber) == bus.Observed {
		ops.release()
	}

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
