package consumer

import (
	"context"
	"sync/atomic"

	bus "github.com/tsarna/vinculum-bus"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// AckMode says who advances a record's offset, and when.
type AckMode int

const (
	// AckAfterHandling settles a record on the outcome of the work, wherever
	// that work finishes. The record's settler travels on its context, so a
	// subscription several bus hops downstream, or a worker behind an async
	// queue, is what moves the low-water mark — not the return of the call that
	// handed the record on. A failure nacks instead.
	//
	// The default, and the mode the receiver has always had. What changed is
	// where the acknowledgement happens: it used to mean "commit once delivery
	// returns", which is exact only while delivery is synchronous.
	AckAfterHandling AckMode = iota

	// AckManual settles nothing until the configuration calls inbound::ack() or
	// inbound::nack(), bounded by settle_timeout.
	//
	// A record this receiver never hands to the subscriber is still settled
	// here: a value that fails to decode never reaches the configuration, so
	// the configuration cannot be the thing that answers for it.
	AckManual

	// AckPeriodic delegates offsets to franz-go's own timer, which advances
	// them regardless of outcome. Nothing per-record can be settled, so records
	// carry no settler — but dlq_topic still applies, because a record that
	// failed is worth keeping even where its offset moves anyway.
	AckPeriodic
)

// recordSettleOps settles one Kafka record. The receiver builds one per record
// and puts the settler wrapping it on that record's context, so anything
// downstream can settle it without knowing it came from Kafka.
//
// It holds the record rather than only its coordinates because a nack may have
// to dead-letter, and that reproduces the key, value and headers.
type recordSettleOps struct {
	c   *KafkaConsumer
	rec *kgo.Record

	key partitionKey
	gen uint64

	// released guards the consumer's unsettled count against being decremented
	// twice for one record. The settler above deduplicates settles, but it
	// releases its claim when an op returns an error, so a failed dead-letter
	// send can be retried and reach Nack a second time.
	released atomic.Bool
}

// release drops this record from the consumer's unsettled count, once.
//
// It runs even when the op that called it failed. A record whose dead-letter
// send did not land is genuinely still unsettled, but the count exists to tell
// a shutdown when to stop waiting, and waiting longer for a broker that is
// refusing produces does not produce a settle.
func (o *recordSettleOps) release() {
	if o.released.CompareAndSwap(false, true) {
		o.c.unsettled.Add(-1)
	}
}

// Ack marks the record complete, which advances the partition's low-water mark
// over it and every contiguous completion above it. Nothing is sent to Kafka
// here: the mark is committed by the poll loop, so an offset moves on one
// goroutine however many settle from elsewhere.
func (o *recordSettleOps) Ack(_ context.Context) error {
	defer o.release()
	o.c.tracker.complete(o.key, o.rec.Offset, o.rec.LeaderEpoch, o.gen)
	return nil
}

// Nack settles the record as not handled.
//
// Kafka has no way to redeliver one record — an offset is an assertion about a
// prefix — so "not handled" has to become one of two things:
//
//   - With dlq_topic, the record is reproduced there and then marked complete,
//     so the mark advances past a failure that has been kept somewhere else. A
//     dead-letter send that itself fails returns the error and settles nothing,
//     which leaves the record for the next owner of the partition.
//   - Without one, the record is never completed, so the partition's mark stops
//     at it. Records after it keep being processed, but nothing past the gap is
//     committed, and everything from it onwards is reprocessed by whoever next
//     owns the partition. That is the at-least-once answer, and it is what this
//     receiver has always done with a failure it could not dead-letter.
//
// The second case can wedge a partition on a record that fails every time.
// tracker.pressure bounds it: once the window reaches maxInFlight the partition
// stops being fetched rather than growing without limit.
//
// Every outcome is logged here rather than by the caller, because a refusal can
// arrive from anywhere: an expression several bus hops downstream calling
// inbound::nack() is a refusal the poll loop never sees, and the settle points
// that reach here discard what a nack returns. This is the one place all of
// them pass through.
func (o *recordSettleOps) Nack(ctx context.Context, reason string) error {
	// Released whichever way this goes, including the branch that deliberately
	// leaves the mark where it is. The partition's committed offset stopping
	// here is Kafka's business and permanent; what the count tracks is whether
	// a settle is still coming, and after a nack none is.
	defer o.release()

	if o.c.dlqTopic == "" {
		o.c.logger.Warn("kafka consumer: record refused and there is no dlq_topic; "+
			"the partition's committed offset stops here",
			o.logFields(zap.String("reason", reason))...)
		o.c.metrics.RecordError(ctx, o.rec.Topic, "nack")
		return nil
	}

	if err := o.c.deadLetter(ctx, o.rec, reason); err != nil {
		o.c.logger.Error("kafka consumer: record refused and the dead-letter send failed; "+
			"the partition's committed offset stops here",
			o.logFields(zap.String("reason", reason), zap.Error(err))...)
		return err
	}
	o.c.logger.Warn("kafka consumer: record refused; dead-lettered",
		o.logFields(zap.String("reason", reason), zap.String("dlq_topic", o.c.dlqTopic))...)
	o.c.tracker.complete(o.key, o.rec.Offset, o.rec.LeaderEpoch, o.gen)
	return nil
}

// logFields names the record every line about it starts from.
func (o *recordSettleOps) logFields(extra ...zap.Field) []zap.Field {
	return append([]zap.Field{
		zap.String("topic", o.rec.Topic),
		zap.Int32("partition", o.rec.Partition),
		zap.Int64("offset", o.rec.Offset),
	}, extra...)
}

// Keepalive extends nothing and says so. A Kafka record has no per-message
// lease: nothing expires while it is outstanding, and the one clock over it is
// the group's own session and rebalance timeouts, which are not per-record and
// cannot be renewed for one.
func (o *recordSettleOps) Keepalive(_ context.Context) (bool, error) {
	return false, nil
}

// Valid reports whether this record can still be settled, by asking whether the
// tracker still holds the assignment it was fetched under.
//
// A partition that has been revoked belongs to another member of the group, and
// that member is reprocessing everything from the last committed mark. Advancing
// the mark from here would commit past records it is still working on — so a
// settle arriving after a rebalance says the partition was reassigned, which
// tells the caller what became of the message, rather than silently doing
// nothing.
//
// A window can also be retired without the partition moving, when the log is
// truncated or expires beneath it; the tracker says which of the two happened.
// Saying no is also where the record stops being this consumer's to settle, so
// it is where the unsettled count lets go of it. The settler asks this before
// every settle *and before every keepalive*, and abandons the record when the
// answer is no — never reaching Ack or Nack, and so never reaching the release
// those two carry. Without this the count would keep a record whose partition
// has gone to another member, and keep it forever: every later shutdown would
// then spend its whole budget waiting for a settle that cannot be made.
//
// Two callers means this can fire twice for one record — a keepalive that finds
// it stale, then a settle that finds the same — which is one of the two reasons
// release is guarded rather than a plain decrement.
//
// A partition is never handed back to the same generation, so a record left
// behind once is never valid again — which is what makes releasing here final
// rather than premature.
func (o *recordSettleOps) Valid() (bool, string) {
	ok, reason := o.c.tracker.valid(o.key, o.gen)
	if !ok {
		o.release()
	}
	return ok, reason
}

// newSettler returns the settler for one record, or nil where there is nothing
// per-record to settle. A nil settler is never put on a context, so
// inbound::ack() on such a record reports false — the honest answer rather than
// a successful-looking no-op.
//
// The tracker's presence is what decides, rather than the mode, because the
// tracker is what a settle actually moves: a mode saying records settle and no
// tracker to settle them into is not a state worth being able to reach. The
// builder creates one exactly when the mode is not AckPeriodic.
//
// Under AckAfterHandling the settler is marked as settled by the framework,
// which is what makes the acknowledgement follow the work rather than the call.
// Handing one out is what makes the record unsettled, so the count is
// incremented here rather than at the record's other end. Teardown waits on
// that count: a record a `queue_size` queue is still carrying settles long
// after the poll that read it has stopped, and the mark it moves has to be
// committed before the client leaves the group.
func (c *KafkaConsumer) newSettler(r *kgo.Record) (bus.Settler, *recordSettleOps) {
	if c.tracker == nil {
		return nil, nil
	}

	key := partitionKey{r.Topic, r.Partition}
	ops := &recordSettleOps{
		c:   c,
		rec: r,
		key: key,
		gen: c.tracker.begin(r),
	}
	c.unsettled.Add(1)
	if c.ackMode == AckAfterHandling {
		return bus.NewSettler(ops, bus.AutoSettle()), ops
	}
	return bus.NewSettler(ops), ops
}

// nackOwn settles a failure this receiver is answering for itself: a record
// that never reached the configuration, and — under AckPeriodic, where there is
// no settler — one whose handler failed.
//
// The settler path logs its own outcome, so nothing is reported twice here. The
// AckPeriodic path has no settler to do that and reports for itself, including
// when there is no dlq_topic to send the record to: the offset moves on
// franz-go's timer either way, so a failure nothing records is a record that
// disappears silently.
func (c *KafkaConsumer) nackOwn(ctx context.Context, r *kgo.Record, settler bus.Settler, reason string) {
	if settler != nil {
		_, _ = settler.Nack(ctx, reason)
		return
	}

	fields := []zap.Field{
		zap.String("topic", r.Topic),
		zap.Int32("partition", r.Partition),
		zap.Int64("offset", r.Offset),
		zap.String("reason", reason),
	}

	if c.dlqTopic == "" {
		c.logger.Warn("kafka consumer: record failed and there is no dlq_topic; "+
			"ack = \"periodic\" advances the offset regardless, so it is not redelivered",
			fields...)
		c.metrics.RecordError(ctx, r.Topic, "nack")
		return
	}
	if err := c.deadLetter(ctx, r, reason); err != nil {
		c.logger.Error("kafka consumer: record failed and the dead-letter send failed",
			append(fields, zap.Error(err))...)
		return
	}
	c.logger.Warn("kafka consumer: record failed; dead-lettered",
		append(fields, zap.String("dlq_topic", c.dlqTopic))...)
}
