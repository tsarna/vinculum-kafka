package consumer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bus "github.com/tsarna/vinculum-bus"
	wire "github.com/tsarna/vinculum-wire"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// drainFixture starts a consumer against an in-process broker, with a producer
// for putting records on the topic. The consumer is not stopped by cleanup:
// these tests stop it themselves, at the point in the sequence they are about.
func drainFixture(t *testing.T, mode AckMode, sub bus.Subscriber) (*KafkaConsumer, func(bodies ...string)) {
	t.Helper()
	addrs := newCluster(t)
	return newConsumer(t, addrs, mode, sub), newProducer(t, addrs)
}

// newCluster starts an in-process broker with the test topic on it.
func newCluster(t *testing.T) []string {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "in"))
	require.NoError(t, err)
	t.Cleanup(cluster.Close)
	return cluster.ListenAddrs()
}

// newConsumer builds and starts a consumer against addrs. It is not stopped by
// cleanup beyond a best-effort call: these tests stop it themselves, at the
// point in the sequence they are about.
func newConsumer(t *testing.T, addrs []string, mode AckMode, sub bus.Subscriber) *KafkaConsumer {
	t.Helper()

	c, err := NewConsumer().
		WithBaseOpts([]kgo.Opt{kgo.SeedBrokers(addrs...)}).
		WithGroupID("g").
		// From the beginning when the group has no committed offset, so a test
		// that starts a second member is reading from what the first one
		// committed rather than from wherever the log happens to be.
		WithStartOffset(kgo.NewOffset().AtStart()).
		WithSubscription(TopicSubscription{
			KafkaTopic: "in",
			VinculumTopicFunc: func(string, *string, map[string]string, any) (string, error) {
				return "t", nil
			},
		}).
		WithSubscriber(sub).
		WithAckMode(mode).
		WithWireFormat(wire.Auto).
		WithLogger(zap.NewNop()).
		Build()
	require.NoError(t, err)
	require.NoError(t, c.Start(context.Background()))
	t.Cleanup(func() { _ = c.Stop() })
	return c
}

// newProducer returns a function that puts records on the test topic.
func newProducer(t *testing.T, addrs []string) func(bodies ...string) {
	t.Helper()
	prod, err := kgo.NewClient(kgo.SeedBrokers(addrs...))
	require.NoError(t, err)
	t.Cleanup(prod.Close)

	return func(bodies ...string) {
		t.Helper()
		for _, b := range bodies {
			require.NoError(t, prod.ProduceSync(context.Background(),
				&kgo.Record{Topic: "in", Value: []byte(b)}).FirstErr())
		}
	}
}

// drained bounds the drain, so a consumer that will not stop polling fails the
// line that knows what went wrong rather than blocking until the package times
// out somewhere else.
func drained(t *testing.T, c *KafkaConsumer) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return c.Drain(ctx)
}

// counter records deliveries and settles nothing itself.
type counter struct {
	bus.BaseSubscriber
	seen atomic.Int64
}

func (c *counter) OnEvent(context.Context, string, any, map[string]string) error {
	c.seen.Add(1)
	return nil
}

func (c *counter) wait(t *testing.T, n int64) {
	t.Helper()
	require.Eventually(t, func() bool { return c.seen.Load() >= n },
		30*time.Second, 20*time.Millisecond, "the consumer never delivered %d records", n)
}

// The point of a drain: records already read are finished, and no more are
// taken on. Stopping does both at once, which is why a shutdown that only has
// Stop cannot stop consuming before it leaves the group.
func TestDrainStopsPolling(t *testing.T) {
	sub := &counter{}
	c, produce := drainFixture(t, AckAfterHandling, sub)

	produce("before")
	sub.wait(t, 1)

	require.NoError(t, drained(t, c))

	produce("after")
	time.Sleep(500 * time.Millisecond)

	assert.Equal(t, int64(1), sub.seen.Load(), "the consumer kept polling after it was drained")
}

// Under `ack = "periodic"` franz-go's own autocommit owns the offsets, and it
// records a poll's offsets as *dirty*, promoting them to committable only at
// the start of the next poll — a deliberate one-poll lag that is what makes its
// autocommit at-least-once.
//
// So a drain that left immediately after dispatching a fetch would strand that
// fetch's offsets dirty, and every record in it would be handled again by the
// next process: a shutdown quietly producing the duplicates it exists to avoid.
// The loop goes round until a poll comes back empty, which is what gives
// franz-go the poll it needs.
// The drain has to land *while a fetch is being dispatched* for this to bite —
// a drain that arrives while the loop is waiting in a poll gets its promoting
// poll for free, because the poll it interrupts is that poll. So the record is
// held inside the subscriber until the drain is under way.
func TestDrainLetsPeriodicOffsetsBecomeCommittable(t *testing.T) {
	addrs := newCluster(t)
	produce := newProducer(t, addrs)

	b := newBlocker()
	first := newConsumer(t, addrs, AckPeriodic, b)

	produce("a")
	<-b.entered

	result := make(chan error, 1)
	go func() { result <- drained(t, first) }()

	// The drain is now waiting on a loop that is mid-dispatch, which is the
	// interleaving under test: a drain arriving while the loop waits in a poll
	// gets its promoting poll for free, because the poll it interrupts is that
	// poll.
	time.Sleep(300 * time.Millisecond)
	close(b.release)

	require.NoError(t, <-result)
	require.NoError(t, first.Stop())

	// Asked of the broker rather than of the client we just closed, which is
	// the only place the answer lives: a second member of the same group reads
	// from the committed offset, so it sees the record again exactly when the
	// first one failed to commit it.
	// Produced before the second member starts, so both records are on the log
	// when it joins and there is no race between the join and the produce. It
	// reads from the group's committed offset: one record if "a" was committed,
	// two if it was not.
	produce("b")

	second := &counter{}
	newConsumer(t, addrs, AckPeriodic, second)
	second.wait(t, 1)
	time.Sleep(time.Second) // long enough for a redelivered "a" to arrive too

	assert.Equal(t, int64(1), second.seen.Load(),
		"the drain left the fetch's offsets dirty, so its records are handled again")
}

// A drained consumer is still in the group and still able to commit, and the
// settler it handed out before the drain still moves its mark. That is the
// whole reason draining and stopping are separate: closing the client leaves
// the group, and every offset not yet committed is then replayed by whoever
// picks the partition up.
func TestDrainLeavesAnOutstandingSettlerAbleToAdvanceTheMark(t *testing.T) {
	sub := &capturingSubscriber{}
	c, produce := drainFixture(t, AckManual, sub)

	produce("hi")
	require.Eventually(t, func() bool { return sub.ctx.Load() != nil },
		30*time.Second, 20*time.Millisecond)
	require.Equal(t, 1, c.Unsettled())

	require.NoError(t, drained(t, c))
	require.Equal(t, 1, c.Unsettled(), "nothing settled it yet")

	settled, err := bus.SettlerFromContext(*sub.ctx.Load()).Ack(context.Background())
	require.NoError(t, err, "the settler went stale during the drain")
	assert.True(t, settled)

	assert.Equal(t, 0, c.Unsettled())
	assert.NotEmpty(t, c.tracker.commitReady(),
		"the mark the settle moved should be committable while the client is still in the group")
}

// What the shutdown phase reads. Under manual settle nothing advances the mark
// until the configuration does, so the count is what says the process still
// owes Kafka an answer.
func TestUnsettledCountsWhatIsStillOwed(t *testing.T) {
	sub := &capturingSubscriber{}
	c, produce := drainFixture(t, AckManual, sub)

	assert.Equal(t, 0, c.Unsettled())

	produce("one")
	require.Eventually(t, func() bool { return sub.ctx.Load() != nil },
		30*time.Second, 20*time.Millisecond)
	assert.Equal(t, 1, c.Unsettled())

	// A nack with no dlq_topic sends nothing and deliberately stops the
	// partition's committed offset — but this process is done with the record,
	// so it stops being something a shutdown waits for.
	_, err := bus.SettlerFromContext(*sub.ctx.Load()).Nack(context.Background(), "no")
	require.NoError(t, err)
	assert.Equal(t, 0, c.Unsettled(),
		"a refused record is still on the books after the process is done with it")

	require.NoError(t, drained(t, c))
}

// Under AckAfterHandling the framework settles when the work finishes, so the
// count comes back to zero on its own and a shutdown waits for nothing.
func TestUnsettledReturnsToZeroWhenTheFrameworkSettles(t *testing.T) {
	sub := &counter{}
	c, produce := drainFixture(t, AckAfterHandling, sub)

	produce("hi")
	sub.wait(t, 1)

	assert.Eventually(t, func() bool { return c.Unsettled() == 0 },
		10*time.Second, 20*time.Millisecond,
		"an automatically settled record stayed on the books")
}

// Under AckPeriodic franz-go's own timer owns the offsets, so there is no
// settler and nothing to count. Counting anyway would leave the number pinned
// and make every later shutdown wait out its whole budget.
func TestPeriodicCountsNothing(t *testing.T) {
	sub := &counter{}
	c, produce := drainFixture(t, AckPeriodic, sub)

	produce("hi")
	sub.wait(t, 1)

	assert.Equal(t, 0, c.Unsettled())
	require.NoError(t, drained(t, c))
	assert.Equal(t, 0, c.Unsettled())
}

// A record whose partition has gone to another member can never be settled: the
// mark it would move belongs to somebody else now. The settler asks Valid()
// *before* it runs Ack or Nack and abandons the record when the answer is no,
// so a stale record never reaches the release those two carry — and a count
// that only released there would keep it forever, with every later shutdown
// spending its whole budget on it.
func TestUnsettledLetsGoOfARecordWhosePartitionWasTaken(t *testing.T) {
	sub := &capturingSubscriber{}
	c, produce := drainFixture(t, AckManual, sub)

	produce("hi")
	require.Eventually(t, func() bool { return sub.ctx.Load() != nil },
		30*time.Second, 20*time.Millisecond)
	require.Equal(t, 1, c.Unsettled())

	// What a rebalance does to this record's partition.
	c.tracker.drop(map[string][]int32{"in": {0}})

	settled, err := bus.SettlerFromContext(*sub.ctx.Load()).Ack(context.Background())
	require.Error(t, err)
	assert.True(t, bus.IsStale(err))
	assert.False(t, settled)

	assert.Equal(t, 0, c.Unsettled(),
		"a record this consumer can no longer settle stayed on the books")
	require.NoError(t, drained(t, c))
}

// The count is decremented once per record however many times a release fires,
// and it fires from more places than the settles do: Valid() releases on its
// false branch, and the settler asks Valid() before a keepalive as well as
// before a settle. So a stale record reaches the release twice by the plainest
// route there is.
//
// A count that went negative would *subtract* from the shutdown phase's total —
// it sums every holder — so one record could cancel another holder's real
// backlog and let the wait end a sampling interval early.
func TestAStaleRecordIsNotReleasedTwice(t *testing.T) {
	c := settlingConsumer(AckManual)
	settler, _ := c.newSettler(rec("t", 0, 0))
	require.Equal(t, 1, c.Unsettled())

	// What a rebalance does to this record's partition.
	c.tracker.drop(map[string][]int32{"t": {0}})

	_, err := settler.Keepalive(context.Background())
	require.Error(t, err, "a keepalive on a reassigned partition should report it stale")
	assert.Equal(t, 0, c.Unsettled())

	_, err = settler.Ack(context.Background())
	require.Error(t, err)
	assert.Equal(t, 0, c.Unsettled(), "the count went negative on a second release")
}

// capturingSubscriber keeps the record's context so a test can settle through
// the settler riding on it, as a configuration does.
type capturingSubscriber struct {
	bus.BaseSubscriber
	ctx atomic.Pointer[context.Context]
}

func (s *capturingSubscriber) OnEvent(ctx context.Context, _ string, _ any, _ map[string]string) error {
	s.ctx.Store(&ctx)
	return nil
}

// watcher sees records go past and settles none of them, which is a disposition
// of its own rather than a subscriber that forgot.
type watcher struct {
	bus.BaseSubscriber
	seen atomic.Int64
}

func (w *watcher) OnEvent(context.Context, string, any, map[string]string) error {
	w.seen.Add(1)
	return nil
}

func (w *watcher) DeliveryDisposition() bus.Disposition { return bus.Observed }

// The only path through a record that reaches no settler at all: every failure
// in processRecord nacks, and a nack releases. An observing subscriber makes
// the framework settle point return without acting, so nothing anywhere is
// going to settle the record.
func TestUnsettledDoesNotLeakOnAnObservingSubscriber(t *testing.T) {
	w := &watcher{}
	c, produce := drainFixture(t, AckAfterHandling, w)

	produce("hi")
	require.Eventually(t, func() bool { return w.seen.Load() == 1 },
		30*time.Second, 20*time.Millisecond)

	assert.Eventually(t, func() bool { return c.Unsettled() == 0 },
		10*time.Second, 20*time.Millisecond,
		"a record nothing will ever settle stayed on the books")
}

// blocker holds a record until it is released, so a test can be sure the
// consumer is mid-record when it drains, and records what the record's own
// context looked like on the far side of the wait.
type blocker struct {
	bus.BaseSubscriber
	entered chan struct{}
	release chan struct{}

	errAfterRelease atomic.Pointer[error]
}

func newBlocker() *blocker {
	return &blocker{entered: make(chan struct{}, 1), release: make(chan struct{})}
}

func (b *blocker) OnEvent(ctx context.Context, _ string, _ any, _ map[string]string) error {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-b.release
	err := ctx.Err()
	b.errAfterRelease.Store(&err)
	return nil
}

// Draining must not cancel the work it is waiting for. Polling and dispatching
// share one context until this splits them, and cancelling that one context to
// stop the loop would abort the record in flight and, with it, the mark that
// record was about to move.
func TestDrainDoesNotCancelTheRecordInFlight(t *testing.T) {
	b := newBlocker()
	c, produce := drainFixture(t, AckAfterHandling, b)

	produce("hi")
	<-b.entered

	result := make(chan error, 1)
	go func() { result <- drained(t, c) }()

	time.Sleep(300 * time.Millisecond)
	close(b.release)

	require.NoError(t, <-result)
	got := b.errAfterRelease.Load()
	require.NotNil(t, got)
	assert.NoError(t, *got, "the drain cancelled the context the record was running on")
}

// Dispatch runs user-supplied work, so a drain that waited for it
// unconditionally would hand one stuck expression the power to stop a process
// from exiting. The caller's context is the bound.
func TestDrainIsBoundedByItsContext(t *testing.T) {
	b := newBlocker()
	c, produce := drainFixture(t, AckAfterHandling, b)
	defer close(b.release)

	produce("hi")
	<-b.entered

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	result := make(chan error, 1)
	go func() { result <- c.Drain(ctx) }()

	select {
	case err := <-result:
		require.Error(t, err, "drain waited for an action that never returned")
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(5 * time.Second):
		t.Fatal("the drain ignored its deadline")
	}

	// And it stays reported.
	assert.Error(t, c.Drain(context.Background()))
}

// The bound above is worth nothing if the same stuck action then meets an
// unbounded wait one phase later. Stop cancels and reports instead.
func TestStopDoesNotWaitAgainForARecordTheDrainGaveUpOn(t *testing.T) {
	b := newBlocker()
	c, produce := drainFixture(t, AckAfterHandling, b)
	defer close(b.release)

	produce("hi")
	<-b.entered

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	require.Error(t, c.Drain(ctx))

	stopped := make(chan error, 1)
	go func() { stopped <- c.Stop() }()

	select {
	case err := <-stopped:
		assert.Error(t, err, "stopping past a running record should say so")
	case <-time.After(20 * time.Second):
		t.Fatal("Stop blocked on the record the drain had already given up on")
	}
}

// A whole phase runs between Drain and Stop — quiesce, bounded at ten seconds
// of its own — so a record that overran the drain's deadline by a moment has
// very likely finished by the time Stop asks. Remembering the drain's verdict
// instead of re-checking puts an error in the log of a shutdown where nothing
// went wrong, which is why `stillDelivering` is a channel and not a flag.
//
// The other three receivers each pin this; kafka did not, and a `gaveUp
// atomic.Bool` latched in Drain's timeout branch passed its whole suite.
func TestStopReportsCleanlyWhenTheRecordFinishedAfterTheDrainGaveUp(t *testing.T) {
	b := newBlocker()
	c, produce := drainFixture(t, AckAfterHandling, b)

	produce("hi")
	<-b.entered

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	require.Error(t, c.Drain(ctx), "the drain should have given up")

	// The record finishes in the window the real shutdown spends quiescing.
	// Polling Drain is how the test sees that: after a drain has already run,
	// the call reads the answer and changes nothing.
	close(b.release)
	require.Eventually(t, func() bool { return c.Drain(context.Background()) == nil },
		10*time.Second, 20*time.Millisecond,
		"Drain kept reporting a timeout for a record that had finished")

	assert.NoError(t, c.Stop(),
		"Stop reported a record still being handled that had already finished")
}

// Draining is terminal. The group session outlives the drain — that is the
// point, so outstanding marks can still be committed — and a Start over the top
// would put the process back to consuming during the phase that waits for it to
// have finished.
func TestStartRefusesAfterADrain(t *testing.T) {
	sub := &counter{}
	c, _ := drainFixture(t, AckAfterHandling, sub)

	require.NoError(t, drained(t, c))

	err := c.Start(context.Background())
	require.Error(t, err, "starting again would put the consumer back to consuming")
	assert.Contains(t, err.Error(), "drained")
}

// The same refusal on a consumer the drain reached before it started, which is
// the interleaving a check made outside the lock cannot close.
func TestStartRefusesAfterADrainThatFoundItUnstarted(t *testing.T) {
	c := settlingConsumer(AckAfterHandling)

	require.NoError(t, c.Drain(context.Background()),
		"draining a consumer that never started is a clean no-op")

	err := c.Start(context.Background())
	require.Error(t, err, "a drain must be remembered by a consumer that had not started")
	assert.Contains(t, err.Error(), "drained")
}

// Teardown calls both, in that order, and a consumer that was never started is
// torn down along with everything else. None of that may panic or block.
func TestDrainAndStopComposeInAnyOrder(t *testing.T) {
	sub := &counter{}
	c, _ := drainFixture(t, AckAfterHandling, sub)

	require.NoError(t, drained(t, c))
	require.NoError(t, drained(t, c))
	require.NoError(t, c.Stop())
	require.NoError(t, c.Stop())
	require.NoError(t, drained(t, c))

	fresh := settlingConsumer(AckAfterHandling)
	assert.NoError(t, fresh.Drain(context.Background()))
	assert.NoError(t, fresh.Stop())
}
