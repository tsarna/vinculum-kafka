package consumer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// rec is a record at one offset of one partition, with a leader epoch derived
// from the offset so a test can tell which record an epoch came from.
func rec(topic string, partition int32, offset int64) *kgo.Record {
	return &kgo.Record{
		Topic:       topic,
		Partition:   partition,
		Offset:      offset,
		LeaderEpoch: int32(100 + offset),
	}
}

// mark returns the offset the tracker would commit for one partition, and
// whether it would commit anything at all.
func mark(t *testing.T, tr *tracker, topic string, partition int32) (int64, bool) {
	t.Helper()
	ready := tr.commitReady()
	eo, ok := ready[topic][partition]
	return eo.Offset, ok
}

func TestTrackerAdvancesOnlyOverAContiguousPrefix(t *testing.T) {
	tr := newTracker(0)
	var gen uint64
	for offset := int64(0); offset < 3; offset++ {
		gen = tr.begin(rec("t", 0, offset))
	}

	// Completing the last record first is the whole reason this type exists:
	// an offset is an assertion about a prefix, so nothing may be committed
	// while 0 and 1 are still outstanding.
	tr.complete(partitionKey{"t", 0}, 2, 102, gen)
	_, ok := mark(t, tr, "t", 0)
	assert.False(t, ok, "nothing below the completion is done, so no offset is committable")

	tr.complete(partitionKey{"t", 0}, 0, 100, gen)
	offset, ok := mark(t, tr, "t", 0)
	require.True(t, ok)
	assert.Equal(t, int64(1), offset, "record 0 is done and 1 is not")

	// Completing the gap releases the completion that was already waiting
	// above it, in one step.
	tr.complete(partitionKey{"t", 0}, 1, 101, gen)
	offset, ok = mark(t, tr, "t", 0)
	require.True(t, ok)
	assert.Equal(t, int64(3), offset, "0, 1 and 2 are all done")
}

func TestTrackerHoldsTheMarkBehindAnUncompletedRecord(t *testing.T) {
	// This is what a nack with no dlq_topic looks like from the tracker's side:
	// record 1 is never completed, so the committed offset stops at it however
	// much work finishes above.
	tr := newTracker(0)
	var gen uint64
	for offset := int64(0); offset < 10; offset++ {
		gen = tr.begin(rec("t", 0, offset))
	}

	for offset := int64(0); offset < 10; offset++ {
		if offset == 1 {
			continue
		}
		tr.complete(partitionKey{"t", 0}, offset, int32(100+offset), gen)
	}

	offset, ok := mark(t, tr, "t", 0)
	require.True(t, ok)
	assert.Equal(t, int64(1), offset,
		"the mark stops at the record nothing settled, so it and everything after it are reprocessed")
}

func TestTrackerCommitsTheEpochOfTheRecordItAdvancedPast(t *testing.T) {
	// Truncation detection compares the epoch of the record at the committed
	// offset, so the epoch has to belong to the record the mark moved over —
	// not to whichever record happened to arrive most recently.
	tr := newTracker(0)
	gen := tr.begin(rec("t", 0, 5))
	tr.begin(rec("t", 0, 6))

	tr.complete(partitionKey{"t", 0}, 5, 105, gen)

	ready := tr.commitReady()
	require.Contains(t, ready["t"], int32(0))
	assert.Equal(t, int32(105), ready["t"][0].Epoch)
	assert.Equal(t, int64(6), ready["t"][0].Offset)
}

func TestTrackerReportsOnlyMarksThatHaveMoved(t *testing.T) {
	tr := newTracker(0)
	gen := tr.begin(rec("t", 0, 0))
	tr.complete(partitionKey{"t", 0}, 0, 100, gen)

	ready := tr.commitReady()
	require.NotEmpty(t, ready)

	tr.noteCommitted(ready)
	assert.Empty(t, tr.commitReady(),
		"a mark Kafka has already accepted is not offered again")

	tr.begin(rec("t", 0, 1))
	tr.complete(partitionKey{"t", 0}, 1, 101, gen)
	assert.NotEmpty(t, tr.commitReady(), "a mark that moved again is offered again")
}

func TestTrackerKeepsAPartitionOutstandingWhenOnlyOthersWereAccepted(t *testing.T) {
	// A commit can be refused for one partition while the request itself
	// succeeded, so commitTo reports what Kafka accepted rather than a verdict
	// on the batch. Recording only that must leave the refused partition to be
	// offered again, and must not make the accepted ones repeat forever.
	tr := newTracker(0)
	genA := tr.begin(rec("t", 0, 0))
	genB := tr.begin(rec("t", 1, 0))
	tr.complete(partitionKey{"t", 0}, 0, 100, genA)
	tr.complete(partitionKey{"t", 1}, 0, 100, genB)

	ready := tr.commitReady()
	require.Len(t, ready["t"], 2)

	// Partition 1 was refused, so only partition 0 is recorded.
	tr.noteCommitted(map[string]map[int32]kgo.EpochOffset{"t": {0: ready["t"][0]}})

	again := tr.commitReady()
	assert.NotContains(t, again["t"], int32(0), "a mark Kafka accepted is not offered again")
	assert.Contains(t, again["t"], int32(1), "a mark Kafka refused is offered again")
}

func TestTrackerRetriesAMarkWhoseCommitFailed(t *testing.T) {
	// noteCommitted is separate from commitReady precisely so a failed commit
	// leaves the mark outstanding. Committing is at-least-once's cheapest
	// operation to repeat and its most expensive to skip.
	tr := newTracker(0)
	gen := tr.begin(rec("t", 0, 0))
	tr.complete(partitionKey{"t", 0}, 0, 100, gen)

	require.NotEmpty(t, tr.commitReady())
	assert.NotEmpty(t, tr.commitReady(),
		"without noteCommitted the same mark is offered until something records it")
}

func TestTrackerDropsCompletionsForARevokedPartition(t *testing.T) {
	tr := newTracker(0)
	gen := tr.begin(rec("t", 0, 0))

	ok, _ := tr.valid(partitionKey{"t", 0}, gen)
	require.True(t, ok)

	tr.drop(map[string][]int32{"t": {0}})

	ok, reason := tr.valid(partitionKey{"t", 0}, gen)
	assert.False(t, ok)
	assert.Equal(t, reasonReassigned, reason)

	// The member that owns the partition now is reprocessing from the last
	// committed mark, so a completion arriving here would commit past records
	// it is still working on.
	tr.complete(partitionKey{"t", 0}, 0, 100, gen)
	assert.Empty(t, tr.commitReady())
}

func TestTrackerGivesAReassignedPartitionAFreshGeneration(t *testing.T) {
	// Generations are numbered tracker-wide rather than per partition, so a
	// partition handed back to this member cannot be given a number an
	// outstanding settler from the previous assignment already holds.
	tr := newTracker(0)
	old := tr.begin(rec("t", 0, 0))
	tr.drop(map[string][]int32{"t": {0}})
	fresh := tr.begin(rec("t", 0, 0))

	assert.NotEqual(t, old, fresh)

	ok, _ := tr.valid(partitionKey{"t", 0}, old)
	assert.False(t, ok, "a settler from the previous assignment must not settle into this one")

	tr.complete(partitionKey{"t", 0}, 0, 100, old)
	assert.Empty(t, tr.commitReady())
}

func TestTrackerPausesAtTheInFlightBoundAndResumesWhenTheWindowHasDrained(t *testing.T) {
	tr := newTracker(4)
	var gen uint64
	for offset := int64(0); offset < 3; offset++ {
		gen = tr.begin(rec("t", 0, offset))
	}

	pause, resume := tr.pressure()
	assert.Empty(t, pause, "three in flight is under the bound of four")
	assert.Empty(t, resume)

	tr.begin(rec("t", 0, 3))
	pause, _ = tr.pressure()
	assert.Equal(t, stalledPartitions{{"t", 0, 0}}, pause,
		"the paused partition is named with the mark it stopped at")

	// Still over the bound, and already paused: pressure reports the crossing,
	// not the state, so the poll loop does not pause the same partition on
	// every pass.
	pause, resume = tr.pressure()
	assert.Empty(t, pause)
	assert.Empty(t, resume)

	// One completion is not a resume. Resuming at the bound would put both
	// thresholds on the same line, and a partition that is merely slow would
	// pause and resume on every poll forever.
	tr.complete(partitionKey{"t", 0}, 0, 100, gen)
	_, resume = tr.pressure()
	assert.Empty(t, resume, "three of four in flight is still too full to refill")

	tr.complete(partitionKey{"t", 0}, 1, 100, gen)
	_, resume = tr.pressure()
	assert.Equal(t, map[string][]int32{"t": {0}}, resume,
		"half the window has drained, so there is somewhere for a fetch to land")
}

func TestTrackerResumesAPausedPartitionItDrops(t *testing.T) {
	// The pause lives in the client and no rebalance clears it, while only a
	// tracker entry can ask for a resume. Dropping a paused entry silently
	// would leave the partition unfetched for good: handed back to this member
	// it would still be paused, so no record would arrive to make an entry, so
	// nothing would ever resume it.
	tr := newTracker(2)
	tr.begin(rec("t", 0, 0))
	tr.begin(rec("t", 0, 1))
	tr.begin(rec("t", 1, 0))

	pause, _ := tr.pressure()
	require.Equal(t, stalledPartitions{{"t", 0, 0}}, pause)

	resume := tr.drop(map[string][]int32{"t": {0, 1}})
	assert.Equal(t, map[string][]int32{"t": {0}}, resume,
		"only the partition that was paused needs resuming")
}

func TestTrackerDropsAnUnpausedPartitionWithNothingToResume(t *testing.T) {
	tr := newTracker(0)
	tr.begin(rec("t", 0, 0))

	assert.Empty(t, tr.drop(map[string][]int32{"t": {0}}))
	assert.Empty(t, tr.drop(map[string][]int32{"t": {0}}),
		"dropping a partition the tracker never held is not a resume")
}

func TestTrackerRestartsAWindowThatRewinds(t *testing.T) {
	// A log truncated or expired under this member is re-consumed from
	// start_offset without a revoke, so records arrive below the mark. The
	// window has to start again from where the data is: left alone it would
	// take every replayed completion for one below the mark and drop it, while
	// going on offering a mark past the end of the log.
	tr := newTracker(0)
	var old uint64
	for offset := int64(100); offset < 103; offset++ {
		old = tr.begin(rec("t", 0, offset))
	}
	tr.complete(partitionKey{"t", 0}, 100, 100, old)
	offset, ready := mark(t, tr, "t", 0)
	require.True(t, ready)
	require.Equal(t, int64(101), offset)

	fresh := tr.begin(rec("t", 0, 50))
	assert.NotEqual(t, old, fresh)

	ok, reason := tr.valid(partitionKey{"t", 0}, old)
	assert.False(t, ok, "a settler for an offset that no longer exists must not settle")
	assert.Equal(t, reasonReset, reason,
		"the partition is still ours; saying it was reassigned would send a reader "+
			"looking for a rebalance that did not happen")

	// The stale window is gone rather than merely superseded: nothing above the
	// replayed records is left to be committed past them.
	_, ready = mark(t, tr, "t", 0)
	assert.False(t, ready, "a restarted window has settled nothing yet")

	tr.complete(partitionKey{"t", 0}, 50, 150, fresh)
	offset, ready = mark(t, tr, "t", 0)
	assert.True(t, ready)
	assert.Equal(t, int64(51), offset)
}

func TestTrackerKeepsThePauseAcrossARestart(t *testing.T) {
	// Restarting the window must not lose what was last asked of the client,
	// for the same reason drop reports it: a cleared flag is a partition that
	// can never be resumed.
	tr := newTracker(2)
	tr.begin(rec("t", 0, 100))
	tr.begin(rec("t", 0, 101))
	pause, _ := tr.pressure()
	require.Equal(t, stalledPartitions{{"t", 0, 100}}, pause)

	tr.begin(rec("t", 0, 50))
	assert.Equal(t, map[string][]int32{"t": {0}}, tr.drop(map[string][]int32{"t": {0}}))
}

func TestTrackerKeepsPartitionsApart(t *testing.T) {
	// One partition's stall must not hold another's mark: they are independent
	// prefixes, and a shared counter would let a wedged partition stop the
	// whole receiver from committing.
	tr := newTracker(0)
	genA := tr.begin(rec("t", 0, 0))
	genB := tr.begin(rec("t", 1, 0))

	tr.complete(partitionKey{"t", 1}, 0, 100, genB)

	ready := tr.commitReady()
	assert.NotContains(t, ready["t"], int32(0), "partition 0 has settled nothing")
	require.Contains(t, ready["t"], int32(1))
	assert.Equal(t, int64(1), ready["t"][1].Offset)

	tr.complete(partitionKey{"t", 0}, 0, 100, genA)
	assert.Contains(t, tr.commitReady()["t"], int32(0))
}

func TestTrackerIgnoresACompletionBelowTheMark(t *testing.T) {
	tr := newTracker(0)
	gen := tr.begin(rec("t", 0, 0))
	tr.complete(partitionKey{"t", 0}, 0, 100, gen)
	tr.noteCommitted(tr.commitReady())

	tr.complete(partitionKey{"t", 0}, 0, 100, gen)
	assert.Empty(t, tr.commitReady(), "a repeated completion moves nothing")
}
