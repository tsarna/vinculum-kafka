package consumer

import (
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap/zapcore"
)

// A Kafka commit is an assertion about a prefix, not about a record: committing
// offset N says everything below N is done. That is the whole of why this
// receiver settles differently from the others. A delivery settles wherever the
// work finishes — behind an async queue, several bus hops away, or whenever an
// action calls inbound::ack() — and those completions do not arrive in the order
// the records did. Committing record 7 while 5 is still outstanding would lose 5
// at the next rebalance or restart.
//
// The tracker turns out-of-order completions back into a prefix. Per partition
// it holds the offset of the oldest record that has not completed — the
// low-water mark — and the completions that have run ahead of it. Only the mark
// is ever committed, so an offset never passes a record still in flight.
//
// Where completions do arrive in order — a synchronous subscriber, or one queue
// draining a partition's records one at a time — none of that machinery comes
// into play: done never holds more than the completion being drained, and the
// mark is always the last completion plus one. The tracker is then an exact
// record of what has been handled, and costs a map lookup to say so.
//
// Where they do not, the mark is pinned at the oldest incomplete record, and
// every completed record above it is delivered again if this member restarts or
// loses the partition — settled, committed, and redelivered regardless. That is
// Kafka rather than a shortcoming here: an offset cannot say "5 is outstanding
// but 6 through 20 are done", only where the prefix ends, and the safe end of
// the prefix is 5. Bounded by maxInFlight, and the reason work that settles out
// of order has to be idempotent.

// DefaultMaxInFlight bounds how far a partition's completions may run ahead of
// its low-water mark before the partition stops being fetched.
const DefaultMaxInFlight = 1024

// resumeDivisor sets how far a paused partition's window must fall before it is
// fetched again: to maxInFlight/resumeDivisor, rather than to one below the
// bound.
//
// Resuming at the bound would make the two thresholds the same line, and a
// partition that is merely slow sits on it — every poll would pause on the
// fetch and resume on the completions that arrived during it, forever. That
// costs a log line each way per poll, and franz-go discards a paused
// partition's buffered fetch, so each cycle re-requests from the broker records
// it had already been given.
//
// Half is the usual answer to that and is what this uses: enough of the window
// has to drain that the next fetch has somewhere to land.
const resumeDivisor = 2

// Why a settler may no longer settle its record. Both retire a window, and the
// difference matters to whoever reads the log: one says the group moved a
// partition to another member, the other says the records themselves are gone.
const (
	// reasonReassigned is a partition this member no longer holds.
	reasonReassigned = "partition reassigned"

	// reasonReset is a partition re-consumed from below the window: a log
	// truncated or expired under this member, which happens without a revoke.
	// The partition is still ours; the offsets the settler names are not.
	reasonReset = "partition reset to an earlier offset"
)

type partitionKey struct {
	topic     string
	partition int32
}

// partitionState is one partition's in-flight window.
type partitionState struct {
	// base is the low-water mark: the offset of the oldest record that has not
	// completed. Kafka's committed offset is the next one to consume, so base
	// is committed exactly as it stands.
	base int64

	// highest is the highest offset handed out. base..highest is the in-flight
	// window, and bounding it is what stops one record nobody settles from
	// growing done without limit.
	highest int64

	// done holds completions above base, each keyed to the leader epoch of the
	// record that produced it, so an advance commits the epoch belonging to the
	// record it advanced past rather than whichever record arrived most
	// recently.
	done map[int64]int32

	// lastEpoch is the leader epoch of the record base last advanced past, and
	// is -1 until base has moved at all.
	lastEpoch int32

	// committed is the last mark Kafka accepted, so a partition whose mark has
	// not moved is not re-committed on every pass. -1 until the first commit.
	committed int64

	// gen identifies this assignment of this partition. A settler captures it
	// and valid compares against it: once the partition is revoked the entry is
	// gone, and a late settle says so rather than advancing a mark that another
	// member of the group now owns.
	gen uint64

	// superseded is what a settler from an earlier generation is told, held on
	// the window that replaced it because the window it describes is gone. A
	// revoked partition has no entry at all and is answered without one.
	superseded string

	// paused is whether fetches for this partition are currently paused, so the
	// poll loop applies only the difference.
	paused bool
}

// stalledPartition is one partition whose fetches are being paused, and the
// mark the bound is waiting on. The offset is what makes the log line
// actionable: it names the record that has not completed, which is the one
// thing an operator needs to go and look at.
type stalledPartition struct {
	topic     string
	partition int32
	offset    int64
}

func (s stalledPartition) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("topic", s.topic)
	enc.AddInt32("partition", s.partition)
	enc.AddInt64("offset", s.offset)
	return nil
}

// stalledPartitions renders as a zap array, so one field carries every paused
// partition rather than one line being logged per partition.
type stalledPartitions []stalledPartition

func (ss stalledPartitions) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, s := range ss {
		if err := enc.AppendObject(s); err != nil {
			return err
		}
	}
	return nil
}

// partitions is the same set in the shape PauseFetchPartitions takes.
func (ss stalledPartitions) partitions() map[string][]int32 {
	var out map[string][]int32
	for _, s := range ss {
		out = appendPartition(out, partitionKey{s.topic, s.partition})
	}
	return out
}

// inFlight is the number of offsets between the mark and the highest handed
// out, inclusive. Zero when everything dispatched has completed.
func (st *partitionState) inFlight() int64 {
	if st.base > st.highest {
		return 0
	}
	return st.highest - st.base + 1
}

// tracker holds the in-flight window of every partition this member is
// assigned. Every method is safe for concurrent use: completions arrive on
// whatever goroutine finished the work, which is rarely the poll loop.
type tracker struct {
	mu    sync.Mutex
	parts map[partitionKey]*partitionState

	// nextGen numbers assignments across the whole tracker rather than per
	// partition, so a partition revoked and handed back cannot be given a
	// generation an outstanding settler already holds.
	nextGen uint64

	// maxInFlight is the window at which a partition stops being fetched, and
	// resumeInFlight the one it has to fall back to before it is fetched again.
	// Two thresholds rather than one: see resumeDivisor.
	maxInFlight    int64
	resumeInFlight int64
}

func newTracker(maxInFlight int) *tracker {
	if maxInFlight <= 0 {
		maxInFlight = DefaultMaxInFlight
	}
	return &tracker{
		parts:          make(map[partitionKey]*partitionState),
		maxInFlight:    int64(maxInFlight),
		resumeInFlight: int64(maxInFlight) / resumeDivisor,
	}
}

// begin registers a record as in flight and returns the generation its settler
// must present to settle it. Called once per record, before it is handed to the
// subscriber.
func (t *tracker) begin(r *kgo.Record) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	k := partitionKey{r.Topic, r.Partition}
	st := t.parts[k]

	// A window is started for a partition not yet seen, and started again for
	// one whose records have gone backwards. A record below the mark means the
	// partition was reset beneath the window — a log truncated or expired under
	// this member is re-consumed from start_offset, and that happens without a
	// revoke — so the window describes offsets that are not coming. Keeping it
	// would drop every replayed completion as one below the mark, and go on
	// offering a mark past the end of the log.
	//
	// Which of the two it was is worth recording rather than reconstructing: no
	// entry means the last one was dropped, which only a revoke does, and an
	// entry we are rewinding past means the partition stayed ours throughout.
	switch {
	case st == nil:
		st = t.restart(k, r.Offset, reasonReassigned)
	case r.Offset < st.base:
		st = t.restart(k, r.Offset, reasonReset)
	}
	if r.Offset > st.highest {
		st.highest = r.Offset
	}
	return st.gen
}

// restart begins a fresh window for a partition at offset, superseding any the
// tracker held for it. superseded is what a settler from the old window is told.
// Called with the lock held.
func (t *tracker) restart(k partitionKey, offset int64, superseded string) *partitionState {
	t.nextGen++
	st := &partitionState{
		base:       offset,
		highest:    offset,
		done:       make(map[int64]int32),
		lastEpoch:  -1,
		committed:  -1,
		gen:        t.nextGen,
		superseded: superseded,
	}
	if old := t.parts[k]; old != nil {
		// Whether the client is paused is not this entry's to forget: the flag
		// mirrors what was last asked of the client, and clearing it here would
		// leave a partition paused with nothing left to ask for it back.
		st.paused = old.paused
	}
	t.parts[k] = st
	return st
}

// complete marks one record done and advances the low-water mark over every
// contiguous completion above it.
//
// A completion for a partition this member no longer holds is dropped. The mark
// it would advance belongs to whoever owns the partition now, and moving it
// from here would commit past a record that member is still working on.
func (t *tracker) complete(k partitionKey, offset int64, epoch int32, gen uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	st := t.parts[k]
	if st == nil || st.gen != gen || offset < st.base {
		return
	}

	st.done[offset] = epoch
	for {
		e, ok := st.done[st.base]
		if !ok {
			return
		}
		delete(st.done, st.base)
		st.lastEpoch = e
		st.base++
	}
}

// valid reports whether a settler holding gen may still settle its record, and
// why not when it may not.
func (t *tracker) valid(k partitionKey, gen uint64) (bool, string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	st := t.parts[k]
	switch {
	case st == nil:
		// Only drop removes an entry, and only a revoke or a loss calls it.
		return false, reasonReassigned
	case st.gen != gen:
		// A window that replaced the settler's own, which recorded why.
		return false, st.superseded
	}
	return true, ""
}

// commitReady returns the marks that have moved since Kafka last accepted them.
func (t *tracker) commitReady() map[string]map[int32]kgo.EpochOffset {
	t.mu.Lock()
	defer t.mu.Unlock()

	var out map[string]map[int32]kgo.EpochOffset
	for k, st := range t.parts {
		if st.base == st.committed || st.lastEpoch < 0 {
			continue
		}
		if out == nil {
			out = make(map[string]map[int32]kgo.EpochOffset)
		}
		byPartition := out[k.topic]
		if byPartition == nil {
			byPartition = make(map[int32]kgo.EpochOffset)
			out[k.topic] = byPartition
		}
		byPartition[k.partition] = kgo.EpochOffset{Epoch: st.lastEpoch, Offset: st.base}
	}
	return out
}

// noteCommitted records marks Kafka accepted.
//
// It is separate from commitReady because only a *successful* commit may be
// remembered: a failed one leaves the mark outstanding, and the next pass
// retries it rather than assuming an offset moved that did not.
func (t *tracker) noteCommitted(offsets map[string]map[int32]kgo.EpochOffset) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for topic, byPartition := range offsets {
		for partition, eo := range byPartition {
			st := t.parts[partitionKey{topic, partition}]
			// A mark ahead of the current base belongs to a previous
			// assignment of this partition, committed while it was being
			// handed back to this member. Recording it would suppress a commit
			// the new assignment still owes.
			if st != nil && eo.Offset <= st.base {
				st.committed = eo.Offset
			}
		}
	}
}

// drop forgets partitions this member no longer holds, and returns those of
// them that were paused, for ResumeFetchPartitions.
//
// The entry going away is what makes an outstanding settler for one report
// stale: generations are numbered tracker-wide, so the one it holds can never
// be matched again.
//
// The pause, though, is the client's state rather than this one's, and no
// rebalance clears it. Dropping a paused entry without saying so would leave
// the partition unfetched for good: only an entry can produce a resume, and if
// the group hands the partition back, the pause stops any record arriving to
// create one.
func (t *tracker) drop(partitions map[string][]int32) (resume map[string][]int32) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for topic, ps := range partitions {
		for _, p := range ps {
			k := partitionKey{topic, p}
			if st := t.parts[k]; st != nil && st.paused {
				resume = appendPartition(resume, k)
			}
			delete(t.parts, k)
		}
	}
	return resume
}

// pressure returns the partitions that have crossed a bound in each direction
// since it was last called, for PauseFetchPartitions and ResumeFetchPartitions.
// The paused ones carry their marks, for the log line that reports them.
//
// The bound is what stops a record nobody settles from growing a partition's
// window without limit — under ack = "manual" nothing obliges the configuration
// to settle promptly, and under auto a chain can stall. Pausing rather than
// dropping is what keeps the guarantee: nothing is lost, the partition simply
// stops being fetched until the mark moves. Records the client had already
// buffered for a paused partition are not delivered either — franz-go strips
// them from the poll and re-fetches them from the same offset on resume.
//
// It is a bound between polls, not a hard ceiling. The crossing is noticed once
// a fetch has been dispatched in full, so a window can exceed maxInFlight by
// the tail of the fetch in hand; the pause then takes effect from the next
// poll. Making it exact would mean holding the undispatched remainder here,
// which is the same records in a different buffer.
func (t *tracker) pressure() (pause stalledPartitions, resume map[string][]int32) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for k, st := range t.parts {
		inFlight := st.inFlight()
		switch {
		case !st.paused && inFlight >= t.maxInFlight:
			st.paused = true
			pause = append(pause, stalledPartition{k.topic, k.partition, st.base})
		case st.paused && inFlight <= t.resumeInFlight:
			st.paused = false
			resume = appendPartition(resume, k)
		}
	}
	return pause, resume
}

func appendPartition(m map[string][]int32, k partitionKey) map[string][]int32 {
	if m == nil {
		m = make(map[string][]int32)
	}
	m[k.topic] = append(m[k.topic], k.partition)
	return m
}
