package consumer

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// A mark wrongly recorded as accepted is never offered again, so every case
// below is one where getting it wrong loses records permanently and silently.
// commitResult is pure so that they can be checked without a broker.

// commitResponse builds a response naming each partition with an error code,
// in the shape CommitOffsetsSync hands to its callback.
func commitResponse(topic string, codes map[int32]int16) *kmsg.OffsetCommitResponse {
	resp := kmsg.NewPtrOffsetCommitResponse()
	rt := kmsg.NewOffsetCommitResponseTopic()
	rt.Topic = topic
	for partition, code := range codes {
		rp := kmsg.NewOffsetCommitResponseTopicPartition()
		rp.Partition = partition
		rp.ErrorCode = code
		rt.Partitions = append(rt.Partitions, rp)
	}
	resp.Topics = append(resp.Topics, rt)
	return resp
}

func offsets(topic string, eos map[int32]kgo.EpochOffset) map[string]map[int32]kgo.EpochOffset {
	return map[string]map[int32]kgo.EpochOffset{topic: eos}
}

func TestCommitResultAcceptsWhatKafkaAccepted(t *testing.T) {
	asked := offsets("t", map[int32]kgo.EpochOffset{
		0: {Epoch: 7, Offset: 100},
		1: {Epoch: 7, Offset: 200},
	})

	accepted, refused := commitResult(asked, commitResponse("t", map[int32]int16{0: 0, 1: 0}), nil)

	assert.Empty(t, refused)
	assert.Equal(t, asked, accepted, "the accepted marks are the ones that were asked for")
}

func TestCommitResultKeepsARefusedPartitionOutstanding(t *testing.T) {
	// The bug this function exists for. CommitOffsetsSync's callback reports the
	// call's own error but not the per-partition codes, so a partition refused
	// for a stale generation arrives inside an otherwise successful response.
	// Recording it as committed would suppress the mark for good.
	asked := offsets("t", map[int32]kgo.EpochOffset{
		0: {Epoch: 7, Offset: 100},
		1: {Epoch: 7, Offset: 200},
	})

	accepted, refused := commitResult(asked, commitResponse("t", map[int32]int16{
		0: 0,
		1: kerr.IllegalGeneration.Code,
	}), nil)

	require.Len(t, refused, 1)
	assert.ErrorIs(t, refused[0], kerr.IllegalGeneration)
	assert.Contains(t, refused[0].Error(), "t[1]", "the refusal names the partition it is about")

	// And the partition that succeeded is still recorded, so one bad partition
	// does not hold back a good one.
	assert.Equal(t, offsets("t", map[int32]kgo.EpochOffset{0: {Epoch: 7, Offset: 100}}), accepted)
}

func TestCommitResultAcceptsNothingWhenTheRequestItselfFailed(t *testing.T) {
	// Nothing in the request landed, so nothing in it may be remembered — even
	// though a response is not what carried the failure.
	asked := offsets("t", map[int32]kgo.EpochOffset{0: {Epoch: 7, Offset: 100}})
	boom := errors.New("broker unreachable")

	accepted, refused := commitResult(asked, commitResponse("t", map[int32]int16{0: 0}), boom)

	assert.Nil(t, accepted)
	assert.Equal(t, []error{boom}, refused)
}

func TestCommitResultIgnoresAPartitionItDidNotAskAbout(t *testing.T) {
	// A response naming a partition this commit did not ask about has no mark to
	// record, and inventing one would be recording an offset nothing produced.
	asked := offsets("t", map[int32]kgo.EpochOffset{0: {Epoch: 7, Offset: 100}})

	accepted, refused := commitResult(asked, commitResponse("t", map[int32]int16{0: 0, 9: 0}), nil)

	assert.Empty(t, refused)
	assert.Equal(t, asked, accepted)
	assert.NotContains(t, accepted["t"], int32(9))
}

func TestCommitResultToleratesAnEmptyResponse(t *testing.T) {
	// franz-go answers a commit it never sent with an empty response and no
	// error. Nothing was accepted and nothing was refused.
	accepted, refused := commitResult(
		offsets("t", map[int32]kgo.EpochOffset{0: {Epoch: 7, Offset: 100}}),
		kmsg.NewPtrOffsetCommitResponse(), nil)

	assert.Nil(t, accepted)
	assert.Empty(t, refused)

	accepted, refused = commitResult(nil, nil, nil)
	assert.Nil(t, accepted)
	assert.Empty(t, refused)
}

// TestCommitResultFeedsBackTheMarkTheTrackerOffered joins the two halves: what
// commitResult accepts is what noteCommitted is given, and a refused mark has
// to come back round on the next pass.
func TestCommitResultFeedsBackTheMarkTheTrackerOffered(t *testing.T) {
	tr := newTracker(0)
	genA := tr.begin(rec("t", 0, 0))
	genB := tr.begin(rec("t", 1, 0))
	tr.complete(partitionKey{"t", 0}, 0, 7, genA)
	tr.complete(partitionKey{"t", 1}, 0, 7, genB)

	asked := tr.commitReady()
	require.Len(t, asked["t"], 2)

	accepted, refused := commitResult(asked, commitResponse("t", map[int32]int16{
		0: 0,
		1: kerr.RebalanceInProgress.Code,
	}), nil)
	require.Len(t, refused, 1)
	tr.noteCommitted(accepted)

	again := tr.commitReady()
	assert.NotContains(t, again["t"], int32(0), "the accepted mark is not offered again")
	require.Contains(t, again["t"], int32(1), "the refused mark is offered again")
	assert.Equal(t, asked["t"][1], again["t"][1], "and it is the same mark, unchanged")
}
