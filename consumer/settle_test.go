package consumer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bus "github.com/tsarna/vinculum-bus"
	"go.uber.org/zap"
)

// settlingConsumer is a receiver with a tracker and no client, which is enough
// for everything a settle does except dead-lettering.
func settlingConsumer(mode AckMode) *KafkaConsumer {
	c := &KafkaConsumer{ackMode: mode, logger: zap.NewNop()}
	if mode != AckPeriodic {
		c.tracker = newTracker(0)
	}
	return c
}

func TestAckAdvancesTheMark(t *testing.T) {
	c := settlingConsumer(AckAfterHandling)
	settler := c.newSettler(rec("t", 0, 7))
	require.NotNil(t, settler)

	settled, err := settler.Ack(context.Background())
	require.NoError(t, err)
	assert.True(t, settled)

	ready := c.tracker.commitReady()
	require.Contains(t, ready["t"], int32(0))
	assert.Equal(t, int64(8), ready["t"][0].Offset)
}

func TestAckSettlesOnce(t *testing.T) {
	c := settlingConsumer(AckManual)
	settler := c.newSettler(rec("t", 0, 0))

	settled, err := settler.Ack(context.Background())
	require.NoError(t, err)
	require.True(t, settled)

	settled, err = settler.Ack(context.Background())
	require.NoError(t, err)
	assert.False(t, settled, "the first settle wins; a later one reports that it did nothing")
}

func TestNackWithoutADLQLeavesTheMarkWhereItIs(t *testing.T) {
	// Kafka cannot redeliver one record, so with nowhere to put a failure the
	// only honest answer is to stop committing at it. Everything from it
	// onwards is reprocessed by whoever next owns the partition.
	c := settlingConsumer(AckManual)
	settler := c.newSettler(rec("t", 0, 4))
	c.tracker.begin(rec("t", 0, 5))

	settled, err := settler.Nack(context.Background(), "handler said no")
	require.NoError(t, err)
	assert.True(t, settled, "the delivery is settled — as not handled")

	assert.Empty(t, c.tracker.commitReady(),
		"the record was refused, so its offset must not be committed")
}

func TestNackDoesNotBlockLaterRecordsFromCompleting(t *testing.T) {
	// The gap holds the mark; it does not stop work. A record above a refusal
	// still completes, it just cannot be committed yet.
	c := settlingConsumer(AckManual)
	refused := c.newSettler(rec("t", 0, 0))
	handled := c.newSettler(rec("t", 0, 1))

	_, err := refused.Nack(context.Background(), "no")
	require.NoError(t, err)
	_, err = handled.Ack(context.Background())
	require.NoError(t, err)

	assert.Empty(t, c.tracker.commitReady())
}

func TestSettleAfterRevokeReportsStale(t *testing.T) {
	c := settlingConsumer(AckManual)
	settler := c.newSettler(rec("t", 0, 0))

	c.tracker.drop(map[string][]int32{"t": {0}})

	settled, err := settler.Ack(context.Background())
	assert.False(t, settled)
	require.Error(t, err)
	assert.True(t, bus.IsStale(err),
		"a settle for a partition another member now owns says so rather than doing nothing")
}

func TestPeriodicRecordsCarryNoSettler(t *testing.T) {
	// Offsets move on franz-go's timer, so there is nothing per-record to
	// settle. A nil settler is what makes inbound::ack() report false rather
	// than look like it worked.
	c := settlingConsumer(AckPeriodic)
	assert.Nil(t, c.newSettler(rec("t", 0, 0)))
}

func TestOnlyAfterHandlingSettlesAutomatically(t *testing.T) {
	// Auto is what SettleOnReturn reads to decide whether a clean return
	// acknowledges. Under manual it must not, or the configuration's decision
	// would be pre-empted by the call that handed the record on.
	assert.True(t, settlingConsumer(AckAfterHandling).newSettler(rec("t", 0, 0)).Auto())
	assert.False(t, settlingConsumer(AckManual).newSettler(rec("t", 0, 0)).Auto())
}

func TestKeepaliveExtendsNothing(t *testing.T) {
	// A Kafka record has no per-message lease to renew, and saying so is what
	// lets shared handling call inbound::keepalive() against any receiver.
	c := settlingConsumer(AckManual)
	extended, err := c.newSettler(rec("t", 0, 0)).Keepalive(context.Background())
	require.NoError(t, err)
	assert.False(t, extended)
}

func TestSettlerTravelsOnTheContext(t *testing.T) {
	// The settler reaches its consumers through the context and nothing else:
	// fields are rewritten per subscription, so they cannot carry it.
	c := settlingConsumer(AckAfterHandling)
	settler := c.newSettler(rec("t", 0, 0))

	ctx := bus.WithSettler(context.Background(), settler)
	assert.Same(t, settler, bus.SettlerFromContext(ctx))
}
