package producer

import (
	"testing"

	bus "github.com/tsarna/vinculum-bus"
)

// The bridge case. Under ProduceModeAsync, OnEvent hands the record to franz-go
// and returns before the broker has taken it — so a caller settling on that
// return would acknowledge the *inbound* message before the outbound one
// existed, in exactly the situation where at-least-once matters most.
//
// Deferral is a configuration choice here rather than a property of the type,
// which is why bus.Deferring is a method. Getting this wrong is silent: the
// default is sync, so an author flipping a throughput knob would give up the
// delivery guarantee with nothing in the configuration mentioning
// acknowledgement.
func TestDeferralFollowsTheProduceMode(t *testing.T) {
	async := &KafkaProducer{produceMode: ProduceModeAsync}
	if got := bus.DispositionOf(async); got != bus.Deferred {
		t.Fatalf("an async produce returns before the broker has the record, so "+
			"the settle belongs in the produce callback rather than on OnEvent's "+
			"return; got %v", got)
	}

	sync := &KafkaProducer{produceMode: ProduceModeSync}
	if got := bus.DispositionOf(sync); got != bus.Handled {
		t.Fatalf("a sync produce has completed by the time OnEvent returns and "+
			"its error is the outcome, so there is nothing to defer; got %v", got)
	}

	// The zero value is ProduceModeSync, and that is the default a producer
	// gets when nothing sets one. A zero value that reported deferral would
	// leave every default-configured producer's messages unsettled.
	if got := bus.DispositionOf(&KafkaProducer{}); got != bus.Handled {
		t.Fatalf("the default produce mode is sync and must report Handled; got %v", got)
	}
}
