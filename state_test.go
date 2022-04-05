package finality

// States
const (
	None           = "none"            // dont send any block
	Stay           = "stay"            // send the current last block
	CanonicalBlock = "canonical_block" // generate a new block with a timestamp of 1
	Rollback       = "rollback"        // remove n blocks and send a new last block
	Sync           = "sync"            // get from the current last block to the last client block of the maximum chain and send them
	Fork           = "fork"            // generate a  with a time stamp of 2
)

// Events
const (
	Event = "_event"

	NoneEvent           = None + Event
	StayEvent           = Stay + Event
	CanonicalBlockEvent = CanonicalBlock + Event
	RollbackEvent       = Rollback + Event
	SyncEvent           = Sync + Event
	ForkEvent           = Fork + Event
)
