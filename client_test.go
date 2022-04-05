package finality

import (
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/looplab/fsm"
	"pgregory.net/rapid"

	configAPI "txservice/api/config"
)

type client struct {
	lastHeaderNum uint64
	eventsResult  []Headers
	headers       []*types.Header

	*fsm.FSM
}

type Headers []*types.Header

func (h *Headers) Clone() Headers {
	return *h
}

func newClient() *client {
	cli := &client{
		eventsResult: []Headers{},
		headers: []*types.Header{
			{
				Number: big.NewInt(0),
			},
		},
	}

	cli.FSM = cli.newFSM()

	return cli
}

func (c *client) newFSM() *fsm.FSM {
	return fsm.NewFSM(
		None,
		fsm.Events{
			{Name: NoneEvent, Src: []string{None, Stay, CanonicalBlock, Sync, Fork}, Dst: None},
			{Name: StayEvent, Src: []string{None, Stay, CanonicalBlock, Sync, Fork}, Dst: Stay},
			{Name: CanonicalBlockEvent, Src: []string{None, Stay, Rollback, CanonicalBlock, Sync, Fork}, Dst: CanonicalBlock},
			{Name: RollbackEvent, Src: []string{None, Stay, CanonicalBlock, Sync, Fork}, Dst: Rollback},
			{Name: SyncEvent, Src: []string{None, Stay, CanonicalBlock, Rollback, Sync, Fork}, Dst: Sync},
			{Name: ForkEvent, Src: []string{None, Stay, CanonicalBlock, Rollback, Sync, Fork}, Dst: Fork},
		},
		fsm.Callbacks{
			NoneEvent:           c.noneEvent(),
			StayEvent:           c.stayEvent(),
			CanonicalBlockEvent: c.canonicalBlockEvent(),
			RollbackEvent:       c.rollback(),
			SyncEvent:           c.sync(),
			ForkEvent:           c.fork(),
		},
	)
}

func (c *client) noneEvent() func(event *fsm.Event) {
	return func(event *fsm.Event) {
		c.eventsResult = append(c.eventsResult, []*types.Header{})
	}
}

func (c *client) stayEvent() func(event *fsm.Event) {
	return func(event *fsm.Event) {
		c.eventsResult = append(c.eventsResult, []*types.Header{c.headers[c.lastHeaderNum]})
	}
}

func (c *client) canonicalBlockEvent() func(event *fsm.Event) {
	return func(event *fsm.Event) {
		timestamp := uint64(1)
		lastHeader := c.headers[c.lastHeaderNum]

		header := &types.Header{
			ParentHash: lastHeader.Hash(),
			Number:     big.NewInt(lastHeader.Number.Int64() + 1),
			Time:       lastHeader.Time + timestamp,
		}

		c.lastHeaderNum++
		c.headers = append(c.headers, header)

		c.eventsResult = append(c.eventsResult, []*types.Header{header})
	}
}

const (
	rollbackMin = 1
)

func (c *client) rollback() func(event *fsm.Event) {
	return func(event *fsm.Event) {
		shift := event.Args[0].(uint64)

		if int(c.lastHeaderNum)-int(shift) < 0 {
			c.lastHeaderNum = 0
		} else {
			c.lastHeaderNum -= shift
		}

		c.headers = c.headers[:c.lastHeaderNum+1]

		c.eventsResult = append(c.eventsResult, []*types.Header{c.headers[c.lastHeaderNum]})
	}
}

const (
	syncMin = 0
	syncMax = 10
)

func (c *client) sync() func(event *fsm.Event) {
	return func(event *fsm.Event) {
		countSyncs := event.Args[0].(uint64)
		syncClient := event.Args[1].(*client)

		var (
			headers []*types.Header
			j       uint64
		)

		for i := c.lastHeaderNum + 1; i <= syncClient.lastHeaderNum; i++ {
			headers = append(headers, syncClient.headers[i])
			c.lastHeaderNum = i

			j++

			if j == countSyncs {
				break
			}
		}

		c.headers = append(c.headers, headers...)
		c.eventsResult = append(c.eventsResult, headers)
	}
}

const forkTimestamp = 2

func (c *client) fork() func(event *fsm.Event) {
	return func(event *fsm.Event) {
		blockNumber := event.Args[0].(uint64)

		timestamp := uint64(forkTimestamp)
		lastHeader := c.headers[c.lastHeaderNum]

		header := &types.Header{
			ParentHash: lastHeader.Hash(),
			Number:     big.NewInt(0).SetUint64(lastHeader.Number.Uint64() + blockNumber),
			Time:       lastHeader.Time + timestamp,
		}

		c.lastHeaderNum += blockNumber
		if blockNumber == 0 {
			c.headers[c.lastHeaderNum] = header
		} else {
			c.headers = append(c.headers, header)
		}

		c.eventsResult = append(c.eventsResult, []*types.Header{header})
	}
}

func (c *client) generateEvents(t *rapid.T, clients []*client, cfg configAPI.Config) {
	rapid.SliceOfN(rapid.SampledFrom([]string{NoneEvent, StayEvent, CanonicalBlockEvent, ForkEvent, RollbackEvent, SyncEvent}).
		Filter(func(event string) bool {
			if !c.Can(event) {
				return false
			}

			var args []interface{}

			switch event {
			case ForkEvent:
				args = append(args, rapid.Uint64Range(0, 1).Draw(t, "fork event"))
			case RollbackEvent:
				args = append(args, rapid.Uint64Range(rollbackMin, cfg.GetEthereum().Finality).Draw(t, "shift rollback"))
			case SyncEvent:
				args = append(args,
					rapid.Uint64Range(syncMin, syncMax).Draw(t, "sync n block"),
					rapid.SampledFrom(clients).Draw(t, "client for sync"),
				)
			}

			err := c.FSM.Event(event, args...)
			if err != nil && !strings.Contains(event, c.FSM.Current()) {
				t.Fatal(err)
			}

			return true
		}), 1, 1000).
		Draw(t, "events")
}
