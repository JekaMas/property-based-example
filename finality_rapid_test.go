package finality

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"pgregory.net/rapid"

	"txservice/api/server/events"
	"txservice/config"
)

// The test may generate incorrect blockchains for ethereum clients and finality throws an error
// Example: event `sync` adds blocks of another chain to itself, without replacing previous blocks, which leads to an incorrect chain
func TestFinalityRapid(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("property-based test")
	}

	cores := 1
	cfg := config.New()

	for i := 0; i < cores; i++ {
		t.Run(fmt.Sprintf("process-%d", i), func(t *testing.T) {
			t.Parallel()

			rapid.Check(t, func(rapidT *rapid.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				finalityFSM := NewFinalFSM(ctx, rapidT, ctrl, cfg, 0)
				finalityFSMCheck := NewFinalFSM(ctx, rapidT, ctrl, cfg, len(finalityFSM.machines))

				defer finalityFSM.Close(rapidT)

				clients := make([]*client, 5)
				for i := range clients {
					clients[i] = newClient()
				}

				var maxCountEvents int

				for _, c := range clients {
					c.generateEvents(rapidT, clients, cfg)

					if len(c.eventsResult) > maxCountEvents {
						maxCountEvents = len(c.eventsResult)
					}
				}

				go events.StartEventsController(ctx, cfg)

				for i := 0; i < maxCountEvents; i++ {
					for j, client := range clients {
						if len(client.eventsResult) <= i {
							continue
						}

						finalityFSM.EventPush(rapidT, j, client.eventsResult[i].Clone())
						finalityFSMCheck.EventPush(rapidT, j, client.eventsResult[i].Clone())
					}
				}

				// compare
				if !reflect.DeepEqual(finalityFSM.events, finalityFSMCheck.events) {
					t.Log("First check")
					for _, event := range finalityFSM.events {
						t.Log(event)
					}
					t.Log("Second check")
					for _, event := range finalityFSMCheck.events {
						t.Log(event)
					}

					rapidT.Fatal("events not equal")
				}
			})
		})
	}
}
