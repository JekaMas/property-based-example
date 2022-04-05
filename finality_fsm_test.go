package finality

import (
	"context"
	"fmt"
	"math/big"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/golang/mock/gomock"
	json "github.com/json-iterator/go"
	"github.com/looplab/fsm"
	"github.com/streadway/amqp"
	"pgregory.net/rapid"

	"txservice/api/btserror"
	configAPI "txservice/api/config"
	rMock "txservice/api/rabbit"
	"txservice/api/server/deps/cluster"
	"txservice/api/server/deps/cluster/connection"
	"txservice/api/server/deps/cluster/reader"
	"txservice/api/server/deps/cluster/writer"
	"txservice/api/server/deps/database"
	"txservice/api/server/deps/database/model"
	"txservice/api/server/deps/database/safesqlx"
	"txservice/api/server/deps/rabbit"
	finalityinterface "txservice/api/server/finality"
	"txservice/api/server/finality/finality"
	"txservice/api/server/proxy/mock"
	"txservice/config"
	"txservice/proto/pb"
)

const (
	errFinalityStop = btserror.BtsError("stop finality")
)

type finalFSMEvent string

const (
	Push                   finalFSMEvent = "Push"
	RemoveCanonical        finalFSMEvent = "RemoveCanonical"
	ConsensusDetected      finalFSMEvent = "ConsensusDetected"
	ConsensusWillBeReached finalFSMEvent = "ConsensusWillBeReached"
	ConsensusReached       finalFSMEvent = "ConsensusReached"
	BlockFinalized         finalFSMEvent = "BlockFinalized"

	ReorgFailed finalFSMEvent = "ReorgDetectedButNotFoundVotes"
)

type FinalFSM struct {
	*fsm.FSM

	mu sync.Mutex

	ctx context.Context

	machines []*finality.SafeFinality

	cfg      *config.Config
	database *database.Database
	sealers  *cluster.Clients

	readerMock *reader.MockReader

	headers         map[common.Hash]*finality.HeaderCount
	headersFinality map[uint64]*types.Header

	latestConsensus uint64

	events []string
}

func NewFinalFSM(ctx context.Context, t *rapid.T, ctrl *gomock.Controller, cfg *config.Config, machineCount int) *FinalFSM {
	f := &FinalFSM{
		cfg: cfg,

		headers:         make(map[common.Hash]*finality.HeaderCount),
		headersFinality: map[uint64]*types.Header{},
	}

	f.ctx = configAPI.WithContext(ctx, cfg)

	f.InitReader(ctrl)
	f.InitRabbit(ctrl, t)
	f.InitDatabase(ctrl)
	f.InitSealers(ctrl)
	f.InitFinalMachines(t, machineCount)

	return f
}

func (f *FinalFSM) InitReader(ctrl *gomock.Controller) {
	f.readerMock = reader.NewMockReader(ctrl)
	f.ctx = reader.WithContext(f.ctx, f.readerMock)
}

func (f *FinalFSM) InitRabbit(ctrl *gomock.Controller, t *rapid.T) {
	rabbitMock := rMock.NewMockRabbit(ctrl)
	rabbitMock.EXPECT().QueueDeclareBind(gomock.Any(), gomock.Any(), amqp.Table{}).Return(nil).AnyTimes()

	checkContinuousRabbit(t, f.cfg, rabbitMock)

	publisher := rabbit.NewPublisher(f.cfg, rabbitMock)
	publisher.ClearStatuses()

	f.ctx = rabbit.WithContext(f.ctx, publisher)
}

func (f *FinalFSM) InitDatabase(ctrl *gomock.Controller) {
	txRepo := mock.NewMockTransactionRepository(ctrl)
	txRepo.EXPECT().GetTxsStatusesByHashes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(map[common.Hash]map[int]*model.TxStatuses{}, errFinalityStop).AnyTimes()

	eventsRepo := mock.NewMockEventsRepository(ctrl)
	eventsRepo.EXPECT().GetStuckEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	blockRepo := mock.NewMockBlockRepository(ctrl)
	blockRepo.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, db safesqlx.SQL) (*model.Block, error) {
		return new(model.Block), nil
	}).AnyTimes()
	blockRepo.EXPECT().Update(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	f.database = database.NewDatabase(
		f.ctx,
		f.cfg,
		nil,
		txRepo,
		blockRepo,
		eventsRepo,
		eventsSetting(ctrl, f.readerMock, f.cfg),
		nil,
		"",
		"",
	)

	f.database.ClearStatuses()

	f.ctx = database.WithContext(f.ctx, f.database)
}

func (f *FinalFSM) InitSealers(ctrl *gomock.Controller) {
	conn := connection.NewMockReadWriteConnection(ctrl)
	conn.EXPECT().HeaderByHash(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	conn.EXPECT().
		HeaderByNumber(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, number uint64) (*types.Header, error) {
		timeout := time.NewTimer(time.Second)
		defer timeout.Stop()

		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-timeout.C:
				f.mu.Lock()
				defer f.mu.Unlock()

				latest := f.headersFinality[number+1]

				return &types.Header{Number: big.NewInt(0).SetUint64(number), ParentHash: latest.ParentHash, Time: latest.Time}, nil
			case <-ticker.C:
				f.mu.Lock()
				header, ok := f.headersFinality[number]
				f.mu.Unlock()

				if ok {
					return header, nil
				}
			}
		}
	}).
		AnyTimes()

	conn.EXPECT().BlockByNumber(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, number uint64) (*types.Block, error) {
		return &types.Block{
			ReceivedAt:   time.Time{},
			ReceivedFrom: nil,
		}, nil
	}).AnyTimes()

	subs := &fakeSubscription{
		make(chan error, 1),
	}

	f.sealers = cluster.NewClients([]*cluster.Client{
		cluster.NewClient(conn, "test1", f.cfg),
		cluster.NewClient(conn, "test2", f.cfg),
		cluster.NewClient(conn, "test3", f.cfg),
		cluster.NewClient(conn, "test4", f.cfg),
		cluster.NewClient(conn, "test5", f.cfg),
	})

	for _, c := range f.sealers.Clients {
		c.ClearStatuses()

		conn.EXPECT().SubscribeNewHead(gomock.Any(), gomock.Any()).Return(subs, nil).AnyTimes()
	}

	f.ctx = writer.WithContext(f.ctx, writer.NewSealers(f.sealers.Clients))
}

func (f *FinalFSM) InitFinalMachines(t *rapid.T, machineCount int) {
	if machineCount == 0 {
		machineCount = rapid.IntRange(1, 1).Draw(t, "finality machine count").(int)
	}

	f.machines = make([]*finality.SafeFinality, machineCount)

	for i := range f.machines {
		var err error

		f.machines[i], err = finality.NewSafeFinality(f.cfg)
		if err != nil {
			t.Error(err)
		}

		f.machines[i].ClearStatuses()

		if err := f.machines[i].Connect(f.ctx); err != nil {
			t.Error(err)
		}

		f.ctx = finalityinterface.WithContext(f.ctx, f.machines[i])
	}

	for i := range f.machines {
		go checkIncreasingFinality(f.ctx, t, f.machines[i])
	}
}

func (f *FinalFSM) EventPush(t *rapid.T, clientIndex int, headers []*types.Header) {
	for _, header := range headers {
		headerVote, ok := f.headers[header.Hash()]
		if !ok {
			headerVote = finality.NewHeaderCount(header)
			f.headers[header.Hash()] = headerVote
		}

		headerVote.Vote(strconv.Itoa(clientIndex))

		wg := &sync.WaitGroup{}

		f.EventConsensus(t, wg)

		for i := range f.machines {
			f.machines[i].EthereumNotifications[clientIndex].NewBlocks <- header
			f.AddEvent(Push, fmt.Sprintf("block %d(%s) to client %d on machine %d", header.Number.Uint64(), header.Hash().Hex(), clientIndex, i))
		}

		wg.Wait()
	}
}

func (f *FinalFSM) EventConsensus(t *rapid.T, wg *sync.WaitGroup) {
	headers := make([]*finality.HeaderCount, 0, len(f.headers))

	for _, headerVote := range f.headers {
		headers = append(headers, headerVote)
	}

	sort.SliceStable(headers, func(i, j int) bool {
		return headers[i].Header.Hash().Hex() < headers[j].Header.Hash().Hex()
	})

	for _, headerVote := range headers {
		if headerVote.Header.Number.Uint64() <= f.latestConsensus {
			delete(f.headers, headerVote.Header.Hash())
			f.AddEvent(RemoveCanonical, fmt.Sprintf("block %d(%s) is deprecated", headerVote.Header.Number.Uint64(), headerVote.Header.Hash().Hex()))

			continue
		}

		if f.sealers.IsConsensus(headerVote.CountVotes()) && headerVote.Header.Number.Uint64() > f.cfg.GetEthereum().Finality {
			wg.Add(1)
			f.ConsensusDetected(t, headerVote, wg)
		}
	}
}

func (f *FinalFSM) ConsensusDetected(t *rapid.T, headerVote *finality.HeaderCount, wg *sync.WaitGroup) {
	f.AddEvent(ConsensusDetected, fmt.Sprintf("block %d(%s) is consensus", headerVote.Header.Number.Uint64(), headerVote.Header.Hash().Hex()))

	numberNew := headerVote.Header.Number.Uint64() - 1
	numberLast := f.machines[0].GetLatestHeaderNumber()

	if numberNew != numberLast && numberNew <= numberLast+f.cfg.GetEthereum().Finality {
		headerVoteReorg, ok := f.headers[headerVote.Header.ParentHash]
		if ok {
			f.mu.Lock()
			f.headersFinality[headerVoteReorg.Header.Number.Uint64()] = headerVoteReorg.Header
			f.mu.Unlock()
		}
	}

	f.latestConsensus = headerVote.Header.Number.Uint64()

	f.mu.Lock()
	f.headersFinality[headerVote.Header.Number.Uint64()] = headerVote.Header
	f.mu.Unlock()

	f.AddEvent(ConsensusWillBeReached, fmt.Sprintf("block %d(%s) got %d votes", headerVote.Header.Number.Uint64(), headerVote.Header.Hash().Hex(), headerVote.CountVotes()))

	ch, cancel, err := f.machines[0].Finality.Subscribe(f.ctx, "rapid"+strconv.FormatUint(rand.Uint64(), 10))
	if err != nil {
		t.Fatal(err)
	}

	go f.EventFinalized(t, headerVote.Header, ch, cancel, wg)
}

const timeoutFinalized = time.Second * 30

func (f *FinalFSM) EventFinalized(t *rapid.T, header *types.Header, ch <-chan uint64, cancel func(), wg *sync.WaitGroup) {
	defer wg.Done()
	defer cancel()

	timeout := time.NewTimer(timeoutFinalized)
	defer timeout.Stop()

	for {
		select {
		case <-timeout.C:
			finalizedHeader := f.machines[0].GetFinalBlock(f.ctx)
			t.Errorf("block %d not finalized and final %d(%s)", header.Number.Uint64()-1, finalizedHeader.Number, finalizedHeader.Hash.Hex())

			return
		case <-ch:
			if !f.IsFinalized(header) {
				continue
			}

			f.AddEvent(BlockFinalized, fmt.Sprintf("block %d(%s) finalized", header.Number.Uint64(), header.Hash().Hex()))

			return
		}
	}
}

func (f *FinalFSM) IsConsensusAccepted(header *types.Header) bool {
	for _, machine := range f.machines {
		if machine.GetLatestHeaderNumber() != header.Number.Uint64() {
			return false
		}
	}

	return true
}

func (f *FinalFSM) IsFinalized(header *types.Header) bool {
	for _, machine := range f.machines {
		if machine.GetFinalBlockNumber(f.ctx) != header.Number.Uint64()-1 {
			return false
		}
	}

	return true
}

func (f *FinalFSM) AddEvent(event finalFSMEvent, msg string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.events = append(f.events, fmt.Sprintf("%s: %s", event, msg))
}

func (f *FinalFSM) Close(t *rapid.T) {
	err := f.database.Terminate()
	if err != nil {
		t.Error(err)
	}

	for _, machine := range f.machines {
		machine.Close()
	}
}

func checkContinuousRabbit(t *rapid.T, cfg *config.Config, rabbitMock *rMock.MockRabbit) {
	// rabbit prepare
	queue, err := pb.GetEventsResponseQueue(cfg, pb.Sources_name[int32(pb.Sources_chainhub)])
	if err != nil {
		t.Error(err)
	}

	var latestFrom uint64

	// Read GetEvents messages
	rabbitMock.EXPECT().
		Publish(gomock.Any(), cfg.Rabbit.Exchange, queue, gomock.Any(), rabbit.ContentTypeText).
		DoAndReturn(func(ctx context.Context, exchange string, key string, body []byte, contentType string) error {
			var resp pb.GetEventsResponse

			innerErr := json.ConfigFastest.Unmarshal(body, &resp)
			if innerErr != nil {
				t.Error(innerErr)
			}

			// INVARIANT: Expect the number of eventsResult to be equal to the last final block
			if latestFrom+1 != resp.FromBlock {
				t.Errorf("skip blocks: %d != %d", latestFrom+1, resp.FromBlock)
			}

			latestFrom = resp.ToBlock

			return nil
		}).
		AnyTimes()
}

func eventsSetting(ctrl *gomock.Controller, readerMock *reader.MockReader, cfg *config.Config) *mock.MockContinuousEventsRepository {
	repo := mock.NewMockContinuousEventsRepository(ctrl)

	currentEventNumber := new(uint64)

	repo.EXPECT().
		GetBlockNumberAndLock(gomock.Any(), gomock.Any(), cfg.GetServer().ContinuousGetEventsLockTime, gomock.Any()).
		DoAndReturn(func(_ context.Context, db *database.Database, lock time.Duration, finalityBlockNumber uint64) (uint64, error) {
			return atomic.LoadUint64(currentEventNumber), nil
		}).
		AnyTimes()

	// Set new continuous eventsResult block number
	repo.EXPECT().
		UpdateBlockNumberAndUnlock(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, db *database.Database, blockNumber uint64) error {
			atomic.StoreUint64(currentEventNumber, blockNumber)

			return nil
		}).AnyTimes()

	readerMock.EXPECT().
		FilterLogs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
			logs := make([]types.Log, query.ToBlock.Uint64()-query.FromBlock.Uint64()+1)
			for i := uint64(0); i < query.ToBlock.Uint64()-query.FromBlock.Uint64()+1; i++ {
				logs[i] = types.Log{
					BlockNumber: i + query.FromBlock.Uint64(),
				}
			}

			return logs, nil
		}).
		AnyTimes()

	return repo
}

func checkIncreasingFinality(ctx context.Context, t *rapid.T, final *finality.SafeFinality) {
	ch, unsubscribe, err := final.Subscriber.Subscribe(ctx, "rapid"+strconv.FormatUint(rand.Uint64(), 10))
	if err != nil {
		t.Error(err)
	}

	defer unsubscribe()

	var currentFinalityNumber uint64

	for {
		select {
		case <-ctx.Done():
			return
		case header := <-ch:
			if header <= currentFinalityNumber {
				t.Errorf("finality not only increasing: current header %d, new header %d", currentFinalityNumber, header)
			}

			currentFinalityNumber = header
		}
	}
}

type fakeSubscription struct {
	err chan error
}

func (s *fakeSubscription) Unsubscribe() {
}

func (s *fakeSubscription) Err() <-chan error {
	return s.err
}
