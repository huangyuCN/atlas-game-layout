package e2e

import (
	"context"
	"sync"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgmongo "github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	battleassemble "github.com/huangyuCN/atlas-game-layout/services/battle/assemble"
	gwassemble "github.com/huangyuCN/atlas-game-layout/services/gateway/assemble"
	matcherassemble "github.com/huangyuCN/atlas-game-layout/services/matcher/assemble"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	wst "github.com/huangyuCN/atlas/transport/websocket"
	natsgo "github.com/nats-io/nats.go"
	"go.mongodb.org/mongo-driver/bson"
	"google.golang.org/protobuf/encoding/protojson"
)

// battleClient 是一端战斗客户端（ws 单通道：认证 + 战斗共用连接），
// 记录服务端推送的帧广播与战斗结束通知。
type battleClient struct {
	cli      *wst.Client
	battle   gatewayv1.GatewayBattleWSClient
	playerID string
	token    string

	mu     sync.Mutex
	frames []*gatewayv1.FrameBroadcast
	ends   []*gatewayv1.BattleEndNotify
}

// newBattleClient 注册登录并挂接推送监听。
func newBattleClient(t *testing.T, ctx context.Context, gw *gwassemble.Gateway) *battleClient {
	t.Helper()
	cli, err := wst.NewClient(ctx, gw.WSURL)
	if err != nil {
		t.Fatalf("ws client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	token, playerID := loginFlow(t, ctx, gatewayv1.NewGatewayAuthWSClient(cli))
	c := &battleClient{
		cli:      cli,
		battle:   gatewayv1.NewGatewayBattleWSClient(cli),
		playerID: playerID,
		token:    token,
	}
	c.watchNotifies()
	return c
}

// watchNotifies 挂接推送监听（帧广播/战斗结束通知记录）。
func (c *battleClient) watchNotifies() {
	c.cli.OnNotify(func(operation string, payload []byte) {
		switch operation {
		case consts.PushOpFrameBroadcast:
			var fb gatewayv1.FrameBroadcast
			if err := protojson.Unmarshal(payload, &fb); err != nil {
				return
			}
			c.mu.Lock()
			c.frames = append(c.frames, &fb)
			c.mu.Unlock()
		case consts.PushOpBattleEnd:
			var end gatewayv1.BattleEndNotify
			if err := protojson.Unmarshal(payload, &end); err != nil {
				return
			}
			c.mu.Lock()
			c.ends = append(c.ends, &end)
			c.mu.Unlock()
		}
	})
}

// joinBattle 加入战斗（战斗 actor 懒激活期间重试）。
func (c *battleClient) joinBattle(t *testing.T, ctx context.Context, battleID string) *gatewayv1.JoinBattleReply {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastJoin *gatewayv1.JoinBattleReply
	var lastErr error
	for time.Now().Before(deadline) {
		join, err := c.battle.JoinBattle(ctx, &gatewayv1.JoinBattleRequest{
			Token: c.token, PlayerId: c.playerID, BattleId: battleID,
		})
		if err == nil && join.GetOk() {
			return join
		}
		lastJoin, lastErr = join, err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("JoinBattle 重试耗尽: err=%v reply=%+v", lastErr, lastJoin)
	return nil
}

// sendFrames 发送 [from,to] 帧输入（payload 单字节步进值）。
func (c *battleClient) sendFrames(t *testing.T, ctx context.Context, battleID string, from, to uint64, step byte) {
	t.Helper()
	for i := from; i <= to; i++ {
		_, err := c.battle.SendFrameInput(ctx, &gatewayv1.SendFrameInputRequest{
			BattleId: battleID,
			Input:    &locksteppb.LockstepInput{FrameId: i, PlayerId: c.playerID, Payload: []byte{step}},
		})
		if err != nil {
			t.Fatalf("SendFrameInput(%d): %v", i, err)
		}
	}
}

// waitFrames 等待收到至少 n 帧广播（断言超时失败）。
func (c *battleClient) waitFrames(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		got := len(c.frames)
		c.mu.Unlock()
		if got >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("仅收到 %d 帧广播", got)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitEnd 等待战斗结束通知并返回胜者。
func (c *battleClient) waitEnd(t *testing.T) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		c.mu.Lock()
		got := len(c.ends)
		var winner string
		if got > 0 {
			winner = c.ends[0].GetWinnerPlayerId()
		}
		c.mu.Unlock()
		if got > 0 {
			return winner
		}
		if time.Now().After(deadline) {
			t.Fatal("未收到战斗结束通知")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// lastFrame 返回最新一帧广播。
func (c *battleClient) lastFrame() *gatewayv1.FrameBroadcast {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.frames) == 0 {
		return nil
	}
	return c.frames[len(c.frames)-1]
}

// startMatcherForBattle 起 matcher 服务（真 sink：nats 发布 + battle actor 开局）。
func startMatcherForBattle(t *testing.T, ctx context.Context) *matcherassemble.Matcher {
	t.Helper()
	m, err := matcherassemble.New(ctx, matcherassemble.Options{
		NodeID:        "matcher-it",
		EtcdEndpoints: []string{itEtcdEndpoints},
		NatsURL:       itNatsURL,
		RedisAddr:     itRedisAddr,
	})
	if err != nil {
		t.Fatalf("matcher 装配: %v", err)
	}
	t.Cleanup(func() { _ = m.Stop(context.Background()) })
	return m
}

// queueTwo 双玩家入队并等待成局事件回执 battleID。
// 直连 matcher 是服务级白盒测试手段（本测试验证 battle 链路，撮合仅作开局前置；
// 客户端全链路形态由 scripts/e2e 验证：gateway → PlayerActor → matcher）。
func queueTwo(t *testing.T, ctx context.Context, m *matcherassemble.Matcher, startedCh <-chan *matcherv1.MatchStartedEvent, a, b *battleClient) string {
	t.Helper()
	gcli, err := atlasgrpc.DialInsecure(ctx, atlasgrpc.WithEndpoint(m.GRPCURL))
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	defer gcli.Close()
	svc := matcherv1.NewMatcherClient(gcli)
	queue := func(c *battleClient, level int32) {
		if _, err := svc.QueueMatch(ctx, &matcherv1.QueueMatchRequest{
			PlayerId: c.playerID,
			Player:   &commonv1.PlayerSummary{PlayerId: c.playerID, Level: level},
		}); err != nil {
			t.Fatalf("入队 %s: %v", c.playerID, err)
		}
	}
	queue(a, 10)
	queue(b, 11)

	select {
	case ev := <-startedCh:
		if ev.GetBattleId() == "" || len(ev.GetPlayerIds()) != 2 {
			t.Fatalf("成局事件不符: %+v", ev)
		}
		return ev.GetBattleId()
	case <-time.After(5 * time.Second):
		t.Fatal("未收到成局事件")
	}
	return ""
}

// subscribeSettled 订阅战斗结算事件。
func subscribeSettled(t *testing.T, nc *natsgo.Conn) <-chan *battlev1.BattleSettledEvent {
	t.Helper()
	ch := make(chan *battlev1.BattleSettledEvent, 4)
	if _, err := nats.Subscribe(nc, consts.EventTopic("battle.settled"), func(_ string, data []byte) {
		var ev battlev1.BattleSettledEvent
		if err := protojson.Unmarshal(data, &ev); err == nil {
			ch <- &ev
		}
	}); err != nil {
		t.Fatalf("订阅结算事件: %v", err)
	}
	return ch
}

// battleObserver 是成局/结算事件订阅（nats 事件总线可观测性）。
type battleObserver struct {
	started <-chan *matcherv1.MatchStartedEvent
	settled <-chan *battlev1.BattleSettledEvent
}

// newBattleObserver 建立观察 nats 连接并订阅成局与结算事件。
func newBattleObserver(t *testing.T) *battleObserver {
	t.Helper()
	nc, err := nats.Connect(nats.Options{URL: itNatsURL, Name: "e2e-battle-observer"})
	if err != nil {
		t.Fatalf("nats: %v", err)
	}
	t.Cleanup(nc.Close)
	startedCh := make(chan *matcherv1.MatchStartedEvent, 4)
	if _, err := nats.Subscribe(nc, consts.MatchStartedTopic(), func(_ string, data []byte) {
		var ev matcherv1.MatchStartedEvent
		if err := protojson.Unmarshal(data, &ev); err == nil {
			startedCh <- &ev
		}
	}); err != nil {
		t.Fatalf("订阅成局事件: %v", err)
	}
	return &battleObserver{started: startedCh, settled: subscribeSettled(t, nc)}
}

// TestE2EBattleFullLoop 验证 M7 验收：双客户端完成一局战斗，对局结果一致
// （双方帧广播/结束通知一致 + 结算事件与 mongo 落库一致）。
func TestE2EBattleFullLoop(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	newGame(t)
	gw := newGateway(t, "a")
	newBattle(t, nil)
	obs := newBattleObserver(t)
	m := startMatcherForBattle(t, ctx)
	a := newBattleClient(t, ctx, gw)
	b := newBattleClient(t, ctx, gw)
	battleID := queueTwo(t, ctx, m, obs.started, a, b)

	playFullBattle(t, ctx, a, b, battleID)
	assertBattleEndConsistency(t, ctx, a, b, battleID, obs.settled)
}

// playFullBattle 双方加入、发送帧输入并等待双方结束通知（胜者一致）。
func playFullBattle(t *testing.T, ctx context.Context, a, b *battleClient, battleID string) {
	t.Helper()
	a.joinBattle(t, ctx, battleID)
	b.joinBattle(t, ctx, battleID)
	a.sendFrames(t, ctx, battleID, 1, 20, 1)
	b.sendFrames(t, ctx, battleID, 1, 20, 0)

	// 双方结束通知胜者一致（a 前进、b 原地 → a 胜）。
	winnerA, winnerB := a.waitEnd(t), b.waitEnd(t)
	if winnerA != a.playerID || winnerB != a.playerID {
		t.Fatalf("结束通知胜者不一致: a=%q b=%q want %q", winnerA, winnerB, a.playerID)
	}
}

// assertBattleEndConsistency 断言帧广播、结算事件与 mongo 落库三方一致。
func assertBattleEndConsistency(t *testing.T, ctx context.Context, a, b *battleClient, battleID string, settledCh <-chan *battlev1.BattleSettledEvent) {
	t.Helper()
	// 双方帧广播一致（末帧快照哈希相同）。
	la, lb := a.lastFrame(), b.lastFrame()
	if la == nil || lb == nil || la.GetFrame().GetFrameId() != lb.GetFrame().GetFrameId() {
		t.Fatalf("末帧帧号不一致: a=%+v b=%+v", la, lb)
	}
	ha, hb := la.GetFrame().GetSnapshot().GetStateHash(), lb.GetFrame().GetSnapshot().GetStateHash()
	if string(ha) != string(hb) {
		t.Fatalf("末帧快照哈希不一致: %x vs %x", ha, hb)
	}
	// 结算事件与结束通知一致。
	select {
	case ev := <-settledCh:
		if ev.GetBattleId() != battleID || settledWinnerOf(ev) != a.playerID || len(ev.GetPlayers()) != 2 {
			t.Fatalf("结算事件不符: %+v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("未收到结算事件")
	}
	// mongo 落库与结算一致。
	assertResultSaved(t, ctx, battleID, a.playerID)
}

// TestE2EBattleReconnect 验证断线重连专项：中途断开 → 重新加入（快照回执）
// → 补帧（缺失帧拉取）→ 双方结局一致。
func TestE2EBattleReconnect(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 拉长战线（25 格）留足断线重连窗口。
	cfg := battleassemble.DefaultBattleConfig()
	cfg.TrackLen = 25
	newGame(t)
	gw := newGateway(t, "a")
	newBattle(t, &cfg)
	obs := newBattleObserver(t)
	m := startMatcherForBattle(t, ctx)
	a := newBattleClient(t, ctx, gw)
	b := newBattleClient(t, ctx, gw)
	battleID := queueTwo(t, ctx, m, obs.started, a, b)

	a.joinBattle(t, ctx, battleID)
	b.joinBattle(t, ctx, battleID)
	a.sendFrames(t, ctx, battleID, 1, 40, 1)
	b.sendFrames(t, ctx, battleID, 1, 40, 0)
	a2 := dropAndReconnect(t, ctx, gw, a, battleID)

	// 双方结局一致（a 服务器侧输入延续推进 → a 胜）。
	winnerA2, winnerB := a2.waitEnd(t), b.waitEnd(t)
	if winnerA2 != a.playerID || winnerB != a.playerID {
		t.Fatalf("重连后结局不一致: a=%q b=%q want %q", winnerA2, winnerB, a.playerID)
	}
	select {
	case ev := <-obs.settled:
		if settledWinnerOf(ev) != a.playerID {
			t.Fatalf("结算事件胜者不符: %+v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("未收到结算事件")
	}
}

// dropAndReconnect 断开 a 的连接后重连：加入（快照回执）+ 补帧（缺失帧拉取）。
func dropAndReconnect(t *testing.T, ctx context.Context, gw *gwassemble.Gateway, a *battleClient, battleID string) *battleClient {
	t.Helper()
	// a 收到 3 帧后断开（模拟掉线）。
	a.waitFrames(t, 3)
	_ = a.cli.Close()

	// a 重新连接并加入（回执携带当前帧与快照）。
	a2 := newBattleClientWithToken(t, ctx, gw, a)
	join := a2.joinBattle(t, ctx, battleID)
	if join.GetCurrentFrame() < 3 || join.GetSnapshot() == nil {
		t.Fatalf("重连加入回执缺快照: %+v", join)
	}
	// 补帧：从 0 拉取缺失帧（断点之前的全部帧）。
	sync, err := a2.battle.SyncFrames(ctx, &locksteppb.SyncFrameRequest{
		SessionId: battleID, FromFrameId: 0, Limit: 100,
	})
	if err != nil {
		t.Fatalf("SyncFrames: %v", err)
	}
	if len(sync.GetFrames()) == 0 || sync.GetConfirmedFrameId() < join.GetCurrentFrame() {
		t.Fatalf("补帧回执不符: frames=%d confirmed=%d current=%d",
			len(sync.GetFrames()), sync.GetConfirmedFrameId(), join.GetCurrentFrame())
	}
	return a2
}

// newBattleClientWithToken 以既有令牌重建 ws 连接（断线重连场景）。
func newBattleClientWithToken(t *testing.T, ctx context.Context, gw *gwassemble.Gateway, prev *battleClient) *battleClient {
	t.Helper()
	cli, err := wst.NewClient(ctx, gw.WSURL)
	if err != nil {
		t.Fatalf("重连 ws client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	auth := gatewayv1.NewGatewayAuthWSClient(cli)
	_, err = auth.Heartbeat(ctx, &gatewayv1.HeartbeatRequest{PlayerId: prev.playerID, Token: prev.token, Ts: 1})
	if err != nil {
		t.Fatalf("重连心跳: %v", err)
	}
	c := &battleClient{
		cli:      cli,
		battle:   gatewayv1.NewGatewayBattleWSClient(cli),
		playerID: prev.playerID,
		token:    prev.token,
	}
	c.watchNotifies()
	return c
}

// settledWinnerOf 从结算事件取胜者玩家 ID。
func settledWinnerOf(ev *battlev1.BattleSettledEvent) string {
	for _, p := range ev.GetPlayers() {
		if p.GetWin() {
			return p.GetPlayerId()
		}
	}
	return ""
}

// assertResultSaved 断言 mongo 已落库战斗结果且胜者一致。
func assertResultSaved(t *testing.T, ctx context.Context, battleID, winner string) {
	t.Helper()
	mc, err := pkgmongo.NewClient(ctx, pkgmongo.Options{URI: itMongoURI, Database: itMongoDB})
	if err != nil {
		t.Fatalf("mongo: %v", err)
	}
	defer mc.Close(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for {
		var res struct {
			Players []struct {
				PlayerID string `bson:"player_id"`
				Win      bool   `bson:"win"`
				Score    int32  `bson:"score"`
			} `bson:"players"`
		}
		err := mc.Collection("battle_results").FindOne(ctx, bson.M{"_id": battleID}).Decode(&res)
		if err == nil {
			for _, p := range res.Players {
				if p.PlayerID == winner && (!p.Win || p.Score != 1) {
					t.Fatalf("落库胜者不符: %+v", res.Players)
				}
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("战斗结果未落库: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
