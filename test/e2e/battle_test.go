package e2e

import (
	"context"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	battleassemble "github.com/huangyuCN/atlas-game-layout/services/battle/assemble"
	gwassemble "github.com/huangyuCN/atlas-game-layout/services/gateway/assemble"
	matcherassemble "github.com/huangyuCN/atlas-game-layout/services/matcher/assemble"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
)

// startMatcherForBattle 起 matcher 服务（真 sink：nats 发布 + battle actor 开局）。
func startMatcherForBattle(t *testing.T, ctx context.Context) *matcherassemble.Matcher {
	t.Helper()
	m, err := matcherassemble.New(ctx, matcherassemble.Options{
		NodeID:        "matcher-it",
		EtcdEndpoints: []string{itEtcdEndpoints},
		NatsURL:       itNatsURL,
		RedisAddrs:    []string{itRedisAddr},
		Namespace:     itNS,
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

// TestE2EBattleFullLoop 验证 M7 验收（口径直连）：双客户端完成一局战斗，对局结果一致
// （双方**直连**帧广播/结束通知一致 + 结算事件与 mongo 落库一致）。
func TestE2EBattleFullLoop(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	newGame(t)
	gw := newGateway(t, "a")
	battle := newBattle(t, nil)
	obs := newBattleObserver(t)
	m := startMatcherForBattle(t, ctx)
	a := newBattleClient(t, ctx, gw, battle, frameKCP)
	b := newBattleClient(t, ctx, gw, battle, frameKCP)
	battleID := queueTwo(t, ctx, m, obs.started, a, b)

	playFullBattle(t, ctx, a, b, battleID)
	assertBattleEndConsistency(t, ctx, a, b, battleID, obs.settled)
}

// playFullBattle 双方加入、发送帧输入并等待双方结束通知（胜者一致）。
func playFullBattle(t *testing.T, ctx context.Context, a, b *battleClient, battleID string) {
	t.Helper()
	a.waitTicket(t)
	b.waitTicket(t)
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

// TestE2EBattleReconnect 验证断线重连专项（口径直连）：直连帧连接断开 → 以同一张票重连
// （业务连接 Resume 免密恢复）→ 重新加入（快照回执）→ 补帧（缺失帧拉取）→ 双方结局一致；
// 掉线窗口内回座不得判负（规格 §9.4：取消计时 + 连接重登记 + 既有 SyncFrames 补帧）。
func TestE2EBattleReconnect(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	// 拉长战线（25 格）并放慢帧间隔：留足「断开 → 空闲超时发现 → 窗口内重连」的观察期。
	// 掉线窗口 5s（数据报面空闲读超时 = 1/3 ≈ 1.67s）：既能让断开被真正发现，又远大于重连耗时。
	cfg := battleassemble.DefaultBattleConfig()
	cfg.TrackLen = 25
	cfg.TickInterval = int64(300 * time.Millisecond)
	cfg.OfflineTimeout = 5 * time.Second
	newGame(t)
	gw := newGateway(t, "a")
	battle := newBattle(t, &cfg)
	obs := newBattleObserver(t)
	m := startMatcherForBattle(t, ctx)
	a := newBattleClient(t, ctx, gw, battle, frameKCP)
	b := newBattleClient(t, ctx, gw, battle, frameKCP)
	battleID := queueTwo(t, ctx, m, obs.started, a, b)

	a.waitTicket(t)
	b.waitTicket(t)
	a.joinBattle(t, ctx, battleID)
	b.joinBattle(t, ctx, battleID)
	a.sendFrames(t, ctx, battleID, 1, 40, 1)
	b.sendFrames(t, ctx, battleID, 1, 40, 0)
	floor := dropAndReconnect(t, ctx, gw, battle, a, battleID, cfg.OfflineTimeout/3+500*time.Millisecond)

	// 回座：未判负（无出局广播）、未提前结算、直连恢复后继续收到新帧。
	a.assertNoOut(t)
	b.assertNoOut(t)
	a.assertNoEnd(t)
	a.waitFrameAfter(t, floor)

	// 双方结局一致（a 服务器侧输入延续推进 → a 胜）。
	winnerA, winnerB := a.waitEnd(t), b.waitEnd(t)
	if winnerA != a.playerID || winnerB != a.playerID {
		t.Fatalf("重连后结局不一致: a=%q b=%q want %q", winnerA, winnerB, a.playerID)
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

// dropAndReconnect 断开 a 的直连帧连接后重连，返回断开瞬间的帧号（回座后新帧的判据）：
//   - 直连帧面：用同一张票重连（帧槽逐帧带票），重新 JoinBattle（回执携带当前帧与快照）
//     并 SyncFrames 拉取断点之前的帧；
//   - 业务连接：同样断线重连（Resume 免密恢复绑定）——业务与战斗是两条独立连接，
//     直连后不再"共通道"，两者互不影响；
//   - detectWait：断开后先等这么久再重连，让帧面空闲读超时真正发现掉线并投递掉线事件，
//     从而覆盖「打点 → 窗口内回座取消计时」这条路径（而不是靠重连接管把断开事件盖过去）。
func dropAndReconnect(t *testing.T, ctx context.Context, gw *gwassemble.Gateway, b *battleassemble.Battle, a *battleClient, battleID string, detectWait time.Duration) uint64 {
	t.Helper()
	// a 收到 3 帧后断开直连帧连接（模拟掉线）。
	a.waitFrames(t, 3)
	floor := a.lastFrame().GetFrame().GetFrameId()
	if err := a.frame.close(); err != nil {
		t.Fatalf("关闭直连帧连接: %v", err)
	}
	time.Sleep(detectWait) // 等空闲读超时把掉线事件送进 actor（窗口内仍可回座）

	a.frame = dialDirectFrame(t, ctx, a.kind, frameAddrOf(b, a.kind), a.ticketOf)
	a.frame.notify(a.watchDirect)

	// 业务连接重连（Resume）：身份与推送通道恢复（战斗连接独立，不受影响）。
	if err := a.cli.Close(); err != nil {
		t.Fatalf("关闭业务连接: %v", err)
	}
	sess, cli := dialWSSession(t, gw.WSURL)
	if _, err := sess.Restore(ctx, a.token, a.playerID); err != nil {
		t.Fatalf("重连 Resume: %v", err)
	}
	a.sess, a.cli = sess, cli
	a.watchNotifies()

	// a 重新连接并加入（回执携带当前帧与快照）。
	join := a.joinBattle(t, ctx, battleID)
	if join.GetCurrentFrame() < 3 || join.GetSnapshot() == nil {
		t.Fatalf("重连加入回执缺快照: %+v", join)
	}
	// 补帧：从 0 拉取缺失帧（断点之前的全部帧）。
	sync, err := a.syncFrames(ctx, battleID, 0)
	if err != nil {
		t.Fatalf("SyncFrames: %v", err)
	}
	if len(sync.GetMissed()) == 0 || sync.GetCurrentFrame() < join.GetCurrentFrame() {
		t.Fatalf("补帧回执不符: frames=%d current=%d joinCurrent=%d",
			len(sync.GetMissed()), sync.GetCurrentFrame(), join.GetCurrentFrame())
	}
	return floor
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
