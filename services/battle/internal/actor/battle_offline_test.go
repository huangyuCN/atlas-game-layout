// 掉线/重连策略用例（规格 §9）：打点 → 到点判负 + 出局广播 → 只剩一人判胜结算；
// 窗口内回座取消计时；过期事件与迁移窗口的忽略语义。
package actor

import (
	"sync"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz"
	"github.com/huangyuCN/atlas/metrics"
)

// fakePresence 是直连在场复核端口的内存实现（未配置的玩家一律视为不在场）。
type fakePresence struct {
	mu     sync.Mutex
	online map[string]bool
}

// newFakePresence 构造在场复核假件（online 里的玩家视为仍有存活直连）。
func newFakePresence(online ...string) *fakePresence {
	p := &fakePresence{online: make(map[string]bool, len(online))}
	for _, id := range online {
		p.online[id] = true
	}
	return p
}

// Online 实现 biz.ConnPresence：返回玩家当前是否有存活直连。
func (p *fakePresence) Online(playerID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.online[playerID]
}

// set 设置玩家的在场状态（模拟重连/断开后的注册表状态）。
func (p *fakePresence) set(playerID string, online bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if online {
		p.online[playerID] = true
		return
	}
	delete(p.online, playerID)
}

// staticCollector 是只记录计数器的指标采集假件（验证掉线/重连指标可见）。
type staticCollector struct {
	mu       sync.Mutex
	counters map[string]float64
}

// newStaticCollector 构造指标假件。
func newStaticCollector() *staticCollector {
	return &staticCollector{counters: make(map[string]float64)}
}

// Counter 实现 metrics.Collector：返回记录到指定名字的计数器。
func (c *staticCollector) Counter(name string, _ ...string) metrics.Counter {
	return counterFunc(func(delta float64) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.counters[name] += delta
	})
}

// Histogram 实现 metrics.Collector：策略用例不观测直方图。
func (c *staticCollector) Histogram(string, ...string) metrics.Histogram { return noopHistogram{} }

// Gauge 实现 metrics.Collector：策略用例不观测仪表。
func (c *staticCollector) Gauge(string, ...string) metrics.Gauge { return noopGauge{} }

// get 返回指定计数器的当前值。
func (c *staticCollector) get(name string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counters[name]
}

// counterFunc 是计数器函数适配（metrics.Counter 只有一个 Add 方法）。
type counterFunc func(float64)

// Add 累加计数。
func (f counterFunc) Add(delta float64) { f(delta) }

// noopHistogram 是空直方图实现。
type noopHistogram struct{}

// Observe 丢弃观测值。
func (noopHistogram) Observe(float64) {}

// noopGauge 是空仪表实现。
type noopGauge struct{}

// Set 丢弃设置值。
func (noopGauge) Set(float64) {}

// Add 丢弃增量。
func (noopGauge) Add(float64) {}

// Delete 丢弃删除。
func (noopGauge) Delete() {}

// outRecord 是一条出局广播记录。
type outRecord struct {
	playerID string
	reason   battlev1.PlayerOutReason
}

// offlineCfg 返回掉线策略用例的战斗参数：超长赛道与帧上限（不自然分胜负）+ 短掉线窗口。
func offlineCfg(players int) Config {
	cfg := testBattleConfig()
	cfg.TrackLen = 1000
	cfg.MaxFrames = 1 << 40
	cfg.MaxPlayers = players
	cfg.OfflineTimeout = 80 * time.Millisecond
	return cfg
}

// offlineEnv 是掉线策略用例环境（战斗环境 + 在场复核假件 + 指标假件）。
type offlineEnv struct {
	*battleEnv
	presence *fakePresence
	metrics  *staticCollector
}

// newOfflineEnv 起掉线策略用例环境（窗口 80ms；presence 为在场复核端口）。
func newOfflineEnv(t *testing.T, players int, presence *fakePresence) *offlineEnv {
	t.Helper()
	return newOfflineEnvWith(t, offlineCfg(players), presence)
}

// newOfflineEnvWith 以给定战斗参数起掉线策略用例环境（presence 为在场复核端口）。
func newOfflineEnvWith(t *testing.T, cfg Config, presence *fakePresence) *offlineEnv {
	t.Helper()
	if presence == nil {
		presence = newFakePresence()
	}
	col := newStaticCollector()
	env := newBattleEnvDeps(t, cfg, battleDeps{Presence: presence, Metrics: col})
	return &offlineEnv{battleEnv: env, presence: presence, metrics: col}
}

// seedAndJoinRoster 开局并让全部参战玩家加入（b-test01）。
func seedAndJoinRoster(t *testing.T, env *offlineEnv, ids ...string) {
	t.Helper()
	ctx := newTestContext()
	if _, err := env.ask(ctx, &battlev1.CreateBattleRequest{MatchId: "m-off", PlayerIds: ids}); err != nil {
		t.Fatalf("开局: %v", err)
	}
	for _, id := range ids {
		if _, err := env.askAs(ctx, id, &battlev1.JoinBattleReq{BattleId: "b-test01"}); err != nil {
			t.Fatalf("加入 %s: %v", id, err)
		}
	}
}

// notifyOnline 投递直连上线消息（帧槽验票登记后的本地非阻塞投递）。
func notifyOnline(t *testing.T, env *offlineEnv, playerID string) {
	t.Helper()
	if err := env.rt.Tell(newTestContext(), env.pid, biz.PlayerOnline{PlayerID: playerID}); err != nil {
		t.Fatalf("上线消息 %s: %v", playerID, err)
	}
}

// notifyOffline 投递直连断开消息（帧引擎生命周期事件映射后的本地非阻塞投递）。
func notifyOffline(t *testing.T, env *offlineEnv, playerID string) {
	t.Helper()
	if err := env.rt.Tell(newTestContext(), env.pid, biz.PlayerOffline{PlayerID: playerID}); err != nil {
		t.Fatalf("断开消息 %s: %v", playerID, err)
	}
}

// setMigration 投递迁移窗口开关（规格 §9.6 接缝）。
func setMigration(t *testing.T, env *offlineEnv, paused bool) {
	t.Helper()
	if err := env.rt.Tell(newTestContext(), env.pid, MigrationPause{Paused: paused}); err != nil {
		t.Fatalf("迁移开关(%v): %v", paused, err)
	}
}

// outsFor 返回某玩家收到的出局广播记录（拷贝）。
func outsFor(env *offlineEnv, playerID string) []outRecord {
	env.pusher.mu.Lock()
	defer env.pusher.mu.Unlock()
	recs := env.pusher.outs[playerID]
	return append([]outRecord(nil), recs...)
}

// waitOut 等待指定玩家收到关于 outPlayer 的出局广播（断言超时失败）。
func waitOut(t *testing.T, env *offlineEnv, playerID, outPlayer string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		for _, r := range outsFor(env, playerID) {
			if r.playerID == outPlayer {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s 未收到 %s 的出局广播", playerID, outPlayer)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// assertNoOut 断言窗口内没有出局广播与结束通知（回座/过期事件/迁移暂停用例）。
func assertNoOut(t *testing.T, env *offlineEnv, d time.Duration) {
	t.Helper()
	time.Sleep(d)
	for _, p := range []string{"p-a", "p-b", "p-c"} {
		if recs := outsFor(env, p); len(recs) != 0 {
			t.Fatalf("%s 收到不该有的出局广播: %+v", p, recs)
		}
	}
	assertNoSettle(t, env)
}

// assertNoSettle 断言本局未结算（无结束通知、未关闭直连）。
func assertNoSettle(t *testing.T, env *offlineEnv) {
	t.Helper()
	_, ends, closed := env.pusher.snapshots()
	if len(ends) != 0 || len(closed) != 0 {
		t.Fatalf("不该结算: ends=%v closed=%v", ends, closed)
	}
}

// inRoster 判断玩家是否仍在参战名单（补帧按名单复核：局外人被拒 INVALID_TOKEN）。
func inRoster(t *testing.T, env *offlineEnv, playerID string) bool {
	t.Helper()
	_, err := env.askAs(newTestContext(), playerID, &battlev1.SyncFramesReq{BattleId: "b-test01"})
	return err == nil
}

// TestOfflineEliminatesPlayerAndBroadcasts 验证掉线超时后果（多人局，规格 §9.3）：
// 掉线者判负并移出名单 + 出局广播；其余玩家继续本局（不结算、不关连接）。
func TestOfflineEliminatesPlayerAndBroadcasts(t *testing.T) {
	env := newOfflineEnv(t, 3, nil)
	seedAndJoinRoster(t, env, "p-a", "p-b", "p-c")

	notifyOffline(t, env, "p-b")
	waitOut(t, env, "p-a", "p-b")

	// 出局广播逐人下发（落败者也在名单里，广播包含本人在内）。
	for _, p := range []string{"p-a", "p-b", "p-c"} {
		recs := outsFor(env, p)
		if len(recs) != 1 || recs[0].playerID != "p-b" ||
			recs[0].reason != battlev1.PlayerOutReason_PLAYER_OUT_REASON_OFFLINE_TIMEOUT {
			t.Fatalf("%s 出局广播不符: %+v", p, recs)
		}
	}
	// 掉线者被移出参战名单；其余玩家仍在。
	if inRoster(t, env, "p-b") {
		t.Fatal("掉线者仍在参战名单")
	}
	if !inRoster(t, env, "p-a") || !inRoster(t, env, "p-c") {
		t.Fatal("其余玩家被误移出参战名单")
	}
	// 本局继续：不结算、不关连接、actor 存活，且帧广播继续下发。
	_, ends, closed := env.pusher.snapshots()
	if len(ends) != 0 || len(closed) != 0 {
		t.Fatalf("多人局不应结算: ends=%v closed=%v", ends, closed)
	}
	if _, ok := env.rt.Stats(env.pid); !ok {
		t.Fatal("多人局掉线后战斗 actor 不应自停")
	}
	if env.metrics.get("battle_offline_timeouts_total") != 1 {
		t.Fatalf("掉线超时指标 = %v, 期望 1", env.metrics.get("battle_offline_timeouts_total"))
	}
}

// TestOfflineLastStandingSettles 验证只剩一人时该玩家判胜并走既有结算路径
// （规格 §9.3）：结算落库 + 结算事件 + 结束广播 + 关闭本局直连 + actor 自停。
func TestOfflineLastStandingSettles(t *testing.T) {
	env := newOfflineEnv(t, 2, nil)
	seedAndJoinRoster(t, env, "p-a", "p-b")

	notifyOffline(t, env, "p-b")
	waitSettled(t, env.battleEnv)
	assertActorStopped(t, env.battleEnv)

	// 结算事件含完整名单：胜者 p-a（判负的 p-b 记 Win=false）。
	env.publisher.mu.Lock()
	ev := env.publisher.ev
	env.publisher.mu.Unlock()
	if ev == nil || len(ev.GetPlayers()) != 2 || settledWinner(ev) != "p-a" {
		t.Fatalf("结算事件不符: %+v", ev)
	}
	for _, p := range ev.GetPlayers() {
		if p.GetPlayerId() == "p-b" && p.GetWin() {
			t.Fatalf("掉线者被判胜: %+v", p)
		}
	}
	// 落库同样含掉线者（判负）与总帧数。
	env.result.mu.Lock()
	saved := env.result.saved
	env.result.mu.Unlock()
	if saved == nil || len(saved.Players) != 2 || saved.BattleID != "b-test01" {
		t.Fatalf("落库结果不符: %+v", saved)
	}
	// 结束广播双方一致 + 本局直连全部关闭（规格 §9.8）。
	_, ends, closed := env.pusher.snapshots()
	if len(ends["p-a"]) != 1 || ends["p-a"][0] != "p-a" || len(ends["p-b"]) != 1 {
		t.Fatalf("结束广播不符: %+v", ends)
	}
	if len(closed) != 1 || closed[0] != "b-test01" {
		t.Fatalf("结算后未关闭本局直连: %+v", closed)
	}
}

// TestOfflineReconnectCancelsTimer 验证掉线窗口内回座（规格 §9.4）：取消计时、不判负、
// 仍能补帧；重连指标可见。
func TestOfflineReconnectCancelsTimer(t *testing.T) {
	env := newOfflineEnv(t, 2, nil)
	seedAndJoinRoster(t, env, "p-a", "p-b")

	notifyOffline(t, env, "p-b")
	notifyOnline(t, env, "p-b") // 窗口内回座（新连接接管）
	assertNoOut(t, env, 3*offlineCfg(2).OfflineTimeout)

	if !inRoster(t, env, "p-b") {
		t.Fatal("回座玩家被移出参战名单")
	}
	if env.metrics.get("battle_reconnects_total") != 1 {
		t.Fatalf("重连指标 = %v, 期望 1", env.metrics.get("battle_reconnects_total"))
	}
}

// TestOfflineStaleEventIgnored 验证过期断开事件被忽略（规格 §9.2 硬约束②）：
// 应用前复核注册表，玩家仍有存活连接即忽略。
func TestOfflineStaleEventIgnored(t *testing.T) {
	env := newOfflineEnv(t, 2, newFakePresence("p-a", "p-b"))
	seedAndJoinRoster(t, env, "p-a", "p-b")

	notifyOffline(t, env, "p-b") // 旧连接的迟到断开（新连接已登记）
	assertNoOut(t, env, 3*offlineCfg(2).OfflineTimeout)

	if !inRoster(t, env, "p-b") {
		t.Fatal("过期断开事件把玩家判负了")
	}
}

// TestOfflineDuplicateIgnored 验证连发两次断开不重复判负：出局广播只发一次，其余玩家继续。
func TestOfflineDuplicateIgnored(t *testing.T) {
	env := newOfflineEnv(t, 3, nil)
	seedAndJoinRoster(t, env, "p-a", "p-b", "p-c")

	notifyOffline(t, env, "p-b")
	notifyOffline(t, env, "p-b")
	waitOut(t, env, "p-a", "p-b")
	time.Sleep(2 * offlineCfg(3).OfflineTimeout) // 重复计时若被启动，此时必然已到点

	if recs := outsFor(env, "p-a"); len(recs) != 1 {
		t.Fatalf("出局广播次数 = %d, 期望 1（重复断开不重复判负）", len(recs))
	}
	if env.metrics.get("battle_offline_timeouts_total") != 1 {
		t.Fatalf("掉线超时指标 = %v, 期望 1", env.metrics.get("battle_offline_timeouts_total"))
	}
	assertNoSettle(t, env)
}

// TestOfflineDuringMigration 验证迁移期暂停计时接缝（规格 §9.6）：窗口内不计掉线，
// 恢复后从恢复时刻重新起算。
func TestOfflineDuringMigration(t *testing.T) {
	env := newOfflineEnv(t, 3, nil)
	seedAndJoinRoster(t, env, "p-a", "p-b", "p-c")

	setMigration(t, env, true)
	notifyOffline(t, env, "p-b") // 拆流导致的断开：迁移窗口内不计
	assertNoOut(t, env, 3*offlineCfg(3).OfflineTimeout)

	setMigration(t, env, false) // 迁移结束：不在线者从此刻重新起算
	waitOut(t, env, "p-a", "p-b")
	if inRoster(t, env, "p-b") {
		t.Fatal("迁移结束后未按掉线判负")
	}
}

// TestOfflineUnknownPlayerIgnored 验证非参战玩家的断开事件被忽略（不误判、不出局广播）。
func TestOfflineUnknownPlayerIgnored(t *testing.T) {
	env := newOfflineEnv(t, 2, nil)
	seedAndJoinRoster(t, env, "p-a", "p-b")

	notifyOffline(t, env, "p-x")
	assertNoOut(t, env, 2*offlineCfg(2).OfflineTimeout)
}

// TestPlayerOnlineUnknownPlayerIgnored 验证已出局/未知玩家的上线消息被忽略（不复活名单）。
func TestPlayerOnlineUnknownPlayerIgnored(t *testing.T) {
	env := newOfflineEnv(t, 3, nil)
	seedAndJoinRoster(t, env, "p-a", "p-b", "p-c")

	notifyOffline(t, env, "p-b")
	waitOut(t, env, "p-a", "p-b")
	notifyOnline(t, env, "p-b") // 已判负者以旧票重连：不得复活
	if inRoster(t, env, "p-b") {
		t.Fatal("已判负玩家被上线消息复活进参战名单")
	}
}

// TestBattleActorOfflineDisabled 验证 offline_timeout ≤ 0 时关闭掉线判定（不启动计时）。
func TestBattleActorOfflineDisabled(t *testing.T) {
	cfg := offlineCfg(2)
	cfg.OfflineTimeout = 0
	env := newOfflineEnvWith(t, cfg, nil)
	seedAndJoinRoster(t, env, "p-a", "p-b")

	notifyOffline(t, env, "p-b")
	assertNoOut(t, env, 3*80*time.Millisecond)
	if !inRoster(t, env, "p-b") {
		t.Fatal("掉线判定关闭时不应移出参战名单")
	}
}

// TestBattleConfigDefaults 验证掉线窗口与容量的默认值（默认 15s，不写死在使用点）。
func TestBattleConfigDefaults(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.OfflineTimeout != DefaultOfflineTimeout || DefaultOfflineTimeout != 15*time.Second {
		t.Fatalf("掉线窗口默认值 = %s, 期望 %s", cfg.OfflineTimeout, 15*time.Second)
	}
	if cfg.MaxPlayers != DefaultMaxPlayers {
		t.Fatalf("会话容量默认值 = %d, 期望 %d", cfg.MaxPlayers, DefaultMaxPlayers)
	}
}

// TestSettleOutcomeWalkover 验证掉线判负的结算复用点：胜负与总帧数来自判负时刻的会话状态。
func TestSettleOutcomeWalkover(t *testing.T) {
	env := newOfflineEnv(t, 2, nil)
	seedAndJoinRoster(t, env, "p-a", "p-b")
	time.Sleep(60 * time.Millisecond) // 让会话先推进若干帧

	notifyOffline(t, env, "p-b")
	waitSettled(t, env.battleEnv)
	env.result.mu.Lock()
	saved := env.result.saved
	env.result.mu.Unlock()
	if saved == nil || saved.TotalFrames == 0 {
		t.Fatalf("判负结算未记录总帧数: %+v", saved)
	}
	winner, loser := "", ""
	for _, p := range saved.Players {
		if p.Win {
			winner = p.PlayerID
		} else {
			loser = p.PlayerID
		}
	}
	if winner != "p-a" || loser != "p-b" {
		t.Fatalf("判负结算胜负不符: winner=%q loser=%q", winner, loser)
	}
}

// presencePortAssert 静态确认在场复核端口由 biz 定义、战斗 actor 消费。
var presencePortAssert biz.ConnPresence = (*fakePresence)(nil)
