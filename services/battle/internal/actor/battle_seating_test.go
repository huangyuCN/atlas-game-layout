package actor

import (
	"context"
	"sync"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz"
	"github.com/huangyuCN/atlas/metrics"
)

// 入座窗口（seat window）用例：成局后不立刻开打，等全部参战者入座或窗口到期。
//
// 为什么需要：旧行为「成局即开打」让对局时钟在门票刚发出时就走——客户端从收到成局通知到
// 建连、握手、首帧要 1–2s，这段时间会被算进对局寿命（缺省 60 帧 × 100ms = 6s），
// 于是晚到玩家拿到的是 BATTLE_ENDED（实测一轮 9,986 次）。

// seatingSpy 是按「指标名 + 标签」记账的采集假件（入座窗口结局按 outcome 分）。
type seatingSpy struct {
	mu   sync.Mutex
	ctrs map[string]float64
}

// newSeatingSpy 构造空假件。
func newSeatingSpy() *seatingSpy { return &seatingSpy{ctrs: map[string]float64{}} }

// Counter 实现 metrics.Collector：按名字与标签记账。
func (s *seatingSpy) Counter(name string, labels ...string) metrics.Counter {
	key := name
	for _, l := range labels {
		key += "|" + l
	}
	return counterFunc(func(delta float64) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.ctrs[key] += delta
	})
}

// Histogram 实现 metrics.Collector：入座用例不观测直方图。
func (s *seatingSpy) Histogram(string, ...string) metrics.Histogram { return noopHistogram{} }

// Gauge 实现 metrics.Collector：入座用例不观测仪表。
func (s *seatingSpy) Gauge(string, ...string) metrics.Gauge { return noopGauge{} }

// count 返回某个「名字 + 标签序列」的累计值。
func (s *seatingSpy) count(name string, labels ...string) float64 {
	key := name
	for _, l := range labels {
		key += "|" + l
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ctrs[key]
}

// newSeatingEnv 起一局带票据配置与入座窗口的战斗 actor（MaxFrames 放大，
// 免得用例等待期间整局先按帧数上限结算）。
func newSeatingEnv(t *testing.T, window time.Duration, d battleDeps) *battleEnv {
	t.Helper()
	cfg := testBattleConfig()
	cfg.MaxFrames = 1000
	cfg.TicketKey = testTicketKey()
	cfg.TicketTTL = DefaultTicketTTL
	cfg.EdgeEndpoints = testEdgeEndpoints()
	cfg.SeatWindow = window
	return newBattleEnvDeps(t, cfg, d)
}

// frameNow 返回会话当前帧号（入座窗口内对局时钟应停在 0）。
func frameNow(t *testing.T, env *battleEnv) uint64 {
	t.Helper()
	raw, err := env.ask(context.Background(), &battlev1.GetStateReq{BattleId: testBattleID})
	if err != nil {
		t.Fatalf("状态查询: %v", err)
	}
	state, ok := raw.(*battlev1.GetStateReply)
	if !ok {
		t.Fatalf("状态回执类型 %T 不符", raw)
	}
	return state.GetCurrentFrame()
}

// markSeated 上报一次「帧连接建立」（生产链路上由直连注册表桥的 PlayerOnline 触发）。
func markSeated(t *testing.T, env *battleEnv, playerID string) {
	t.Helper()
	if err := env.rt.Tell(context.Background(), env.pid, biz.PlayerOnline{PlayerID: playerID}); err != nil {
		t.Fatalf("入座 %s: %v", playerID, err)
	}
}

// waitFrameAdvance 在 timeout 内等待对局时钟开始推进（帧号 > 0）。
func waitFrameAdvance(t *testing.T, env *battleEnv, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if frameNow(t, env) > 0 {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// TestBattleSeatWindow_WaitsForAllSeats 验证**坐满再开打**：
// 窗口内坐不满就一帧都不走；最后一名玩家入座即开打（outcome=all_seated）。
func TestBattleSeatWindow_WaitsForAllSeats(t *testing.T) {
	spy := newSeatingSpy()
	env := newSeatingEnv(t, 5*time.Second, battleDeps{Metrics: spy})
	admit(t, env, "p-a", "p-b")

	time.Sleep(80 * time.Millisecond)
	if f := frameNow(t, env); f != 0 {
		t.Fatalf("无人入座时已推进 %d 帧（对局时钟不该走）", f)
	}
	markSeated(t, env, "p-a")
	time.Sleep(50 * time.Millisecond)
	if f := frameNow(t, env); f != 0 {
		t.Fatalf("只坐 1/2 就开打（已推进 %d 帧）", f)
	}
	markSeated(t, env, "p-b")
	if !waitFrameAdvance(t, env, 500*time.Millisecond) {
		t.Fatal("坐满后未开打")
	}
	if got := spy.count(MetricSeatWindow, "outcome", "all_seated"); got != 1 {
		t.Fatalf("outcome=all_seated 计数 = %v, 期望 1", got)
	}
}

// TestBattleSeatWindow_StartsOnTimeout 验证窗口到期即开打：
// 未入座的玩家不阻塞对局（语义：到期后不再等待，未入座者视为迟到）。
func TestBattleSeatWindow_StartsOnTimeout(t *testing.T) {
	spy := newSeatingSpy()
	env := newSeatingEnv(t, 80*time.Millisecond, battleDeps{Metrics: spy})
	admit(t, env, "p-a", "p-b")
	markSeated(t, env, "p-a")

	if !waitFrameAdvance(t, env, time.Second) {
		t.Fatal("入座窗口到期后仍未开打")
	}
	if got := spy.count(MetricSeatWindow, "outcome", "timeout"); got != 1 {
		t.Fatalf("outcome=timeout 计数 = %v, 期望 1", got)
	}
}

// TestBattleSeatWindow_DisabledStartsImmediately 验证浅出路径：窗口关闭（≤0）时
// 成局即开打（旧行为），不等待任何入座。
func TestBattleSeatWindow_DisabledStartsImmediately(t *testing.T) {
	env := newSeatingEnv(t, 0, battleDeps{})
	admit(t, env, "p-a", "p-b")

	if !waitFrameAdvance(t, env, 300*time.Millisecond) {
		t.Fatal("窗口关闭时应成局即开打")
	}
}

// TestBattleSeatWindow_LateSeatStillAllowed 验证超时后的语义：**不剔除、不重配**——
// 开打后晚到玩家仍可加入本局（不因「晚到」被拒绝），只是不再等他。
func TestBattleSeatWindow_LateSeatStillAllowed(t *testing.T) {
	env := newSeatingEnv(t, 50*time.Millisecond, battleDeps{})
	admit(t, env, "p-a", "p-b")
	if !waitFrameAdvance(t, env, time.Second) {
		t.Fatal("窗口到期后仍未开打")
	}
	if _, err := env.askAs(context.Background(), "p-b", &battlev1.JoinBattleReq{BattleId: testBattleID}); err != nil {
		t.Fatalf("晚到玩家加入被拒（应允许）: %v", err)
	}
}
