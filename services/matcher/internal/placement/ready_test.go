package placement

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
)

// 本文件钉住「冷启动候选集未就绪」的保护：重启后 battle 实例已经注册、但某一台的帧面实例
// 还没进注册中心时，候选集只有 1 台。旧实现立刻出票 ⇒ 首轮对局全落同一台（实测 113:297，
// 复测即恢复 199:211），而 1s 候选快照复用把这段窗口放大成整批的系统性偏斜。
//
// 新语义：候选数少于**注册的 battle 实例数**时先等（上限 ReadyTimeout），等齐再按轮询出票；
// 等不齐则 WARN（带注册数 vs 候选数）并照常出票——选点只是分摊手段，不能永久卡住成局路径。

// coldStartFixture 是「battle 两台都已注册、帧面只就绪一台」的冷启动夹具：
// 返回选点器、注册桩与「补齐第二台帧面注册」的函数。
func coldStartFixture(t *testing.T, o Options) (*Planner, *serviceDiscovery, func()) {
	t.Helper()
	disc := &serviceDiscovery{}
	disc.stage(consts.ServiceBattle,
		battleInstance("battle-1", "127.0.0.1:9301"),
		battleInstance("battle-b2", "127.0.0.1:9311"),
	)
	disc.stage(consts.ServiceBattleFrame, frameInstance("battle-1", "ws://127.0.0.1:9411"))
	ready := func() {
		disc.stage(consts.ServiceBattleFrame,
			frameInstance("battle-1", "ws://127.0.0.1:9411"),
			frameInstance("battle-b2", "ws://127.0.0.1:9412"),
		)
	}
	return New(disc, o), disc, ready
}

// TestReadyCandidatesSplitEvenlyWithoutWaiting ①：两台帧面都已就绪时正常出票、50/50，
// 且**不得**为「等候选」付任何延迟（守回归）。
func TestReadyCandidatesSplitEvenlyWithoutWaiting(t *testing.T) {
	cl := installCaptureLogger(t)
	p, _, ready := coldStartFixture(t, Options{CacheTTL: time.Minute})
	ready()
	start := time.Now()
	counts := placeN(t, p, 100)
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Fatalf("候选已齐时不得等待：100 次选点耗时 %v", d)
	}
	assertEven(t, counts, 100)
	if n := cl.soleWarns(); n != 0 {
		t.Fatalf("候选齐备时不应告警，实际 %d 条", n)
	}
}

// TestColdStartWaitsForLateFrameRegistration ② 是本修复的核心用例（旧实现真红）：
// 第二台的帧面实例在首轮进行中才落地，选点必须**先等再出票**，让首轮 100 局接近 50/50。
//
// 旧实现直接拿「只有 1 台」的候选出票 ⇒ 100 局全落先就绪的那台（0/100），
// 与压测实测的首轮偏斜（113:297）同机理。
func TestColdStartWaitsForLateFrameRegistration(t *testing.T) {
	cl := installCaptureLogger(t)
	p, _, ready := coldStartFixture(t, Options{
		CacheTTL:     time.Minute,
		ReadyTimeout: 2 * time.Second,
		ReadyPoll:    5 * time.Millisecond,
	})
	go func() {
		time.Sleep(120 * time.Millisecond)
		ready()
	}()
	start := time.Now()
	counts := placeN(t, p, 100)
	elapsed := time.Since(start)
	t.Logf("冷启动首轮 100 局分布（修复后）: %v，首局等待 %v", counts, elapsed)
	if elapsed < 120*time.Millisecond {
		t.Fatalf("第二台帧面注册前不得出票（实际 %v 就出票了），首轮分布 %v", elapsed, counts)
	}
	assertEven(t, counts, 100)
	if n := cl.soleWarns(); n != 0 {
		t.Fatalf("等到了第二台就不该报「候选集未就绪」，实际 %d 条", n)
	}
}

// TestColdStartTimeoutWarnsThenPlaces ③：始终等不齐时**不得永久阻塞**——
// 到上限即 WARN（带注册数 vs 候选数）并照常出票，且后续选点不再各等一轮。
func TestColdStartTimeoutWarnsThenPlaces(t *testing.T) {
	cl := installCaptureLogger(t)
	p, _, _ := coldStartFixture(t, Options{
		CacheTTL:     time.Minute,
		ReadyTimeout: 40 * time.Millisecond,
		ReadyPoll:    5 * time.Millisecond,
	})
	start := time.Now()
	addr := p.Place(context.Background())
	elapsed := time.Since(start)
	if addr == "" {
		t.Fatal("候选集未就绪超限后仍须出票（不得阻塞成局路径）")
	}
	if elapsed < 40*time.Millisecond || elapsed > time.Second {
		t.Fatalf("等待应到上限即止（上限 40ms），实际耗时 %v", elapsed)
	}
	text := cl.warnsText()
	if !strings.Contains(text, "候选 1") || !strings.Contains(text, "注册 2") {
		t.Fatalf("超限告警必须带「注册数 vs 候选数」，实际告警：%q", text)
	}
	start = time.Now()
	for i := 0; i < 20; i++ {
		if p.Place(context.Background()) == "" {
			t.Fatalf("第 %d 次选点为空", i)
		}
	}
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Fatalf("超限后快照 TTL 内不得反复等待：20 次选点耗时 %v", d)
	}
}

// TestSoleRegisteredBattlePlacesWithoutWaiting ④：只注册了 1 台 battle（单机部署）时
// 候选数本来就等于注册数，必须**不等待**直接出票。
func TestSoleRegisteredBattlePlacesWithoutWaiting(t *testing.T) {
	disc := &serviceDiscovery{}
	disc.stage(consts.ServiceBattle, battleInstance("battle-1", "127.0.0.1:9301"))
	disc.stage(consts.ServiceBattleFrame, frameInstance("battle-1", "ws://127.0.0.1:9411"))
	p := New(disc, Options{ReadyTimeout: 2 * time.Second, ReadyPoll: 5 * time.Millisecond})
	start := time.Now()
	if addr := p.Place(context.Background()); addr != "127.0.0.1:9301" {
		t.Fatalf("单机应选唯一候选，实际 %q", addr)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("候选数=注册数时不得等待，实际耗时 %v", d)
	}
}

// TestColdStartFirstRoundDistribution 量化「重启后首轮 100 局」的分布：
// 第二台的帧面注册在首轮进行中才落地。修复后是 50/50；旧实现整轮落单台（见同文件末的旧口径用例）。
func TestColdStartFirstRoundDistribution(t *testing.T) {
	p, _, ready := coldStartFixture(t, Options{
		CacheTTL:     time.Minute,
		ReadyTimeout: 2 * time.Second,
		ReadyPoll:    5 * time.Millisecond,
	})
	go func() {
		time.Sleep(150 * time.Millisecond)
		ready()
	}()
	counts := placeN(t, p, 100)
	t.Logf("首轮 100 局分布: %v", counts)
	assertEven(t, counts, 100)
}

// TestColdStartReproOldBehaviorIsSkewed 记录**旧口径**（候选就绪前直接出票）的分布：
// 首轮 100 局全部落到先就绪的那台，这就是 113:297 的成因（用「不等」的选点器复现）。
func TestColdStartReproOldBehaviorIsSkewed(t *testing.T) {
	disc := &serviceDiscovery{}
	disc.stage(consts.ServiceBattle,
		battleInstance("battle-1", "127.0.0.1:9301"),
		battleInstance("battle-b2", "127.0.0.1:9311"),
	)
	disc.stage(consts.ServiceBattleFrame, frameInstance("battle-1", "ws://127.0.0.1:9411"))
	p := New(disc, Options{CacheTTL: time.Minute, ReadyTimeout: time.Nanosecond})
	counts := placeN(t, p, 100)
	t.Logf("旧口径（不等候选）首轮 100 局分布: %v", counts)
	if counts["127.0.0.1:9301"] != 100 {
		t.Fatalf("旧口径应整轮落先就绪的那台（记录偏斜成因），实际 %v", counts)
	}
}
