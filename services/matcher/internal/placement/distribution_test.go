package placement

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/huangyuCN/atlas/registry"
	"github.com/huangyuCN/atlas/selector/filter"
)

// 本文件钉住「压测规模下选点是否真的均衡」这件事（500 局 / 1000 局 / 跨 tick 并发）。
//
// 为什么要它：偏斜故障（501 局里 486 局落同一台）在现象上与「轮询计数被每 tick 重建」
// 完全一致，而这类回归只在成百上千局后才显形（100 局的旧用例容差就有 ±5 局）。
// 故把规模的量级直接写进用例：够大才能把「偶发漂移」与「系统性偏斜」分开。

// twoLocalBattles 返回同主机两台 battle 的注册发现桩（LOCAL_FIRST 下两台都是本机候选）。
func twoLocalBattles() *fakeDiscovery {
	return &fakeDiscovery{ins: []*registry.ServiceInstance{
		battleInstance("battle-1", "127.0.0.1:9301"),
		battleInstance("battle-b2", "127.0.0.1:9311"),
	}}
}

// pinAndCount 连续处置 total 局，返回「本局目标端点」的分布：
// 每局按生产路径 Pin 一次（预热与入场共用同一 ctx ⇒ 两跳同台），并断言两跳读到的目标相同。
func pinAndCount(t *testing.T, p *Planner, total int) map[string]int {
	t.Helper()
	counts := make(map[string]int, 2)
	for i := 0; i < total; i++ {
		ctx := p.Pin(context.Background())
		warm, okWarm := Target(ctx)
		admit, okAdmit := Target(ctx)
		if !okWarm || !okAdmit || warm != admit || warm == "" {
			t.Fatalf("第 %d 局：两跳目标不一致（warm=%q admit=%q）", i, warm, admit)
		}
		counts[warm]++
	}
	return counts
}

// TestPlannerDistributes500GamesEvenly 是本次偏斜排查的核心用例：
// 压测规模（500 局）下两台本机 battle 必须各得 ≈50%（45–55%）。
//
// 若偏斜来自「轮询计数每局/每 tick 重建」，本用例会退化成「全落第一台」⇒ 真红；
// 反之（本用例绿）说明偏斜不可能来自选点计数，只可能来自「候选集里只剩一台」。
func TestPlannerDistributes500GamesEvenly(t *testing.T) {
	p := newTestPlanner(twoLocalBattles())
	counts := pinAndCount(t, p, 500)
	t.Logf("500 局分布: %v", counts)
	assertEven(t, counts, 500)
}

// TestPlannerDistributes1000GamesEvenly 把规模再翻一倍：轮询若按「每 tick 重建」实现，
// 偏差会随局数线性放大；严格轮询则恒为 ±1 局。
func TestPlannerDistributes1000GamesEvenly(t *testing.T) {
	p := newTestPlanner(twoLocalBattles())
	counts := pinAndCount(t, p, 1000)
	t.Logf("1000 局分布: %v", counts)
	assertEven(t, counts, 1000)
	for addr, n := range counts {
		if n < 499 || n > 501 {
			t.Fatalf("%s 分到 %d 局，严格轮询应为 500±1（分布 %v）", addr, n, counts)
		}
	}
}

// TestPlannerStaysEvenAcrossConcurrentTicks 验证「跨 tick 并发分配」仍均衡：
// 每个 tick 并发成 100 局、共 6 个 tick，且候选快照每局都过期重建（模拟注册中心抖动/
// 每 tick 重建快照的最坏情况）——轮询游标必须跨 tick 延续，不能按 tick 或按 worker 重置。
func TestPlannerStaysEvenAcrossConcurrentTicks(t *testing.T) {
	p := New(twoLocalBattles(), Options{Local: filter.PreferLocal("127.0.0.1"), CacheTTL: time.Nanosecond})
	const ticks, perTick = 6, 100
	var (
		mu     sync.Mutex
		counts = make(map[string]int, 2)
		wg     sync.WaitGroup
	)
	for tick := 0; tick < ticks; tick++ {
		for i := 0; i < perTick; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				addr := p.Place(context.Background())
				mu.Lock()
				counts[addr]++
				mu.Unlock()
			}()
		}
	}
	wg.Wait()
	got := counts["127.0.0.1:9301"] + counts["127.0.0.1:9311"]
	if got != ticks*perTick {
		t.Fatalf("并发分配丢次数：%d != %d（分布 %v）", got, ticks*perTick, counts)
	}
	t.Logf("6 tick × 100 局并发分布: %v", counts)
	assertEven(t, counts, ticks*perTick)
}

// TestPlannerConcentratesWhenOnlyOneCandidate 是「97/3 从哪来」的判定性用例：
// 候选集**只剩一台**时（另一台的 battle 实例在注册中心缺失/被帧面过滤掉），
// 严格轮询必然退化成「全落这一台」——即 501 局里 486 局同台的充分条件。
//
// 本用例是诊断记录而非缺陷断言：它钉住「偏斜 ⇔ 候选集塌成 1」这一等价关系，
// 便于下次压测直接按「候选集大小」排查，而不必再怀疑轮询计数。
func TestPlannerConcentratesWhenOnlyOneCandidate(t *testing.T) {
	disc := &fakeDiscovery{ins: []*registry.ServiceInstance{
		battleInstance("battle-b2", "127.0.0.1:9311"),
	}}
	counts := pinAndCount(t, newTestPlanner(disc), 500)
	if counts["127.0.0.1:9311"] != 500 {
		t.Fatalf("只有一个候选时应全部落到它，实际 %v", counts)
	}
	t.Logf("候选集塌成 1 台时的 500 局分布: %v（与 97/3 现象同构）", counts)
}

// TestRebuiltPlannerConcentratesOnFirstBattle 是「本组用例能否抓住假设的缺陷」的灵敏度证明：
// 若轮询计数被每局（或每 tick）重建——即每次决策都新建一个游标为 0 的选点器——500 局会
// **全部落在第一台**（500/0），本文件的三条分布用例随即真红。
//
// 存在的意义：它把「偏斜故障的假设实现」固化下来，证明 250/250 不是恒真断言
// （实现改成每局重建时它立刻会失败），避免分布用例退化成自我复述。
func TestRebuiltPlannerConcentratesOnFirstBattle(t *testing.T) {
	counts := make(map[string]int, 2)
	for i := 0; i < 500; i++ {
		// 每局新建一个选点器 = 轮询游标每局重置（假设的缺陷实现）。
		addr := newTestPlanner(twoLocalBattles()).Place(context.Background())
		counts[addr]++
	}
	if counts["127.0.0.1:9301"] != 500 {
		t.Fatalf("游标每局重建时应全部落第一台，实际 %v", counts)
	}
	t.Logf("假设缺陷（游标每局重建）下的 500 局分布: %v", counts)
}
