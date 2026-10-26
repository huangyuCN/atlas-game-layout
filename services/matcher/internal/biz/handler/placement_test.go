package handler

import (
	"context"
	"fmt"
	"testing"

	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/placement"
	"github.com/huangyuCN/atlas/matchmaker"
	"github.com/huangyuCN/atlas/registry"
	"github.com/huangyuCN/atlas/selector/filter"
)

// errNoWatchStub 表示 handler 用例的发现桩不支持 Watch（选点只走 GetService）。
var errNoWatchStub = fmt.Errorf("handler: 测试桩不支持 Watch")

// handlerDiscovery 是 handler 用例的最小注册发现桩：固定两台本机 battle 实例。
type handlerDiscovery struct {
	ins []*registry.ServiceInstance
}

// GetService 返回固定实例列表。
func (d *handlerDiscovery) GetService(context.Context, string) ([]*registry.ServiceInstance, error) {
	return d.ins, nil
}

// Watch 不支持（选点不走监听）。
func (d *handlerDiscovery) Watch(context.Context, string) (registry.Watcher, error) {
	return nil, errNoWatchStub
}

// twoLocalBattles 返回两台同主机 battle 的注册实例（LOCAL_FIRST 命中多台本机）。
func twoLocalBattles() *handlerDiscovery {
	return &handlerDiscovery{ins: []*registry.ServiceInstance{
		{ID: "battle-1", Name: "battle", Endpoints: []string{"grpc://127.0.0.1:9301"}},
		{ID: "battle-b2", Name: "battle", Endpoints: []string{"grpc://127.0.0.1:9311"}},
	}}
}

// newPinnedTestHandler 装配带选点器的测试 handler（本机优先 + 轮询）。
func newPinnedTestHandler(disc *handlerDiscovery, warmer *fakeWarmer) (*MatcherHandler, *fakeSink) {
	planner := placement.New(disc, placement.Options{Local: filter.PreferLocal("127.0.0.1")})
	h, _, _, sink := newTestHandlerWith(testHandlerOpts{warmer: warmer, placer: planner})
	return h, sink
}

// TestAdmitMatchPinsWarmAndAdmitToSameBattle 是本修复在处置链上的核心断言：
// 同一局的预热与入场必须落在**同一台** battle，且 100 局按 50/50 分摊。
//
// 旧实现在此红：两跳各自负载均衡，两台等权本机 battle 下 100/100 局劈到不同进程
// （预热白做，落点由两跳竞态决定；竞态系统性偏向一侧时就「全挤一台」）。
func TestAdmitMatchPinsWarmAndAdmitToSameBattle(t *testing.T) {
	warmer := &fakeWarmer{}
	h, sink := newPinnedTestHandler(twoLocalBattles(), warmer)
	counts := make(map[string]int, 2)
	for i := 0; i < 100; i++ {
		ev := matchmaker.TicketEvent{Match: &matchmaker.Match{
			ID:      fmt.Sprintf("m-%d", i),
			Tickets: []matchmaker.Ticket{testTicket(fmt.Sprintf("t-%d", i), "p-1", "p-2")},
		}}
		if !h.admitMatch(context.Background(), ev, fmt.Sprintf("b-%d", i), []string{"p-1", "p-2"}) {
			t.Fatalf("第 %d 局成局处置应成功", i)
		}
		warm := warmer.waitTargets(t, i+1)
		gotWarm, gotAdmit := warm[i], sink.lastTarget()
		if gotWarm == "" || gotAdmit == "" {
			t.Fatalf("第 %d 局：预热与入场都应带选点目标，实际 warm=%q admit=%q", i, gotWarm, gotAdmit)
		}
		if gotWarm != gotAdmit {
			t.Fatalf("第 %d 局：预热落到 %s、入场落到 %s（同一局必须同一台）", i, gotWarm, gotAdmit)
		}
		counts[gotWarm]++
	}
	t.Logf("100 局的落点分布（预热=入场）: %v", counts)
	if len(counts) != 2 {
		t.Fatalf("两台本机 battle 都应分到对局，实际 %v", counts)
	}
	for addr, n := range counts {
		if n < 45 || n > 55 {
			t.Fatalf("%s 分到 %d 局，超出 [45,55]（分布 %v）", addr, n, counts)
		}
	}
}

// TestAdmitMatchWithoutPlacerFallsBack 验证未装配选点器时处置照旧成功（不钉、不失败）：
// 选点是分摊手段，不能成为成局路径的前置条件。
func TestAdmitMatchWithoutPlacerFallsBack(t *testing.T) {
	warmer := &fakeWarmer{}
	h, _, _, sink := newTestHandlerWith(testHandlerOpts{warmer: warmer})
	ev := matchmaker.TicketEvent{Match: &matchmaker.Match{
		ID: "m-np", Tickets: []matchmaker.Ticket{testTicket("t-np", "p-9")},
	}}
	if !h.admitMatch(context.Background(), ev, "b-np", []string{"p-9"}) {
		t.Fatal("未装配选点器时成局处置应照常成功")
	}
	warmer.waitTargets(t, 1)
	if len(sink.published) == 0 {
		t.Fatal("未装配选点器时应照常发布成局事件")
	}
	if sink.lastTarget() != "" {
		t.Fatalf("未装配选点器时不应有选点目标，实际 %q", sink.lastTarget())
	}
}
