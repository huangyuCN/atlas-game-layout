package resolver

import (
	"context"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas-game-layout/services/edge/internal/testlocator"
	"github.com/huangyuCN/atlas/contrib/actor/proto/actorv1"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/locator"
	"github.com/huangyuCN/atlas/registry"
)

// keyOf 返回目录事件的键（与适配层按 PID 派生的键同源）。
func keyOf(t *testing.T, pid string) locator.Key {
	t.Helper()
	parsed, err := types.ParsePID(pid)
	if err != nil {
		t.Fatalf("ParsePID(%s): %v", pid, err)
	}
	return parsed.Key()
}

// locEvent 构造一次「属主记录被重写为 owner」的目录事件。
func locEvent(t *testing.T, pid, owner string) locator.WatchEvent {
	t.Helper()
	return locator.WatchEvent{
		Key: keyOf(t, pid),
		Loc: &types.ActorLocation{Location: &actorv1.Location{OwnerNode: owner}},
	}
}

// TestInstanceOfMapsMetadataAndEndpoints 覆盖注册实例 → 帧面实例的元数据契约映射。
func TestInstanceOfMapsMetadataAndEndpoints(t *testing.T) {
	inst := instanceOf(&registry.ServiceInstance{
		ID: "battle-node-2-frame",
		Metadata: map[string]string{
			consts.FrameMetaNodeID: "battle-node-2",
			consts.FrameMetaHost:   "10.0.0.7",
			consts.FrameMetaPortWS: "9401", consts.FrameMetaPortKCP: "9402", consts.FrameMetaPortUDP: "9403",
		},
		Endpoints: []string{"tcp://10.0.0.7:9401"},
	})
	if inst.NodeID != "battle-node-2" || inst.Host != "10.0.0.7" || inst.EndpointHost != "10.0.0.7" {
		t.Fatalf("实例映射不对: %+v", inst)
	}
	if inst.Address(consts.FrameMetaPortKCP) != "10.0.0.7:9402" {
		t.Fatalf("kcp 面地址不对: %s", inst.Address(consts.FrameMetaPortKCP))
	}
	if inst.hasPort(consts.FrameMetaPortWS) != true || inst.hasPort("grpc") {
		t.Fatal("端口判定不对")
	}
}

// TestInstanceOfFallsBackToEndpointHost 覆盖未配 host 元数据时取端点主机；无端点则为空。
func TestInstanceOfFallsBackToEndpointHost(t *testing.T) {
	inst := instanceOf(&registry.ServiceInstance{
		ID:        "f1",
		Metadata:  map[string]string{consts.FrameMetaPortWS: "9401"},
		Endpoints: []string{"grpc://10.1.1.1:9300", "tcp://10.2.2.2:9401"},
	})
	if inst.Host != "" || inst.EndpointHost != "10.1.1.1" {
		t.Fatalf("端点主机兜底不对: %+v", inst)
	}
	if inst.Address(consts.FrameMetaPortWS) != "10.1.1.1:9401" {
		t.Fatalf("地址应为端点主机:端口，实际 %s", inst.Address(consts.FrameMetaPortWS))
	}
	empty := instanceOf(&registry.ServiceInstance{ID: "f2"})
	if empty.EndpointHost != "" || empty.hasPort(consts.FrameMetaPortWS) {
		t.Fatalf("空实例不应有主机与端口: %+v", empty)
	}
}

// TestAdaptersValidateDeps 覆盖适配器构造的依赖校验。
func TestAdaptersValidateDeps(t *testing.T) {
	if _, err := NewLocatorDirectory(nil); err == nil {
		t.Fatal("locator 为空应报错")
	}
	if _, err := NewDiscoveryRegistry(nil); err == nil {
		t.Fatal("服务发现为空应报错")
	}
}

// TestWatchOwnerCancelStopsWatcher 覆盖评审 P1-1：cancel 必须停掉真 watcher 并取消派生 ctx
// （etcd 客户端侧 watch 流挂在 ctx 上）。只 close 停止信号的实现会让 goroutine 永久阻塞在
// Next()——每局泄漏 1 watch + 1 goroutine。
func TestWatchOwnerCancelStopsWatcher(t *testing.T) {
	loc := &testlocator.Locator{}
	dir, err := NewLocatorDirectory(loc)
	if err != nil {
		t.Fatalf("NewLocatorDirectory: %v", err)
	}
	cancel, err := dir.WatchOwner(context.Background(), "battle:b1", func(string) {})
	if err != nil {
		t.Fatalf("WatchOwner: %v", err)
	}
	if loc.Count() != 1 || loc.Prefix(0) != "battle:b1" {
		t.Fatalf("应按 PID 建一条前缀监听，实际 count=%d prefix=%q", loc.Count(), loc.Prefix(0))
	}

	cancel()
	if !loc.Watcher(0).Stopped(testlocator.WaitTimeout) {
		t.Fatal("cancel 未停掉真 watcher：goroutine 永久阻塞在 Next()（每局泄漏 1 watch + 1 goroutine）")
	}
	select {
	case <-loc.Ctx(0).Done():
	case <-time.After(testlocator.WaitTimeout):
		t.Fatal("cancel 未取消传给 locator.Watch 的 ctx：客户端侧 watch 流不会关闭")
	}
	cancel() // 幂等：不得二次 Stop（etcd 实现重复 Stop 会 panic）
	if n := loc.Watcher(0).Stops(); n != 1 {
		t.Fatalf("watcher 应恰好停止一次，实际 %d", n)
	}
}

// TestWatchOwnerFiltersByKey 覆盖监听回调：同前缀的别局事件被过滤（battle:b1 不收 battle:b10），
// 本局事件按属主回调，删除事件按「属主为空」回调（drain 与 activate 之间的窗口要拆流）。
func TestWatchOwnerFiltersByKey(t *testing.T) {
	loc := &testlocator.Locator{}
	dir, err := NewLocatorDirectory(loc)
	if err != nil {
		t.Fatalf("NewLocatorDirectory: %v", err)
	}
	owners := make(chan string, 4)
	cancel, err := dir.WatchOwner(context.Background(), "battle:b1", func(owner string) { owners <- owner })
	if err != nil {
		t.Fatalf("WatchOwner: %v", err)
	}
	defer cancel()
	w := loc.Watcher(0)
	w.Emit(locEvent(t, "battle:b10", "node-x"))
	w.Emit(locEvent(t, "battle:b1", "node-a"))
	w.Emit(locator.WatchEvent{Key: keyOf(t, "battle:b1")})

	for _, want := range []string{"node-a", ""} {
		select {
		case got := <-owners:
			if got != want {
				t.Fatalf("回调属主应为 %q，实际 %q", want, got)
			}
		case <-time.After(testlocator.WaitTimeout):
			t.Fatalf("等待属主回调 %q 超时", want)
		}
	}
	select {
	case got := <-owners:
		t.Fatalf("不该有额外回调：%q", got)
	case <-time.After(20 * time.Millisecond):
	}
}
