// 连接生命周期投递用例（批次 8 缺陷回归）：帧面对端断开 → 桥按注册表反查玩家 →
// 投递端口 → 战斗 actor 的**本地类型路由**。用例用真实集群运行时（内存目录 + 内存传输 +
// 本节点服务发现）复现生产投递路径，覆盖「本机实例在场 / 本机无实例 / 实例已自停」三种状态。

package app

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	battlev1actor "github.com/huangyuCN/atlas-game-layout/api/battle/v1/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/stream"
	"github.com/huangyuCN/atlas/contrib/actor/cluster"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/contrib/locator/memory"
	"github.com/huangyuCN/atlas/registry"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame/engine"
)

// 用例固定的节点/对局/玩家/端点标识（端口按 battleID 组 PID，桥按端点反查玩家）。
const (
	bridgeTestNode   = "battle-bridge-test"
	bridgeTestBattle = "b-bridge-1"
	bridgeTestPlayer = "p-a"
	bridgeTestPeer   = "127.0.0.1:40001"
)

// staticDiscovery 是只报本节点的服务发现：懒激活选点据此选中本节点（无 discovery 时选点直接失败）。
type staticDiscovery struct {
	// node 是唯一候选节点 ID。
	node string
}

// GetService 实现 registry.Discovery：恒返回本节点。
func (d staticDiscovery) GetService(context.Context, string) ([]*registry.ServiceInstance, error) {
	return []*registry.ServiceInstance{{ID: d.node}}, nil
}

// Watch 实现 registry.Discovery：生命周期投递用例不做服务监听。
func (d staticDiscovery) Watch(context.Context, string) (registry.Watcher, error) {
	return nil, fmt.Errorf("用例不支持 Watch")
}

// lifecycleRecorder 是 battle 类型的记录型处理器：生命周期消息经与 actor 侧同样的本地类型
// 路由（core.WithLocalTell）收取；拉起/停止计数用于验证「已停实例不被复活」。
type lifecycleRecorder struct {
	router  *core.LocalRouter
	tells   chan any
	started atomic.Int64
	stopped atomic.Int64
}

// newLifecycleRecorder 构造记录型处理器（在线/下线各注册一条本地类型路由）。
func newLifecycleRecorder() *lifecycleRecorder {
	r := &lifecycleRecorder{tells: make(chan any, 16)}
	r.router = core.NewLocalRouter(
		core.WithLocalTell(func(_ core.ActorContext, m biz.PlayerOnline) error { r.push(m); return nil }),
		core.WithLocalTell(func(_ core.ActorContext, m biz.PlayerOffline) error { r.push(m); return nil }),
	)
	return r
}

// push 记录一条已投递的本地消息（缓冲写：投递路径不得被断言阻塞）。
func (r *lifecycleRecorder) push(msg any) {
	select {
	case r.tells <- msg:
	default:
	}
}

// OnStart 实现 core.Handler：累计实例拉起次数。
func (r *lifecycleRecorder) OnStart(core.ActorContext) error {
	r.started.Add(1)
	return nil
}

// OnStop 实现 core.Handler：累计实例停止次数。
func (r *lifecycleRecorder) OnStop(core.ActorContext, types.ExitReason) error {
	r.stopped.Add(1)
	return nil
}

// OnTell 实现 core.Handler：走本地类型路由（未注册类型返回 ErrUnknownMessage）。
func (r *lifecycleRecorder) OnTell(ctx core.ActorContext, msg any) error {
	return r.router.Tell(ctx, msg)
}

// OnAsk 实现 core.Handler：生命周期投递用例不使用 Ask。
func (r *lifecycleRecorder) OnAsk(core.ActorContext, any) (any, error) {
	return nil, fmt.Errorf("用例不支持 Ask")
}

// newBridgeTestRuntime 起单节点集群运行时（内存目录 + 内存传输 + 本节点发现）并注册 battle 类型
// （SpawnAuto：与生产一致，未知 PID 会被懒激活——用例据此验证「迟到事件不得复活已停实例」）。
func newBridgeTestRuntime(t *testing.T) (*cluster.Runtime, cluster.Directory, *lifecycleRecorder) {
	t.Helper()
	dir := cluster.NewDirectory(memory.NewLocator(), bridgeTestNode, time.Minute)
	rt, err := cluster.NewRuntime(
		cluster.Config{NodeID: bridgeTestNode, Mode: cluster.ModeCluster, Lease: cluster.DefaultLease()},
		cluster.WithDirectory(dir),
		cluster.WithTransport(cluster.NewMemTransport()),
		cluster.WithDiscovery(staticDiscovery{node: bridgeTestNode}, "battle"),
	)
	if err != nil {
		t.Fatalf("构造集群运行时: %v", err)
	}
	if err := rt.Start(context.Background()); err != nil {
		t.Fatalf("启动集群运行时: %v", err)
	}
	t.Cleanup(func() { _ = rt.Shutdown(context.Background(), cluster.ShutdownKill) })

	rec := newLifecycleRecorder()
	if err := rt.Register(core.Props{
		Type:       battlev1actor.BattleServiceActorType,
		NewHandler: func(types.PID) core.Handler { return rec },
		SpawnMode:  core.SpawnAuto,
	}); err != nil {
		t.Fatalf("注册 battle 类型: %v", err)
	}
	return rt, dir, rec
}

// battlePID 返回用例对局的战斗 PID。
func battlePID(t *testing.T) types.PID {
	t.Helper()
	pid, err := types.NewPID(battlev1actor.BattleServiceActorType, bridgeTestBattle)
	if err != nil {
		t.Fatalf("构造战斗 PID: %v", err)
	}
	return pid
}

// driveDisconnect 走完整链路投递一次「帧面对端空闲淘汰」：真实 Bridge 登记连接后，
// 用它给帧面的生命周期回调投递断开事件（注册表反查玩家 → 投递端口），返回该次投递的错误。
// 桥自身按契约吞掉错误只记日志，故用例在端口回调里留一份错误用于断言。
func driveDisconnect(port lifecyclePort) error {
	reg := stream.NewRegistry()
	var lastErr error
	br := stream.NewBridge(reg, func(battleID string, msg any) error {
		lastErr = port.Tell(battleID, msg)
		return lastErr
	})
	conn := stream.Conn{Kind: transport.KindUDP, Peer: bridgeTestPeer, BattleID: bridgeTestBattle}
	br.Record(bridgeTestPlayer, conn)
	br.ForFace(transport.KindUDP)(engine.ConnEvent{Kind: engine.ConnEventDisconnected, ID: bridgeTestPeer})
	return lastErr
}

// waitOffline 等待记录到指定玩家的断开消息（未送达即失败：本地类型路由未命中）。
func waitOffline(t *testing.T, rec *lifecycleRecorder, playerID string) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case msg := <-rec.tells:
			if off, ok := msg.(biz.PlayerOffline); ok && off.PlayerID == playerID {
				return
			}
		case <-deadline:
			t.Fatalf("%s 的断开消息未送达 actor（本地类型路由未命中）", playerID)
		}
	}
}

// assertDropped 断言本机没有实例时事件被丢弃：无消息送达、无实例被拉起。
func assertDropped(t *testing.T, rec *lifecycleRecorder) {
	t.Helper()
	select {
	case msg := <-rec.tells:
		t.Fatalf("本机没有实例却收到生命周期投递: %#v", msg)
	case <-time.After(50 * time.Millisecond):
	}
	if got := rec.started.Load(); got != 0 {
		t.Fatalf("本机没有实例却被懒激活拉起：拉起次数 = %d", got)
	}
}

// TestStreamBridgeLifecycleLocalRoute 验证连接生命周期消息的投递契约：
// 本机实例在场即经本地类型路由送达；本机没有实例（尚未拉起/已结算自停）即静默丢弃，
// 绝不允许落进集群投递的 wire 编码路径（unsupported actor payload type）。
func TestStreamBridgeLifecycleLocalRoute(t *testing.T) {
	t.Run("本机实例在场：经本地类型路由送达", func(t *testing.T) {
		rt, _, rec := newBridgeTestRuntime(t)
		if _, err := rt.Spawn(context.Background(), battlePID(t)); err != nil {
			t.Fatalf("拉起战斗实例: %v", err)
		}

		if err := driveDisconnect(newLifecyclePort(rt.Local())); err != nil {
			t.Fatalf("生命周期消息投递失败: %v", err)
		}
		waitOffline(t, rec, bridgeTestPlayer)
	})

	t.Run("本机无实例：丢弃且不进 wire 编码路径", func(t *testing.T) {
		rt, dir, rec := newBridgeTestRuntime(t)
		// 目录里已有本节点的 ACTIVE 归属（懒激活窗口/迟到事件），但本机没有实例。
		if _, err := dir.Claim(context.Background(), battlePID(t)); err != nil {
			t.Fatalf("目录登记归属: %v", err)
		}

		if err := driveDisconnect(newLifecyclePort(rt.Local())); err != nil {
			t.Fatalf("本机无实例时生命周期消息进了集群投递路径: %v", err)
		}
		assertDropped(t, rec)
	})

	t.Run("实例已自停：丢弃且不复活", func(t *testing.T) {
		rt, _, rec := newBridgeTestRuntime(t)
		pid := battlePID(t)
		if _, err := rt.Spawn(context.Background(), pid); err != nil {
			t.Fatalf("拉起战斗实例: %v", err)
		}
		if err := rt.Local().Stop(context.Background(), pid); err != nil {
			t.Fatalf("停止战斗实例: %v", err)
		}

		if err := driveDisconnect(newLifecyclePort(rt.Local())); err != nil {
			t.Fatalf("已停实例的迟到断开事件进了集群投递路径: %v", err)
		}
		if got := rec.started.Load(); got != 1 {
			t.Fatalf("已停实例被复活：拉起次数 = %d, 期望 1（结算自停后不得重建）", got)
		}
	})
}
