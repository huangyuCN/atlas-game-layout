// 帧面：battle 的 KCP/UDP/WS 三个直连帧监听（规格 §1.2 裁定 5）与帧 op 服务端装配。
// 每个 access=CLIENT 的 battle op 注册一个 handler（注解驱动，新增 op 零 server 代码），
// handler 本体是框架组件 frameops.Handler.Serve：帧槽验票（身份）→ 路由表（寻址）→
// LocalDeliverer（本地 actor 投递）；「遍历路由表逐 op 注册」与网关共用 pkg/frameroute。

package server

import (
	"fmt"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	gamev1actor "github.com/huangyuCN/atlas-game-layout/api/game/v1/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/frameroute"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/stream"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/frameops"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame/engine"
	kcpt "github.com/huangyuCN/atlas/transport/kcp"
	udpt "github.com/huangyuCN/atlas/transport/udp"
	wst "github.com/huangyuCN/atlas/transport/websocket"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"
)

// FrameFaces 是 battle 的三个直连帧面的具体类型（未启用的面为 nil）：
// 装配侧据此取端点（注册帧面实例）与挂直连推送端口。
type FrameFaces struct {
	KCP *kcpt.Server
	UDP *udpt.Server
	WS  *wst.Server
}

// FramePolicy 是帧面的掉线检测策略参数（规格 §9.2 硬约束①）：数据报面（KCP/UDP）没有
// 关闭握手，掉线只能靠空闲读超时发现，故其 idle_timeout 按本策略推导并在装配期硬校验。
type FramePolicy struct {
	// OfflineTimeout 是掉线判定窗口（≤0 = 关闭掉线判定：不推导、不校验数据报面空闲超时）。
	OfflineTimeout time.Duration
}

// FrameServers 是帧面集合：具体类型供装配使用，值组供 atlas.App 统一启停
// （未启用的面为 nil，由 pkg/bootstrap 在消费侧过滤，与网关四协议同口径）。
type FrameServers struct {
	fx.Out

	Faces FrameFaces
	KCP   transport.Server `group:"servers"`
	UDP   transport.Server `group:"servers"`
	WS    transport.Server `group:"servers"`
}

// FrameOption 是帧面装配的可选依赖（缺省全部为 noop，调用点不必判空）。
type FrameOption func(*frameDeps)

// frameDeps 是帧面装配的可选依赖集合。
type frameDeps struct {
	metrics *FrameMetrics // 观测出口（票据拒绝/reason、op 耗时与失败）
}

// WithFrameMetrics 注入帧面观测出口（P1-4）：票据拒绝按 reason 计数、帧 op 耗时直方图与
// 失败按 reason 计数，并在拒绝路径留 `stream_id` 关联日志。
func WithFrameMetrics(m *FrameMetrics) FrameOption {
	return func(d *frameDeps) { d.metrics = m }
}

// frameDepsOf 汇总帧面选项；观测出口未注入时取 noop（装配缺省不影响协议行为）。
func frameDepsOf(opts []FrameOption) frameDeps {
	var d frameDeps
	for _, opt := range opts {
		if opt != nil {
			opt(&d)
		}
	}
	if d.metrics == nil {
		d.metrics = NewFrameMetrics(nil)
	}
	return d
}

// NewFrameOps 组装 battle 帧面的帧 op 服务端：battle 的注解路由表（access=CLIENT 的 op 全体）
// + 帧槽验票身份（验票通过即登记直连并上报上线；对局已结束则补投结果 + 稳定 reason 拒绝）
// + 投递前目标校验（票面 battle 与正文 battle_id 对齐 + 目标对局结束补判，见 guard.go）
// + 本地 actor 投递（LocalDeliverer，按 PID 寻址）+ 观测出口（WithFrameMetrics）。
// invoker 是本地投递端口：生产为 pkg/actor.Runtime（集群运行时），单测为 core.LocalRuntime。
func NewFrameOps(invoker core.ActorInvoker, conn FrameConn, ticketKey []byte, opts ...FrameOption) (*frameops.Handler, error) {
	table, err := relay.Merge(battlev1.BattleServiceRouteTable)
	if err != nil {
		return nil, fmt.Errorf("server: battle 帧面路由表合并失败: %w", err)
	}
	deps := frameDepsOf(opts)
	return frameops.NewHandler(table,
		NewTicketIdentity(ticketKey, conn),
		frameops.LocalDeliverer{Invoker: invoker},
		// 发起者身份前缀与网关同约定：玩家发起（sender = player:<playerID>），
		// battle actor 的参战名单校验读的就是它。
		frameops.WithSenderActorType(gamev1actor.PlayerServiceActorType),
		// 目标不一致的请求不投递（也就不触发懒激活）：否则票面 b-A + 正文 b-B 能拉起任意 battle。
		frameops.WithTargetGuard(NewFrameGuard(conn)),
		// 身份解析失败（票据类拒绝）按 reason 计数 + stream_id 关联日志（P1-4①）。
		frameops.WithHooks(deps.metrics.Hooks()),
	), nil
}

// NewFrameServers 构造三个直连帧面并逐面注册帧 op 路由（未启用的面为 nil，不进启停组）。
// 数据报面按策略挂载空闲读超时（缺省取掉线窗口 1/3）与连接生命周期回调（掉线事件 → actor）；
// 校验不通过即启动失败（不静默放过——否则掉线事件迟到缺省的 120s，掉线策略形同虚设）。
// 注册时用观测出口包一层 handler（耗时直方图 + 失败按 reason 计数，P1-4②）。
func NewFrameServers(cfg *conf.Bootstrap, ops *frameops.Handler, policy FramePolicy, life *stream.Bridge,
	opts ...FrameOption) (FrameServers, error) {
	deps := frameDepsOf(opts)
	var out FrameServers
	kcpSrv, err := newDatagramFace(datagramFace[*configspb.Server_KCP, *kcpt.Server, kcpt.ServerOption]{
		field: "server.kcp", conf: cfg.GetServer().GetKcp(), kind: transport.KindKCP,
		withIdle: kcpt.WithIdleTimeout, withLife: kcpt.WithConnLifecycle, build: serverutil.KCPServer,
		setBody: func(c *configspb.Server_KCP, n int64) { c.MaxBodySize = n },
	}, policy, life)
	if err != nil {
		return FrameServers{}, err
	}
	if kcpSrv != nil {
		if err := registerFace(ops, "KCP", kcpSrv.Subscribe, deps.metrics); err != nil {
			return FrameServers{}, err
		}
		out.Faces.KCP, out.KCP = kcpSrv, kcpSrv
	}
	udpSrv, err := newDatagramFace(datagramFace[*configspb.Server_UDP, *udpt.Server, udpt.ServerOption]{
		field: "server.udp", conf: cfg.GetServer().GetUdp(), kind: transport.KindUDP,
		withIdle: udpt.WithIdleTimeout, withLife: udpt.WithPeerLifecycle, build: serverutil.UDPServer,
		setBody: func(c *configspb.Server_UDP, n int64) { c.MaxBodySize = n },
	}, policy, life)
	if err != nil {
		return FrameServers{}, err
	}
	if udpSrv != nil {
		if err := registerFace(ops, "UDP", udpSrv.Subscribe, deps.metrics); err != nil {
			return FrameServers{}, err
		}
		out.Faces.UDP, out.UDP = udpSrv, udpSrv
	}
	wsSrv, err := newWebSocketFace(cfg, life)
	if err != nil {
		return FrameServers{}, err
	}
	if wsSrv != nil {
		if err := registerFace(ops, "WS", wsSrv.Subscribe, deps.metrics); err != nil {
			return FrameServers{}, err
		}
		out.Faces.WS, out.WS = wsSrv, wsSrv
	}
	return out, nil
}

// datagramConf 是数据报帧面配置的最小读数（KCP/UDP 两个配置消息均满足）；
// 内嵌 proto.Message 是为了在装配期克隆配置（单包上限收敛不得改动注入的共享对象）。
type datagramConf interface {
	proto.Message
	// GetAddr 返回该帧面的监听地址（空 = 未启用）。
	GetAddr() string
	// GetIdleTimeout 返回显式配置的空闲读超时（time.ParseDuration 字符串，空 = 未配置）。
	GetIdleTimeout() string
	// GetMaxBodySize 返回显式配置的单帧 body 上限（0 = 未配置，按单包契约补默认）。
	GetMaxBodySize() int64
}

// datagramFace 是一类数据报帧面的装配材料：配置来源、选项构造与服务端构造
// （KCP/UDP 两面共用同一套「推导空闲超时 → 硬校验 → 挂生命周期回调」的口径）。
type datagramFace[C datagramConf, S any, O any] struct {
	field    string                         // 配置字段前缀（错误定位用，如 server.kcp）
	conf     C                              // 该帧面的配置段
	kind     transport.Kind                 // 帧面类型（生命周期事件按它归属）
	withIdle func(time.Duration) O          // 空闲读超时选项构造
	withLife func(func(engine.ConnEvent)) O // 连接生命周期回调选项构造
	build    func(C, ...O) (S, error)       // 服务端构造（serverutil.KCP/UDPServer）
	setBody  func(C, int64)                 // 写入收敛后的单包上限（在克隆对象上调用）
}

// newDatagramFace 构造一类数据报帧面（未启用返回零值）：先把单包上限收敛到「单包 ≤16 KiB」
// 契约（未配置补默认、显式超限即失败），再按策略推导/校验空闲读超时
// （规格 §9.2 硬约束①：数据报面无关闭握手，掉线只能靠空闲读超时发现），最后构造服务端。
func newDatagramFace[C datagramConf, S any, O any](f datagramFace[C, S, O], policy FramePolicy, life *stream.Bridge) (S, error) {
	var zero S
	if f.conf.GetAddr() == "" { // 该面未启用：不构造也不校验（没有连接就无所谓掉线检测）
		return zero, nil
	}
	conf, err := limitBody(f.conf, f.field+".max_body_size",
		func(c C) int64 { return c.GetMaxBodySize() }, f.setBody)
	if err != nil {
		return zero, err
	}
	configured, err := idleTimeoutOf(conf.GetIdleTimeout(), f.field+".idle_timeout")
	if err != nil {
		return zero, err
	}
	opts, err := datagramOptions(policy, configured, f.withIdle)
	if err != nil {
		return zero, fmt.Errorf("server: %s 帧面: %w", f.field, err)
	}
	return f.build(conf, append(opts, f.withLife(connHook(life, f.kind)))...)
}

// newWebSocketFace 构造 WS 帧面（未启用返回 nil，由 serverutil 处理）：
// WS 是流式面，有可靠 EOF，掉线即时可见，故不推导空闲超时、只挂生命周期回调；
// 单包上限同样收敛到「单包 ≤16 KiB」契约（WS 走 TCP 不分片，但超包回执在接入层同样发不出去）。
func newWebSocketFace(cfg *conf.Bootstrap, life *stream.Bridge) (*wst.Server, error) {
	wsConf, err := limitBody(cfg.GetServer().GetWebsocket(), "server.websocket.max_body_size",
		(*configspb.Server_WebSocket).GetMaxBodySize,
		func(c *configspb.Server_WebSocket, n int64) { c.MaxBodySize = n })
	if err != nil {
		return nil, err
	}
	return serverutil.WSServer(wsConf, wst.WithConnLifecycle(connHook(life, transport.KindWebSocket)))
}

// registerFace 把一个帧面的订阅入口逐 op 注册进帧 op 路由表（未启用的面由调用方跳过）：
// handler 本体仍是 pkg/frameroute 的统一产物（「哪些 op 要注册」只有一份事实源），
// 这里把 registrar 包两层——内层限包（超包回执换可判定拒绝，P0-4）、外层观测
// （耗时直方图 + 失败按 reason 计数，P1-4②），故超包拒绝同样计入 op 错误口径。
func registerFace(ops *frameops.Handler, name string, subscribe frameroute.Registrar, m *FrameMetrics) error {
	err := frameroute.Register(ops, func(operation string, h engine.MsgHandler) error {
		return subscribe(operation, frameHandler(operation, h, m))
	})
	if err != nil {
		return fmt.Errorf("server: %s 帧面注册失败: %w", name, err)
	}
	return nil
}

// datagramOptions 返回数据报面的空闲读超时选项（规格 §9.2 硬约束①）：
// 未配置即取 offline_timeout/3（缺省 15s → 5s），显式配置必须严格小于掉线窗口。
func datagramOptions[O any](policy FramePolicy, configured time.Duration, withIdle func(time.Duration) O) ([]O, error) {
	idle, err := datagramIdle(configured, policy.OfflineTimeout)
	if err != nil {
		return nil, err
	}
	if idle <= 0 {
		return nil, nil
	}
	return []O{withIdle(idle)}, nil
}

// datagramIdle 返回数据报面生效的空闲读超时：未配置取掉线窗口的 1/3；
// 显式配置必须严格小于掉线窗口，否则报错（该错误在装配期即让进程启动失败）。
// 掉线判定关闭（offline ≤ 0）时不推导也不校验，显式配置原样保留。
func datagramIdle(configured, offline time.Duration) (time.Duration, error) {
	if offline <= 0 {
		return configured, nil
	}
	if configured <= 0 {
		return offline / 3, nil
	}
	if configured >= offline {
		return 0, fmt.Errorf("数据报面 idle_timeout=%s 必须小于 battle.offline_timeout=%s"+
			"（KCP/UDP 无关闭握手，只能靠空闲读超时发现掉线；否则掉线事件会迟到缺省的 120s，掉线策略形同虚设）",
			configured, offline)
	}
	return configured, nil
}

// idleTimeoutOf 解析帧面配置里的空闲读超时（time.ParseDuration 字符串；空串 = 未配置）。
// 非法值即报错（与 serverutil 的配置映射同口径，不静默按未配置处理）；
// "0s" 等非正值按**未配置**处理——数据报面靠空闲读超时发现掉线，关闭它等于关闭掉线判定。
func idleTimeoutOf(raw, field string) (time.Duration, error) {
	if raw == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("server: %s 非法（time.ParseDuration 字符串，如 5s）: %w", field, err)
	}
	if d <= 0 {
		return 0, nil
	}
	return d, nil
}

// connHook 返回指定帧面的连接生命周期回调（桥未注入或面未启用时返回 nil，等价于不挂载）。
func connHook(life *stream.Bridge, kind transport.Kind) func(engine.ConnEvent) {
	if life == nil {
		return nil
	}
	return life.ForFace(kind)
}
