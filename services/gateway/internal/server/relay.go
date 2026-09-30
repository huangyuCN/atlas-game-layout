// Package server 实现 gateway 的传输层组装：会话生命周期 handler（gateway.v1.Session
// 自留）与透传引擎——后者按 atlas.route.v1 注解生成的路由表（relay.Table）把客户端
// 业务 op 原样转发到域 rpc/ 平面的 Edge 接口（查表 → 身份识别 → 组装 metadata 身份 →
// gRPC 一元调用 → 原样回执），新增域 op 时 Gateway 零代码。
//
// 帧请求的四步（解码 → 身份 → 投递 → 回执）由框架组件 frameops.Handler 承载（与 battle
// 帧面共用同一份实现，见 contrib/actor/frameops）；网关只注入自己的三件：会话身份解析
// （sessionIdentity）、远端投递端口（RemoteDeliverer）、售后副作用（指标口径）。
package server

import (
	"context"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1actor "github.com/huangyuCN/atlas-game-layout/api/game/v1/actor"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/session"
	"github.com/huangyuCN/atlas/contrib/actor/frameops"
	"github.com/huangyuCN/atlas/contrib/actor/opcall"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	"github.com/huangyuCN/atlas/metrics"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame"
	"google.golang.org/protobuf/proto"
)

// Relay 是业务 op 的透传引擎：路由表（注解生成）+ 会话校验 + 域 Edge 面投递。
type Relay struct {
	// ops 是帧 op 服务端（框架组件，与 battle 帧面共用）：路由表 + 身份解析 + 投递端口
	// + 售后副作用四件齐备后的唯一实现；网关不再自持透传桩。
	ops *frameops.Handler
	// deliver 是网关自留会话联动（挤下线后取消匹配/退出队伍）复用的同一投递端口。
	deliver frameops.RemoteDeliverer
	// sess 与 connOf 摘取连接、寻址会话（身份解析与指标口径共用）。
	sess   *session.Manager
	connOf func(ctx context.Context) *session.Conn
	// meter 是可选指标采集器（nil = 不打点；noop 零开销）。
	meter metrics.Collector
}

// NewRelay 构造透传引擎（table 为各域生成路由表经 relay.Merge 合并的结果；
// inv 是按方法寻址的跨服务投递端口：目标 PID 的解析与惰性激活在目标域服务内完成，
// 网关不持有 actor 集群运行时）。
func NewRelay(table relay.Table, sess *session.Manager, inv opcall.MethodInvoker, meter metrics.Collector, connOf func(ctx context.Context) *session.Conn) *Relay {
	r := &Relay{
		deliver: frameops.RemoteDeliverer{Invoker: inv},
		sess:    sess,
		connOf:  connOf,
		meter:   meter,
	}
	r.ops = frameops.NewHandler(table,
		sessionIdentity{sess: sess, connOf: connOf},
		r.deliver,
		frameops.WithSenderActorType(gamev1actor.PlayerServiceActorType),
		frameops.WithHooks(frameops.Hooks{PostDeliver: r.postDeliver}),
	)
	return r
}

// Lookup 查询 operation 的路由条目（测试与观测用）。
func (r *Relay) Lookup(operation string) (relay.RouteEntry, bool) {
	return r.ops.Lookup(operation)
}

// Each 以未定义序遍历 access=CLIENT 的路由条目（注册侧逐一 Subscribe）；
// fn 返回错误即中断。
func (r *Relay) Each(fn func(entry relay.RouteEntry) error) error {
	return r.ops.Each(fn)
}

// Forward 处理一次已解码的透传请求（薄壳）：与帧 handler 共用同一个 frameops.Handler，
// 身份 → 投递 → 副作用（指标口径 + 通道绑定）三步完全同口径。
func (r *Relay) Forward(ctx context.Context, entry relay.RouteEntry, req proto.Message) (proto.Message, error) {
	return r.ops.Handle(ctx, entry, req)
}

// postDeliver 是帧请求处理后的网关专属副作用（注入 frameops.Hooks）：
// 无论成败都按路由 op 打 gateway_requests_total，口径与自留会话接口（meteredSession）
// 一致，面板按 op 汇总全部入口 QPS。
func (r *Relay) postDeliver(_ context.Context, entry relay.RouteEntry, _ frameops.Identity, err error) {
	meterRequest(r.meter, entry.Operation, err)
}

// call 以显式玩家身份发起一次**客户端 op**（网关自身的会话联动用，如挤下线后
// 取消匹配/退出队伍）：与透传共用同一张路由表与同一个投递端口——身份由会话模型给定，
// 不来自载荷，也不为这类调用另开一条投递路径。
func (r *Relay) call(ctx context.Context, playerID, operation string, req proto.Message) error {
	entry, ok := r.ops.Lookup(operation)
	if !ok {
		return errorv1.ErrInternal("路由条目缺失: %s", operation)
	}
	ctx = opcall.WithCallInfo(ctx, callInfoOf(ctx, playerID, true))
	_, err := r.deliver.Deliver(ctx, entry, req)
	return err
}

// sessionIdentity 按网关的会话模型解析帧请求的载体身份（注入 frameops.IdentityResolver）：
// 帧会话槽凭据优先（UDP/KCP 每帧验证），否则按连接绑定反查（TCP/WS 长连接）。
type sessionIdentity struct {
	sess   *session.Manager
	connOf func(ctx context.Context) *session.Conn
}

// Resolve 解析一次帧请求的玩家身份：均未命中即拒绝——身份只来自会话，不来自载荷。
func (s sessionIdentity) Resolve(ctx context.Context, _ relay.RouteEntry) (frameops.Identity, error) {
	if tr, ok := transport.FromServerContext(ctx); ok {
		if hdr := tr.RequestHeader(); hdr != nil {
			if tok := hdr.Get(frame.RequestHeaderKeySession); tok != "" {
				playerID, ok := s.sess.PlayerByToken(tok)
				if !ok {
					return frameops.Identity{}, errorv1.ErrInvalidToken("会话凭据无效或已过期")
				}
				return frameops.Identity{PlayerID: playerID}, nil
			}
		}
	}
	conn := s.connOf(ctx)
	if conn == nil {
		return frameops.Identity{}, errorv1.ErrInvalidToken("请求缺少连接上下文")
	}
	playerID, ok := s.sess.PlayerByRef(conn.Ref)
	if !ok {
		return frameops.Identity{}, errorv1.ErrInvalidToken("通道未绑定玩家身份")
	}
	return frameops.Identity{PlayerID: playerID}, nil
}
