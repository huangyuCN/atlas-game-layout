// Package server 实现 gateway 的传输层组装：会话生命周期 handler（gateway.v1.Session
// 自留）与透传引擎——后者按 atlas.route.v1 注解生成的路由表（relay.Table）把客户端
// 业务 op 原样转发到域 rpc/ 平面的 Edge 接口（查表 → 身份识别 → 组装 metadata 身份 →
// gRPC 一元调用 → 原样回执），新增域 op 时 Gateway 零代码。
package server

import (
	"context"

	"github.com/huangyuCN/atlas/contrib/actor/opcall"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/session"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	"github.com/huangyuCN/atlas/metrics"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame"
	"google.golang.org/protobuf/proto"
)

// Relay 是业务 op 的透传引擎：路由表（注解生成）+ 会话校验 + 域 Edge 面投递。
type Relay struct {
	table relay.Table
	sess  *session.Manager
	// inv 是按方法寻址的跨服务投递端口（域 rpc/ 平面的 Edge 面，经 gRPC）：
	// 目标 PID 的解析与惰性激活在目标域服务内完成，网关不持有 actor 集群运行时。
	inv opcall.MethodInvoker
	// meter 是可选指标采集器（nil = 不打点；noop 零开销）。
	meter metrics.Collector
	// conn 摘取与 Gateway 共用（连接寻址 → 会话绑定反查）。
	connOf func(ctx context.Context) *session.Conn
	// bindings 声明「转发成功后把当前连接绑定到玩家会话的哪个槽位」（如
	// JoinBattle → 战斗通道）。槽位集合属于本网关的会话模型，经装配声明，
	// 不进注解协议（见 relay.Slot）。
	bindings map[string]relay.Slot
}

// NewRelay 构造透传引擎（table 为各域生成路由表经 relay.Merge 合并的结果）。
func NewRelay(table relay.Table, sess *session.Manager, inv opcall.MethodInvoker, meter metrics.Collector, connOf func(ctx context.Context) *session.Conn) *Relay {
	return &Relay{table: table, sess: sess, inv: inv, meter: meter, connOf: connOf, bindings: make(map[string]relay.Slot)}
}

// WithChannelBinding 声明「op 转发成功后绑定会话通道槽」的局部规则（装配期注入）。
func (r *Relay) WithChannelBinding(operation string, slot relay.Slot) *Relay {
	r.bindings[operation] = slot
	return r
}

// Lookup 查询 operation 的路由条目（测试与观测用）。
func (r *Relay) Lookup(operation string) (relay.RouteEntry, bool) {
	return r.table.Lookup(operation)
}

// Each 以未定义序遍历 access=CLIENT 的路由条目（注册侧逐一 Subscribe）；
// fn 返回错误即中断。
func (r *Relay) Each(fn func(entry relay.RouteEntry) error) error {
	for op, e := range r.table {
		if e.Access != relay.AccessClient {
			continue // 服务端内部方法不注册传输路由
		}
		e.Operation = op
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

// Forward 处理一次透传请求（计数薄壳）：op 标签取路由条目 operation，
// result 按投递结果 success/error——自留会话接口（meteredSession）与
// 透传共用 gateway_requests_total，面板按 op 汇总全部入口 QPS。
func (r *Relay) Forward(ctx context.Context, entry relay.RouteEntry, req proto.Message) (proto.Message, error) {
	rep, err := r.deliver(ctx, entry, req)
	meterRequest(r.meter, entry.Operation, err)
	return rep, err
}

// deliver 是透传的业务本体：会话身份 → metadata 身份 → 域 Edge 面唯一投递 → 回执。
//
// 身份只来自会话（客户端不可影响）：player 身份决定会话寻址的目标 actor，发起者固定为
// 该玩家；请求 ID 取帧头（所有 op 作观测头，幂等 op 由接收侧按条目作为去重键）。
// 目标 PID 的解析与懒激活都在目标域服务内完成（rpc/ 平面 Edge 接口 → opcall → actor），
// 网关不再自行组装 PID，也不再持有 actor 集群运行时。
func (r *Relay) deliver(ctx context.Context, entry relay.RouteEntry, req proto.Message) (proto.Message, error) {
	playerID, err := r.playerOf(ctx)
	if err != nil {
		return nil, err
	}
	ctx = opcall.WithCallInfo(ctx, callInfoOf(ctx, playerID, true))
	rep, err := opcall.DeliverRemote(ctx, r.inv, entry, req)
	if err != nil {
		return nil, err // 业务错误原样透传（产生点即语义）
	}
	r.bindChannel(ctx, entry.Operation, playerID)
	return rep, nil
}

// call 以显式玩家身份发起一次**客户端 op**（网关自身的会话联动用，如挤下线后
// 取消匹配/退出队伍）：与透传共用同一张路由表与同一个投递端口——身份由会话模型给定，
// 不来自载荷，也不为这类调用另开一条投递路径。
func (r *Relay) call(ctx context.Context, playerID, operation string, req proto.Message) error {
	entry, ok := r.table.Lookup(operation)
	if !ok {
		return errorv1.ErrInternal("路由条目缺失: %s", operation)
	}
	ctx = opcall.WithCallInfo(ctx, callInfoOf(ctx, playerID, true))
	_, err := opcall.DeliverRemote(ctx, r.inv, entry, req)
	return err
}

// requestIDOf 从请求头取帧请求幂等键（FlagRequestID 置位时引擎已解析写入）。
// 包级函数：relay 透传与 Gateway 自留会话接口（Register/Login/Heartbeat）共用，
// 保证全部 op 的 actor 日志都能带上 request_id。
func requestIDOf(ctx context.Context) string {
	tr, ok := transport.FromServerContext(ctx)
	if !ok {
		return ""
	}
	hdr := tr.RequestHeader()
	if hdr == nil {
		return ""
	}
	return hdr.Get(frame.RequestHeaderKeyRequestID)
}

// bindChannel 在透传成功后按装配声明（WithChannelBinding）把当前连接绑定到
// 玩家会话的对应槽位（如 JoinBattle → 战斗通道）。绑定凭据优先取帧会话槽
// （UDP/KCP 每帧验证形态）；无槽帧（TCP/WS 长连接）复用本地会话当前凭据。
// 绑定失败不吞回执（转发已成功，会话可经重绑/心跳收敛）。
func (r *Relay) bindChannel(ctx context.Context, operation, playerID string) {
	slot, ok := r.bindings[operation]
	if !ok {
		return
	}
	conn := r.connOf(ctx)
	if conn == nil {
		return
	}
	token := r.slotToken(ctx)
	if token == "" {
		if local, ok := r.sess.LocalSession(playerID); ok {
			token = local.Token
		}
	}
	_, _ = r.sess.Bind(ctx, playerID, conn, session.Channel(slot), token)
}

// slotToken 从请求上下文取帧会话槽凭据（无槽帧返回空串——长连接绑定场景）。
func (r *Relay) slotToken(ctx context.Context) string {
	tr, ok := transport.FromServerContext(ctx)
	if !ok {
		return ""
	}
	hdr := tr.RequestHeader()
	if hdr == nil {
		return ""
	}
	return hdr.Get("Atlas-Frame-Session")
}

// playerOf 解析请求的玩家身份：帧会话槽凭据优先（UDP/KCP 每帧验证），
// 否则按连接绑定反查（TCP/WS 长连接）。均未命中即拒绝——身份只来自会话，不来自载荷。
func (r *Relay) playerOf(ctx context.Context) (string, error) {
	if tr, ok := transport.FromServerContext(ctx); ok {
		if hdr := tr.RequestHeader(); hdr != nil {
			if tok := hdr.Get("Atlas-Frame-Session"); tok != "" {
				playerID, ok := r.sess.PlayerByToken(tok)
				if !ok {
					return "", errorv1.ErrInvalidToken("会话凭据无效或已过期")
				}
				return playerID, nil
			}
		}
	}
	conn := r.connOf(ctx)
	if conn == nil {
		return "", errorv1.ErrInvalidToken("请求缺少连接上下文")
	}
	playerID, ok := r.sess.PlayerByRef(conn.Ref)
	if !ok {
		return "", errorv1.ErrInvalidToken("通道未绑定玩家身份")
	}
	return playerID, nil
}
