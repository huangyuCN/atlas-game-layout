// Package server 实现 gateway 的传输层组装：会话生命周期 handler（gateway.v1.Session
// 自留）与透传引擎——后者按 atlas.route.v1 注解生成的路由表（relay.Table）把客户端
// 业务 op 原样转发到域 actor（查表 → 身份识别 → 组 target PID → 注入 sender →
// 集群 Ask/Tell → 原样回执），新增域 op 时 Gateway 零代码。
package server

import (
	"context"
	"fmt"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/actorclient"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/session"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/transport"
	"google.golang.org/protobuf/proto"
)

// Relay 是业务 op 的透传引擎：路由表（注解生成）+ 会话校验 + 集群投递。
type Relay struct {
	table relay.Table
	sess  *session.Manager
	rt    actorclient.Runtime
	// conn 摘取与 Gateway 共用（连接寻址 → 会话绑定反查）。
	connOf func(ctx context.Context) *session.Conn
	// bindings 声明「转发成功后把当前连接绑定到玩家会话的哪个槽位」（如
	// JoinBattle → 战斗通道）。槽位集合属于本网关的会话模型，经装配声明，
	// 不进注解协议（见 relay.Slot）。
	bindings map[string]relay.Slot
}

// NewRelay 构造透传引擎（table 为各域生成路由表经 relay.Merge 合并的结果）。
func NewRelay(table relay.Table, sess *session.Manager, rt actorclient.Runtime, connOf func(ctx context.Context) *session.Conn) *Relay {
	return &Relay{table: table, sess: sess, rt: rt, connOf: connOf, bindings: make(map[string]relay.Slot)}
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

// Forward 处理一次透传请求：身份识别 → target PID → sender 注入 → Ask/Tell。
// 业务错误原样透传（code/reason 经集群 error 通道往返保留，产生点即语义）。
func (r *Relay) Forward(ctx context.Context, entry relay.RouteEntry, req proto.Message) (proto.Message, error) {
	playerID, err := r.identify(ctx)
	if err != nil {
		return nil, err
	}
	pid, err := entry.ResolvePID(req, playerID)
	if err != nil {
		return nil, r.mapRouteErr(err)
	}
	sender, err := types.NewPID("player", playerID)
	if err != nil {
		return nil, errorv1.ErrInternal("透传组装发起者身份失败")
	}
	if entry.IsTell {
		if err := r.rt.Tell(ctx, pid, req, core.WithSender(sender)); err != nil {
			return nil, err
		}
		r.bindChannel(ctx, entry.Operation, playerID)
		return nil, nil
	}
	rep, err := r.rt.Ask(ctx, pid, req, core.WithSender(sender))
	if err != nil {
		return nil, err // 业务错误透传（产生点即语义）
	}
	resp, ok := rep.(proto.Message)
	if !ok {
		return nil, errorv1.ErrInternal("透传回执类型异常: %T", rep)
	}
	r.bindChannel(ctx, entry.Operation, playerID)
	return resp, nil
}

// bindChannel 在透传成功后按装配声明（WithChannelBinding）把当前连接绑定到
// 玩家会话的对应槽位（如 JoinBattle → 战斗通道）。绑定凭据优先取帧会话槽
//（UDP/KCP 每帧验证形态）；无槽帧（TCP/WS 长连接）复用本地会话当前凭据。
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

// identify 识别请求玩家身份：帧会话槽凭据优先（UDP/KCP 每帧验证），
// 否则按连接绑定反查（TCP/WS 长连接）。均未命中即拒绝。
func (r *Relay) identify(ctx context.Context) (string, error) {
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

// mapRouteErr 把路由错误映射为对外错误码（产生点即语义）。
func (r *Relay) mapRouteErr(err error) error {
	switch {
	case err == relay.ErrNoSession:
		return errorv1.ErrInvalidToken("会话未登录或已过期")
	case err == relay.ErrNoUID:
		return errorv1.ErrInvalidParams("路由 UID 为空（客体字段缺失）")
	default:
		return fmt.Errorf("relay: %w", err)
	}
}
