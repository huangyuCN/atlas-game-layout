// Package server 负责 gateway 服务的传输层组装与统一 handler：
// 五协议 Server（tcp/ws/kcp/udp/http）+ 会话绑定 + 挤下线 + 下行推送。
package server

import (
	"context"
	"encoding/json"
	"time"

	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	libsession "github.com/huangyuCN/atlas-game-layout/lib/session"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/actorclient"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/session"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/transport"
	udpt "github.com/huangyuCN/atlas/transport/udp"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/encoding/protojson"
)

// pushServer 是传输 Server 的推送能力子集（四协议通用，ADR-0002）。
type pushServer interface {
	PushRaw(connID uint64, operation string, payload []byte) error
}

// 推送 operation 约定：消息的 protobuf 完整名（客户端 OnNotify 按此分发）。
const (
	PushOpKickedOffline  = "gateway.v1.KickedNotify"
	PushOpFrameBroadcast = "gateway.v1.FrameBroadcast"
	PushOpMatchStarted   = "gateway.v1.MatchStartedNotify"
	PushOpBattleEnd      = "gateway.v1.BattleEndNotify"
)

// Gateway 是 gateway 统一 handler：鉴权 → 会话绑定 → 按 operation 路由。
// 实现全部生成的服务端接口（GatewayAuthTCP/WS + GatewayBattleWS/KCP/UDP）。
type Gateway struct {
	instanceID string
	sess       *session.Manager
	actors     *actorclient.Client
	nc         *nats.Conn
	pushers    map[transport.Kind]pushServer
	udpSrv     *udpt.Server // UDP 按 peer 寻址（无 connID 语义）
}

// NewGateway 构造统一 handler 并按协议注册到各传输 Server。
func NewGateway(
	instanceID string,
	sess *session.Manager,
	actors *actorclient.Client,
	nc *nats.Conn,
	tcpSrv, wsSrv, kcpSrv pushServer,
	udpSrv *udpt.Server,
) *Gateway {
	return &Gateway{
		instanceID: instanceID,
		sess:       sess,
		actors:     actors,
		nc:         nc,
		pushers: map[transport.Kind]pushServer{
			transport.KindTCP:       tcpSrv,
			transport.KindWebSocket: wsSrv,
			transport.KindKCP:       kcpSrv,
		},
		udpSrv: udpSrv,
	}
}

// connFrom 从请求上下文提取连接寻址信息（connID + 传输种类 + 回写函数）。
// UDP 无 connID 语义，按 peer 地址寻址。
func (g *Gateway) connFrom(ctx context.Context) *session.Conn {
	tr, ok := transport.FromServerContext(ctx)
	if !ok {
		return nil
	}
	if tr.Kind() == transport.KindUDP {
		peer := transport.PeerFromContext(ctx)
		if peer == "" || g.udpSrv == nil {
			return nil
		}
		return &session.Conn{
			Kind: string(tr.Kind()),
			Send: func(operation string, payload []byte) error {
				return g.udpSrv.PushToRaw(peer, operation, payload)
			},
		}
	}
	connID := transport.ConnIDFromContext(ctx)
	if connID == 0 {
		return nil
	}
	pusher, ok := g.pushers[tr.Kind()]
	if !ok {
		return nil
	}
	return &session.Conn{
		ID:   connID,
		Kind: string(tr.Kind()),
		Send: func(operation string, payload []byte) error {
			return pusher.PushRaw(connID, operation, payload)
		},
	}
}

// Register 注册：玩家数据创建在 game 服务（D9，M5 里程碑接入），当前返回未接入错误。
func (g *Gateway) Register(ctx context.Context, req *gatewayv1.RegisterRequest) (*gatewayv1.RegisterReply, error) {
	if req.GetAccount() == "" || req.GetPassword() == "" {
		return nil, errorv1.ErrInvalidParams("账号与口令不能为空")
	}
	return nil, errorv1.ErrInternal("注册链路需 game 服务接入（M5 里程碑）")
}

// Login 登录：建立会话（签发令牌 + 绑定通道 + 路由登记），并触发旧会话挤下线。
// M5 里程碑将替换为「actor 转发 game PlayerActor 裁决」；M4 为连接级最小实现。
func (g *Gateway) Login(ctx context.Context, req *gatewayv1.LoginRequest) (*gatewayv1.LoginReply, error) {
	conn := g.connFrom(ctx)
	if conn == nil {
		return nil, errorv1.ErrInvalidToken("请求缺少连接上下文")
	}
	playerID := req.GetAccount()
	if playerID == "" || req.GetPassword() == "" {
		return nil, errorv1.ErrInvalidParams("账号与口令不能为空")
	}
	token, err := libsession.NewToken()
	if err != nil {
		return nil, errorv1.ErrInternal("签发会话令牌失败")
	}
	// 绑定前快照旧会话（本实例挤下线推送用），随后原子覆盖路由。
	oldSess, _ := g.sess.LocalSession(playerID)
	old, err := g.sess.Bind(ctx, playerID, conn, session.ChannelBiz, token)
	if err != nil {
		return nil, errorv1.ErrInternal("会话绑定失败")
	}
	g.kickOld(ctx, playerID, old, oldSess)

	return &gatewayv1.LoginReply{
		PlayerId: playerID,
		Token:    token,
		Player:   &commonv1.PlayerSummary{PlayerId: playerID, Nickname: req.GetAccount()},
	}, nil
}

// Logout 登出：清理会话与路由。
func (g *Gateway) Logout(ctx context.Context, req *gatewayv1.LogoutRequest) (*gatewayv1.LogoutReply, error) {
	ok, err := g.sess.Validate(ctx, req.GetPlayerId(), req.GetToken())
	if err != nil {
		return nil, errorv1.ErrInternal("登出校验失败")
	}
	if !ok {
		return nil, errorv1.ErrInvalidToken("会话令牌无效或已被接管")
	}
	if conn := g.connFrom(ctx); conn != nil {
		g.sess.Unbind(ctx, req.GetPlayerId(), conn.ID)
	}
	return &gatewayv1.LogoutReply{}, nil
}

// Heartbeat 心跳：续租会话 TTL 并对时。
func (g *Gateway) Heartbeat(ctx context.Context, req *gatewayv1.HeartbeatRequest) (*gatewayv1.HeartbeatReply, error) {
	if !g.sess.Heartbeat(ctx, req.GetPlayerId(), req.GetToken()) {
		return nil, errorv1.ErrInvalidToken("会话不存在或令牌失效")
	}
	return &gatewayv1.HeartbeatReply{
		Ts:               req.GetTs(),
		ServerTimeUnixMs: uint64(time.Now().UnixMilli()),
	}, nil
}

// JoinBattle 加入战斗：令牌校验后绑定战斗通道（同连接回退场景自然支持）。
// 会话元信息/当前帧/快照由 battle 服务提供（M7 里程碑），M4 回执绑定结果。
func (g *Gateway) JoinBattle(ctx context.Context, req *gatewayv1.JoinBattleRequest) (*gatewayv1.JoinBattleReply, error) {
	ok, err := g.sess.Validate(ctx, req.GetPlayerId(), req.GetToken())
	if err != nil {
		return nil, errorv1.ErrInternal("加入战斗校验失败")
	}
	if !ok {
		return nil, errorv1.ErrInvalidToken("会话令牌无效或已被接管")
	}
	conn := g.connFrom(ctx)
	if conn == nil {
		return nil, errorv1.ErrInvalidToken("请求缺少连接上下文")
	}
	if _, err := g.sess.Bind(ctx, req.GetPlayerId(), conn, session.ChannelBattle, req.GetToken()); err != nil {
		return nil, errorv1.ErrInternal("战斗通道绑定失败")
	}
	return &gatewayv1.JoinBattleReply{Ok: true}, nil
}

// SendFrameInput 帧输入：透传 battle 战斗 actor 帧通道（M7 里程碑接入）。
func (g *Gateway) SendFrameInput(ctx context.Context, req *gatewayv1.SendFrameInputRequest) (*gatewayv1.SendFrameInputReply, error) {
	return nil, errorv1.ErrInternal("帧通道需 battle 服务接入（M7 里程碑）")
}

// SyncFrames 补帧：断线重连补帧（M7 里程碑接入）。
func (g *Gateway) SyncFrames(ctx context.Context, req *locksteppb.SyncFrameRequest) (*locksteppb.SyncFrameReply, error) {
	return nil, errorv1.ErrInternal("补帧通道需 battle 服务接入（M7 里程碑）")
}

// kickOld 处理挤下线（D10+D13）：新登录覆盖旧路由后，
// 本实例旧连接直接推送被挤下线通知；跨实例经 nats 定向控制通道通知旧实例。
func (g *Gateway) kickOld(ctx context.Context, playerID string, old *session.Route, oldSess *session.Session) {
	if old == nil {
		return
	}
	if old.InstanceID == g.instanceID {
		if oldSess != nil {
			g.pushKicked(oldSess)
		}
		return
	}
	data, err := json.Marshal(kickNotice{PlayerID: playerID})
	if err != nil {
		return
	}
	_ = publishControl(ctx, g.nc, old.InstanceID, data)
}

// pushKicked 向会话的全部通道推送「被挤下线」通知。
func (g *Gateway) pushKicked(sess *session.Session) {
	payload, err := protojson.Marshal(&gatewayv1.KickedNotify{Reason: "logged_in_elsewhere"})
	if err != nil {
		return
	}
	for _, c := range []*session.Conn{sess.Battle, sess.Biz} {
		if c != nil {
			_ = c.Send(PushOpKickedOffline, payload)
		}
	}
}
