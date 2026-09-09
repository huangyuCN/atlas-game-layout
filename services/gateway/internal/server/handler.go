// Package server 负责 gateway 服务的传输层组装与统一 handler：
// 五协议 Server（tcp/ws/kcp/udp/http）+ 会话绑定 + 挤下线 + 下行推送。
package server

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
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

// Gateway 是 gateway 统一 handler：鉴权 → 会话绑定 → 按 operation 路由。
// 实现全部生成的服务端接口（GatewayAuthTCP/WS + GatewayBattleWS/KCP/UDP）。
type Gateway struct {
	instanceID string
	sess       *session.Manager
	actors     *actorclient.Client
	players    *gamev1.PlayerActorClient  // 生成的玩家 actor client stub
	battles    *battlev1.BattleActorClient // 生成的战斗 actor client stub
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
		players:    gamev1.NewPlayerActorClient(actors.PlayerInvoker()),
		battles:    battlev1.NewBattleActorClient(actors.BattleInvoker()),
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
// UDP 无 connID 语义，按 peer 地址寻址。Ref 为连接身份键（会话反向索引用）。
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
			Ref:  "udp:" + peer,
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
		Ref:  connRef(connID),
		Send: func(operation string, payload []byte) error {
			return pusher.PushRaw(connID, operation, payload)
		},
	}
}

// connRef 是连接 ID 的身份键（与 session 反向索引约定一致）。
func connRef(connID uint64) string {
	return "conn:" + strconv.FormatUint(connID, 10)
}

// Register 注册：经 actor 转发 game PlayerActor（懒激活），创建玩家数据并回执（D9）。
func (g *Gateway) Register(ctx context.Context, req *gatewayv1.RegisterRequest) (*gatewayv1.RegisterReply, error) {
	if req.GetAccount() == "" || req.GetPassword() == "" {
		return nil, errorv1.ErrInvalidParams("账号与口令不能为空")
	}
	playerID := req.GetAccount()
	pid, err := actorclient.PlayerPID(playerID)
	if err != nil {
		return nil, err
	}
	reply, err := g.players.Register(ctx, pid, &gamev1.RegisterActorReq{
		Account:         req.GetAccount(),
		Password:        req.GetPassword(),
		Nickname:        req.GetNickname(),
		GatewayInstance: g.instanceID,
	})
	if err != nil {
		return nil, err // 业务错误透传：code/reason 经集群 error 通道往返保留
	}
	return &gatewayv1.RegisterReply{PlayerId: reply.GetPlayerId()}, nil
}

// Login 登录：经 actor 转发 game PlayerActor 裁决（玩家数据 + 会话令牌，D9/D10），
// 成功后建立网关会话（签发令牌 + 绑定通道 + 路由登记）并触发旧会话挤下线。
func (g *Gateway) Login(ctx context.Context, req *gatewayv1.LoginRequest) (*gatewayv1.LoginReply, error) {
	conn := g.connFrom(ctx)
	if conn == nil {
		return nil, errorv1.ErrInvalidToken("请求缺少连接上下文")
	}
	playerID := req.GetPlayerId()
	if playerID == "" || req.GetPassword() == "" {
		return nil, errorv1.ErrInvalidParams("玩家与口令不能为空")
	}
	token, err := libsession.NewToken()
	if err != nil {
		return nil, errorv1.ErrInternal("签发会话令牌失败")
	}
	// 先经 actor 裁决（玩家数据校验 + 会话令牌覆盖），成功后再绑定网关会话。
	pid, err := actorclient.PlayerPID(playerID)
	if err != nil {
		return nil, err
	}
	reply, err := g.players.Login(ctx, pid, &gamev1.LoginActorReq{
		PlayerId:        playerID,
		Password:        req.GetPassword(),
		Token:           token,
		GatewayInstance: g.instanceID,
	})
	if err != nil {
		return nil, err // 业务错误透传（PLAYER_NOT_FOUND / PASSWORD_WRONG 等语义不变）
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
		Player:   reply.GetPlayer(),
	}, nil
}

// Logout 登出：清理会话与路由，并联动 game PlayerActor 停止（Locator 移除）。
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
	// 联动 game PlayerActor：令牌匹配才清理会话并停止（异步投递）。
	if pid, perr := actorclient.PlayerPID(req.GetPlayerId()); perr == nil {
		_ = g.players.Logout(ctx, pid, &gamev1.LogoutActorMsg{Token: req.GetToken(), Reason: "logout"})
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

// JoinBattle 加入战斗：令牌校验 → battle actor 裁决参战资格并回执
// 会话元信息/当前帧/快照（M7）→ 绑定战斗通道。
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
	// battle actor 裁决参战资格（懒激活）并回执会话元信息/当前帧/快照。
	pid, err := actorclient.BattlePID(req.GetBattleId())
	if err != nil {
		return nil, err
	}
	reply, err := g.battles.Join(ctx, pid, &battlev1.JoinBattleReq{PlayerId: req.GetPlayerId()})
	if err != nil {
		return nil, err // BATTLE_NOT_FOUND 等业务错误透传（产生点即语义，与现状对外一致）
	}
	if _, err := g.sess.Bind(ctx, req.GetPlayerId(), conn, session.ChannelBattle, req.GetToken()); err != nil {
		return nil, errorv1.ErrInternal("战斗通道绑定失败")
	}
	return &gatewayv1.JoinBattleReply{
		Ok:           true,
		Meta:         reply.GetMeta(),
		CurrentFrame: reply.GetCurrentFrame(),
		Snapshot:     reply.GetSnapshot(),
	}, nil
}

// SendFrameInput 帧输入：透传 battle 战斗 actor 帧通道（lockstep 输入）。
// 玩家身份以连接会话绑定为准（忽略客户端自报 player_id，防伪造）。
func (g *Gateway) SendFrameInput(ctx context.Context, req *gatewayv1.SendFrameInputRequest) (*gatewayv1.SendFrameInputReply, error) {
	playerID, err := g.playerFromConn(ctx)
	if err != nil {
		return nil, err
	}
	in := req.GetInput()
	if req.GetBattleId() == "" || in == nil {
		return nil, errorv1.ErrInvalidParams("战斗 ID 与输入不能为空")
	}
	in.PlayerId = playerID
	pid, err := actorclient.BattlePID(req.GetBattleId())
	if err != nil {
		return nil, err
	}
	if _, err := g.battles.FrameInput(ctx, pid, &battlev1.FrameInputReq{PlayerId: playerID, Input: in}); err != nil {
		return nil, err
	}
	return &gatewayv1.SendFrameInputReply{}, nil
}

// SyncFrames 补帧：断线重连后拉取缺失帧（battle actor 补帧回执）。
// 玩家身份以连接会话绑定为准；battle actor 按参战名单复核。
func (g *Gateway) SyncFrames(ctx context.Context, req *locksteppb.SyncFrameRequest) (*locksteppb.SyncFrameReply, error) {
	playerID, err := g.playerFromConn(ctx)
	if err != nil {
		return nil, err
	}
	pid, err := actorclient.BattlePID(req.GetSessionId())
	if err != nil {
		return nil, err
	}
	reply, err := g.battles.Reconnect(ctx, pid, &battlev1.ReconnectReq{
		PlayerId:      playerID,
		LastSeenFrame: req.GetFromFrameId(),
	})
	if err != nil {
		return nil, err // INVALID_TOKEN 等业务错误透传（battle 产生点即语义）
	}
	out := &locksteppb.SyncFrameReply{
		Frames:           make([]*locksteppb.LockstepFrame, 0, len(reply.GetMissed())),
		ConfirmedFrameId: reply.GetCurrentFrame(),
	}
	for _, group := range reply.GetMissed() {
		out.Frames = append(out.Frames, &locksteppb.LockstepFrame{
			FrameId: group.GetFrameId(),
			Inputs:  group.GetInputs(),
		})
	}
	return out, nil
}

// playerFromConn 按请求连接反查会话绑定的玩家身份（帧上行鉴权）。
func (g *Gateway) playerFromConn(ctx context.Context) (string, error) {
	conn := g.connFrom(ctx)
	if conn == nil {
		return "", errorv1.ErrInvalidToken("请求缺少连接上下文")
	}
	playerID, ok := g.sess.PlayerByRef(conn.Ref)
	if !ok {
		return "", errorv1.ErrInvalidToken("战斗通道未绑定玩家身份")
	}
	return playerID, nil
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
			_ = c.Send(consts.PushOpKickedOffline, payload)
		}
	}
}
