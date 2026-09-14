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
	"github.com/huangyuCN/atlas/contrib/actor/types"
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
// 实现全部生成的服务端接口（GatewayAuthTCP/WS + GatewayMatchTCP/WS + GatewayBattleWS/KCP/UDP）。
type Gateway struct {
	instanceID string
	sess       *session.Manager
	actors     *actorclient.Client
	players    *gamev1.PlayerActorClient   // 生成的玩家 actor client stub
	battles    *battlev1.BattleActorClient // 生成的战斗 actor client stub
	nc         *nats.Conn
	pushers    map[transport.Kind]pushServer
	udpSrv     *udpt.Server // UDP 按 peer 寻址（无 connID 语义）
}

// onSessionsExpired 异常下线联动撮合域（会话过期清扫回调）：
// 对「确认属主且未被接管」的过期会话，向 game PlayerActor 发 Logout（SESSION_EXPIRED）
// → 触发现有 OnStop 联动（取消匹配 / 离队 / 下线落库），登出与断线语义归一。
// 携带过期会话的旧令牌：game 侧据此裁决接管（新登录存在不同令牌则跳过停止）。
func (g *Gateway) onSessionsExpired(swept []session.SweptSession) {
	ctx, cancel := context.WithTimeout(context.Background(), relayTimeout)
	defer cancel()
	for _, s := range swept {
		pid, err := actorclient.PlayerPID(s.PlayerID)
		if err != nil {
			continue
		}
		_ = g.players.Logout(ctx, pid, &gamev1.LogoutActorMsg{
			Token:  s.Token,
			Reason: gamev1.LogoutReason_LOGOUT_REASON_SESSION_EXPIRED,
		})
	}
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
	g := &Gateway{
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
	// 会话过期联动撮合域（异常下线兜底）。
	g.sess.SetSweptHook(g.onSessionsExpired)
	return g
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
		_ = g.players.Logout(ctx, pid, &gamev1.LogoutActorMsg{
			Token: req.GetToken(), Reason: gamev1.LogoutReason_LOGOUT_REASON_LOGOUT,
		})
	}
	return &gatewayv1.LogoutReply{}, nil
}

// QueueMatch 入队匹配：令牌校验 → game PlayerActor 入队转发。
// 客户端只选规则集；匹配属性由 PlayerActor 从聚合根权威填充（反作弊）。
func (g *Gateway) QueueMatch(ctx context.Context, req *gatewayv1.MatchQueueRequest) (*gatewayv1.MatchQueueReply, error) {
	pid, err := g.playerCall(ctx, req.GetPlayerId(), req.GetToken(), "入队匹配")
	if err != nil {
		return nil, err
	}
	if _, err := g.players.EnterMatchQueue(ctx, pid, &gamev1.EnterMatchQueueActorReq{Ruleset: req.GetRuleset()}); err != nil {
		return nil, err // ALREADY_IN_MATCH 等业务错误透传（产生点即语义）
	}
	return &gatewayv1.MatchQueueReply{}, nil
}

// CancelMatch 取消匹配：令牌校验 → actor 转发（未在队幂等 canceled=false）。
func (g *Gateway) CancelMatch(ctx context.Context, req *gatewayv1.MatchCancelRequest) (*gatewayv1.MatchCancelReply, error) {
	pid, err := g.playerCall(ctx, req.GetPlayerId(), req.GetToken(), "取消匹配")
	if err != nil {
		return nil, err
	}
	rep, err := g.players.CancelMatch(ctx, pid, &gamev1.CancelMatchActorReq{})
	if err != nil {
		return nil, err
	}
	return &gatewayv1.MatchCancelReply{Canceled: rep.GetCanceled()}, nil
}

// MatchStatus 查询匹配状态：轮询兜底（成局/失败另有主动推送）。
func (g *Gateway) MatchStatus(ctx context.Context, req *gatewayv1.MatchStatusRequest) (*gatewayv1.MatchStatusReply, error) {
	pid, err := g.playerCall(ctx, req.GetPlayerId(), req.GetToken(), "匹配状态")
	if err != nil {
		return nil, err
	}
	rep, err := g.players.GetMatchStatus(ctx, pid, &gamev1.GetMatchStatusActorReq{})
	if err != nil {
		return nil, err
	}
	return &gatewayv1.MatchStatusReply{
		State:    rep.GetState(),
		TicketId: rep.GetTicketId(),
		MatchId:  rep.GetMatchId(),
		BattleId: rep.GetBattleId(),
	}, nil
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
func (g *Gateway) JoinBattle(ctx context.Context, req *gatewayv1.JoinBattleRequest) (*battlev1.JoinBattleReply, error) {
	if _, err := g.playerCall(ctx, req.GetPlayerId(), req.GetToken(), "加入战斗"); err != nil {
		return nil, err
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
	return &battlev1.JoinBattleReply{
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
// 接管即联动撮合域：清理被挤会话的匹配/组队在线态（覆盖「先重登后清扫」窗口，
// 新会话以干净状态起步），失败仅忽略（幂等，未在队/未组队为 no-op）。
func (g *Gateway) kickOld(ctx context.Context, playerID string, old *session.Route, oldSess *session.Session) {
	if old == nil {
		return
	}
	if pid, perr := actorclient.PlayerPID(playerID); perr == nil {
		_, _ = g.players.CancelMatch(ctx, pid, &gamev1.CancelMatchActorReq{})
		_, _ = g.players.LeaveParty(ctx, pid, &gamev1.LeavePartyActorReq{})
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
	payload, err := protojson.Marshal(&gatewayv1.KickedNotify{
		Reason: gatewayv1.KickedReason_KICKED_REASON_LOGGED_IN_ELSEWHERE,
	})
	if err != nil {
		return
	}
	for _, c := range []*session.Conn{sess.Battle, sess.Biz} {
		if c != nil {
			_ = c.Send(consts.PushOpKickedOffline, payload)
		}
	}
}

// ---- 灵活组队（客户端只传队伍标识/规则集；身份由会话校验，属性服务端权威填充）----

// playerCall 是匹配/组域请求的公共前置：会话令牌校验 + 玩家 PID 组装。
// action 用于校验错误消息定位（如「入队匹配」）；业务错误由调用方透传。
func (g *Gateway) playerCall(ctx context.Context, playerID, token, action string) (types.PID, error) {
	ok, err := g.sess.Validate(ctx, playerID, token)
	if err != nil {
		return types.PID{}, errorv1.ErrInternal("%s校验失败", action)
	}
	if !ok {
		return types.PID{}, errorv1.ErrInvalidToken("会话令牌无效或已被接管")
	}
	return actorclient.PlayerPID(playerID)
}

// partyInfoReply 把 actor 名册回执映射为网关快照回执。
func partyInfoReply(rep *gamev1.PartyActorReply) *gatewayv1.PartyInfoReply {
	return &gatewayv1.PartyInfoReply{
		PartyId:  rep.GetPartyId(),
		LeaderId: rep.GetLeaderId(),
		Members:  rep.GetMembers(),
	}
}

// partyActorCall 是组域无额外参数请求的公共路径：前置校验 → 调 actor → 映射快照。
func (g *Gateway) partyActorCall(ctx context.Context, playerID, token, action string,
	call func(pid types.PID) (*gamev1.PartyActorReply, error)) (*gatewayv1.PartyInfoReply, error) {
	pid, err := g.playerCall(ctx, playerID, token, action)
	if err != nil {
		return nil, err
	}
	rep, err := call(pid)
	if err != nil {
		return nil, err
	}
	return partyInfoReply(rep), nil
}

// PartyCreate 建队：本玩家为队长，回执名册快照。
func (g *Gateway) PartyCreate(ctx context.Context, req *gatewayv1.PartyCreateRequest) (*gatewayv1.PartyInfoReply, error) {
	return g.partyActorCall(ctx, req.GetPlayerId(), req.GetToken(), "建队",
		func(pid types.PID) (*gamev1.PartyActorReply, error) {
			return g.players.CreateParty(ctx, pid, &gamev1.CreatePartyActorReq{})
		})
}

// PartyJoin 按 party_id 加入：容量原子校验（满员/队伍不存在业务错误透传）。
func (g *Gateway) PartyJoin(ctx context.Context, req *gatewayv1.PartyJoinRequest) (*gatewayv1.PartyInfoReply, error) {
	pid, err := g.playerCall(ctx, req.GetPlayerId(), req.GetToken(), "加入队伍")
	if err != nil {
		return nil, err
	}
	rep, err := g.players.JoinParty(ctx, pid, &gamev1.JoinPartyActorReq{PartyId: req.GetPartyId()})
	if err != nil {
		return nil, err
	}
	return partyInfoReply(rep), nil
}

// PartyLeave 离开队伍（幂等；空快照表示未组队）。
func (g *Gateway) PartyLeave(ctx context.Context, req *gatewayv1.PartyLeaveRequest) (*gatewayv1.PartyInfoReply, error) {
	return g.partyActorCall(ctx, req.GetPlayerId(), req.GetToken(), "离开队伍",
		func(pid types.PID) (*gamev1.PartyActorReply, error) {
			return g.players.LeaveParty(ctx, pid, &gamev1.LeavePartyActorReq{})
		})
}

// PartyStatus 名册快照（轮询兜底）。
func (g *Gateway) PartyStatus(ctx context.Context, req *gatewayv1.PartyStatusRequest) (*gatewayv1.PartyInfoReply, error) {
	return g.partyActorCall(ctx, req.GetPlayerId(), req.GetToken(), "队伍状态",
		func(pid types.PID) (*gamev1.PartyActorReply, error) {
			return g.players.GetParty(ctx, pid, &gamev1.GetPartyActorReq{})
		})
}

// PartyQueue 队长发整队入队（1..N 人都可入队）。
func (g *Gateway) PartyQueue(ctx context.Context, req *gatewayv1.PartyQueueRequest) (*gatewayv1.PartyQueueReply, error) {
	pid, err := g.playerCall(ctx, req.GetPlayerId(), req.GetToken(), "整队入队")
	if err != nil {
		return nil, err
	}
	rep, err := g.players.QueueParty(ctx, pid, &gamev1.QueuePartyActorReq{Ruleset: req.GetRuleset()})
	if err != nil {
		return nil, err
	}
	return &gatewayv1.PartyQueueReply{TicketId: rep.GetTicketId()}, nil
}
