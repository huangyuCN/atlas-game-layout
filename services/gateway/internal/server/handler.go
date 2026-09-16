// Package server 负责 gateway 服务的传输层组装与统一 handler：
// 五协议 Server（tcp/ws/kcp/udp/http）+ 会话绑定 + 挤下线 + 下行推送。
// 业务 op 经透传引擎（relay.go）原样转发域 actor，本文件只实现 Gateway
// 自留的会话生命周期接口（gateway.v1.Session）与登录联动逻辑。
package server

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	libsession "github.com/huangyuCN/atlas-game-layout/lib/session"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/actorclient"
	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/session"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/transport"
	udpt "github.com/huangyuCN/atlas/transport/udp"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/encoding/protojson"
)

// playerPID 是 actorclient.PlayerPID 的容错包装（PID 非法返回零值，调用方忽略错误）。
func playerPID(playerID string) types.PID {
	pid, _ := actorclient.PlayerPID(playerID)
	return pid
}

// connRef 是连接 ID 的身份键（与 session 反向索引约定一致）。
// 带传输种类前缀：各传输 Server 的 connID 独立计数，裸 ID 会在
// TCP/WS 等多条连接间撞键（会话反向索引错绑到别的连接）。
func connRef(kind transport.Kind, connID uint64) string {
	return string(kind) + ":conn:" + strconv.FormatUint(connID, 10)
}

// connFrom 从请求上下文提取连接寻址信息（connID + 传输种类 + 回写函数）。
// UDP 无 connID 语义，按 peer 地址寻址。Ref 为连接身份键（会话反向索引用）。
func connFrom(ctx context.Context, pushers map[transport.Kind]pushServer, udpSrv *udpt.Server) *session.Conn {
	tr, ok := transport.FromServerContext(ctx)
	if !ok {
		return nil
	}
	if tr.Kind() == transport.KindUDP {
		peer := transport.PeerFromContext(ctx)
		if peer == "" || udpSrv == nil {
			return nil
		}
		return &session.Conn{
			Kind: string(tr.Kind()),
			Ref:  "udp:" + peer,
			Send: func(operation string, payload []byte) error {
				return udpSrv.PushToRaw(peer, operation, payload)
			},
		}
	}
	connID := transport.ConnIDFromContext(ctx)
	if connID == 0 {
		return nil
	}
	pusher, ok := pushers[tr.Kind()]
	if !ok {
		return nil
	}
	return &session.Conn{
		ID:   connID,
		Kind: string(tr.Kind()),
		Ref:  connRef(tr.Kind(), connID),
		Send: func(operation string, payload []byte) error {
			return pusher.PushRaw(connID, operation, payload)
		},
	}
}

// pushServer 是传输 Server 的推送能力子集（四协议通用，ADR-0002）。
type pushServer interface {
	PushRaw(connID uint64, operation string, payload []byte) error
}

// Gateway 是 gateway 的会话生命周期 handler：实现 gateway.v1.SessionServer
// （Register/Login/Resume/Logout/Heartbeat），业务 op 走透传引擎（relay.go）。
type Gateway struct {
	instanceID string
	sess       *session.Manager
	actors     *actorclient.Client
	players    *gamev1.PlayerServiceClusterClient // 生成的玩家域集群互调 client
	nc         *nats.Conn
	pushers    map[transport.Kind]pushServer
	udpSrv     *udpt.Server // UDP 按 peer 寻址（无 connID 语义）
	relay      *Relay       // 业务 op 透传引擎（表由注解生成，见 relay.go）
}

// onSessionsExpired 异常下线联动撮合域（会话过期清扫回调）：
// 对「确认属主且未被接管」的过期会话，向 game PlayerActor 发 Logout（SESSION_EXPIRED）
// → 触发现有 OnStop 联动（取消匹配 / 离队 / 下线落库），登出与断线语义归一。
func (g *Gateway) onSessionsExpired(swept []session.SweptSession) {
	ctx, cancel := context.WithTimeout(context.Background(), relayTimeout)
	defer cancel()
	for _, s := range swept {
		_ = g.players.Logout(ctx, playerPID(s.PlayerID), &gamev1.LogoutMsg{
			Reason: gamev1.LogoutReason_LOGOUT_REASON_SESSION_EXPIRED,
		})
	}
}

// NewGateway 构造统一 handler 并装配透传依赖。
func NewGateway(
	instanceID string,
	table relay.Table,
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
		players:    gamev1.NewPlayerServiceClusterClient(actors.PlayerInvoker()),
		nc:         nc,
		pushers: map[transport.Kind]pushServer{
			transport.KindTCP:       tcpSrv,
			transport.KindWebSocket: wsSrv,
			transport.KindKCP:       kcpSrv,
		},
		udpSrv: udpSrv,
	}
	// 透传引擎与 Gateway 共用连接摘取与会话管理器。
	g.relay = NewRelay(table, sess, actors, func(ctx context.Context) *session.Conn {
		return connFrom(ctx, g.pushers, g.udpSrv)
	})
	// 会话过期联动撮合域（异常下线兜底）。
	g.sess.SetSweptHook(g.onSessionsExpired)
	return g
}

// Relay 暴露透传引擎（注册与测试用）。
func (g *Gateway) Relay() *Relay { return g.relay }

// connFrom 从请求上下文提取连接寻址信息（connID + 传输种类 + 回写函数）。
// UDP 无 connID 语义，按 peer 地址寻址。Ref 为连接身份键（会话反向索引用）。
func (g *Gateway) connFrom(ctx context.Context) *session.Conn {
	return connFrom(ctx, g.pushers, g.udpSrv)
}

// Register 注册：经 actor 转发 game PlayerService（懒激活），创建玩家数据并回执。
// 注册不建立会话（客户端仍需 Login）。
func (g *Gateway) Register(ctx context.Context, req *gatewayv1.RegisterRequest) (*gatewayv1.RegisterReply, error) {
	if req.GetAccount() == "" || req.GetPassword() == "" {
		return nil, errorv1.ErrInvalidParams("账号与口令不能为空")
	}
	pid, err := actorclient.PlayerPID(req.GetAccount())
	if err != nil {
		return nil, err
	}
	reply, err := g.players.Register(ctx, pid, &gamev1.RegisterReq{
		Account:  req.GetAccount(),
		Password: req.GetPassword(),
		Nickname: req.GetNickname(),
	})
	if err != nil {
		return nil, err // 业务错误透传：code/reason 经集群 error 通道往返保留
	}
	return &gatewayv1.RegisterReply{PlayerId: reply.GetPlayerId()}, nil
}

// Login 登录：签发会话凭据 → game PlayerService 裁决（密码校验）→ 建立网关会话
// （绑定通道 + 路由登记）→ 触发旧会话挤下线。token 只在本回执出现一次，
// 后续业务请求经连接绑定或帧会话槽承载身份。
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
	pid, err := actorclient.PlayerPID(playerID)
	if err != nil {
		return nil, err
	}
	reply, err := g.players.Login(ctx, pid, &gamev1.LoginReq{
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

// Resume 断线重连恢复会话：凭据校验 → 重绑当前连接（免密）。
// 凭据沿旧会话沿用（不轮换）；校验失败拒绝（旧会话可能已过期或被接管）。
func (g *Gateway) Resume(ctx context.Context, req *gatewayv1.ResumeRequest) (*gatewayv1.ResumeReply, error) {
	conn := g.connFrom(ctx)
	if conn == nil {
		return nil, errorv1.ErrInvalidToken("请求缺少连接上下文")
	}
	playerID := req.GetPlayerId()
	if playerID == "" || req.GetToken() == "" {
		return nil, errorv1.ErrInvalidParams("恢复凭据与玩家 ID 不能为空")
	}
	sess, ok := g.sess.LocalSession(playerID)
	if !ok || sess.Token != req.GetToken() {
		return nil, errorv1.ErrInvalidToken("会话凭据无效或已被接管")
	}
	if _, err := g.sess.Bind(ctx, playerID, conn, session.ChannelBiz, req.GetToken()); err != nil {
		return nil, errorv1.ErrInternal("会话恢复绑定失败")
	}
	return &gatewayv1.ResumeReply{PlayerId: playerID}, nil
}

// Logout 登出：按连接反查玩家身份，清理会话与路由并联动 game 停止。
func (g *Gateway) Logout(ctx context.Context, req *gatewayv1.LogoutRequest) (*gatewayv1.LogoutReply, error) {
	playerID, err := g.playerFromConn(ctx)
	if err != nil {
		return nil, err
	}
	if conn := g.connFrom(ctx); conn != nil {
		g.sess.Unbind(ctx, playerID, conn.ID)
	}
	// 联动 game PlayerActor：保存并停止自身（异步投递）。
	if pid, perr := actorclient.PlayerPID(playerID); perr == nil {
		_ = g.players.Logout(ctx, pid, &gamev1.LogoutMsg{Reason: gamev1.LogoutReason_LOGOUT_REASON_LOGOUT})
	}
	return &gatewayv1.LogoutReply{}, nil
}

// Heartbeat 心跳：续租会话 TTL 并对时（身份由连接承载）。
func (g *Gateway) Heartbeat(ctx context.Context, req *gatewayv1.HeartbeatRequest) (*gatewayv1.HeartbeatReply, error) {
	playerID, err := g.playerFromConn(ctx)
	if err != nil {
		return nil, err
	}
	// 身份由连接承载：以本地会话自身凭据续租（客户端无需再传令牌）。
	sess, ok := g.sess.LocalSession(playerID)
	if !ok || !g.sess.Heartbeat(ctx, playerID, sess.Token) {
		return nil, errorv1.ErrInvalidToken("会话不存在或已过期")
	}
	return &gatewayv1.HeartbeatReply{
		ServerTimeUnixMs: uint64(time.Now().UnixMilli()),
	}, nil
}

// playerFromConn 按请求连接反查会话绑定的玩家身份（帧上行鉴权）。
func (g *Gateway) playerFromConn(ctx context.Context) (string, error) {
	conn := g.connFrom(ctx)
	if conn == nil {
		return "", errorv1.ErrInvalidToken("请求缺少连接上下文")
	}
	playerID, ok := g.sess.PlayerByRef(conn.Ref)
	if !ok {
		return "", errorv1.ErrInvalidToken("通道未绑定玩家身份")
	}
	return playerID, nil
}

// kickOld 处理挤下线：新登录覆盖旧路由后，本实例旧连接直接推送被挤下线通知；
// 跨实例经 nats 定向控制通道通知旧实例。接管即联动撮合域清理被挤会话的
// 匹配/组队在线态（新会话以干净状态起步），失败仅忽略（幂等 no-op）。
func (g *Gateway) kickOld(ctx context.Context, playerID string, old *session.Route, oldSess *session.Session) {
	if old == nil {
		return
	}
	pid, err := actorclient.PlayerPID(playerID)
	if err == nil {
		_, _ = g.players.CancelMatch(ctx, pid, &gamev1.CancelMatchReq{})
		_, _ = g.players.LeaveParty(ctx, pid, &gamev1.LeavePartyReq{})
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
