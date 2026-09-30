// player.go 是 e2e 脚本的 SDK 客户端驱动层：会话（Session）+ 业务通道（Client）装配、
// 域强类型 stub、服务端推送分发与登录/入队/战斗的公共步骤。
//
// 阶段 3 批次 5 起战斗帧不经网关：客户端凭成局通知里的 battle_ticket **直连 battle 帧面**
// （框架传输客户端 + 帧槽票据，见 dialBattle），战斗域通知（帧广播/战斗结束）也由直连推送到达。
package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-game-layout/api/battle/v1/opclient"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gamev1opclient "github.com/huangyuCN/atlas-game-layout/api/game/v1/opclient"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	gatewayv1opclient "github.com/huangyuCN/atlas-game-layout/api/gateway/v1/opclient"
	"github.com/huangyuCN/atlas-game-layout/scripts/sdksession"
	sdkclient "github.com/huangyuCN/atlas-sdk-go/client"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"google.golang.org/protobuf/encoding/protojson"
)

// bizPushOps 是**业务连接**上订阅的服务端推送 op 集（战斗域通知改走直连帧面，见 dialBattle）。
var bizPushOps = []string{
	gamev1opclient.PlayerServicePushOps.MatchStartedNotify,
	gamev1opclient.PlayerServicePushOps.MatchFailedNotify,
	gamev1opclient.PlayerServicePushOps.PartyRosterNotify,
	gatewayv1opclient.SessionPushOps.KickedNotify,
}

// player 是一端客户端：SDK 会话 + 业务通道 + 域强类型 stub + 推送收集。
// 战斗域 op 的底层是**直连帧连接**（成局拿到本人票据后建立，见 dialBattle）。
type player struct {
	tag     string // 客户端标签（账号前缀，与登录回执的 id 区分）
	id      string // 玩家 ID（登录回执）
	sess    *sdkclient.Session
	cli     *sdkclient.Client
	players *gamev1opclient.PlayerService   // 玩家域 op（业务通道，会话承载身份）
	battle  *battlev1opclient.BattleService // 战斗域 op（直连帧面，dialBattle 后非 nil）

	frameKind directTransport // 本玩家的直连传输面（dual=kcp / single=ws）
	frameAddr string          // battle 帧面地址（本局通知不携带，进程内形态由装配给出）
	ticket    []byte          // 本人入场票据（成局通知逐人下发）
	frame     *directClient   // 直连帧连接（推送与调用都在它上面）

	started chan *gamev1.MatchStartedNotify
	ros     chan *gamev1.PartyRosterNotify
	failed  chan *gamev1.MatchFailedNotify
}

// newSession 构造 SDK 会话管理器：会话协议接缝取自模板生成描述符（scripts/sdksession），
// 内置会话心跳（10s，无载荷——服务端按连接/帧槽定位会话续租，空载荷已由框架按零值消息解码）。
// opts 是会话级选项（如 WithOnKicked：被挤下线原因经接缝按帧头版本解码，见 S0.5 修订 1）。
func newSession(opts ...sdkclient.SessionOption) *sdkclient.Session {
	return sdksession.NewSession(append([]sdkclient.SessionOption{
		sdkclient.WithSessionHeartbeatInterval(10 * time.Second),
	}, opts...)...)
}

// commonDialOpts 客户端公共拨号参数：传输保活心跳（会话心跳由 Session 内置承担）。
func commonDialOpts(sess *sdkclient.Session) []sdkclient.Option {
	return []sdkclient.Option{
		sdkclient.WithHeartbeatInterval(5 * time.Second),
	}
}

// frameInvoker 把直连帧客户端适配为 SDK 的 Invoker 接缝（生成 stub 只认四件 Invoke）。
type frameInvoker struct {
	invoke func(ctx context.Context, operation string, req, resp any) error
}

// Invoke 实现 sdkclient.Invoker（单次调用选项在此剥掉：直连帧面无额外调用选项）。
func (f frameInvoker) Invoke(ctx context.Context, operation string, req, resp any, _ ...sdkclient.InvokeOption) error {
	return f.invoke(ctx, operation, req, resp)
}

// newPlayer 构造玩家骨架：会话/业务通道绑定 + 域 stub + 业务推送监听。
// 战斗域 stub 在 dialBattle（成局拿票）后建立——战斗帧不再经业务连接。
func newPlayer(tag string, sess *sdkclient.Session, cli *sdkclient.Client, frameAddr string, kind directTransport) *player {
	p := &player{
		tag:       tag,
		sess:      sess,
		cli:       cli,
		frameAddr: frameAddr,
		frameKind: kind,
		started:   make(chan *gamev1.MatchStartedNotify, 4),
		ros:       make(chan *gamev1.PartyRosterNotify, 4),
		failed:    make(chan *gamev1.MatchFailedNotify, 4),
	}
	p.players = gamev1opclient.NewPlayerService(sess)
	p.watchNotifies()
	return p
}

// newDualPlayer 建立双通道客户端（TCP 业务通道 + KCP 直连帧面；
// extra 为形态级覆盖，如容错场景关闭自动重连）。
func newDualPlayer(tcpAddr, frameAddr string, extra ...sdkclient.Option) (*player, error) {
	sess := newSession()
	opts := append(append(commonDialOpts(sess), sess.ChannelOptions()...), extra...)
	cli, err := sdkclient.Dial(tcpAddr, opts...)
	if err != nil {
		return nil, fmt.Errorf("dual 连接失败: %w", err)
	}
	if err := sess.Bind(cli); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("会话绑定失败: %w", err)
	}
	return newPlayer(fmt.Sprintf("dual-%d", time.Now().UnixNano()%1000), sess, cli, frameAddr, directKCP), nil
}

// newSinglePlayer 建立 WS 形态客户端（WS 业务通道 + WS 直连帧面）。
func newSinglePlayer(wsURL, frameAddr string, extra ...sdkclient.Option) (*player, error) {
	sess := newSession()
	opts := append(append(commonDialOpts(sess), sess.ChannelOptions()...), extra...)
	cli, err := sdkclient.DialWS(wsURL, "", opts...)
	if err != nil {
		return nil, fmt.Errorf("ws 连接失败: %w", err)
	}
	if err := sess.Bind(cli); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("会话绑定失败: %w", err)
	}
	return newPlayer(fmt.Sprintf("single-%d", time.Now().UnixNano()%1000), sess, cli, frameAddr, directWS), nil
}

// newBizPlayer 建立仅业务通道客户端（容错/顶号场景的轻量形态；不参与战斗）。
func newBizPlayer(tcpAddr string, extra ...sdkclient.Option) (*player, error) {
	return newBizPlayerWith(tcpAddr, nil, extra...)
}

// newBizPlayerWith 在 newBizPlayer 基础上追加**会话级**选项（如 WithOnKicked：被挤下线
// 原因经 SessionProtocol 接缝按帧头版本解码，见 SDK 接缝 S0.5 修订 1）。
func newBizPlayerWith(tcpAddr string, sessOpts []sdkclient.SessionOption, extra ...sdkclient.Option) (*player, error) {
	sess := newSession(sessOpts...)
	opts := append(append(commonDialOpts(sess), sess.ChannelOptions()...), extra...)
	cli, err := sdkclient.Dial(tcpAddr, opts...)
	if err != nil {
		return nil, fmt.Errorf("tcp 连接失败: %w", err)
	}
	if err := sess.Bind(cli); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("会话绑定失败: %w", err)
	}
	return newPlayer(fmt.Sprintf("biz-%d", time.Now().UnixNano()%1000), sess, cli, "", ""), nil
}

// dialBattle 以成局通知里的**本人**票据直连 battle 帧面并装配战斗域 stub（幂等）。
// 帧槽取值约定（base64url 无填充）与三传输的客户端构造复用 direct.go 的同一份实现，
// 票据在每帧上由框架客户端的会话槽提供者携带（服务端按帧验票取身份）。
func (p *player) dialBattle(ctx context.Context) error {
	if p.frame != nil {
		return nil
	}
	if len(p.ticket) == 0 {
		return fmt.Errorf("%s 没有本局票据（成局通知缺 battle_ticket）", p.id)
	}
	slot := base64.RawURLEncoding.EncodeToString(p.ticket)
	dc, err := dialDirect(ctx, p.frameKind, p.frameAddr, slot, p.id)
	if err != nil {
		return err
	}
	p.frame = dc
	p.battle = battlev1opclient.NewBattleService(frameInvoker{invoke: dc.invoke})
	return nil
}

// watchNotifies 在业务连接挂接推送监听：成局通知（含本人票据，直连入场凭据的唯一来源）、
// 匹配失败、名册变更与被挤下线。战斗域通知不在此——它们只从直连帧面到达（见 dialBattle）。
func (p *player) watchNotifies() {
	for _, op := range bizPushOps {
		p.cli.On(op, p.watch)
	}
}

// watch 分发业务推送：成局（存票）/失败/名册 → 信号通道。
func (p *player) watch(operation string, payload []byte) {
	switch operation {
	case gamev1opclient.PlayerServicePushOps.MatchStartedNotify:
		var n gamev1.MatchStartedNotify
		if protojson.Unmarshal(payload, &n) == nil {
			p.ticket = append([]byte(nil), n.GetBattleTicket()...)
			push(p.started, &n)
		}
	case gamev1opclient.PlayerServicePushOps.MatchFailedNotify:
		var n gamev1.MatchFailedNotify
		if protojson.Unmarshal(payload, &n) == nil {
			push(p.failed, &n)
		}
	case gamev1opclient.PlayerServicePushOps.PartyRosterNotify:
		var n gamev1.PartyRosterNotify
		if protojson.Unmarshal(payload, &n) == nil {
			push(p.ros, &n)
		}
	}
}

// push 非阻塞投递（缓冲通道满则丢弃新事件，信号通道容量足量）。
func push[T any](ch chan T, v T) {
	select {
	case ch <- v:
	default:
	}
}

// disconnect 断开底层全部连接（模拟杀进程：不发 Logout，服务端经会话过期联动清理）。
func (p *player) disconnect() error {
	if p.frame != nil {
		_ = p.frame.close()
	}
	return p.cli.Close()
}

// registerLogin 注册并登录（账号按时间戳唯一，可重复执行），随后全量数据同步
// （GetPlayerData：摘要 + 背包，登录轻回执的按需补充）。
func (p *player) registerLogin(ctx context.Context, step byte) error {
	account := fmt.Sprintf("e2e-%s-%d", p.tag, time.Now().UnixNano())
	rawReg, err := p.sess.Register(ctx, &gatewayv1.RegisterRequest{Account: account, Password: "pw", Nickname: account})
	if err != nil {
		return fmt.Errorf("注册失败: %w", err)
	}
	reg, err := sdksession.ReplyAs[*gatewayv1.RegisterReply](rawReg)
	if err != nil {
		return fmt.Errorf("注册回执: %w", err)
	}
	if _, err := p.sess.Login(ctx, &gatewayv1.LoginRequest{PlayerId: reg.GetPlayerId(), Password: "pw"}); err != nil {
		return fmt.Errorf("登录失败: %w", err)
	}
	p.id = p.sess.PlayerID()
	synced, err := p.players.GetPlayerData(ctx, &gamev1.GetPlayerDataReq{})
	if err != nil {
		return fmt.Errorf("数据同步失败: %w", err)
	}
	if synced.GetPlayer().GetPlayerId() != p.id {
		return fmt.Errorf("数据同步回执不符: %s != %s", synced.GetPlayer().GetPlayerId(), p.id)
	}
	fmt.Printf("[%c] 注册+登录+数据同步 ok（player=%s）\n", 'A'+step, p.id)
	return nil
}

// queueMatch 经 gateway 业务通道 op 入队（等级相近 1v1；属性由服务端权威填充）。
func queueMatch(ctx context.Context, ps ...*player) error {
	for _, p := range ps {
		if _, err := p.players.EnterMatchQueue(ctx, &gamev1.EnterMatchQueueReq{Ruleset: "casual"}); err != nil {
			return fmt.Errorf("%s 入队失败: %w", p.id, err)
		}
	}
	return nil
}

// waitStarted 等待各方开局通知并核对 battle ID 一致；顺带收取**本人**票据
// （直连入场凭据：缺票即无法直连，必须在这里显式失败而不是留到"连不上"）。
func waitStarted(ps ...*player) (string, error) {
	var battleID string
	for _, p := range ps {
		select {
		case n := <-p.started:
			if n.GetBattleId() == "" {
				return "", fmt.Errorf("%s 开局通知缺 battle_id", p.id)
			}
			if len(n.GetBattleTicket()) == 0 {
				return "", fmt.Errorf("%s 开局通知缺 battle_ticket（出票/逐人扇出未接线）", p.id)
			}
			if battleID == "" {
				battleID = n.GetBattleId()
			}
			if n.GetBattleId() != battleID {
				return "", fmt.Errorf("双方 battle ID 不一致: %q vs %q", n.GetBattleId(), battleID)
			}
		case <-time.After(10 * time.Second):
			return "", fmt.Errorf("%s 未收到开局通知", p.id)
		}
	}
	return battleID, nil
}

// joinWithRetry 直连帧面入局（battle actor 懒激活期间重试）：先建立直连（凭本人票据），
// 再调 JoinBattle。
func (p *player) joinWithRetry(ctx context.Context, battleID string) error {
	if err := p.dialBattle(ctx); err != nil {
		return err
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		join, err := p.battle.JoinBattle(ctx, &battlev1.JoinBattleReq{BattleId: battleID})
		if err == nil {
			fmt.Printf("[%s] 加入战斗 ok（当前帧 %d，快照 %v）\n",
				p.id, join.GetCurrentFrame(), join.GetSnapshot() != nil)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("加入战斗重试耗尽: err=%v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// sendInputs 发送 [1,frames] 帧输入（payload 单字节步进值；Tell 无回执，
// SDK Invoke 空回执即返回）。载荷不带 player_id：身份由帧槽票据注入。
func (p *player) sendInputs(ctx context.Context, battleID string, frames uint64, step byte) error {
	for i := uint64(1); i <= frames; i++ {
		req := &battlev1.FrameInputReq{
			BattleId: battleID,
			Input:    &locksteppb.LockstepInput{FrameId: i, Payload: []byte{step}},
		}
		if err := p.battle.SendFrameInput(ctx, req); err != nil {
			return fmt.Errorf("%s 帧输入 %d 失败: %w", p.id, i, err)
		}
	}
	return nil
}

// waitEnd 等待**直连帧面**上的战斗结束通知并返回胜者。
func (p *player) waitEnd() (string, error) {
	if p.frame == nil {
		return "", fmt.Errorf("%s 未建立直连帧连接", p.id)
	}
	select {
	case n := <-p.frame.watcher.end:
		return n.GetWinnerPlayerId(), nil
	case <-time.After(20 * time.Second):
		return "", fmt.Errorf("%s 未收到战斗结束通知", p.id)
	}
}

// frameCount 返回直连帧面上收到的帧广播数（结算打印/断言用）。
func (p *player) frameCount() int64 {
	if p.frame == nil {
		return 0
	}
	return p.frame.watcher.frames.Load()
}
