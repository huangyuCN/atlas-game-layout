// player.go 是 e2e 脚本的 SDK 客户端驱动层：会话（Session）+ 通道（Client）装配、
// 域强类型 stub、服务端推送分发与登录/入队/战斗的公共步骤。
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	sdkclient "github.com/huangyuCN/atlas-sdk-go/client"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"google.golang.org/protobuf/encoding/protojson"
)

// pushOps 是客户端订阅的服务端推送 op 集（op 为消息完整名）。
var pushOps = []string{
	consts.PushOpMatchStarted,
	consts.PushOpMatchFailed,
	consts.PushOpPartyRoster,
	consts.PushOpFrameBroadcast,
	consts.PushOpBattleEnd,
	consts.PushOpKickedOffline,
}

// player 是一端客户端：SDK 会话 + 通道 + 域强类型 stub + 推送收集。
type player struct {
	tag     string // 客户端标签（账号前缀，与登录回执的 id 区分）
	id      string // 玩家 ID（登录回执）
	sess    *sdkclient.Session
	cli     *sdkclient.Client
	players *gamev1.PlayerServiceClient   // 玩家域 op（业务通道，会话承载身份）
	battle  *battlev1.BattleServiceClient // 战斗域 op（dual 战斗通道视图 / 单通道复用业务通道）

	started chan *gamev1.MatchStartedNotify
	end     chan *battlev1.BattleEndNotify
	ros     chan *gamev1.PartyRosterNotify
	failed  chan *gamev1.MatchFailedNotify
	frames  atomic.Int64 // 收到的帧广播数
}

// newSession 构造 SDK 会话管理器：内置会话心跳（10s，无载荷——服务端按
// 连接/帧槽定位会话续租，空载荷已由框架按零值消息解码）。
func newSession() *sdkclient.Session {
	return sdkclient.NewSession(sdkclient.WithSessionHeartbeatInterval(10 * time.Second))
}

// commonDialOpts 客户端公共拨号参数：传输保活心跳（会话心跳由 Session 内置承担）。
func commonDialOpts(sess *sdkclient.Session) []sdkclient.Option {
	return []sdkclient.Option{
		sdkclient.WithHeartbeatInterval(5 * time.Second),
	}
}

// battleOpts 战斗通道选项：帧会话槽凭据提供者（KCP/UDP 每帧验证身份）。
func battleOpts(sess *sdkclient.Session) []sdkclient.Option {
	return []sdkclient.Option{
		sdkclient.WithSessionTokenProvider(func() string { return sess.Token() }),
	}
}

// newPlayer 构造玩家骨架：会话/通道绑定 + 域 stub + 推送监听。
// 玩家域恒走业务通道；战斗域 dual 形态走战斗通道视图，单通道形态复用业务通道。
func newPlayer(tag string, sess *sdkclient.Session, cli *sdkclient.Client) *player {
	p := &player{
		tag:     tag,
		sess:    sess,
		cli:     cli,
		started: make(chan *gamev1.MatchStartedNotify, 4),
		end:     make(chan *battlev1.BattleEndNotify, 4),
		ros:     make(chan *gamev1.PartyRosterNotify, 4),
		failed:  make(chan *gamev1.MatchFailedNotify, 4),
	}
	p.players = gamev1.NewPlayerServiceClient(sess)
	if bv := cli.Channel(sdkclient.KindBattle); bv != nil {
		p.battle = battlev1.NewBattleServiceClient(bv)
	} else {
		p.battle = battlev1.NewBattleServiceClient(cli)
	}
	p.watchNotifies()
	return p
}

// newDualPlayer 建立双通道客户端（SDK dual：TCP 业务 + KCP 战斗；
// extra 为形态级覆盖，如容错场景关闭自动重连）。
func newDualPlayer(tcpAddr, kcpAddr string, extra ...sdkclient.Option) (*player, error) {
	sess := newSession()
	cli, err := sdkclient.DialDual(
		sdkclient.ChannelConfig{Addr: tcpAddr, Opts: sess.ChannelOptions()},
		sdkclient.ChannelConfig{
			Transport: sdkclient.TransportKCP,
			Addr:      kcpAddr,
			Opts:      battleOpts(sess),
		},
		append(commonDialOpts(sess), extra...)...,
	)
	if err != nil {
		return nil, fmt.Errorf("dual 连接失败: %w", err)
	}
	sess.Bind(cli)
	return newPlayer(fmt.Sprintf("dual-%d", time.Now().UnixNano()%1000), sess, cli), nil
}

// newSinglePlayer 建立单通道客户端（WS 业务+战斗共用一条连接）。
func newSinglePlayer(wsURL string, extra ...sdkclient.Option) (*player, error) {
	sess := newSession()
	opts := append(append(commonDialOpts(sess), sess.ChannelOptions()...), extra...)
	cli, err := sdkclient.DialWS(wsURL, "", opts...)
	if err != nil {
		return nil, fmt.Errorf("ws 连接失败: %w", err)
	}
	sess.Bind(cli)
	return newPlayer(fmt.Sprintf("single-%d", time.Now().UnixNano()%1000), sess, cli), nil
}

// newBizPlayer 建立仅业务通道客户端（容错/顶号场景的轻量形态）。
func newBizPlayer(tcpAddr string, extra ...sdkclient.Option) (*player, error) {
	sess := newSession()
	opts := append(append(commonDialOpts(sess), sess.ChannelOptions()...), extra...)
	cli, err := sdkclient.Dial(tcpAddr, opts...)
	if err != nil {
		return nil, fmt.Errorf("tcp 连接失败: %w", err)
	}
	sess.Bind(cli)
	return newPlayer(fmt.Sprintf("biz-%d", time.Now().UnixNano()%1000), sess, cli), nil
}

// watchNotifies 在全部通道挂接推送监听：开局通知先于战斗通道绑定、走业务通道回退，
// 帧广播走战斗通道（推送只落一条通道，双通道订阅不会重复计数）。
func (p *player) watchNotifies() {
	for _, op := range pushOps {
		p.cli.On(op, p.watch)
	}
	if bv := p.cli.Channel(sdkclient.KindBattle); bv != nil {
		for _, op := range pushOps {
			bv.On(op, p.watch)
		}
	}
}

// watch 分发服务端推送：成局/失败/名册/结束 → 信号通道，帧广播 → 计数。
func (p *player) watch(operation string, payload []byte) {
	switch operation {
	case consts.PushOpMatchStarted:
		var n gamev1.MatchStartedNotify
		if protojson.Unmarshal(payload, &n) == nil {
			push(p.started, &n)
		}
	case consts.PushOpMatchFailed:
		var n gamev1.MatchFailedNotify
		if protojson.Unmarshal(payload, &n) == nil {
			push(p.failed, &n)
		}
	case consts.PushOpPartyRoster:
		var n gamev1.PartyRosterNotify
		if protojson.Unmarshal(payload, &n) == nil {
			push(p.ros, &n)
		}
	case consts.PushOpFrameBroadcast:
		p.frames.Add(1)
	case consts.PushOpBattleEnd:
		var n battlev1.BattleEndNotify
		if protojson.Unmarshal(payload, &n) == nil {
			push(p.end, &n)
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

// disconnect 断开底层全部通道连接（模拟杀进程：不发 Logout，服务端经会话过期联动清理）。
func (p *player) disconnect() error {
	return p.cli.Close()
}

// registerLogin 注册并登录（账号按时间戳唯一，可重复执行），随后全量数据同步
// （GetPlayerData：摘要 + 背包，登录轻回执的按需补充）。
func (p *player) registerLogin(ctx context.Context, step byte) error {
	account := fmt.Sprintf("e2e-%s-%d", p.tag, time.Now().UnixNano())
	reg, err := p.sess.Register(ctx, &gatewayv1.RegisterRequest{Account: account, Password: "pw", Nickname: account})
	if err != nil {
		return fmt.Errorf("注册失败: %w", err)
	}
	if _, err := p.sess.Login(ctx, &gatewayv1.LoginRequest{PlayerId: reg.PlayerID, Password: "pw"}); err != nil {
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

// waitStarted 等待各方开局通知并核对 battle ID 一致。
func waitStarted(ps ...*player) (string, error) {
	var battleID string
	for _, p := range ps {
		select {
		case n := <-p.started:
			if n.GetBattleId() == "" {
				return "", fmt.Errorf("%s 开局通知缺 battle_id", p.id)
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

// joinWithRetry 加入战斗（battle actor 懒激活期间重试）。
func (p *player) joinWithRetry(ctx context.Context, battleID string) error {
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
// SDK Invoke 空回执即返回）。
func (p *player) sendInputs(ctx context.Context, battleID string, frames uint64, step byte) error {
	for i := uint64(1); i <= frames; i++ {
		req := &battlev1.FrameInputReq{
			BattleId: battleID,
			Input:    &locksteppb.LockstepInput{FrameId: i, PlayerId: p.id, Payload: []byte{step}},
		}
		if err := p.battle.SendFrameInput(ctx, req); err != nil {
			return fmt.Errorf("%s 帧输入 %d 失败: %w", p.id, i, err)
		}
	}
	return nil
}

// waitEnd 等待战斗结束通知并返回胜者。
func (p *player) waitEnd() (string, error) {
	select {
	case n := <-p.end:
		return n.GetWinnerPlayerId(), nil
	case <-time.After(20 * time.Second):
		return "", fmt.Errorf("%s 未收到战斗结束通知", p.id)
	}
}
