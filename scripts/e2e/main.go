// e2e 客户端脚本：双客户端走通「注册 → 登录 → 匹配 → 战斗 → 结算」闭环。
//
// 双形态（-mode）：
//   - dual（默认）：TCP 业务通道 + KCP 战斗通道（原生双通道）
//   - single：WS 单通道（业务与战斗共用一条连接）
//
// 匹配经 matcher gRPC 直连（v1 简化：客户端直连撮合服务入队）；
// 成局后 gateway 向参战玩家推送开局通知（MatchStartedNotify，含 battle ID），
// 客户端据此加入战斗、发送帧输入，直至收到双方一致的战斗结束通知。
//
// 用法：go run ./scripts/e2e -mode dual
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	kcpt "github.com/huangyuCN/atlas/transport/kcp"
	tcpt "github.com/huangyuCN/atlas/transport/tcp"
	wst "github.com/huangyuCN/atlas/transport/websocket"
	"google.golang.org/protobuf/encoding/protojson"
)

// authAPI 是认证协议客户端最小接口（TCP/WS 生成客户端满足）。
type authAPI interface {
	Register(context.Context, *gatewayv1.RegisterRequest) (*gatewayv1.RegisterReply, error)
	Login(context.Context, *gatewayv1.LoginRequest) (*gatewayv1.LoginReply, error)
	Heartbeat(context.Context, *gatewayv1.HeartbeatRequest) (*gatewayv1.HeartbeatReply, error)
	Logout(context.Context, *gatewayv1.LogoutRequest) (*gatewayv1.LogoutReply, error)
}

// battleAPI 是战斗协议客户端最小接口（KCP/WS 生成客户端满足）。
type battleAPI interface {
	JoinBattle(context.Context, *gatewayv1.JoinBattleRequest) (*gatewayv1.JoinBattleReply, error)
	SendFrameInput(context.Context, *gatewayv1.SendFrameInputRequest) (*gatewayv1.SendFrameInputReply, error)
}

// player 是一端客户端：认证通道 + 战斗通道 + 推送收集。
type player struct {
	id     string
	token  string
	auth   authAPI
	battle battleAPI

	started chan *gatewayv1.MatchStartedNotify
	end     chan *gatewayv1.BattleEndNotify
	frames  atomic.Int64 // 收到的帧广播数
}

// watch 挂接推送监听（开局通知/帧广播/结束通知）。
func (p *player) watch(operation string, payload []byte) {
	switch operation {
	case "gateway.v1.MatchStartedNotify":
		var n gatewayv1.MatchStartedNotify
		if err := protojson.Unmarshal(payload, &n); err == nil {
			select {
			case p.started <- &n:
			default:
			}
		}
	case "gateway.v1.FrameBroadcast":
		p.frames.Add(1)
	case "gateway.v1.BattleEndNotify":
		var n gatewayv1.BattleEndNotify
		if err := protojson.Unmarshal(payload, &n); err == nil {
			select {
			case p.end <- &n:
			default:
			}
		}
	}
}

// newPlayer 构造玩家骨架。
func newPlayer(id string) *player {
	return &player{
		id:      id,
		started: make(chan *gatewayv1.MatchStartedNotify, 2),
		end:     make(chan *gatewayv1.BattleEndNotify, 2),
	}
}

// newDualPlayer 建立双通道客户端（TCP 业务 + KCP 战斗）。
func newDualPlayer(ctx context.Context, tcpAddr, kcpAddr string) (*player, error) {
	tcpCli, err := tcpt.NewClient(tcpAddr)
	if err != nil {
		return nil, fmt.Errorf("tcp 连接失败: %w", err)
	}
	kcpCli, err := kcpt.NewClient(kcpAddr)
	if err != nil {
		_ = tcpCli.Close()
		return nil, fmt.Errorf("kcp 连接失败: %w", err)
	}
	p := newPlayer("")
	p.auth = gatewayv1.NewGatewayAuthTCPClient(tcpCli)
	p.battle = gatewayv1.NewGatewayBattleKCPClient(kcpCli)
	// 开局通知在绑定战斗通道前走业务通道（回退），帧广播走战斗通道。
	tcpCli.OnNotify(p.watch)
	kcpCli.OnNotify(p.watch)
	return p, nil
}

// newSinglePlayer 建立单通道客户端（WS 业务+战斗共用）。
func newSinglePlayer(ctx context.Context, wsAddr string) (*player, error) {
	wsCli, err := wst.NewClient(ctx, wsAddr)
	if err != nil {
		return nil, fmt.Errorf("ws 连接失败: %w", err)
	}
	p := newPlayer("")
	p.auth = gatewayv1.NewGatewayAuthWSClient(wsCli)
	p.battle = gatewayv1.NewGatewayBattleWSClient(wsCli)
	wsCli.OnNotify(p.watch)
	return p, nil
}

// registerLogin 注册并登录（账号按时间戳唯一，可重复执行）。
func (p *player) registerLogin(ctx context.Context, step byte) error {
	account := fmt.Sprintf("e2e-%s-%d", p.id, time.Now().UnixMilli())
	reg, err := p.auth.Register(ctx, &gatewayv1.RegisterRequest{Account: account, Password: "pw", Nickname: account})
	if err != nil {
		return fmt.Errorf("注册失败: %w", err)
	}
	login, err := p.auth.Login(ctx, &gatewayv1.LoginRequest{PlayerId: reg.GetPlayerId(), Password: "pw"})
	if err != nil {
		return fmt.Errorf("登录失败: %w", err)
	}
	p.id = login.GetPlayerId()
	p.token = login.GetToken()
	fmt.Printf("[%c] 注册+登录 ok（player=%s）\n", 'A'+step, p.id)
	return nil
}

// queueMatch 经 matcher gRPC 入队（等级相近 1v1）。
func queueMatch(ctx context.Context, matcherAddr string, ps ...*player) error {
	conn, err := atlasgrpc.DialInsecure(ctx, atlasgrpc.WithEndpoint(matcherAddr))
	if err != nil {
		return fmt.Errorf("matcher 连接失败: %w", err)
	}
	defer conn.Close()
	svc := matcherv1.NewMatcherClient(conn)
	for i, p := range ps {
		if _, err := svc.QueueMatch(ctx, &matcherv1.QueueMatchRequest{
			PlayerId: p.id,
			Player:   &commonv1.PlayerSummary{PlayerId: p.id, Level: int32(10 + i)},
		}); err != nil {
			return fmt.Errorf("%s 入队失败: %w", p.id, err)
		}
	}
	return nil
}

// waitStarted 等待双方开局通知并核对 battle ID 一致。
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
		join, err := p.battle.JoinBattle(ctx, &gatewayv1.JoinBattleRequest{
			Token: p.token, PlayerId: p.id, BattleId: battleID,
		})
		if err == nil && join.GetOk() {
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

// sendInputs 发送 [1,frames] 帧输入（payload 单字节步进值）。
func (p *player) sendInputs(ctx context.Context, battleID string, frames uint64, step byte) error {
	for i := uint64(1); i <= frames; i++ {
		if _, err := p.battle.SendFrameInput(ctx, &gatewayv1.SendFrameInputRequest{
			BattleId: battleID,
			Input:    &locksteppb.LockstepInput{FrameId: i, PlayerId: p.id, Payload: []byte{step}},
		}); err != nil {
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

// run 执行闭环；返回错误则脚本非零退出。
func run(ctx context.Context, mode, tcpAddr, kcpAddr, wsAddr, matcherAddr string, frames uint64) error {
	var ps [2]*player
	var err error
	if mode == "single" {
		for i := range ps {
			if ps[i], err = newSinglePlayer(ctx, wsAddr); err != nil {
				return err
			}
		}
	} else {
		for i := range ps {
			if ps[i], err = newDualPlayer(ctx, tcpAddr, kcpAddr); err != nil {
				return err
			}
		}
	}

	for i, p := range ps {
		if err := p.registerLogin(ctx, byte(i)); err != nil {
			return err
		}
	}
	fmt.Println("[匹配] 双玩家入队（等级相近）")
	if err := queueMatch(ctx, matcherAddr, ps[0], ps[1]); err != nil {
		return err
	}
	battleID, err := waitStarted(ps[0], ps[1])
	if err != nil {
		return err
	}
	fmt.Printf("[开局] battle=%s\n", battleID)
	for _, p := range ps {
		if err := p.joinWithRetry(ctx, battleID); err != nil {
			return err
		}
	}
	// A 每帧前进 1（率先到终点），B 原地不动。
	if err := ps[0].sendInputs(ctx, battleID, frames, 1); err != nil {
		return err
	}
	if err := ps[1].sendInputs(ctx, battleID, frames, 0); err != nil {
		return err
	}
	winnerA, err := ps[0].waitEnd()
	if err != nil {
		return err
	}
	winnerB, err := ps[1].waitEnd()
	if err != nil {
		return err
	}
	fmt.Printf("[结算] 帧广播 A=%d B=%d\n", ps[0].frames.Load(), ps[1].frames.Load())
	if winnerA != ps[0].id || winnerB != ps[0].id {
		return fmt.Errorf("双方结局不一致: A=%q B=%q want %q", winnerA, winnerB, ps[0].id)
	}
	fmt.Printf("[结算] 胜者一致: %s\n", winnerA)
	return nil
}

func main() {
	mode := flag.String("mode", "dual", "客户端形态：dual（TCP+KCP 双通道）/ single（WS 单通道）")
	tcpAddr := flag.String("gw", "127.0.0.1:9001", "gateway TCP 地址")
	kcpAddr := flag.String("kcp", "127.0.0.1:9003", "gateway KCP 地址")
	wsAddr := flag.String("ws", "ws://127.0.0.1:9002", "gateway WS 地址")
	matcherAddr := flag.String("matcher", "127.0.0.1:9200", "matcher gRPC 地址")
	frames := flag.Uint64("frames", 20, "每客户端帧输入数")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := run(ctx, *mode, *tcpAddr, *kcpAddr, *wsAddr, *matcherAddr, *frames); err != nil {
		fmt.Fprintf(os.Stderr, "e2e 失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("e2e 闭环通过（%s 形态）\n", *mode)
}
