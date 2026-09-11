// e2e 客户端脚本：双客户端走通「注册 → 登录 → 匹配 → 战斗 → 结算」闭环。
//
// 双形态（-mode）：
//   - dual（默认）：TCP 业务通道 + KCP 战斗通道（原生双通道）
//   - single：WS 单通道（业务与战斗共用一条连接）
//   - party：4 客户端组队 2v2（A 建队拉 B 整队入队，C/D solo）+ 取消路径
//   - fault：异常下线容错（排队中断连 → 会话过期联动取消 → 重登状态归零）
//
// 匹配经 gateway 业务通道 op 入队（gateway → game PlayerActor → matcher）；
// 成局/失败由 gateway 主动推送（MatchStartedNotify / MatchFailedNotify），
// 客户端据开局通知加入战斗、发送帧输入，直至收到双方一致的战斗结束通知。
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

	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
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

// matchAPI 是匹配/组队协议客户端最小接口（TCP/WS 生成客户端满足）。
type matchAPI interface {
	QueueMatch(context.Context, *gatewayv1.MatchQueueRequest) (*gatewayv1.MatchQueueReply, error)
	CancelMatch(context.Context, *gatewayv1.MatchCancelRequest) (*gatewayv1.MatchCancelReply, error)
	MatchStatus(context.Context, *gatewayv1.MatchStatusRequest) (*gatewayv1.MatchStatusReply, error)
	PartyCreate(context.Context, *gatewayv1.PartyCreateRequest) (*gatewayv1.PartyInfoReply, error)
	PartyJoin(context.Context, *gatewayv1.PartyJoinRequest) (*gatewayv1.PartyInfoReply, error)
	PartyLeave(context.Context, *gatewayv1.PartyLeaveRequest) (*gatewayv1.PartyInfoReply, error)
	PartyQueue(context.Context, *gatewayv1.PartyQueueRequest) (*gatewayv1.PartyQueueReply, error)
}

// battleAPI 是战斗协议客户端最小接口（KCP/WS 生成客户端满足）。
type battleAPI interface {
	JoinBattle(context.Context, *gatewayv1.JoinBattleRequest) (*gatewayv1.JoinBattleReply, error)
	SendFrameInput(context.Context, *gatewayv1.SendFrameInputRequest) (*gatewayv1.SendFrameInputReply, error)
}

// closer 是可关闭连接的最小接口（模拟杀进程用）。
type closer interface {
	Close() error
}

// player 是一端客户端：认证通道 + 战斗通道 + 推送收集。
type player struct {
	id     string
	token  string
	auth   authAPI
	match  matchAPI
	battle battleAPI
	conns  []closer // 底层连接（disconnect 模拟杀进程用）

	started chan *gatewayv1.MatchStartedNotify
	end     chan *gatewayv1.BattleEndNotify
	ros     chan *gatewayv1.PartyRosterNotify
	failed  chan *gatewayv1.MatchFailedNotify
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
	case "gateway.v1.PartyRosterNotify":
		var n gatewayv1.PartyRosterNotify
		if err := protojson.Unmarshal(payload, &n); err == nil {
			select {
			case p.ros <- &n:
			default:
			}
		}
	case "gateway.v1.MatchFailedNotify":
		var n gatewayv1.MatchFailedNotify
		if err := protojson.Unmarshal(payload, &n); err == nil {
			select {
			case p.failed <- &n:
			default:
			}
		}
	}
}

// newPlayer 构造玩家骨架。
func newPlayer(id string) *player {
	return &player{
		id:      id,
		started: make(chan *gatewayv1.MatchStartedNotify, 4),
		end:     make(chan *gatewayv1.BattleEndNotify, 4),
		ros:     make(chan *gatewayv1.PartyRosterNotify, 4),
		failed:  make(chan *gatewayv1.MatchFailedNotify, 4),
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
	p.conns = append(p.conns, tcpCli, kcpCli)
	p.auth = gatewayv1.NewGatewayAuthTCPClient(tcpCli)
	p.match = gatewayv1.NewGatewayMatchTCPClient(tcpCli)
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
	p.conns = append(p.conns, wsCli)
	p.auth = gatewayv1.NewGatewayAuthWSClient(wsCli)
	p.match = gatewayv1.NewGatewayMatchWSClient(wsCli)
	p.battle = gatewayv1.NewGatewayBattleWSClient(wsCli)
	wsCli.OnNotify(p.watch)
	return p, nil
}

// disconnect 断开底层连接（模拟杀进程：不发 Logout，服务端经会话过期联动清理）。
func (p *player) disconnect() error {
	var firstErr error
	for _, c := range p.conns {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
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

// queueMatch 经 gateway 业务通道 op 入队（等级相近 1v1；属性由服务端权威填充）。
func queueMatch(ctx context.Context, ps ...*player) error {
	for _, p := range ps {
		if _, err := p.match.QueueMatch(ctx, &gatewayv1.MatchQueueRequest{
			Token: p.token, PlayerId: p.id, Ruleset: "casual",
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
func run(ctx context.Context, mode, tcpAddr, kcpAddr, wsAddr string, frames uint64) error {
	if mode == "party" {
		return runParty(ctx, tcpAddr, kcpAddr, wsAddr, frames)
	}
	if mode == "fault" {
		return runFault(ctx, tcpAddr, kcpAddr, wsAddr)
	}
	ps, err := connectPlayers(ctx, mode, tcpAddr, kcpAddr, wsAddr)
	if err != nil {
		return err
	}
	battleID, err := matchAndStart(ctx, ps)
	if err != nil {
		return err
	}
	if err := sendBattleInputs(ctx, ps, battleID, frames); err != nil {
		return err
	}
	return verifySettlement(ps)
}

// connectPlayers 按形态建立双客户端连接：single 走 WS 单通道，否则走 TCP+KCP 双通道。
func connectPlayers(ctx context.Context, mode, tcpAddr, kcpAddr, wsAddr string) (ps [2]*player, err error) {
	for i := range ps {
		if mode == "single" {
			ps[i], err = newSinglePlayer(ctx, wsAddr)
		} else {
			ps[i], err = newDualPlayer(ctx, tcpAddr, kcpAddr)
		}
		if err != nil {
			return ps, err
		}
	}
	return ps, nil
}

// matchAndStart 双玩家注册登录后入队，等待撮合成局并返回 battleID。
func matchAndStart(ctx context.Context, ps [2]*player) (string, error) {
	for i, p := range ps {
		if err := p.registerLogin(ctx, byte(i)); err != nil {
			return "", err
		}
	}
	fmt.Println("[匹配] 双玩家入队（等级相近）")
	if err := queueMatch(ctx, ps[0], ps[1]); err != nil {
		return "", err
	}
	battleID, err := waitStarted(ps[0], ps[1])
	if err != nil {
		return "", err
	}
	fmt.Printf("[开局] battle=%s\n", battleID)
	for _, p := range ps {
		if err := p.joinWithRetry(ctx, battleID); err != nil {
			return "", err
		}
	}
	return battleID, nil
}

// sendBattleInputs 双玩家向战斗发帧：A 每帧前进 1（率先到终点），B 原地不动。
func sendBattleInputs(ctx context.Context, ps [2]*player, battleID string, frames uint64) error {
	if err := ps[0].sendInputs(ctx, battleID, frames, 1); err != nil {
		return err
	}
	return ps[1].sendInputs(ctx, battleID, frames, 0)
}

// verifySettlement 校验双方结算一致：胜者相同且为 A。
func verifySettlement(ps [2]*player) error {
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
	mode := flag.String("mode", "dual", "客户端形态：dual（TCP+KCP）/ single（WS）/ party（组队 2v2）/ fault（异常下线联动）")
	tcpAddr := flag.String("gw", "127.0.0.1:9001", "gateway TCP 地址")
	kcpAddr := flag.String("kcp", "127.0.0.1:9003", "gateway KCP 地址")
	wsAddr := flag.String("ws", "ws://127.0.0.1:9002", "gateway WS 地址")
	frames := flag.Uint64("frames", 20, "每客户端帧输入数")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := run(ctx, *mode, *tcpAddr, *kcpAddr, *wsAddr, *frames); err != nil {
		fmt.Fprintf(os.Stderr, "e2e 失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("e2e 闭环通过（%s 形态）\n", *mode)
}

// ---- 组队形态（-mode party）：4 客户端 2v2 + 取消路径 ----

// runParty 组队闭环：A 建队 → B 加入（双方收名册推送）→ A 整队入队 →
// C/D solo 入队 → 2v2 成局 → 帧同步 → 结算；随后取消路径：A 重建队入队再离开，
// 收整队取消的失败通知（PartyRosterNotify 的 leave 推送一并打印）。
func runParty(ctx context.Context, tcpAddr, kcpAddr, wsAddr string, frames uint64) error {
	ps, err := connectN(ctx, 4, tcpAddr, kcpAddr, wsAddr)
	if err != nil {
		return err
	}
	a, b, c, d := ps[0], ps[1], ps[2], ps[3]
	for i, p := range ps {
		if err := p.registerLogin(ctx, byte(i)); err != nil {
			return err
		}
	}

	// A 建队 → B 加入。
	cp, err := a.match.PartyCreate(ctx, &gatewayv1.PartyCreateRequest{Token: a.token, PlayerId: a.id})
	if err != nil {
		return fmt.Errorf("A 建队: %w", err)
	}
	partyID := cp.GetPartyId()
	fmt.Printf("[组队] A 建队 ok（party=%s leader=%s）\n", partyID, cp.GetLeaderId())
	if _, err := b.match.PartyJoin(ctx, &gatewayv1.PartyJoinRequest{Token: b.token, PlayerId: b.id, PartyId: partyID}); err != nil {
		return fmt.Errorf("B 加入: %w", err)
	}
	if err := waitRoster(b, matcherv1.PartyRosterReason_PARTY_ROSTER_REASON_JOIN); err != nil {
		return err
	}
	if err := waitRoster(a, matcherv1.PartyRosterReason_PARTY_ROSTER_REASON_JOIN); err != nil {
		return err
	}

	// A 整队入队 → C/D solo 入队 → 2v2 成局。
	if _, err := a.match.PartyQueue(ctx, &gatewayv1.PartyQueueRequest{Token: a.token, PlayerId: a.id, Ruleset: "casual"}); err != nil {
		return fmt.Errorf("A 整队入队: %w", err)
	}
	fmt.Println("[组队] A 整队入队，C/D solo 入队")
	for _, p := range []*player{c, d} {
		if _, err := p.match.QueueMatch(ctx, &gatewayv1.MatchQueueRequest{Token: p.token, PlayerId: p.id, Ruleset: "casual"}); err != nil {
			return fmt.Errorf("%s 入队: %w", p.id, err)
		}
	}
	battleID, err := waitStarted(a, b, c, d)
	if err != nil {
		return err
	}
	fmt.Printf("[开局] battle=%s（4 人 2v2）\n", battleID)

	if err := sendBattleInputsN(ctx, ps, battleID, frames); err != nil {
		return err
	}
	if err := verifySettlementN(ps); err != nil {
		return err
	}

	// 取消路径：C 重建队 → 整队入队 → 离开 → 收整队取消的失败通知。
	if _, err := c.match.PartyCreate(ctx, &gatewayv1.PartyCreateRequest{Token: c.token, PlayerId: c.id}); err != nil {
		return fmt.Errorf("C 取消路径建队: %w", err)
	}
	if _, err := c.match.PartyQueue(ctx, &gatewayv1.PartyQueueRequest{Token: c.token, PlayerId: c.id, Ruleset: "casual"}); err != nil {
		return fmt.Errorf("C 取消路径入队: %w", err)
	}
	if _, err := c.match.PartyLeave(ctx, &gatewayv1.PartyLeaveRequest{Token: c.token, PlayerId: c.id}); err != nil {
		return fmt.Errorf("C 取消路径离开: %w", err)
	}
	select {
	case n := <-c.failed:
		fmt.Printf("[取消] 整队取消通知 ok（ticket=%s reason=%s）\n", n.GetTicketId(), n.GetReason())
	case <-time.After(10 * time.Second):
		return fmt.Errorf("取消路径未收到整队取消通知")
	}
	return nil
}

// connectN 建立 n 个客户端（形态同 connectPlayers）。
func connectN(ctx context.Context, n int, tcpAddr, kcpAddr, wsAddr string) (ps []*player, err error) {
	for i := 0; i < n; i++ {
		var p *player
		if i%2 == 1 {
			p, err = newSinglePlayer(ctx, wsAddr) // 与 [2] 形态对齐：奇数下标走单通道（示例覆盖）
		} else {
			p, err = newDualPlayer(ctx, tcpAddr, kcpAddr)
		}
		if err != nil {
			return nil, err
		}
		ps = append(ps, p)
	}
	return ps, nil
}

// waitRoster 等待指定玩家收到 reason 的名册推送。
func waitRoster(p *player, wantReason matcherv1.PartyRosterReason) error {
	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case n := <-p.ros:
			if n.GetReason() == wantReason {
				return nil
			}
		case <-time.After(deadline.Sub(time.Now())):
			return fmt.Errorf("%s 未收到名册推送 %s", p.id, wantReason.String())
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s 未收到名册推送 %s", p.id, wantReason.String())
		}
	}
}

// sendBattleInputsN/sendBattleInputs 复用：多人形态的输入与结算校验。
func sendBattleInputsN(ctx context.Context, ps []*player, battleID string, frames uint64) error {
	steps := []byte{'A', 'B', 'C', 'D'}
	for i, p := range ps {
		if err := p.joinWithRetry(ctx, battleID); err != nil {
			return err
		}
		if err := p.sendInputs(ctx, battleID, frames, steps[i]); err != nil {
			return err
		}
	}
	return nil
}

// verifySettlementN 多人版结算校验：全部客户端胜者一致。
func verifySettlementN(ps []*player) error {
	var winner string
	for _, p := range ps {
		w, err := p.waitEnd()
		if err != nil {
			return err
		}
		if winner == "" {
			winner = w
		}
		if w != winner {
			return fmt.Errorf("%s 结局不一致: %q vs %q", p.id, w, winner)
		}
	}
	fmt.Printf("[结算] 胜者一致: %s\n", winner)
	return nil
}

// ---- 容错形态（-mode fault）：异常下线联动闭环 ----

// runFault 验证「排队中杀进程」的异常下线兜底：
// A 登录入队 → 断开连接（模拟杀进程，不发 Logout）→ gateway 会话过期清扫
// 联动 PlayerActor（SESSION_EXPIRED）→ 撮合域取消票据；A 重登后查询状态应归零（NONE）。
func runFault(ctx context.Context, tcpAddr, kcpAddr, wsAddr string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// 1) 登录（记录凭据供重登）+ 入队。
	p, err := newDualPlayer(ctx, tcpAddr, kcpAddr)
	if err != nil {
		return err
	}
	if err := p.registerLogin(ctx, 0); err != nil {
		return err
	}
	playerID := p.id
	if _, err := p.match.QueueMatch(ctx, &gatewayv1.MatchQueueRequest{
		Token: p.token, PlayerId: p.id, Ruleset: "casual",
	}); err != nil {
		return fmt.Errorf("入队: %w", err)
	}
	st, err := p.match.MatchStatus(ctx, &gatewayv1.MatchStatusRequest{Token: p.token, PlayerId: p.id})
	if err != nil {
		return fmt.Errorf("入队后查询: %w", err)
	}
	fmt.Printf("[容错] 入队 ok（state=%s ticket=%s）\n", st.GetState(), st.GetTicketId())

	// 2) 断开连接（模拟杀进程；不做任何显式取消/登出）。
	fmt.Println("[容错] 断开连接（模拟杀进程）")
	if err := p.disconnect(); err != nil {
		return err
	}

	// 3) 等待会话过期清扫联动（ttl 30s + 清扫周期 15s）后重登验证状态归零。
	fmt.Println("[容错] 等待会话过期联动（≤75s）…")
	deadline := time.Now().Add(75 * time.Second)
	var recovered *gatewayv1.MatchStatusReply
	for time.Now().Before(deadline) {
		time.Sleep(5 * time.Second)
		p2, err := newDualPlayer(ctx, tcpAddr, kcpAddr)
		if err != nil {
			return err
		}
		login, lerr := p2.auth.Login(ctx, &gatewayv1.LoginRequest{PlayerId: playerID, Password: "pw"})
		if lerr != nil {
			_ = p2.disconnect()
			continue // sweep 尚未完成时重登可能被旧路由挤下线，稍后重试
		}
		p2.id = login.GetPlayerId()
		p2.token = login.GetToken()
		st2, qerr := p2.match.MatchStatus(ctx, &gatewayv1.MatchStatusRequest{Token: p2.token, PlayerId: p2.id})
		_ = p2.disconnect()
		if qerr != nil {
			return fmt.Errorf("重登查询: %w", qerr)
		}
		if st2.GetState() != matcherv1.MatchState_MATCH_STATE_NONE {
			continue // 联动尚未生效，继续等待
		}
		recovered = st2
		break
	}
	if recovered == nil {
		return fmt.Errorf("重登验证超时")
	}
	fmt.Println("[容错] 会话过期联动生效：票据已取消，重登状态归零（NONE）")
	return nil
}
