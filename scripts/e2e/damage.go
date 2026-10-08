// damage.go 提供损伤形态（-mode damage，规格 §11 第 7 项）：UDP 直连 + 每客户端一个
// **应用内**损伤中继（丢包/抖动/断网）。损伤注入选应用内实现：无需 root/iptables，
// 权限无关、参数可复现，且只影响被测链路本身。
package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-game-layout/api/battle/v1/opclient"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
)

// ---- 网络损伤形态 ----

// damageRelay 是应用内 UDP 损伤中继：客户端 → 中继（丢包/抖动/断网）→ battle 帧面。
// 每个客户端一个中继，故彼此损伤独立；中继用独立上行 socket，battle 侧看到的对端键
// 仍是「一个客户端一个对端」，不会把两个玩家混成一个对端。
type damageRelay struct {
	listen *net.UDPConn
	up     *net.UDPConn
	upAddr *net.UDPAddr
	cfg    migrateOpts
	rnd    *rand.Rand
	black  atomic.Bool
	closed atomic.Bool

	mu     sync.Mutex
	client *net.UDPAddr
}

// startDamageRelay 起一个损伤中继：返回其监听地址（客户端拨它）。
func startDamageRelay(upstream string, cfg migrateOpts) (*damageRelay, error) {
	upAddr, err := net.ResolveUDPAddr("udp", upstream)
	if err != nil {
		return nil, fmt.Errorf("解析 battle UDP 帧面地址失败: %w", err)
	}
	listen, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return nil, fmt.Errorf("损伤中继监听失败: %w", err)
	}
	up, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		_ = listen.Close()
		return nil, fmt.Errorf("损伤中继上行 socket 失败: %w", err)
	}
	r := &damageRelay{listen: listen, up: up, upAddr: upAddr, cfg: cfg,
		rnd: rand.New(rand.NewSource(time.Now().UnixNano()))}
	go r.pumpClient()
	go r.pumpUpstream()
	return r, nil
}

// Addr 返回中继监听地址（客户端拨它）。
func (r *damageRelay) Addr() string { return r.listen.LocalAddr().String() }

// setBlackout 开关断网窗口（窗口内双向全丢）。
func (r *damageRelay) setBlackout(on bool) { r.black.Store(on) }

// Close 关闭中继（幂等）。
func (r *damageRelay) Close() {
	if r.closed.Swap(true) {
		return
	}
	_ = r.listen.Close()
	_ = r.up.Close()
}

// drop 按丢包率与断网窗口判定是否丢弃该包。
func (r *damageRelay) drop() bool {
	if r.black.Load() {
		return true
	}
	return r.cfg.lossPercent > 0 && r.rnd.Intn(100) < r.cfg.lossPercent
}

// delay 按抖动上限随机延迟（0 表示不延迟）。
func (r *damageRelay) delay() time.Duration {
	if r.cfg.jitter <= 0 {
		return 0
	}
	return time.Duration(r.rnd.Int63n(int64(r.cfg.jitter)))
}

// pumpClient 客户端 → battle（首包记录客户端地址；带 flow-id 的包原样转发）。
func (r *damageRelay) pumpClient() {
	buf := make([]byte, 64*1024)
	for {
		n, from, err := r.listen.ReadFromUDP(buf)
		if err != nil {
			return
		}
		r.mu.Lock()
		r.client = from
		r.mu.Unlock()
		if r.drop() {
			continue
		}
		payload := append([]byte(nil), buf[:n]...)
		go func() {
			if d := r.delay(); d > 0 {
				time.Sleep(d)
			}
			_, _ = r.up.WriteToUDP(payload, r.upAddr)
		}()
	}
}

// pumpUpstream battle → 客户端。
func (r *damageRelay) pumpUpstream() {
	buf := make([]byte, 64*1024)
	for {
		n, _, err := r.up.ReadFromUDP(buf)
		if err != nil {
			return
		}
		r.mu.Lock()
		to := r.client
		r.mu.Unlock()
		if to == nil || r.drop() {
			continue
		}
		payload := append([]byte(nil), buf[:n]...)
		go func() {
			if d := r.delay(); d > 0 {
				time.Sleep(d)
			}
			_, _ = r.listen.WriteToUDP(payload, to)
		}()
	}
}

// runDamage 损伤形态闭环：UDP 直连 + 应用内丢包/抖动/断网，断言战斗可继续与最终一致。
func runDamage(ctx context.Context, a addrs, do directOpts, dmg migrateOpts, frames uint64) error {
	ps, err := connectDirectPlayers(a.tcp)
	if err != nil {
		return err
	}
	battleID, tickets, err := matchAndCollectTickets(ctx, ps)
	if err != nil {
		return err
	}
	relays, addrs, err := startPlayerRelays(a.battleUDP, dmg, len(ps))
	if err != nil {
		return err
	}
	defer func() {
		for _, r := range relays {
			r.Close()
		}
	}()
	clients := [2]*directClient{}
	for i, p := range ps {
		raw := tickets[p.id]
		dc, err := dialDirect(ctx, directUDP, addrs[i], base64.RawURLEncoding.EncodeToString(raw), p.id)
		if err != nil {
			return err
		}
		clients[i] = dc
	}
	defer closeDirectPlayers(clients)
	fmt.Printf("[损伤] 丢包=%d%% 抖动=%s 断网=%s（应用内中继，UDP 直连）\n",
		dmg.lossPercent, dmg.jitter, dmg.blackout)
	for _, dc := range clients {
		if err := retryJoin(ctx, dc, battleID); err != nil {
			return err
		}
	}
	return driveDamagedBattle(ctx, clients, relays, battleID, frames, dmg, ps)
}

// startPlayerRelays 为每个玩家起一个损伤中继，返回中继与其地址。
func startPlayerRelays(upstream string, cfg migrateOpts, n int) ([]*damageRelay, []string, error) {
	relays := make([]*damageRelay, 0, n)
	addrs := make([]string, 0, n)
	for i := 0; i < n; i++ {
		r, err := startDamageRelay(upstream, cfg)
		if err != nil {
			return relays, addrs, err
		}
		relays = append(relays, r)
		addrs = append(addrs, r.Addr())
	}
	return relays, addrs, nil
}

// damageCallTimeout 是损伤形态下单次帧 op 的调用上限：UDP 无重传，丢一个包就是一次超时；
// 必须有界超时才能重试——否则首次握手包一丢，调用会一直挂到用例总超时（实测 90s 假死）。
const damageCallTimeout = 3 * time.Second

// joinAttempts 是损伤形态下入局的重试次数：JoinBattle 一次往返跨两跳（请求 + 回执），
// 20% 丢包下单次成功率约 0.64，10 次重试之下「入不了局」的概率可忽略。
const joinAttempts = 10

// retryJoin 在不可靠传输下重试入局（UDP 无重传：丢一个包就是一次超时）。
func retryJoin(ctx context.Context, dc *directClient, battleID string) error {
	var lastErr error
	for attempt := 0; attempt < joinAttempts; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, damageCallTimeout)
		joinErr := joinOnce(callCtx, dc, battleID)
		cancel()
		if joinErr == nil {
			fmt.Printf("[损伤] %s 入局 ok\n", dc.tag)
			return nil
		}
		lastErr = joinErr
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("%s 损伤下入局失败（重试 %d 次）: %w", dc.tag, joinAttempts, lastErr)
}

// joinOnce 尝试一次入局：**JoinBattle 成功即算入局**（它登记本次直连并回执会话元信息）；
// 补帧查询（SyncFrames）只是随后的取缺口动作，失败不判负——否则一次丢包会把「已入局」
// 误报成「入局失败」（损伤形态下这两件事必须分开，否则高丢包下必现假失败）。
func joinOnce(ctx context.Context, dc *directClient, battleID string) error {
	var join battlev1.JoinBattleReply
	if err := dc.invoke(ctx, battlev1opclient.BattleServiceProtocolOps.JoinBattle,
		&battlev1.JoinBattleReq{BattleId: battleID}, &join); err != nil {
		return err
	}
	var sync battlev1.SyncFramesReply
	if err := dc.invoke(ctx, battlev1opclient.BattleServiceProtocolOps.SyncFrames,
		&battlev1.SyncFramesReq{BattleId: battleID, LastSeenFrame: 0}, &sync); err != nil {
		fmt.Printf("[损伤] %s 补帧查询失败（不判负，帧照常推进）: %v\n", dc.tag, err)
	}
	return nil
}

// sendInputsTolerant 逐帧发送输入并容忍单帧失败：不可靠传输下丢包是常态，
// 本形态断言的是「战斗可继续并最终一致」，不是「每一帧都必达」。
// 收到 BATTLE_ENDED 即停：对局已结束，后续帧 op 会被稳定拒绝——这正是 SDK 停止发送的依据。
func sendInputsTolerant(ctx context.Context, dc *directClient, battleID string,
	from, n uint64, step byte) int {
	failed := 0
	for i := uint64(0); i < n; i++ {
		req := &battlev1.FrameInputReq{BattleId: battleID,
			Input: &locksteppb.LockstepInput{FrameId: from + i, Payload: []byte{step}}}
		callCtx, cancel := context.WithTimeout(ctx, damageCallTimeout)
		err := dc.invoke(callCtx, battlev1opclient.BattleServiceProtocolOps.SendFrameInput, req, nil)
		cancel()
		if err == nil {
			continue
		}
		if errorv1.IsBattleEnded(err) {
			fmt.Printf("[损伤] %s 结算后停止发送（第 %d 帧，reason=%s）\n", dc.tag, from+i, errorv1.ReasonBattleEnded())
			return failed
		}
		failed++
	}
	return failed
}

// 损伤形态的保活与结算等待策略（A2 主验收）：
//   - 保活：SDK 契约要求客户端以 ≤ offline_timeout/3 的周期发包（缺省 15s → 5s），否则数据报面
//     的空闲驱逐会把「还在等结果」判成掉线，15s 后判负结算——胜者随之变成另一方（断言失真）；
//   - 结算后：每条迟到 op（Ping）都会触发一次留档结果的**补投**（幂等、每玩家有界）。
const (
	endWaitWindow    = 45 * time.Second       // 结算通知等待窗口
	keepAlivePeriod  = time.Second            // 保活周期（远快于 offline_timeout/3 的驱逐窗口）
	keepAliveTimeout = 300 * time.Millisecond // 保活调用上限（Ping 是 Tell，正常不回帧）
)

// startKeepAlive 为每个客户端起一个保活协程（周期发 Ping，直到 stop 被调用）：
// 全程保活既避免误判掉线，又让「结算后继续发帧」在损伤形态下自然发生（触发结果补投）。
func startKeepAlive(ctx context.Context, clients [2]*directClient, battleID string) func() {
	done := make(chan struct{})
	var wg sync.WaitGroup
	for _, dc := range clients {
		wg.Add(1)
		go func(dc *directClient) {
			defer wg.Done()
			t := time.NewTicker(keepAlivePeriod)
			defer t.Stop()
			for {
				select {
				case <-done:
					return
				case <-t.C:
					pingLate(ctx, dc, battleID)
				}
			}
		}(dc)
	}
	return func() {
		close(done)
		wg.Wait()
	}
}

// waitDamagedEnd 等待双方结算通知（保活协程在跑）：最终要求**双方都拿到结果**——
// 高丢包下「双方都没收到」正是本形态要盯住的收敛缺陷（补投兜底后仍收不到才算失败）。
func waitDamagedEnd(clients [2]*directClient, want string) error {
	deadline := time.Now().Add(endWaitWindow)
	for time.Now().Before(deadline) {
		if err := collectEnds(clients, want); err != nil {
			return err
		}
		if allEndSeen(clients) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("窗口 %s 内未收到结算通知: %v（结果已留档，补投未达）",
		endWaitWindow, missedTags(clients))
}

// pingLate 发一次迟到 op（保活 Ping）：未结算即保活刷新数据报面的空闲计时，
// 已结算即触发一次结果补投。Ping 是 Tell，正常路径不回帧，故用短超时且忽略错误——
// 是否到达由调用方（等待循环）判定。
func pingLate(ctx context.Context, dc *directClient, battleID string) {
	callCtx, cancel := context.WithTimeout(ctx, keepAliveTimeout)
	defer cancel()
	_ = dc.invoke(callCtx, battlev1opclient.BattleServiceProtocolOps.Ping,
		&battlev1.PingReq{BattleId: battleID}, nil)
}

// collectEnds 把已到达的结算通知收进 endSeen 并核对胜者一致。
func collectEnds(clients [2]*directClient, want string) error {
	for _, dc := range clients {
		if dc.watcher.endSeen.Load() {
			continue
		}
		select {
		case n := <-dc.watcher.end:
			if n.GetWinnerPlayerId() != want {
				return fmt.Errorf("%s 结束通知胜者 = %q, want %q", dc.tag, n.GetWinnerPlayerId(), want)
			}
			dc.watcher.endSeen.Store(true)
			fmt.Printf("[损伤] %s 收到结算通知（胜者 %s）\n", dc.tag, want)
		default:
		}
	}
	return nil
}

// allEndSeen 返回双方是否都已收到结算通知。
func allEndSeen(clients [2]*directClient) bool {
	for _, dc := range clients {
		if !dc.watcher.endSeen.Load() {
			return false
		}
	}
	return true
}

// missedTags 返回尚未收到结算通知的客户端标签（失败报文用）。
func missedTags(clients [2]*directClient) []string {
	var out []string
	for _, dc := range clients {
		if !dc.watcher.endSeen.Load() {
			out = append(out, dc.tag)
		}
	}
	return out
}

// driveDamagedBattle 在损伤下推进对局：前半段正常、中段断网、之后恢复，最后核对结算一致。
// 全程挂保活（SDK 契约：静默会被数据报面驱逐并误判掉线），结算后的迟到 op 由保活自然产生。
func driveDamagedBattle(ctx context.Context, clients [2]*directClient, relays []*damageRelay,
	battleID string, frames uint64, dmg migrateOpts, ps [2]*player) error {
	stopKeepAlive := startKeepAlive(ctx, clients, battleID)
	defer stopKeepAlive()

	half := frames / 2
	for i, dc := range clients {
		if n := sendInputsTolerant(ctx, dc, battleID, 1, half, stepOf(i)); n > 0 {
			fmt.Printf("[损伤] %s 前半段丢帧 %d/%d（可容忍）\n", dc.tag, n, half)
		}
	}
	if dmg.blackout > 0 {
		for _, r := range relays {
			r.setBlackout(true)
		}
		fmt.Printf("[损伤] 断网 %s（期间输入全部丢弃）\n", dmg.blackout)
		time.Sleep(dmg.blackout)
		for _, r := range relays {
			r.setBlackout(false)
		}
	}
	for i, dc := range clients {
		if n := sendInputsTolerant(ctx, dc, battleID, half+1, frames-half, stepOf(i)); n > 0 {
			fmt.Printf("[损伤] %s 后半段丢帧 %d/%d（可容忍）\n", dc.tag, n, frames-half)
		}
	}
	if err := waitDamagedEnd(clients, ps[0].id); err != nil {
		return err
	}
	for _, dc := range clients {
		if dc.watcher.outs.Load() > 0 {
			return fmt.Errorf("%s 在损伤下被判掉线（收到出局通知）", dc.tag)
		}
		if dc.watcher.frames.Load() == 0 {
			return fmt.Errorf("%s 在损伤下未收到任何帧广播", dc.tag)
		}
	}
	fmt.Printf("[损伤] 帧广播 A=%d B=%d；无出局通知；胜者一致: %s\n",
		clients[0].watcher.frames.Load(), clients[1].watcher.frames.Load(), ps[0].id)
	fmt.Println("闭环通过（damage 形态）")
	return nil
}
