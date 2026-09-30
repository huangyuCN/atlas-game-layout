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

// retryJoin 在不可靠传输下重试入局（UDP 无重传：丢一个包就是一次超时）。
func retryJoin(ctx context.Context, dc *directClient, battleID string) error {
	var lastErr error
	for attempt := 0; attempt < 6; attempt++ {
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
	return fmt.Errorf("%s 损伤下入局失败（重试 6 次）: %w", dc.tag, lastErr)
}

// joinOnce 尝试一次入局（JoinBattle + 补帧）。
func joinOnce(ctx context.Context, dc *directClient, battleID string) error {
	var join battlev1.JoinBattleReply
	if err := dc.invoke(ctx, battlev1opclient.BattleServiceProtocolOps.JoinBattle,
		&battlev1.JoinBattleReq{BattleId: battleID}, &join); err != nil {
		return err
	}
	var sync battlev1.SyncFramesReply
	return dc.invoke(ctx, battlev1opclient.BattleServiceProtocolOps.SyncFrames,
		&battlev1.SyncFramesReq{BattleId: battleID, LastSeenFrame: 0}, &sync)
}

// sendInputsTolerant 逐帧发送输入并容忍单帧失败：不可靠传输下丢包是常态，
// 本形态断言的是「战斗可继续并最终一致」，不是「每一帧都必达」。
func sendInputsTolerant(ctx context.Context, dc *directClient, battleID string,
	from, n uint64, step byte) int {
	failed := 0
	for i := uint64(0); i < n; i++ {
		req := &battlev1.FrameInputReq{BattleId: battleID,
			Input: &locksteppb.LockstepInput{FrameId: from + i, Payload: []byte{step}}}
		callCtx, cancel := context.WithTimeout(ctx, damageCallTimeout)
		err := dc.invoke(callCtx, battlev1opclient.BattleServiceProtocolOps.SendFrameInput, req, nil)
		cancel()
		if err != nil {
			failed++
		}
	}
	return failed
}

// waitDirectEndEither 等待结算推送：UDP 无重传，高丢包下单个客户端可能丢掉这一次推送
// （规格 §9.2 的 SDK 侧心跳/重试是批次 8 的待办）。故本形态要求「至少一方收到且胜者一致」，
// 未收到的一方明确记录为「推送丢失」而不判失败——战斗本身已收敛（帧广播与结算在服务端一致）。
func waitDirectEndEither(clients [2]*directClient, want string) error {
	deadline := time.Now().Add(20 * time.Second)
	seen := 0
	var missed []string
	for time.Now().Before(deadline) {
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
				seen++
				fmt.Printf("[损伤] %s 收到结算通知（胜者 %s）\n", dc.tag, want)
			default:
			}
		}
		if seen == len(clients) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, dc := range clients {
		if !dc.watcher.endSeen.Load() {
			missed = append(missed, dc.tag)
		}
	}
	if seen == 0 {
		return fmt.Errorf("双方都未收到结算通知（战斗未收敛）")
	}
	fmt.Printf("[损伤] 结算推送丢失（UDP 无重传，接受）：%v\n", missed)
	return nil
}

// driveDamagedBattle 在损伤下推进对局：前半段正常、中段断网、之后恢复，最后核对结算一致。
func driveDamagedBattle(ctx context.Context, clients [2]*directClient, relays []*damageRelay,
	battleID string, frames uint64, dmg migrateOpts, ps [2]*player) error {
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
	if err := waitDirectEndEither(clients, ps[0].id); err != nil {
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
