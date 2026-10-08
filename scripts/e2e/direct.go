// direct.go 提供 direct 形态（-mode direct）的客户端驱动：走正常业务链路（网关）拿到成局通知里的
// battle_ticket 后，**不经接入层**直连 battle 的帧端口，跑通
// JoinBattle → SendFrameInput → SyncFrames → 收到帧广播/结算。
//
// 为什么用框架帧客户端而不是 SDK：本形态验证的是「battle 帧面 + 帧槽验票 + 直连推送」本身；
// 直连帧槽（base64url 票据）由框架客户端逐帧携带（transport/kcp|udp|websocket 的
// WithClientSessionProvider），三 SDK 的直连会话改造属批次 4（本形态不依赖 SDK 新版本）。
package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync/atomic"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-game-layout/api/battle/v1/opclient"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	kcpt "github.com/huangyuCN/atlas/transport/kcp"
	udpt "github.com/huangyuCN/atlas/transport/udp"
	wst "github.com/huangyuCN/atlas/transport/websocket"
	"google.golang.org/protobuf/encoding/protojson"
)

// directTransport 是直连帧面的传输类型（-transport 取值）。
type directTransport string

// 三类直连传输（脚本可只用其中一种跑闭环）。
const (
	directKCP directTransport = "kcp"
	directUDP directTransport = "udp"
	directWS  directTransport = "ws"
)

// directOpts 是 direct 形态参数（其余形态忽略）。
type directOpts struct {
	mode      string // 客户端形态（-mode）
	transport string // 直连传输（-transport：kcp|udp|ws）
	frameAddr string // battle 帧端口（-battle-frame，如 127.0.0.1:9401）
}

// parseDirectTransport 解析 -transport 取值（未知取值即报错，不静默回落 kcp）。
func parseDirectTransport(raw string) (directTransport, error) {
	switch directTransport(raw) {
	case directKCP, directUDP, directWS:
		return directTransport(raw), nil
	default:
		return "", fmt.Errorf("未知直连传输 %q（kcp|udp|ws）", raw)
	}
}

// directWatcher 记录一条直连连接上收到的战斗域通知（帧广播/出局计数 + 战斗结束信号）。
type directWatcher struct {
	frames  atomic.Int64
	outs    atomic.Int64 // 出局通知计数（迁移/损伤下必须为 0：不误判掉线）
	endSeen atomic.Bool  // 结算通知是否已收到（损伤形态允许单侧丢包）
	end     chan *battlev1.BattleEndNotify
}

// newDirectWatcher 构造通知记录器。
func newDirectWatcher() *directWatcher {
	return &directWatcher{end: make(chan *battlev1.BattleEndNotify, 4)}
}

// watch 分发服务端推送：帧广播计数、战斗结束进信号通道。
func (w *directWatcher) watch(operation string, payload []byte) {
	switch operation {
	case battlev1opclient.BattleServicePushOps.FrameBroadcast:
		w.frames.Add(1)
	case battlev1opclient.BattleServicePushOps.PlayerOutNotify:
		w.outs.Add(1)
	case battlev1opclient.BattleServicePushOps.BattleEndNotify:
		var n battlev1.BattleEndNotify
		if protojson.Unmarshal(payload, &n) == nil {
			push(w.end, &n)
		}
	}
}

// directClient 是一条直连帧连接（三类传输的 Invoke/推送/关闭在此收口）。
type directClient struct {
	tag     string         // 客户端标签（玩家 ID）
	watcher *directWatcher // 收到的战斗域通知
	invoke  func(ctx context.Context, operation string, req, resp any) error
	notify  func(fn func(operation string, payload []byte))
	close   func() error
}

// dialDirect 建立一条直连帧连接：票据以 base64url 编码写进每帧的会话槽（帧槽取值约定）。
func dialDirect(ctx context.Context, kind directTransport, addr, slot, tag string) (*directClient, error) {
	watcher := newDirectWatcher()
	dc := &directClient{tag: tag, watcher: watcher}
	slotOf := func() string { return slot }
	switch kind {
	case directKCP:
		cli, err := kcpt.NewClient(addr, kcpt.WithClientSessionProvider(slotOf))
		if err != nil {
			return nil, fmt.Errorf("%s KCP 直连失败: %w", tag, err)
		}
		dc.invoke, dc.notify, dc.close = func(ctx context.Context, op string, req, resp any) error {
			return cli.Invoke(ctx, op, req, resp)
		}, cli.OnNotify, cli.Close
	case directUDP:
		cli, err := udpt.NewClient(addr, udpt.WithClientSessionProvider(slotOf))
		if err != nil {
			return nil, fmt.Errorf("%s UDP 直连失败: %w", tag, err)
		}
		dc.invoke, dc.notify, dc.close = func(ctx context.Context, op string, req, resp any) error {
			return cli.Invoke(ctx, op, req, resp)
		}, cli.OnNotify, cli.Close
	case directWS:
		cli, err := wst.NewClient(ctx, "ws://"+addr+"/", wst.WithClientSessionProvider(slotOf))
		if err != nil {
			return nil, fmt.Errorf("%s WS 直连失败: %w", tag, err)
		}
		dc.invoke, dc.notify, dc.close = func(ctx context.Context, op string, req, resp any) error {
			return cli.Invoke(ctx, op, req, resp)
		}, cli.OnNotify, cli.Close
	default:
		return nil, fmt.Errorf("未知直连传输 %q", kind)
	}
	dc.notify(watcher.watch)
	return dc, nil
}

// directJoin 直连后的入局流程：JoinBattle → SyncFrames（补帧）→ 逐帧输入。
func directJoin(ctx context.Context, dc *directClient, battleID string, frames uint64, step byte) error {
	var join battlev1.JoinBattleReply
	if err := dc.invoke(ctx, battlev1opclient.BattleServiceProtocolOps.JoinBattle,
		&battlev1.JoinBattleReq{BattleId: battleID}, &join); err != nil {
		return fmt.Errorf("%s 直连 JoinBattle 失败: %w", dc.tag, err)
	}
	if join.GetMeta().GetSessionId() != battleID {
		return fmt.Errorf("%s 直连 JoinBattle 回执不符: %+v", dc.tag, join.GetMeta())
	}
	var sync battlev1.SyncFramesReply
	if err := dc.invoke(ctx, battlev1opclient.BattleServiceProtocolOps.SyncFrames,
		&battlev1.SyncFramesReq{BattleId: battleID, LastSeenFrame: 0}, &sync); err != nil {
		return fmt.Errorf("%s 直连 SyncFrames 失败: %w", dc.tag, err)
	}
	fmt.Printf("[直连] %s 入局 ok（当前帧 %d，补帧组 %d）\n", dc.tag, sync.GetCurrentFrame(), len(sync.GetMissed()))
	return sendDirectInputs(ctx, dc, battleID, frames, step)
}

// sendDirectInputs 逐帧发送输入（Tell 无回执）。身份由帧槽票据经投递 sender 注入，
// 载荷里的 player_id 不参与寻址，故不填。
// 收到 BATTLE_ENDED 即停并视为正常收尾：结算已发生，后续帧 op 会被稳定拒绝
// （这正是 SDK 的停止发送依据；继续发只会拿到同一个可判定的 reason）。
func sendDirectInputs(ctx context.Context, dc *directClient, battleID string, frames uint64, step byte) error {
	for i := uint64(1); i <= frames; i++ {
		req := &battlev1.FrameInputReq{
			BattleId: battleID,
			Input:    &locksteppb.LockstepInput{FrameId: i, Payload: []byte{step}},
		}
		err := dc.invoke(ctx, battlev1opclient.BattleServiceProtocolOps.SendFrameInput, req, nil)
		if err == nil {
			continue
		}
		if errorv1.IsBattleEnded(err) {
			fmt.Printf("[直连] %s 结算后停止发送（第 %d 帧，reason=%s）\n", dc.tag, i, errorv1.ReasonBattleEnded())
			return nil
		}
		return fmt.Errorf("%s 直连帧输入 %d 失败: %w", dc.tag, i, err)
	}
	return nil
}

// waitDirectEnd 等待双方直连连接上的战斗结束通知并核对胜者一致。
func waitDirectEnd(clients [2]*directClient, want string) error {
	for _, dc := range clients {
		select {
		case n := <-dc.watcher.end:
			if n.GetWinnerPlayerId() != want {
				return fmt.Errorf("%s 直连结束通知胜者 = %q, want %q", dc.tag, n.GetWinnerPlayerId(), want)
			}
		case <-time.After(20 * time.Second):
			return fmt.Errorf("%s 未收到直连战斗结束通知", dc.tag)
		}
	}
	return nil
}

// runDirect 直连形态闭环：业务链路拿票 → 直连 battle 帧端口 → 入局/补帧/帧输入 → 帧广播与结算。
func runDirect(ctx context.Context, a addrs, o directOpts, frames uint64) error {
	kind, err := parseDirectTransport(o.transport)
	if err != nil {
		return err
	}
	ps, err := connectDirectPlayers(a.tcp)
	if err != nil {
		return err
	}
	battleID, tickets, err := matchAndCollectTickets(ctx, ps)
	if err != nil {
		return err
	}
	clients, err := dialDirectPlayers(ctx, ps, tickets, kind, o.frameAddr)
	if err != nil {
		return err
	}
	defer closeDirectPlayers(clients)
	fmt.Printf("[直连] 传输=%s 帧端口=%s battle=%s（不经接入层）\n", kind, o.frameAddr, battleID)
	if err := verifyDirectNegatives(ctx, kind, o.frameAddr, battleID, tickets[ps[0].id]); err != nil {
		return err
	}
	if err := directJoin(ctx, clients[0], battleID, frames, 1); err != nil {
		return err
	}
	if err := directJoin(ctx, clients[1], battleID, frames, 0); err != nil {
		return err
	}
	if err := waitDirectEnd(clients, ps[0].id); err != nil {
		return err
	}
	for _, dc := range clients {
		if dc.watcher.frames.Load() == 0 {
			return fmt.Errorf("%s 未收到直连帧广播", dc.tag)
		}
	}
	fmt.Printf("[直连] 帧广播 A=%d B=%d；胜者一致: %s\n",
		clients[0].watcher.frames.Load(), clients[1].watcher.frames.Load(), ps[0].id)
	fmt.Println("闭环通过（direct 形态）")
	return nil
}

// connectDirectPlayers 建立仅业务通道的双客户端（战斗帧走直连，不再经网关）。
func connectDirectPlayers(tcpAddr string) (ps [2]*player, err error) {
	for i := range ps {
		if ps[i], err = newBizPlayer(tcpAddr); err != nil {
			return ps, err
		}
	}
	return ps, nil
}

// matchAndCollectTickets 注册登录入队，等待成局通知并取回 battleID 与各玩家**本人**的票据。
func matchAndCollectTickets(ctx context.Context, ps [2]*player) (string, map[string][]byte, error) {
	for i, p := range ps {
		if err := p.registerLogin(ctx, byte(i)); err != nil {
			return "", nil, err
		}
	}
	fmt.Println("[匹配] 双玩家入队（等级相近）")
	if err := queueMatch(ctx, ps[0], ps[1]); err != nil {
		return "", nil, err
	}
	battleID, tickets := "", make(map[string][]byte, len(ps))
	for _, p := range ps {
		select {
		case n := <-p.started:
			if n.GetBattleId() == "" {
				return "", nil, fmt.Errorf("%s 开局通知缺 battle_id", p.id)
			}
			if battleID == "" {
				battleID = n.GetBattleId()
			}
			if n.GetBattleId() != battleID {
				return "", nil, fmt.Errorf("双方 battle ID 不一致: %q vs %q", n.GetBattleId(), battleID)
			}
			if len(n.GetBattleTicket()) == 0 {
				return "", nil, fmt.Errorf("%s 开局通知缺 battle_ticket（出票/逐人扇出未接线）", p.id)
			}
			if err := checkEdgeEndpoints(n.GetEndpoints()); err != nil {
				return "", nil, fmt.Errorf("%s %w", p.id, err)
			}
			tickets[p.id] = n.GetBattleTicket()
		case <-time.After(10 * time.Second):
			return "", nil, fmt.Errorf("%s 未收到开局通知", p.id)
		}
	}
	fmt.Printf("[开局] battle=%s（已拿到逐人票据）\n", battleID)
	return battleID, tickets, nil
}

// checkEdgeEndpoints 断言开局通知的面列表与本脚本给 battle 的配置（e2eEdgeEndpoints）
// 逐字一致：接入层地址的唯一来源是 battle 配置，面错/地址错都会让 SDK 拨错端口——
// 这类错误在直连形态下只表现为「连不上」，必须在闭环里显式断言。
func checkEdgeEndpoints(got []*battlev1.EdgeEndpoint) error {
	if len(got) != len(e2eEdgeEndpoints) {
		return fmt.Errorf("开局通知面数 = %d, 期望 %d（battle 配置逐面下发）", len(got), len(e2eEdgeEndpoints))
	}
	for i, want := range e2eEdgeEndpoints {
		if got[i].GetTransport() != want.GetTransport() || got[i].GetAddress() != want.GetAddress() {
			return fmt.Errorf("开局通知 endpoints[%d] = %s/%s, 期望 %s/%s",
				i, got[i].GetTransport(), got[i].GetAddress(), want.GetTransport(), want.GetAddress())
		}
	}
	return nil
}

// dialDirectPlayers 为每个玩家建立直连帧连接（票据以 base64url 编码进会话槽）。
func dialDirectPlayers(ctx context.Context, ps [2]*player, tickets map[string][]byte, kind directTransport, addr string) ([2]*directClient, error) {
	var out [2]*directClient
	for i, p := range ps {
		raw, ok := tickets[p.id]
		if !ok {
			return out, fmt.Errorf("%s 没有本局票据", p.id)
		}
		dc, err := dialDirect(ctx, kind, addr, base64.RawURLEncoding.EncodeToString(raw), p.id)
		if err != nil {
			return out, err
		}
		out[i] = dc
	}
	return out, nil
}

// closeDirectPlayers 关闭全部直连连接（幂等）。
func closeDirectPlayers(clients [2]*directClient) {
	for _, dc := range clients {
		if dc != nil && dc.close != nil {
			_ = dc.close()
		}
	}
}

// verifyDirectNegatives 校验帧槽验票的负例语义（与单测同口径，但走真实帧链路）：
// 无槽/被篡改 → BATTLE_TICKET_INVALID；过期 → BATTLE_TICKET_EXPIRED（两个 reason 必须分开）。
func verifyDirectNegatives(ctx context.Context, kind directTransport, addr, battleID string, raw []byte) error {
	expired, err := expiredSlot("p-neg", battleID)
	if err != nil {
		return err
	}
	cases := []struct {
		name string
		slot string
		want func(error) bool
	}{
		{"无槽", "", errorv1.IsBattleTicketInvalid},
		{"被篡改", tamperedSlot(raw), errorv1.IsBattleTicketInvalid},
		{"过期票", expired, errorv1.IsBattleTicketExpired},
	}
	for _, tc := range cases {
		dc, err := dialDirect(ctx, kind, addr, tc.slot, "负例-"+tc.name)
		if err != nil {
			return err
		}
		var join battlev1.JoinBattleReply
		err = dc.invoke(ctx, battlev1opclient.BattleServiceProtocolOps.JoinBattle,
			&battlev1.JoinBattleReq{BattleId: battleID}, &join)
		_ = dc.close()
		if err == nil || !tc.want(err) {
			return fmt.Errorf("%s：期望被拒，实际 err=%v（reason=%s）", tc.name, err, atlaserrors.Reason(err))
		}
		fmt.Printf("[直连] 负例 %s 被拒（reason=%s）\n", tc.name, atlaserrors.Reason(err))
	}
	return nil
}

// expiredSlot 用 e2e 出票密钥签一张已过期的票（脚本侧构造，验证 TTL 判定与坏票可区分）。
func expiredSlot(playerID, battleID string) (string, error) {
	key, err := base64.StdEncoding.DecodeString(e2eTicketKey)
	if err != nil {
		return "", fmt.Errorf("e2e 票据密钥不是合法 base64: %w", err)
	}
	now := time.Now()
	raw, err := ticket.Encode(ticket.Ticket{
		Version:   ticket.Version1,
		KID:       1,
		PlayerID:  playerID,
		BattleID:  battleID,
		IssuedAt:  now.Add(-time.Hour),
		ExpiresAt: now.Add(-time.Minute),
	}, key)
	if err != nil {
		return "", fmt.Errorf("签过期票失败: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// tamperedSlot 翻转票面密文最后一字节（认证必然失败）。
func tamperedSlot(raw []byte) string {
	bad := append([]byte(nil), raw...)
	bad[len(bad)-1] ^= 0xff
	return base64.RawURLEncoding.EncodeToString(bad)
}
