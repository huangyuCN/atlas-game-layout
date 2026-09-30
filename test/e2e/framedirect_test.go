package e2e

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1actor "github.com/huangyuCN/atlas-game-layout/api/battle/v1/actor"
	battleassemble "github.com/huangyuCN/atlas-game-layout/services/battle/assemble"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	kcpt "github.com/huangyuCN/atlas/transport/kcp"
	udpt "github.com/huangyuCN/atlas/transport/udp"
	wst "github.com/huangyuCN/atlas/transport/websocket"
)

// frameKind 是 battle 直连帧面的传输类型（与 server.kcp/udp/websocket 三段配置一一对应）。
type frameKind string

// 三种直连帧面（阶段 3 批次 5 起战斗帧的唯一承载：客户端直连接入层 → battle 帧面）。
const (
	frameKCP frameKind = "kcp"
	frameUDP frameKind = "udp"
	frameWS  frameKind = "ws"
)

// frameHeartbeat 是直连帧客户端的保活心跳间隔：数据报面（KCP/UDP）没有关闭握手，
// 掉线只能靠帧面的空闲读超时发现（battle 侧取 offline_timeout/3），故客户端必须周期发帧
// 证明自己还在——生产由 SDK 开心跳（框架内建 /atlas.internal.Heartbeat/Ping），用例用同一约定。
const frameHeartbeat = 250 * time.Millisecond

// directFrame 是一条直连 battle 帧面的连接（三面在 Invoke/推送/关闭三件事上同构收口）。
// 用框架传输客户端 + 帧会话槽（WithClientSessionProvider）承载身份，不自己实现 wire。
type directFrame struct {
	invoke func(ctx context.Context, operation string, req, resp any) error
	notify func(fn func(operation string, payload []byte))
	close  func() error
}

// frameAddrOf 取进程内 battle 实例指定帧面的 host:port（未启用的面为空串）。
func frameAddrOf(b *battleassemble.Battle, kind frameKind) string {
	switch kind {
	case frameKCP:
		return b.KCPURL
	case frameUDP:
		return b.UDPURL
	default:
		return b.WSURL
	}
}

// dialDirectFrame 建立一条直连帧连接：票据按**帧槽约定**（base64url 无填充）逐帧携带，
// 服务端在每帧上验票取身份（provider 返回空即匿名帧，用于负例；票后到时由闭包读到新值）。
func dialDirectFrame(t *testing.T, ctx context.Context, kind frameKind, addr string, ticket func() []byte) *directFrame {
	t.Helper()
	if addr == "" {
		t.Fatalf("%s 帧面未启用（地址为空）", kind)
	}
	slotOf := func() string {
		raw := ticket()
		if len(raw) == 0 {
			return ""
		}
		return base64.RawURLEncoding.EncodeToString(raw)
	}
	frame := &directFrame{}
	switch kind {
	case frameKCP:
		cli, err := kcpt.NewClient(addr, kcpt.WithClientSessionProvider(slotOf), kcpt.ClientHeartbeat(frameHeartbeat))
		if err != nil {
			t.Fatalf("KCP 直连失败: %v", err)
		}
		bindFrame(frame, cli.Invoke, cli.OnNotify, cli.Close)
	case frameUDP:
		cli, err := udpt.NewClient(addr, udpt.WithClientSessionProvider(slotOf))
		if err != nil {
			t.Fatalf("UDP 直连失败: %v", err)
		}
		// UDP 客户端的 Invoke 没有单次调用选项（签名与生成 stub 的 Invoker 逐字一致）。
		frame.invoke, frame.notify, frame.close = cli.Invoke, cli.OnNotify, cli.Close
	case frameWS:
		cli, err := wst.NewClient(ctx, "ws://"+addr+"/", wst.WithClientSessionProvider(slotOf))
		if err != nil {
			t.Fatalf("WS 直连失败: %v", err)
		}
		bindFrame(frame, cli.Invoke, cli.OnNotify, cli.Close)
	default:
		t.Fatalf("未知直连帧面 %q", kind)
	}
	t.Cleanup(func() { _ = frame.close() })
	return frame
}

// bindFrame 把框架传输客户端的三件事收口到 directFrame：
// Invoke 的单次调用选项（CallOption 变参）在此剥掉——生成 stub 的 Invoker 接缝不吃它，
// 其余（推送订阅、关闭）原样透传。
func bindFrame[C any](frame *directFrame,
	invoke func(ctx context.Context, operation string, req, resp any, opts ...C) error,
	notify func(fn func(operation string, payload []byte)),
	closeFn func() error,
) {
	frame.invoke = func(ctx context.Context, operation string, req, resp any) error {
		return invoke(ctx, operation, req, resp)
	}
	frame.notify = notify
	frame.close = closeFn
}

// issueTicket 向指定对局的 battle actor 取该玩家的入场票据（INTERNAL 出票，
// 与 matcher 成局路径同一份实现；直连用例用它拿票，不依赖网关推送）。
func issueTicket(t *testing.T, ctx context.Context, b *battleassemble.Battle, battleID, playerID string) []byte {
	t.Helper()
	pid, err := types.NewPID(battlev1actor.BattleServiceActorType, battleID)
	if err != nil {
		t.Fatalf("IssueEntryTicket PID: %v", err)
	}
	rep, err := battlev1actor.NewBattleServiceClusterClient(b.Runtime).
		IssueEntryTicket(ctx, pid, &battlev1.IssueEntryTicketReq{BattleId: battleID})
	if err != nil {
		t.Fatalf("出票 %s/%s: %v", battleID, playerID, err)
	}
	for _, e := range rep.GetTickets() {
		if e.GetPlayerId() == playerID {
			return e.GetTicket()
		}
	}
	t.Fatalf("出票名单缺少玩家 %s: %+v", playerID, rep.GetTickets())
	return nil
}
