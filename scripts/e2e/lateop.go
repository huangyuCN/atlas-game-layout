// lateop.go 提供「结算后继续发帧」形态（-mode lateop，验收③）：对局结算、结算时的直连已被关闭，
// 客户端凭**同一张票**重连后继续发帧输入与保活（验收现场每轮 9–13 次重建的输入源），断言：
//  1. 每条迟到 op 都拿到稳定拒绝 BATTLE_ENDED（语义＝该对局已结束，SDK 据此停止发送）；
//  2. 战斗 actor 不再被重建：起停各一次（不再 9–13 次），结算事件也只有一次；
//  3. 直连不再被关闭：重连后的连接接连收下多条迟到 op 都拿到业务拒绝（不是传输错误）；
//  4. 补投到位：留档的结算结果（胜者）在重连的连接上再次到达，与首投一致（幂等）。
//
// 观测口径（进程内装配，不依赖日志）：结算事件数取自本命名空间的 nats 事件主题，
// actor 停止次数取自本机运行时的停止钩子——重建一轮必然多一次「结算 + 停止」。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1actor "github.com/huangyuCN/atlas-game-layout/api/battle/v1/actor"
	battlev1opclient "github.com/huangyuCN/atlas-game-layout/api/battle/v1/opclient"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	natsgo "github.com/nats-io/nats.go"
)

// lateOpRounds 是结算后继续发送的迟到 op 轮数（每轮 = 一次帧输入 + 一次保活）。
const lateOpRounds = 3

// rebuildQuietWindow 是等「潜在的重建-再结算」跑完的静默窗口：重建实例要靠下一次快照
// （缺省 10 帧 × 100ms = 1s）才会再次结算，故窗口取 2s 覆盖它。
const rebuildQuietWindow = 2 * time.Second

// lateOpProbe 是形态运行的观测点：结算事件数与战斗 actor 停止次数（重建各多一次）。
type lateOpProbe struct {
	settles atomic.Int64
	stops   atomic.Int64
}

// watch 订阅本命名空间的结算事件并按 battleID 计数，返回停止订阅函数。
// 主题用 `event.>` 通配（结算主题名不在此处复制一份），按载荷里的 battle_id 过滤。
func (p *lateOpProbe) watch(natsURL, battleID string) (func(), error) {
	nc, err := natsgo.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("观测结算事件：连接 nats 失败: %w", err)
	}
	topics, err := consts.NewTopics(e2eDerived)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("观测结算事件：派生 topic 失败: %w", err)
	}
	sub, err := nc.Subscribe(topics.Event(">"), func(m *natsgo.Msg) {
		var ev struct {
			BattleID string `json:"battleId"`
		}
		if json.Unmarshal(m.Data, &ev) == nil && ev.BattleID == battleID {
			p.settles.Add(1)
		}
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("观测结算事件：订阅失败: %w", err)
	}
	if err := nc.Flush(); err != nil {
		nc.Close()
		return nil, fmt.Errorf("观测结算事件：flush 失败: %w", err)
	}
	return func() {
		_ = sub.Unsubscribe()
		nc.Close()
	}, nil
}

// watchStops 挂载本机运行时的 actor 停止钩子，只统计战斗 actor（重建一轮多一次停止）。
func (p *lateOpProbe) watchStops(st *stack) {
	if st == nil || st.bat == nil || st.bat.Runtime == nil {
		return
	}
	st.bat.Runtime.Raw().Local().AddStopHook(func(pid types.PID) {
		if pid.Type() == battlev1actor.BattleServiceActorType {
			p.stops.Add(1)
		}
	})
}

// assertOnce 核对「战斗 actor 起停=1、结算=1」：任何重建都会各多一次，
// 故不需要（也拿不到）逐个 spawn 计数——停止数即重建轮数的等价观测。
func (p *lateOpProbe) assertOnce(battleID string) error {
	stops, settles := p.stops.Load(), p.settles.Load()
	fmt.Printf("[迟到] 战斗 actor 停止=%d 结算事件=%d（battle=%s）\n", stops, settles, battleID)
	if stops != 1 {
		return fmt.Errorf("战斗 actor 停止 %d 次，期望 1（结算后迟到 op 重建了空名单实例）", stops)
	}
	if settles != 1 {
		return fmt.Errorf("结算事件 %d 次，期望 1（重建实例恢复快照后再次结算）", settles)
	}
	return nil
}

// runLateOp 执行「结算后继续发帧」形态：正常打到结算 → 同票重连 → 连续迟到 op → 核对收口。
func runLateOp(ctx context.Context, st *stack, a addrs, o directOpts, mw middlewareAddrs, frames uint64) error {
	kind, err := parseDirectTransport(o.transport)
	if err != nil {
		return err
	}
	addr, err := frameAddrOf(a, kind)
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
	probe := new(lateOpProbe)
	stopWatch, err := probe.watch(mw.natsURL, battleID)
	if err != nil {
		return err
	}
	defer stopWatch()
	probe.watchStops(st)

	clients, err := dialDirectPlayers(ctx, ps, tickets, kind, addr)
	if err != nil {
		return err
	}
	defer closeDirectPlayers(clients)
	fmt.Printf("[迟到] 传输=%s 帧端口=%s battle=%s（结算后继续发帧）\n", kind, addr, battleID)
	if err := playThenLateOps(ctx, ps, clients, tickets, kind, addr, battleID, frames); err != nil {
		return err
	}
	time.Sleep(rebuildQuietWindow)
	return probe.assertOnce(battleID)
}

// playThenLateOps 正常打完一局（等双方结算）→ 同票重连 → 连续迟到 op → 核对补投到位。
func playThenLateOps(ctx context.Context, ps [2]*player, clients [2]*directClient,
	tickets map[string][]byte, kind directTransport, addr, battleID string, frames uint64) error {
	for i, dc := range clients {
		if err := directJoin(ctx, dc, battleID, frames, stepOf(i)); err != nil {
			return err
		}
	}
	if err := waitDirectEnd(clients, ps[0].id); err != nil {
		return err
	}
	// 结算已发生：同票重连（原直连已被结算关闭），在**新连接**上继续发帧输入与保活。
	fresh, err := dialDirectPlayers(ctx, ps, tickets, kind, addr)
	if err != nil {
		return err
	}
	defer closeDirectPlayers(fresh)
	if err := lateOpsRejected(ctx, fresh[0], battleID); err != nil {
		return err
	}
	return waitFreshEnd(fresh[0], ps[0].id)
}

// frameAddrOf 按直连传输取帧面地址（进程内装配为随机端口，从装配句柄回捞）。
func frameAddrOf(a addrs, kind directTransport) (string, error) {
	switch kind {
	case directKCP:
		return a.battleKCP, nil
	case directUDP:
		return a.battleUDP, nil
	case directWS:
		return a.battleWS, nil
	default:
		return "", fmt.Errorf("未知直连传输 %q（kcp|udp|ws）", kind)
	}
}

// lateOpsRejected 连续发送迟到 op（帧输入 + 保活）并要求每条都被稳定 reason 拒绝：
// 传输错误（超时/连接被关）不算通过——那正是「反复丢弃客户端直连」的旧病。
func lateOpsRejected(ctx context.Context, dc *directClient, battleID string) error {
	for i := 0; i < lateOpRounds; i++ {
		frame := uint64(100 + i)
		if err := assertLateOpRejected(ctx, dc, battlev1opclient.BattleServiceProtocolOps.SendFrameInput,
			&battlev1.FrameInputReq{BattleId: battleID,
				Input: &locksteppb.LockstepInput{FrameId: frame, Payload: []byte{1}}}); err != nil {
			return err
		}
		if err := assertLateOpRejected(ctx, dc, battlev1opclient.BattleServiceProtocolOps.Ping,
			&battlev1.PingReq{BattleId: battleID}); err != nil {
			return err
		}
	}
	fmt.Printf("[迟到] %s 连续 %d 轮迟到 op 全被稳定拒绝（reason=%s）\n",
		dc.tag, lateOpRounds, errorv1.ReasonBattleEnded())
	return nil
}

// assertLateOpRejected 断言一条迟到 op 拿到稳定拒绝（BATTLE_ENDED）：可判定，SDK 据此停止发送。
func assertLateOpRejected(ctx context.Context, dc *directClient, operation string, req any) error {
	callCtx, cancel := context.WithTimeout(ctx, damageCallTimeout)
	defer cancel()
	err := dc.invoke(callCtx, operation, req, nil)
	if err == nil {
		return fmt.Errorf("%s 迟到 op %s 未被拒（战斗已结束仍被接受）", dc.tag, operation)
	}
	if !errorv1.IsBattleEnded(err) {
		return fmt.Errorf("%s 迟到 op %s 的拒绝不可判定: err=%v（reason=%s），期望 %s",
			dc.tag, operation, err, atlaserrors.Reason(err), errorv1.ReasonBattleEnded())
	}
	return nil
}

// waitFreshEnd 等待重连连接上的补投结果并核对胜者与首投一致（幂等：重复收到不出错）。
func waitFreshEnd(dc *directClient, want string) error {
	select {
	case n := <-dc.watcher.end:
		if n.GetWinnerPlayerId() != want {
			return fmt.Errorf("%s 补投结算胜者 = %q，期望 %q（首投与补投必须一致）",
				dc.tag, n.GetWinnerPlayerId(), want)
		}
		fmt.Printf("[迟到] %s 重连后补投到位（胜者 %s）\n", dc.tag, want)
		return nil
	case <-time.After(10 * time.Second):
		return fmt.Errorf("%s 重连后未收到补投的结算结果（错过推送的玩家仍不知道结果）", dc.tag)
	}
}
