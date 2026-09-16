// party.go 是组队形态（-mode party）：4 客户端 2v2 成局 + 整队取消路径。
package main

import (
	"context"
	"fmt"
	"time"

	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
)

// runParty 组队闭环：A 建队 → B 加入（双方收名册推送）→ A 整队入队 →
// C/D solo 入队 → 2v2 成局 → 帧同步 → 结算；随后取消路径：C 重建队入队再离开，
// 收整队取消的失败通知（PartyRosterNotify 的 leave 推送一并校验）。
func runParty(ctx context.Context, a addrs, frames uint64) error {
	ps, err := connectN(4, a)
	if err != nil {
		return err
	}
	a1, b, c, d := ps[0], ps[1], ps[2], ps[3]
	for i, p := range ps {
		if err := p.registerLogin(ctx, byte(i)); err != nil {
			return err
		}
	}

	// A 建队 → B 加入。
	cp, err := a1.players.CreateParty(ctx, &gamev1.CreatePartyReq{})
	if err != nil {
		return fmt.Errorf("A 建队: %w", err)
	}
	partyID := cp.GetPartyId()
	fmt.Printf("[组队] A 建队 ok（party=%s leader=%s）\n", partyID, cp.GetLeaderId())
	if _, err := b.players.JoinParty(ctx, &gamev1.JoinPartyReq{PartyId: partyID}); err != nil {
		return fmt.Errorf("B 加入: %w", err)
	}
	for _, p := range []*player{b, a1} {
		if err := waitRoster(p, matcherv1.PartyRosterReason_PARTY_ROSTER_REASON_JOIN); err != nil {
			return err
		}
	}

	// A 整队入队 → C/D solo 入队 → 2v2 成局。
	if _, err := a1.players.QueueParty(ctx, &gamev1.QueuePartyReq{Ruleset: "casual"}); err != nil {
		return fmt.Errorf("A 整队入队: %w", err)
	}
	fmt.Println("[组队] A 整队入队，C/D solo 入队")
	for _, p := range []*player{c, d} {
		if _, err := p.players.EnterMatchQueue(ctx, &gamev1.EnterMatchQueueReq{Ruleset: "casual"}); err != nil {
			return fmt.Errorf("%s 入队: %w", p.id, err)
		}
	}
	battleID, err := waitStarted(a1, b, c, d)
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
	return partyCancelPath(ctx, c)
}

// partyCancelPath 取消路径：C 重建队 → 整队入队 → 离开 → 收整队取消的失败通知。
func partyCancelPath(ctx context.Context, c *player) error {
	if _, err := c.players.CreateParty(ctx, &gamev1.CreatePartyReq{}); err != nil {
		return fmt.Errorf("C 取消路径建队: %w", err)
	}
	if _, err := c.players.QueueParty(ctx, &gamev1.QueuePartyReq{Ruleset: "casual"}); err != nil {
		return fmt.Errorf("C 取消路径入队: %w", err)
	}
	if _, err := c.players.LeaveParty(ctx, &gamev1.LeavePartyReq{}); err != nil {
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

// connectN 建立 n 个客户端（奇数下标走单通道 WS，偶数走双通道，覆盖两形态）。
func connectN(n int, a addrs) (ps []*player, err error) {
	for i := 0; i < n; i++ {
		var p *player
		if i%2 == 1 {
			p, err = newSinglePlayer(a.ws) // 与 [2] 形态对齐：奇数下标走单通道（示例覆盖）
		} else {
			p, err = newDualPlayer(a.tcp, a.kcp)
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
	for time.Now().Before(deadline) {
		select {
		case n := <-p.ros:
			if n.GetReason() == wantReason {
				return nil
			}
		case <-time.After(time.Until(deadline)):
			return fmt.Errorf("%s 未收到名册推送 %s", p.id, wantReason.String())
		}
	}
	return fmt.Errorf("%s 未收到名册推送 %s", p.id, wantReason.String())
}

// sendBattleInputsN 多人形态的输入：全员加入战斗后各自发送帧（payload 步进区分玩家）。
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
