// 入座窗口（seat window）：成局后不立刻开打，而是「坐满再开打」。
//
// 为什么需要它：旧行为「成局即开打」让对局时钟在门票刚发出时就开始走，而客户端从收到
// 成局通知到建连、握手、发出首帧要 1–2s——这段时间被算进对局寿命（缺省 60 帧 × 100ms
// = 6s），于是晚到玩家拿到的是 BATTLE_ENDED（压测一轮 9,986 次）。入座窗口把
// 「客户端还没到场」的时间从对局时间里扣掉。
//
// 语义（与配置 battle.seat_window_ms 一一对应）：
//   - 成局（Admit）即暂停 lockstep 会话：帧号停在 0，不产生任何对局时间；
//   - 每有一名参战者的帧连接建立（连接生命周期 PlayerOnline）即记一个座位，**坐满立即开打**；
//   - 窗口到期仍未坐满：不再等待，立即开打（outcome=timeout）。未入座者**不剔除、不重配**——
//     他在对局结束前仍可加入并参与，只是对局不再等他（迟到者的确定性回执仍是 BATTLE_ENDED）；
//   - seat_window_ms < 0 或未配置为 0（配置层语义见 conf.proto）：窗口关闭，成局即开打（旧行为）。
//
// 窗口内掉线会释放座位（重新回到等入座），窗口截止时刻不变。

package actor

import (
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/lockstep"
)

// seatOutcome 是入座窗口的结束原因（指标标签与日志共用同一份字面量）。
type seatOutcome string

const (
	// seatOutcomeAllSeated 是「全部参战者入座，立即开打」。
	seatOutcomeAllSeated seatOutcome = "all_seated"
	// seatOutcomeTimeout 是「窗口到期仍有未入座者，不再等待」。
	seatOutcomeTimeout seatOutcome = "timeout"
)

// seatWindowExpired 是窗口到期消息（actor 定时器自发自收，不新起 goroutine）。
type seatWindowExpired struct{}

// beginSeating 打开入座窗口：先暂停对局时钟，再等入座或窗口到期。
// 窗口关闭（≤0）或名单为空时什么都不做——成局即开打（旧行为、无额外开销）。
func (b *BattleActor) beginSeating(ctx core.ActorContext) {
	if b.cfg.SeatWindow <= 0 || len(b.players) == 0 {
		return
	}
	if err := b.rt.Tell(ctx.Context(), b.sessionPID, lockstep.PauseSession{Reason: "seat_window"}); err != nil {
		// 暂停失败按「窗口关闭」处理：宁可早开打，也不要让对局悬着等一个不存在的时钟。
		return
	}
	b.seating = true
	b.seatTimer = ctx.After(b.cfg.SeatWindow, seatWindowExpired{})
}

// takeSeat 记一个座位；坐满立即开打。
func (b *BattleActor) takeSeat(ctx core.ActorContext, playerID string) {
	if !b.seating {
		return
	}
	if _, ok := b.players[playerID]; !ok {
		return
	}
	b.seated[playerID] = struct{}{}
	if len(b.seated) >= len(b.players) {
		b.startBattle(ctx, seatOutcomeAllSeated)
	}
}

// releaseSeat 释放一个座位（窗口内掉线者重新回到「等入座」，截止时刻不变）。
func (b *BattleActor) releaseSeat(playerID string) {
	if !b.seating {
		return
	}
	delete(b.seated, playerID)
}

// onSeatWindowExpired 实现本地消息：窗口到期，不再等未入座者。
func (b *BattleActor) onSeatWindowExpired(ctx core.ActorContext, _ seatWindowExpired) error {
	b.startBattle(ctx, seatOutcomeTimeout)
	return nil
}

// startBattle 结束入座窗口并恢复对局时钟（幂等：坐满与到期只有先到的那次生效）。
func (b *BattleActor) startBattle(ctx core.ActorContext, outcome seatOutcome) {
	if !b.seating {
		return
	}
	b.cancelSeating()
	b.metrics.Counter(MetricSeatWindow, "outcome", string(outcome)).Add(1)
	ctx.Logger().Info("battle: 入座窗口结束", "battle", b.battleID,
		"outcome", string(outcome), "seated", len(b.seated), "players", len(b.players))
	_ = b.rt.Tell(ctx.Context(), b.sessionPID, lockstep.ResumeSession{})
}

// cancelSeating 只收敛窗口状态（结算等提前结束对局的路径调用，不打点、不恢复时钟）。
func (b *BattleActor) cancelSeating() {
	b.seating = false
	if b.seatTimer != nil {
		b.seatTimer()
		b.seatTimer = nil
	}
}
