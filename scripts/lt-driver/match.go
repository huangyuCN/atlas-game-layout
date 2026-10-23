package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
)

// matchAll 让全部玩家入队并等待成局通知，返回阶段边界、已匹配玩家数与**真实局数**。
//
// 局数口径：按成局通知里的 match_id 去重（旧实现写死 matched/2，按 1v1 折算，
// 把 5v5 的 984 局读成 4,920 局——纯伪数）。人数口径本来就是真的，无需折算。
//
// 入池口径：以 qc.conc 并发入队（旧实现串行，单票 110–200ms 把入场压在 300–580/秒）；
// qc.prefill > 0 时先把前 N 张票以同一并发灌进池子、静默 qc.settle 再开匹配——
// 这样量的是「成局侧容量」而不是「入池速率」。
//
// 等待口径：全部玩家成局、或连续 quiet 无新成局（成局阶段收尾）、或 matchTimeout 到点
// 即立刻返回开打——旧实现一路等到 -connect-timeout（实测空等 276s，期间零成局）。
func matchAll(ctx context.Context, ps []*loadPlayer, matchTimeout, quiet time.Duration, qc queueCfg) (matchOutcome, error) {
	out := matchOutcome{}
	pre := min(max(qc.prefill, 0), len(ps))
	if pre > 0 {
		out.prefill = enterPhase(ctx, ps, 0, pre, qc.conc)
		fmt.Printf("[预灌] %d 票 / %d 并发：%.2fs（%.0f 票/秒，失败 %d）；静默 %v 后再开匹配\n",
			out.prefill.entered, qc.conc, out.prefill.dur(), out.prefill.qps(), out.prefill.failed, qc.settle)
		settle(qc.settle)
	}
	out.queue = enterPhase(ctx, ps, pre, len(ps), qc.conc)
	fmt.Printf("[入池] 其余 %d 票 / %d 并发：%.2fs（%.0f 票/秒，失败 %d）\n",
		out.queue.entered, qc.conc, out.queue.dur(), out.queue.qps(), out.queue.failed)
	out.match.from = time.Now()
	out.matched, out.battles = waitMatches(ctx, ps, matchTimeout, quiet)
	out.match.to = time.Now()
	if out.matched == 0 {
		return out, fmt.Errorf("无任何玩家收到成局通知（入队未匹配 / 出票未接线）")
	}
	return out, nil
}

// queueCfg 是入池阶段的装配参数（并发度 / 预灌票数 / 预灌后静默）。
type queueCfg struct {
	conc    int           // 入池并发度（-queue-concurrency）
	prefill int           // 预灌票数（-prefill，0 = 不预灌）
	settle  time.Duration // 预灌后静默（让池子积起来再开匹配）
}

// perBattleActual 返回每局实际人数（局数为 0 时返回 0，避免除零）。
func perBattleActual(matched, battles int) float64 {
	if battles <= 0 {
		return 0
	}
	return float64(matched) / float64(battles)
}

// waitMatches 等待成局阶段收尾，返回已匹配玩家数与按 match_id 去重的局数。
// 结束条件见 collectMatches；返回前等全部监听协程退出，避免与后续读 battleID 竞争。
func waitMatches(ctx context.Context, ps []*loadPlayer, matchTimeout, quiet time.Duration) (int, int) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan *gamev1.MatchStartedNotify, len(ps))
	var wg sync.WaitGroup
	for _, p := range ps {
		wg.Add(1)
		go func(p *loadPlayer) {
			defer wg.Done()
			n, err := p.waitStarted(ctx, matchTimeout)
			if err != nil {
				return
			}
			done <- n
		}(p)
	}
	matched, battles := collectMatches(ctx, done, len(ps), matchTimeout, quiet)
	cancel()
	wg.Wait()
	return matched, battles
}

// collectMatches 汇聚成局通知：全部到齐、或静默 quiet、或 matchTimeout 到点即收尾。
// 局数按 match_id 去重（match_id 缺失时退回 battle_id），与「人数/2」无关。
func collectMatches(ctx context.Context, done <-chan *gamev1.MatchStartedNotify, want int,
	matchTimeout, quiet time.Duration) (int, int) {
	deadline := time.NewTimer(matchTimeout)
	defer deadline.Stop()
	idle := time.NewTimer(quiet)
	defer idle.Stop()
	ids := make(map[string]struct{}, want/2)
	matched := 0
	for matched < want {
		select {
		case n := <-done:
			matched++
			id := n.GetMatchId()
			if id == "" {
				id = n.GetBattleId()
			}
			ids[id] = struct{}{}
			idle.Reset(quiet)
		case <-idle.C:
			return matched, len(ids)
		case <-deadline.C:
			return matched, len(ids)
		case <-ctx.Done():
			return matched, len(ids)
		}
	}
	return matched, len(ids)
}

// matchedPlayers 返回已拿到票据的玩家（未匹配者不参与帧面负载）。
func matchedPlayers(ps []*loadPlayer) []*loadPlayer {
	out := make([]*loadPlayer, 0, len(ps))
	for _, p := range ps {
		if p.battleID != "" && len(p.ticket) > 0 {
			out = append(out, p)
		}
	}
	return out
}
