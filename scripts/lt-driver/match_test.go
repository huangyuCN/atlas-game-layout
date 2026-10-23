package main

import (
	"context"
	"testing"
	"time"

	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
)

// notify 构造一条成局通知（局数口径只看 match_id，票据另有用例覆盖）。
func notify(matchID, battleID string) *gamev1.MatchStartedNotify {
	return &gamev1.MatchStartedNotify{MatchId: matchID, BattleId: battleID}
}

// TestCollectMatches_CountsDistinctMatchIDs 验证局数按 match_id 去重统计：
// 10 名玩家落进 2 局 5v5 就是 2 局——旧实现 matched/2 会读成 5 局（伪数）。
func TestCollectMatches_CountsDistinctMatchIDs(t *testing.T) {
	done := make(chan *gamev1.MatchStartedNotify, 10)
	for i := 0; i < 5; i++ {
		done <- notify("m-1", "b-1")
		done <- notify("m-2", "b-2")
	}
	matched, battles := collectMatches(context.Background(), done, 10, time.Second, time.Second)
	if matched != 10 {
		t.Fatalf("匹配玩家数 = %d, 期望 10", matched)
	}
	if battles != 2 {
		t.Fatalf("局数 = %d, 期望 2（10 人 5v5 两局；旧实现 matched/2 = 5）", battles)
	}
}

// TestCollectMatches_FallsBackToBattleID 验证 match_id 缺失时退回 battle_id 去重。
func TestCollectMatches_FallsBackToBattleID(t *testing.T) {
	done := make(chan *gamev1.MatchStartedNotify, 2)
	done <- notify("", "b-1")
	done <- notify("", "b-2")
	_, battles := collectMatches(context.Background(), done, 2, time.Second, time.Second)
	if battles != 2 {
		t.Fatalf("局数 = %d, 期望 2（回退 battle_id 去重）", battles)
	}
}

// TestCollectMatches_QuietExitBeatsTimeout 验证「成局静默即开打」：
// 静默期到点立刻返回，不等满 matchTimeout（旧实现会一路等到 -connect-timeout 到点）。
func TestCollectMatches_QuietExitBeatsTimeout(t *testing.T) {
	done := make(chan *gamev1.MatchStartedNotify, 10)
	done <- notify("m-1", "b-1")
	done <- notify("m-1", "b-1")
	start := time.Now()
	matched, battles := collectMatches(context.Background(), done, 100, 30*time.Second, 50*time.Millisecond)
	if spent := time.Since(start); spent > 2*time.Second {
		t.Fatalf("静默收尾耗时 %v, 期望远小于 matchTimeout", spent)
	}
	if matched != 2 || battles != 1 {
		t.Fatalf("matched/battles = %d/%d, 期望 2/1", matched, battles)
	}
}

// TestCollectMatches_StopsAtTimeout 验证一直无成局时到点收尾（不死等）。
func TestCollectMatches_StopsAtTimeout(t *testing.T) {
	done := make(chan *gamev1.MatchStartedNotify, 1)
	start := time.Now()
	matched, battles := collectMatches(context.Background(), done, 4, 60*time.Millisecond, time.Second)
	if spent := time.Since(start); spent > time.Second {
		t.Fatalf("到点收尾耗时 %v, 期望 ≈ matchTimeout", spent)
	}
	if matched != 0 || battles != 0 {
		t.Fatalf("matched/battles = %d/%d, 期望 0/0", matched, battles)
	}
}
