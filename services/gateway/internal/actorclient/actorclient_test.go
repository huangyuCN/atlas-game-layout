package actorclient

import (
	"context"
	"testing"

	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// mockRuntime 记录调用并返回预置结果。
type mockRuntime struct {
	tells []types.PID
	asks  []types.PID
	reply any
}

func (m *mockRuntime) Tell(_ context.Context, pid types.PID, _ any) error {
	m.tells = append(m.tells, pid)
	return nil
}

func (m *mockRuntime) Ask(_ context.Context, pid types.PID, _ any) (any, error) {
	m.asks = append(m.asks, pid)
	return m.reply, nil
}

// TestPlayerPID 验证玩家 actor PID 构造（路由 game PlayerActor）。
func TestPlayerPID(t *testing.T) {
	pid, err := PlayerPID("p-0001")
	if err != nil {
		t.Fatalf("PlayerPID: %v", err)
	}
	if got := pid.String(); got != "player:p-0001" {
		t.Fatalf("PID = %q, want player:p-0001", got)
	}
	if _, err := PlayerPID("bad:uid"); err == nil {
		t.Fatal("非法玩家 ID 应报错")
	}
}

// TestBattlePID 验证战斗 actor PID 构造（路由 battle 战斗 actor）。
func TestBattlePID(t *testing.T) {
	pid, err := BattlePID("b-0001")
	if err != nil {
		t.Fatalf("BattlePID: %v", err)
	}
	if got := pid.String(); got != "battle:b-0001" {
		t.Fatalf("PID = %q, want battle:b-0001", got)
	}
}

// TestTellPlayerRoutesToGame 验证业务消息路由到 game 玩家 actor。
func TestTellPlayerRoutesToGame(t *testing.T) {
	rt := &mockRuntime{}
	c := NewClient(rt)
	if err := c.TellPlayer(context.Background(), "p-1", "msg"); err != nil {
		t.Fatalf("TellPlayer: %v", err)
	}
	if len(rt.tells) != 1 || rt.tells[0].String() != "player:p-1" {
		t.Fatalf("Tell 路由不符: %v", rt.tells)
	}
}

// TestAskBattleRoutesToBattle 验证请求消息路由到 battle 战斗 actor。
func TestAskBattleRoutesToBattle(t *testing.T) {
	rt := &mockRuntime{reply: "ok"}
	c := NewClient(rt)
	got, err := c.AskBattle(context.Background(), "b-9", "req")
	if err != nil {
		t.Fatalf("AskBattle: %v", err)
	}
	if got != "ok" {
		t.Fatalf("AskBattle 回执 = %v, want ok", got)
	}
	if len(rt.asks) != 1 || rt.asks[0].String() != "battle:b-9" {
		t.Fatalf("Ask 路由不符: %v", rt.asks)
	}
}

// TestAskPlayerRoutesToGame 验证玩家 actor 请求路由。
func TestAskPlayerRoutesToGame(t *testing.T) {
	rt := &mockRuntime{}
	c := NewClient(rt)
	if _, err := c.AskPlayer(context.Background(), "p-2", "req"); err != nil {
		t.Fatalf("AskPlayer: %v", err)
	}
	if len(rt.asks) != 1 || rt.asks[0].String() != "player:p-2" {
		t.Fatalf("Ask 路由不符: %v", rt.asks)
	}
}
