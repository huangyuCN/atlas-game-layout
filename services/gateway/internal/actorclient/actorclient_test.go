package actorclient

import (
	"context"
	"testing"

	"github.com/huangyuCN/atlas/contrib/actor/core"
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

func (m *mockRuntime) Ask(_ context.Context, pid types.PID, _ any, _ ...core.SendOption) (any, error) {
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

// TestPlayerInvokerAdaptsRuntime 验证 PlayerInvoker 适配：按玩家 ID 构造 PID
// 并透传 Ask/Tell（生成的 PlayerActorClient 经此调用集群运行时）。
func TestPlayerInvokerAdaptsRuntime(t *testing.T) {
	rt := &mockRuntime{reply: "reply-obj"}
	c := NewClient(rt)
	inv := c.PlayerInvoker()

	if err := inv.Tell(context.Background(), mustPID(t, "player", "i-1"), "msg"); err != nil {
		t.Fatalf("invoker Tell: %v", err)
	}
	if len(rt.tells) != 1 || rt.tells[0].String() != "player:i-1" {
		t.Fatalf("invoker Tell 路由不符: %v", rt.tells)
	}
	got, err := inv.Ask(context.Background(), mustPID(t, "player", "i-2"), "req")
	if err != nil {
		t.Fatalf("invoker Ask: %v", err)
	}
	if got != "reply-obj" {
		t.Fatalf("invoker Ask 回执 = %v", got)
	}
}

// TestBattleInvokerAdaptsRuntime 验证 BattleInvoker 适配按战斗 ID 构造 PID。
func TestBattleInvokerAdaptsRuntime(t *testing.T) {
	rt := &mockRuntime{}
	c := NewClient(rt)
	inv := c.BattleInvoker()

	if err := inv.Tell(context.Background(), mustPID(t, "battle", "i-3"), "msg"); err != nil {
		t.Fatalf("invoker Tell: %v", err)
	}
	if len(rt.tells) != 1 || rt.tells[0].String() != "battle:i-3" {
		t.Fatalf("invoker Tell 路由不符: %v", rt.tells)
	}
}

// mustPID 构造测试 PID（非法输入直接失败）。
func mustPID(t *testing.T, typ, uid string) types.PID {
	t.Helper()
	pid, err := types.NewPID(typ, uid)
	if err != nil {
		t.Fatalf("NewPID: %v", err)
	}
	return pid
}
