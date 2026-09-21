package handler

import (
	"context"
	"testing"

	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/spanstest"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"go.opentelemetry.io/otel/codes"
)

// fakeStateAccess 是 biz.PlayerStateAccess 的假实现（记录调用并回放错误）。
type fakeStateAccess struct{ err error }

// GrantItem 回放注入的错误。
func (f fakeStateAccess) GrantItem(context.Context, string, uint32, uint32, string) error {
	return f.err
}

// GetBackpack 返回空背包。
func (f fakeStateAccess) GetBackpack(context.Context, string) ([]*gamev1.BackpackItem, error) {
	return nil, f.err
}

// GetPlayer 返回固定摘要。
func (f fakeStateAccess) GetPlayer(_ context.Context, playerID string) (*commonv1.PlayerSummary, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &commonv1.PlayerSummary{PlayerId: playerID}, nil
}

// TestPlayerHandlerSpans 验证玩家注册/登录埋点：成功与失败路径都产出业务 span。
func TestPlayerHandlerSpans(t *testing.T) {
	exp, restore := spanstest.Install()
	t.Cleanup(restore)

	h := NewPlayerHandler(newMemRepo(), biz.PlayerServiceOptions{
		NewPlayerID: func() string { return "p-1" },
	})
	ctx := context.Background()

	if _, err := h.Register(ctx, &gamev1.RegisterReq{Account: "acc-1", Password: "pw"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	// 失败路径：缺口令 → InvalidParams。
	if _, err := h.Login(ctx, &gamev1.LoginReq{PlayerId: "p-1"}); err == nil {
		t.Fatal("缺口令登录应报错")
	}

	spans := spanstest.ByName(exp)
	reg, ok := spans["game.Player.Register"]
	if !ok {
		t.Fatalf("缺少 game.Player.Register span: %v", spans)
	}
	if got := spanstest.Attr(reg, "player.account"); got != "acc-1" {
		t.Fatalf("Register span 属性 player.account = %q, 期望 acc-1", got)
	}
	login, ok := spans["game.Player.Login"]
	if !ok {
		t.Fatalf("缺少 game.Player.Login span: %v", spans)
	}
	if login.Status.Code != codes.Error {
		t.Fatalf("登录失败路径应置 Error，实际 %v", login.Status.Code)
	}
}

// TestGameHandlerSpans 验证查询/发道具埋点（含 item.id 属性）。
func TestGameHandlerSpans(t *testing.T) {
	exp, restore := spanstest.Install()
	t.Cleanup(restore)

	h := NewGameHandler(fakeStateAccess{})
	ctx := context.Background()

	if _, err := h.GetPlayer(ctx, &gamev1.GetPlayerRequest{PlayerId: "p-1"}); err != nil {
		t.Fatalf("GetPlayer: %v", err)
	}
	if _, err := h.GrantItem(ctx, &gamev1.GrantItemRequest{PlayerId: "p-1", ItemId: 7, Count: 2}); err != nil {
		t.Fatalf("GrantItem: %v", err)
	}

	spans := spanstest.ByName(exp)
	got, ok := spans["game.Game.GetPlayer"]
	if !ok || spanstest.Attr(got, "player.id") != "p-1" {
		t.Fatalf("game.Game.GetPlayer span 不符: %v", spans)
	}
	grant, ok := spans["game.Game.GrantItem"]
	if !ok || spanstest.Attr(grant, "item.id") != "7" {
		t.Fatalf("game.Game.GrantItem span 不符: %v", spans)
	}
}
