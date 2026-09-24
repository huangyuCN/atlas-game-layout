package handler

import (
	"context"
	"testing"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/spanstest"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"go.opentelemetry.io/otel/codes"
)

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

// TestAdminHandlerSpans 验证管理面埋点：管理面动作产出 admin.Game.* span
// （属性带 operator/玩家/道具/数量），业务失败路径置 Error。
func TestAdminHandlerSpans(t *testing.T) {
	t.Run("发放成功", func(t *testing.T) {
		exp, restore := spanstest.Install()
		t.Cleanup(restore)

		f := newAdminFixture(100)
		if _, err := f.h.GrantItem(context.Background(), grantReq(writeCtx("gm-1", "k-1"), "p-1", 5)); err != nil {
			t.Fatalf("GrantItem: %v", err)
		}
		grant, ok := spanstest.ByName(exp)["admin.Game.GrantItem"]
		if !ok {
			t.Fatalf("缺少 admin.Game.GrantItem span: %v", spanstest.ByName(exp))
		}
		for key, want := range map[string]string{"admin.operator": "gm-1", "player.id": "p-1", "item.id": "1001"} {
			if got := spanstest.Attr(grant, key); got != want {
				t.Fatalf("GrantItem span 属性 %s = %q, 期望 %q", key, got, want)
			}
		}
		if grant.Status.Code == codes.Error {
			t.Fatal("成功路径不应置 Error")
		}
	})
	t.Run("业务失败", func(t *testing.T) {
		exp, restore := spanstest.Install()
		t.Cleanup(restore)

		f := newAdminFixture(100)
		f.state.err = errorv1.ErrPlayerNotFound("玩家不存在")
		if _, err := f.h.GrantItem(context.Background(), grantReq(writeCtx("gm-1", "k-2"), "p-404", 1)); err == nil {
			t.Fatal("业务失败应报错")
		}
		grant, ok := spanstest.ByName(exp)["admin.Game.GrantItem"]
		if !ok {
			t.Fatalf("缺少 admin.Game.GrantItem span: %v", spanstest.ByName(exp))
		}
		if grant.Status.Code != codes.Error {
			t.Fatalf("失败路径应置 Error，实际 %v", grant.Status.Code)
		}
		if got := spanstest.Attr(grant, "player.id"); got != "p-404" {
			t.Fatalf("GrantItem span 属性 player.id = %q, 期望 p-404", got)
		}
	})
}
