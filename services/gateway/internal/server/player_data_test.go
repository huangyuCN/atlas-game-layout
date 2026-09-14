// 玩家数据同步（GatewayPlayer/GetPlayerData）转发测试：登录后全量拉取
// （摘要 + 背包聚合根内存态）；无效令牌拒绝。
package server

import (
	"context"
	"testing"
	"time"

	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	atlaserrors "github.com/huangyuCN/atlas/errors"
)

// TestGetPlayerDataForwardsToPlayerActor 验证玩家数据同步转发：
// 有效令牌聚合回执（摘要 + 背包），无效令牌拒绝。
func TestGetPlayerDataForwardsToPlayerActor(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)
	cli, auth := env.newTCPAuthClient(t)
	playerCli := gatewayv1.NewGatewayPlayerTCPClient(cli)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	login, err := auth.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p-1", Password: "x"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}

	// 无效令牌拒绝。
	if _, err := playerCli.GetPlayerData(ctx, &gatewayv1.PlayerDataRequest{Token: "bad", PlayerId: "p-1"}); atlaserrors.Reason(err) != "INVALID_TOKEN" {
		t.Fatalf("无效令牌应拒绝, got %v", err)
	}

	// 有效令牌：聚合回执（摘要 + 背包）。
	rep, err := playerCli.GetPlayerData(ctx, &gatewayv1.PlayerDataRequest{Token: login.GetToken(), PlayerId: "p-1"})
	if err != nil {
		t.Fatalf("GetPlayerData: %v", err)
	}
	if rep.GetPlayer().GetPlayerId() != "p-1" || len(rep.GetItems()) != 1 || rep.GetItems()[0].GetItemId() != 1001 {
		t.Fatalf("数据同步回执不符: %+v", rep)
	}
}
