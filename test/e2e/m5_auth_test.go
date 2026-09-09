package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	gameassemble "github.com/huangyuCN/atlas-game-layout/services/game/assemble"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	tcpt "github.com/huangyuCN/atlas/transport/tcp"
	"google.golang.org/protobuf/encoding/protojson"
)

// testAccount 生成本轮测试唯一账号（mongo 数据残留隔离）。
func testAccount(t *testing.T, base string) string {
	t.Helper()
	return base + "-" + uuid.NewString()[:8]
}

// TestE2ERegisterLogin 验证闭环片段：注册 → 登录 → 心跳 → 背包 → 登出 → actor 停止。
func TestE2ERegisterLogin(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	game := newGame(t)
	gw := newGateway(t, "a")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli, err := tcpt.NewClient(gw.TCPURL)
	if err != nil {
		t.Fatalf("tcp client: %v", err)
	}
	defer cli.Close()
	auth := gatewayv1.NewGatewayAuthTCPClient(cli)
	account := testAccount(t, "alice")

	// 注册。
	reg, err := auth.Register(ctx, &gatewayv1.RegisterRequest{Account: account, Password: "pw", Nickname: "爱丽丝"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if reg.GetPlayerId() == "" {
		t.Fatal("注册回执缺少 player_id")
	}
	// 重复注册被拒。存量语义：注册临时实例自停并释放目录归属后，同账号重复注册
	// 在目录未命中窗口内返回 ACTOR_NOT_FOUND（发送侧无该类型 Props 不触发懒激活）；
	// 该行为改造前后一致。若需重复注册返回 PLAYER_ALREADY_EXISTS，需独立实现
	// 跨节点懒激活选型（见 2026-09-08-actor-proto-dispatch-codegen-design 已知事项）。
	if _, err := auth.Register(ctx, &gatewayv1.RegisterRequest{Account: account, Password: "pw"}); err == nil {
		t.Fatal("重复注册应失败")
	}

	// 登录。
	login, err := auth.Login(ctx, &gatewayv1.LoginRequest{PlayerId: reg.GetPlayerId(), Password: "pw"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if login.GetToken() == "" || login.GetPlayer().GetPlayerId() != reg.GetPlayerId() {
		t.Fatalf("登录回执不符: %+v", login)
	}
	// 心跳。
	if _, err := auth.Heartbeat(ctx, &gatewayv1.HeartbeatRequest{PlayerId: login.GetPlayerId(), Token: login.GetToken(), Ts: 1}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	// 背包示例（grpc 直连 game）。
	gcli, err := atlasgrpc.DialInsecure(ctx, atlasgrpc.WithEndpoint(game.GRPCURL))
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	defer gcli.Close()
	playerSvc := gamev1.NewPlayerClient(gcli)
	if _, err := playerSvc.GrantItem(ctx, &gamev1.GrantItemRequest{PlayerId: login.GetPlayerId(), ItemId: 1001, Count: 5, Reason: "e2e"}); err != nil {
		t.Fatalf("GrantItem: %v", err)
	}
	bp, err := playerSvc.GetBackpack(ctx, &gamev1.GetBackpackRequest{PlayerId: login.GetPlayerId()})
	if err != nil {
		t.Fatalf("GetBackpack: %v", err)
	}
	if len(bp.GetItems()) != 1 || bp.GetItems()[0].GetCount() != 5 {
		t.Fatalf("背包不符: %+v", bp.GetItems())
	}

	// 登出 → 联动 PlayerActor 停止（Locator 移除）。
	if _, err := auth.Logout(ctx, &gatewayv1.LogoutRequest{PlayerId: login.GetPlayerId(), Token: login.GetToken()}); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	waitActorStopped(t, game, login.GetPlayerId())
}

// TestE2ECrossGatewayKick 验证跨 gateway 重复登录挤下线 + actor 存活（新会话接管）。
func TestE2ECrossGatewayKick(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	game := newGame(t)
	gwA := newGateway(t, "a")
	gwB := newGateway(t, "b")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cliA, err := tcpt.NewClient(gwA.TCPURL)
	if err != nil {
		t.Fatalf("tcp A: %v", err)
	}
	defer cliA.Close()
	authA := gatewayv1.NewGatewayAuthTCPClient(cliA)
	account := testAccount(t, "bob")

	regA, err := authA.Register(ctx, &gatewayv1.RegisterRequest{Account: account, Password: "pw"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	loginA, err := authA.Login(ctx, &gatewayv1.LoginRequest{PlayerId: regA.GetPlayerId(), Password: "pw"})
	if err != nil {
		t.Fatalf("A 登录: %v", err)
	}
	kicked := make(chan struct{}, 1)
	cliA.OnNotify(func(operation string, payload []byte) {
		if operation != consts.PushOpKickedOffline {
			return
		}
		var kn gatewayv1.KickedNotify
		if err := protojson.Unmarshal(payload, &kn); err == nil && kn.GetReason() == "logged_in_elsewhere" {
			kicked <- struct{}{}
		}
	})

	// B 登录同一账号：A 旧连接被挤下线，PlayerActor 由新会话接管（不停止）。
	cliB, err := tcpt.NewClient(gwB.TCPURL)
	if err != nil {
		t.Fatalf("tcp B: %v", err)
	}
	defer cliB.Close()
	authB := gatewayv1.NewGatewayAuthTCPClient(cliB)
	loginB, err := authB.Login(ctx, &gatewayv1.LoginRequest{PlayerId: regA.GetPlayerId(), Password: "pw"})
	if err != nil {
		t.Fatalf("B 登录: %v", err)
	}
	select {
	case <-kicked:
	case <-time.After(3 * time.Second):
		t.Fatal("A 旧连接未收到被挤下线通知")
	}
	// A 旧令牌失效。
	if _, err := authA.Heartbeat(ctx, &gatewayv1.HeartbeatRequest{PlayerId: loginA.GetPlayerId(), Token: loginA.GetToken(), Ts: 1}); err == nil {
		t.Fatal("A 旧令牌心跳应失败")
	}
	// B 会话与 actor 均存活。
	if _, err := authB.Heartbeat(ctx, &gatewayv1.HeartbeatRequest{PlayerId: loginB.GetPlayerId(), Token: loginB.GetToken(), Ts: 1}); err != nil {
		t.Fatalf("B 心跳失败: %v", err)
	}
	if _, ok := game.Runtime.Raw().Local().Stats(playerPID(loginB.GetPlayerId())); !ok {
		t.Fatal("挤下线后 PlayerActor 不应停止（新会话接管）")
	}
}

// playerPID 构造玩家 actor PID。
func playerPID(playerID string) types.PID {
	pid, err := types.NewPID("player", playerID)
	if err != nil {
		panic(err)
	}
	return pid
}

// waitActorStopped 轮询直至 PlayerActor 停止（登出联动 Locator 移除）。
func waitActorStopped(t *testing.T, game *gameassemble.Game, playerID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := game.Runtime.Raw().Local().Stats(playerPID(playerID)); !ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("登出后 PlayerActor(%s) 未停止", playerID)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
