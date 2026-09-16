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
	"google.golang.org/protobuf/encoding/protojson"
)

// testAccount 生成本轮测试唯一账号（mongo 数据残留隔离）。
func testAccount(t *testing.T, base string) string {
	t.Helper()
	return base + "-" + uuid.NewString()[:8]
}

// TestE2ERegisterLogin 验证闭环片段（SDK 会话驱动）：注册 → 登录 → 心跳 → 背包 → 登出 → actor 停止。
func TestE2ERegisterLogin(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	game := newGame(t)
	gw := newGateway(t, "a")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sess, _ := dialBizSession(t, gw.TCPURL)
	account := testAccount(t, "alice")

	// 注册。
	reg, err := sess.Register(ctx, &gatewayv1.RegisterRequest{Account: account, Password: "pw", Nickname: "爱丽丝"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if reg.PlayerID == "" {
		t.Fatal("注册回执缺少 player_id")
	}
	// 重复注册被拒。存量语义：注册临时实例自停并释放目录归属后，同账号重复注册
	// 在目录未命中窗口内返回 ACTOR_NOT_FOUND（发送侧无该类型 Props 不触发懒激活）；
	// 该行为改造前后一致。若需重复注册返回 PLAYER_ALREADY_EXISTS，需独立实现
	// 跨节点懒激活选型（见 2026-09-08-actor-proto-dispatch-codegen-design 已知事项）。
	if _, err := sess.Register(ctx, &gatewayv1.RegisterRequest{Account: account, Password: "pw"}); err == nil {
		t.Fatal("重复注册应失败")
	}

	// 登录（SDK 会话保管回执凭据）。
	if _, err := sess.Login(ctx, &gatewayv1.LoginRequest{PlayerId: reg.PlayerID, Password: "pw"}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	if sess.Token() == "" || sess.PlayerID() != reg.PlayerID {
		t.Fatalf("登录凭据不符: token=%q player=%q", sess.Token(), sess.PlayerID())
	}
	// 心跳（Session 手动单次心跳：会话续租往返）。
	if _, err := sess.Heartbeat(ctx); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	// 背包示例（grpc 直连 game 管理面）。
	gcli, err := atlasgrpc.DialInsecure(ctx, atlasgrpc.WithEndpoint(game.GRPCURL))
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	defer gcli.Close()
	playerSvc := gamev1.NewPlayerClient(gcli)
	if _, err := playerSvc.GrantItem(ctx, &gamev1.GrantItemRequest{PlayerId: sess.PlayerID(), ItemId: 1001, Count: 5, Reason: "e2e"}); err != nil {
		t.Fatalf("GrantItem: %v", err)
	}
	bp, err := playerSvc.GetBackpack(ctx, &gamev1.GetBackpackRequest{PlayerId: sess.PlayerID()})
	if err != nil {
		t.Fatalf("GetBackpack: %v", err)
	}
	if len(bp.GetItems()) != 1 || bp.GetItems()[0].GetCount() != 5 {
		t.Fatalf("背包不符: %+v", bp.GetItems())
	}

	// 登出 → 联动 PlayerActor 停止（Locator 移除）。
	logoutFlow(t, ctx, sess)
	waitActorStopped(t, game, reg.PlayerID)
}

// TestE2ECrossGatewayKick 验证跨 gateway 顶号（SDK 会话驱动）：旧端收 KickedNotify、
// 旧凭据请求被拒，PlayerActor 由新会话接管（不停止）。
func TestE2ECrossGatewayKick(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	game := newGame(t)
	gwA := newGateway(t, "a")
	gwB := newGateway(t, "b")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sessA, cliA := dialBizSession(t, gwA.TCPURL)
	account := testAccount(t, "bob")

	regA, err := sessA.Register(ctx, &gatewayv1.RegisterRequest{Account: account, Password: "pw"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := sessA.Login(ctx, &gatewayv1.LoginRequest{PlayerId: regA.PlayerID, Password: "pw"}); err != nil {
		t.Fatalf("A 登录: %v", err)
	}
	sessTokenBeforeKick := sessA.Token() // 快照：顶号后验证旧凭据被服务端拒绝
	kicked := make(chan struct{}, 1)
	cliA.On(consts.PushOpKickedOffline, func(operation string, payload []byte) {
		var kn gatewayv1.KickedNotify
		if err := protojson.Unmarshal(payload, &kn); err == nil && kn.GetReason() == gatewayv1.KickedReason_KICKED_REASON_LOGGED_IN_ELSEWHERE {
			kicked <- struct{}{}
		}
	})

	// B 登录同一账号：A 旧连接被挤下线，PlayerActor 由新会话接管（不停止）。
	sessB, _ := dialBizSession(t, gwB.TCPURL)
	if _, err := sessB.Login(ctx, &gatewayv1.LoginRequest{PlayerId: regA.PlayerID, Password: "pw"}); err != nil {
		t.Fatalf("B 登录: %v", err)
	}
	select {
	case <-kicked:
	case <-time.After(3 * time.Second):
		t.Fatal("A 旧连接未收到被挤下线通知")
	}
	// A 旧凭据被服务端拒绝（Kicked 后 SDK 已自动清凭据，Restore 显式携带旧凭据）。
	if _, err := sessA.Restore(ctx, sessTokenBeforeKick, regA.PlayerID); err == nil {
		t.Fatal("A 旧凭据恢复应被服务端拒绝")
	}
	// B 会话与 actor 均存活（会话续租往返）。
	if _, err := sessB.Heartbeat(ctx); err != nil {
		t.Fatalf("B 心跳失败: %v", err)
	}
	if _, ok := game.Runtime.Raw().Local().Stats(playerPID(sessB.PlayerID())); !ok {
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
