package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	admingamev1 "github.com/huangyuCN/atlas-game-layout/api/admin/game/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	gameassemble "github.com/huangyuCN/atlas-game-layout/services/game/assemble"
	sdkclient "github.com/huangyuCN/atlas-sdk-go/client"
	"github.com/huangyuCN/atlas/contrib/actor/types"
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
	rawReg, err := sess.Register(ctx, &gatewayv1.RegisterRequest{Account: account, Password: "pw", Nickname: "爱丽丝"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	reg := replyOf[*gatewayv1.RegisterReply](t, rawReg)
	if reg.GetPlayerId() == "" {
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
	if _, err := sess.Login(ctx, &gatewayv1.LoginRequest{PlayerId: reg.GetPlayerId(), Password: "pw"}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	if sess.Token() == "" || sess.PlayerID() != reg.GetPlayerId() {
		t.Fatalf("登录凭据不符: token=%q player=%q", sess.Token(), sess.PlayerID())
	}
	// 心跳（Session 手动单次心跳：会话续租往返）。
	if _, err := sess.Heartbeat(ctx); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	// 背包校验（P7 接管）：旧 service Player 的 gRPC/REST 管理入口已按统一设计删除，
	// 发放与查询改由管理面（admin.game.v1.AdminService，仅 internal 面可达）覆盖。
	admin := dialAdmin(t, game.GRPCURL)
	grant, err := admin.GrantItem(ctx, &admingamev1.GrantItemRequest{
		Context:  adminCtx("e2e-m5", "m5-grant-"+uuid.NewString(), false),
		PlayerId: reg.GetPlayerId(), ItemId: 1001, Count: 5,
	})
	if err != nil {
		t.Fatalf("管理面 GrantItem: %v", err)
	}
	if !grant.GetApplied() || grant.GetReplayed() || grant.GetAuditId() == "" {
		t.Fatalf("管理面发放回执不符（applied=true/replayed=false/audit_id 非空）: %+v", grant)
	}
	bag, err := admin.QueryBackpack(ctx, &admingamev1.QueryBackpackRequest{
		Context: adminCtx("e2e-m5", "", false), PlayerId: reg.GetPlayerId(),
	})
	if err != nil {
		t.Fatalf("管理面 QueryBackpack: %v", err)
	}
	if got := itemCount(bag.GetItems(), 1001); got != 5 {
		t.Fatalf("背包道具 1001 数量 = %d, 期望 5", got)
	}

	// 登出 → 联动 PlayerActor 停止（Locator 移除）。
	logoutFlow(t, ctx, sess)
	waitActorStopped(t, game, reg.GetPlayerId())
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

	// 被挤下线原因经 SDK 会话接缝（SessionProtocol.Kicked）取得：接缝按推送信封的帧头版本
	// 选解码器（S0.5 修订 1），故断言的是「版本分派后解出的原因枚举名」，不是裸字节。
	kicked := make(chan string, 1)
	sessA, _ := dialBizSessionWith(t, gwA.TCPURL, []sdkclient.SessionOption{
		sdkclient.WithOnKicked(func(reason string) {
			select {
			case kicked <- reason:
			default:
			}
		}),
	})
	account := testAccount(t, "bob")

	rawRegA, err := sessA.Register(ctx, &gatewayv1.RegisterRequest{Account: account, Password: "pw"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	playerA := replyOf[*gatewayv1.RegisterReply](t, rawRegA).GetPlayerId()
	if _, err := sessA.Login(ctx, &gatewayv1.LoginRequest{PlayerId: playerA, Password: "pw"}); err != nil {
		t.Fatalf("A 登录: %v", err)
	}
	sessTokenBeforeKick := sessA.Token() // 快照：顶号后验证旧凭据被服务端拒绝

	// B 登录同一账号：A 旧连接被挤下线，PlayerActor 由新会话接管（不停止）。
	sessB, _ := dialBizSession(t, gwB.TCPURL)
	if _, err := sessB.Login(ctx, &gatewayv1.LoginRequest{PlayerId: playerA, Password: "pw"}); err != nil {
		t.Fatalf("B 登录: %v", err)
	}
	select {
	case reason := <-kicked:
		if want := gatewayv1.KickedReason_KICKED_REASON_LOGGED_IN_ELSEWHERE.String(); reason != want {
			t.Fatalf("被挤下线原因 = %q, 期望 %q（接缝须按帧头版本解码，ver=2 不得静默丢失）", reason, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("A 旧连接未收到被挤下线通知")
	}
	// A 旧凭据被服务端拒绝（Kicked 后 SDK 已自动清凭据，Restore 显式携带旧凭据）。
	if _, err := sessA.Restore(ctx, sessTokenBeforeKick, playerA); err == nil {
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
