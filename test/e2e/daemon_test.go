package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
)

// TestDaemonProbe 是**守护形态**回归用例：经真实守护进程（而非进程内装配）跑
// 注册 → 登录 → 心跳 → 路由 TTL 断言 → 登出。
//
// 为什么单列一条：守护形态与进程内形态的差异（actor NodeID、命名空间、NATS subject）
// 曾让守护形态的注册链路整体不可用（ACTOR_NOT_FOUND，见
// docs/superpowers/specs/2026-09-22-actor-node-identity-and-send-only-client-design.md），
// 而既有 e2e 全部跑进程内形态，覆盖不到这条路径。
//
// 环境变量：
//   - ATLAS_DAEMON_ADDR（必填，否则跳过）：守护 gateway 业务通道地址，如 127.0.0.1:9001
//   - ATLAS_DAEMON_REDIS（可选，默认 127.0.0.1:16379）：用于断言会话路由键 TTL
//   - ATLAS_DAEMON_TTL（可选，默认 30s）：期望的会话租期（= 守护进程 session.ttl 配置）
//   - ATLAS_DAEMON_NS（可选，默认 test）：守护进程的 redis 键命名空间（= 其 runtime.env 或 actor_namespace）
func TestDaemonProbe(t *testing.T) {
	addr := os.Getenv("ATLAS_DAEMON_ADDR")
	if addr == "" {
		t.Skip("未设置 ATLAS_DAEMON_ADDR，跳过守护形态用例")
	}
	redisAddr := os.Getenv("ATLAS_DAEMON_REDIS")
	if redisAddr == "" {
		redisAddr = itRedisAddr
	}
	// 守护进程的业务键命名空间：默认取模板 config.yaml 的 `env: test`，
	// 部署改了 env（或显式配了 runtime.actor_namespace）时用 ATLAS_DAEMON_NS 覆盖。
	daemonNS := os.Getenv("ATLAS_DAEMON_NS")
	if daemonNS == "" {
		daemonNS = "test"
	}
	daemonKeys := pkredis.NewKeys(daemonNS)
	wantTTL := 30 * time.Second
	if v := os.Getenv("ATLAS_DAEMON_TTL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatalf("ATLAS_DAEMON_TTL 解析失败: %v", err)
		}
		wantTTL = d
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sess, cli := dialBizSession(t, addr)
	t.Cleanup(func() { _ = cli.Close() })

	account := testAccount(t, "daemon")
	reg, err := sess.Register(ctx, &gatewayv1.RegisterRequest{Account: account, Password: "pw", Nickname: "守护探针"})
	if err != nil {
		t.Fatalf("守护形态注册失败（懒激活/身份链路回归）: %v", err)
	}
	if reg.PlayerID == "" {
		t.Fatal("注册回执缺少 player_id（幽灵实例吞消息的典型症状）")
	}
	if _, err := sess.Login(ctx, &gatewayv1.LoginRequest{PlayerId: reg.PlayerID, Password: "pw"}); err != nil {
		t.Fatalf("守护形态登录失败: %v", err)
	}
	if _, err := sess.Heartbeat(ctx); err != nil {
		t.Fatalf("守护形态心跳失败: %v", err)
	}

	// 会话路由 TTL 断言：redis 不可达时只跳过该断言（其余链路已验证）。
	rc, err := pkredis.NewClient(pkredis.Options{Addrs: []string{redisAddr}})
	if err != nil {
		t.Logf("跳过 TTL 断言（redis 构造失败）: %v", err)
	}
	if rc != nil {
		defer func() { _ = rc.Close() }()
		ttl, terr := rc.Raw().TTL(ctx, daemonKeys.GatewayRoute(reg.PlayerID)).Result()
		if terr != nil {
			t.Logf("跳过 TTL 断言（redis 不可用）: %v", terr)
		} else if ttl <= wantTTL/2 || ttl > wantTTL {
			t.Fatalf("会话路由 TTL = %v，期望落在 (%v, %v]（= 配置的 session.ttl）", ttl, wantTTL/2, wantTTL)
		}
	}

	// 可选的「过期清扫」断言（默认关：要等一个租期，慢；服务器验证时用
	// ATLAS_DAEMON_EXPIRY_WAIT=<略大于 session.ttl> 打开）：停发心跳后路由键应被清扫删除
	// （TTL 到期 + 清扫周期），这是会话租期配置真正生效的端到端证据。
	if wait := os.Getenv("ATLAS_DAEMON_EXPIRY_WAIT"); wait != "" && rc != nil {
		d, derr := time.ParseDuration(wait)
		if derr != nil {
			t.Fatalf("ATLAS_DAEMON_EXPIRY_WAIT 解析失败: %v", derr)
		}
		t.Logf("等待 %v 后断言会话过期清扫…", d)
		time.Sleep(d)
		ttl, terr := rc.Raw().TTL(context.Background(), daemonKeys.GatewayRoute(reg.PlayerID)).Result()
		if terr != nil {
			t.Fatalf("读路由 TTL 失败: %v", terr)
		}
		if ttl != -2 {
			t.Fatalf("等待 %v 后路由键仍存在（TTL=%v）——会话过期清扫未生效", d, ttl)
		}
	}

	// 登出用独立 ctx：上面的过期清扫断言可能已等过一个会话租期，主 ctx（30s）可能已到期。
	logoutCtx, cancelLogout := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelLogout()
	if err := sess.Logout(logoutCtx); err != nil {
		t.Fatalf("守护形态登出失败: %v", err)
	}
}
