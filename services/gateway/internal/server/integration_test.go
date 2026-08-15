package server

import (
	"context"
	"testing"
	"time"

	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	natss "github.com/nats-io/nats.go"
	"google.golang.org/protobuf/encoding/protojson"
)

// 集成测试约定（AGENTS.md）：真实中间件地址按集成服务器（10.10.9.36）端口填写，
// 连不上即跳过（Skipf 风格），在服务器上运行时执行真验证。
const (
	integrationRedisAddr = "127.0.0.1:16379"
	integrationNatsURL   = "nats://127.0.0.1:14222"
)

// probeBackends 探测真实 redis/nats；不可用返回 skip 消息。
func probeBackends(t *testing.T) string {
	t.Helper()
	cli, err := pkredis.NewClient(pkredis.Options{Addr: integrationRedisAddr})
	if err != nil {
		return "redis 客户端构造失败: " + err.Error()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cli.Ping(ctx); err != nil {
		_ = cli.Close()
		return "redis 不可用: " + err.Error()
	}
	_ = cli.Close()

	nc, err := newProbeNats()
	if err != nil {
		return "nats 不可用: " + err.Error()
	}
	nc.Close()
	return ""
}

// newProbeNats 建立 nats 探测连接。
func newProbeNats() (*natss.Conn, error) {
	return nats.Connect(nats.Options{URL: integrationNatsURL, Name: "probe"})
}

// TestIntegrationKickCrossInstance 真 redis/nats 跨实例挤下线专项（M4 验收）。
func TestIntegrationKickCrossInstance(t *testing.T) {
	if reason := probeBackends(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	envA := newGWEnvWithRedis(t, "gw-a", integrationRedisAddr, integrationNatsURL)
	envB := newGWEnvWithRedis(t, "gw-b", integrationRedisAddr, integrationNatsURL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cliA, authA := envA.newTCPAuthClient(t)
	loginA, err := authA.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "it-p-1", Password: "x"})
	if err != nil {
		t.Fatalf("A 登录: %v", err)
	}
	kicked := make(chan struct{}, 1)
	cliA.OnNotify(func(operation string, payload []byte) {
		if operation != PushOpKickedOffline {
			return
		}
		var kn gatewayv1.KickedNotify
		if err := protojson.Unmarshal(payload, &kn); err == nil && kn.GetReason() == "logged_in_elsewhere" {
			kicked <- struct{}{}
		}
	})

	_, authB := envB.newTCPAuthClient(t)
	if _, err := authB.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "it-p-1", Password: "x"}); err != nil {
		t.Fatalf("B 登录: %v", err)
	}
	select {
	case <-kicked:
	case <-time.After(3 * time.Second):
		t.Fatal("A 旧连接未收到被挤下线通知")
	}
	if _, err := authA.Heartbeat(ctx, &gatewayv1.HeartbeatRequest{PlayerId: "it-p-1", Token: loginA.GetToken(), Ts: 1}); err == nil {
		t.Fatal("A 旧令牌心跳应失败")
	}
}

// TestIntegrationPushOnlyOwnerDelivers 真 redis/nats 双实例推送专项（M4 验收）。
func TestIntegrationPushOnlyOwnerDelivers(t *testing.T) {
	if reason := probeBackends(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	envA := newGWEnvWithRedis(t, "gw-a", integrationRedisAddr, integrationNatsURL)
	envB := newGWEnvWithRedis(t, "gw-b", integrationRedisAddr, integrationNatsURL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cliA, authA := envA.newTCPAuthClient(t)
	if _, err := authA.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "it-p-1", Password: "x"}); err != nil {
		t.Fatalf("A 登录: %v", err)
	}
	cliB, authB := envB.newTCPAuthClient(t)
	if _, err := authB.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "it-p-2", Password: "x"}); err != nil {
		t.Fatalf("B 登录: %v", err)
	}
	gotA := make(chan struct{}, 1)
	gotB := make(chan struct{}, 1)
	cliA.OnNotify(func(operation string, payload []byte) {
		if operation == PushOpMatchStarted {
			gotA <- struct{}{}
		}
	})
	cliB.OnNotify(func(operation string, payload []byte) {
		if operation == PushOpMatchStarted {
			gotB <- struct{}{}
		}
	})

	nc, err := newProbeNats()
	if err != nil {
		t.Fatalf("nats 发布连接: %v", err)
	}
	defer nc.Close()

	if err := PublishPush(ctx, nc, "it-p-1", PushOpMatchStarted, []byte(`{"match_id":"m-1"}`)); err != nil {
		t.Fatalf("PublishPush: %v", err)
	}
	select {
	case <-gotA:
	case <-time.After(3 * time.Second):
		t.Fatal("A 未收到 p-1 推送")
	}
	select {
	case <-gotB:
		t.Fatal("B 不应收到 p-1 推送")
	case <-time.After(300 * time.Millisecond):
	}
}
