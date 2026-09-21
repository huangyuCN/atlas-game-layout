package server

import (
	"context"
	"testing"
	"time"

	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	"github.com/huangyuCN/atlas/transport"
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
	cli, err := pkredis.NewClient(pkredis.Options{Addrs: []string{integrationRedisAddr}})
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

// TestIntegrationKickCrossInstance 真 redis/nats 跨实例挤下线专项（M4 验收）：
// 实例 A 登录 → 实例 B 登录 → 经控制通道 A 的旧连接收到被挤下线通知。
func TestIntegrationKickCrossInstance(t *testing.T) {
	if reason := probeBackends(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	envA := newGWEnvWithRedis(t, "gw-a", integrationRedisAddr, integrationNatsURL)
	envB := newGWEnvWithRedis(t, "gw-b", integrationRedisAddr, integrationNatsURL)

	envA.login(t, 1, "it-p-1")
	envB.login(t, 1, "it-p-1")

	if !waitFor(3*time.Second, func() bool {
		return hasPush(envA.push.snapshot(), 1, consts.PushOpKickedOffline)
	}) {
		t.Fatalf("A 旧连接未收到被挤下线通知: %+v", envA.push.snapshot())
	}
	// 挤下线通知原因枚举校验。
	pushes := envA.push.snapshot()
	var kn gatewayv1.KickedNotify
	if err := protojson.Unmarshal(pushes[len(pushes)-1].payload, &kn); err != nil {
		t.Fatalf("挤下线通知解码: %v", err)
	}
	if kn.GetReason() != gatewayv1.KickedReason_KICKED_REASON_LOGGED_IN_ELSEWHERE {
		t.Fatalf("挤下线原因不符: %+v", &kn)
	}
	// A 旧连接心跳被拒。
	if _, err := envA.g.Heartbeat(connCtx(transport.KindTCP, gatewayv1.OperationSessionHeartbeatTCP, 1, ""), &gatewayv1.HeartbeatRequest{Ts: 1}); err == nil {
		t.Fatal("A 旧连接心跳应失败")
	}
}

// TestIntegrationPushOnlyOwnerDelivers 真 redis/nats 双实例推送专项（M4 验收）：
// 推送事件仅由持有连接的实例下发。
func TestIntegrationPushOnlyOwnerDelivers(t *testing.T) {
	if reason := probeBackends(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	envA := newGWEnvWithRedis(t, "gw-a", integrationRedisAddr, integrationNatsURL)
	envB := newGWEnvWithRedis(t, "gw-b", integrationRedisAddr, integrationNatsURL)

	envA.login(t, 1, "it-p-1")
	envB.login(t, 1, "it-p-2")

	nc, err := newProbeNats()
	if err != nil {
		t.Fatalf("nats 发布连接: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := PublishPush(ctx, nc, "it-p-1", consts.PushOpMatchStarted, []byte(`{"match_id":"m-1"}`)); err != nil {
		t.Fatalf("PublishPush: %v", err)
	}
	if !waitFor(3*time.Second, func() bool { return hasPush(envA.push.snapshot(), 1, consts.PushOpMatchStarted) }) {
		t.Fatalf("A 未收到 p-1 推送: %+v", envA.push.snapshot())
	}
	time.Sleep(300 * time.Millisecond)
	if hasPush(envB.push.snapshot(), 1, consts.PushOpMatchStarted) {
		t.Fatal("B 不应收到 p-1 推送")
	}
}
