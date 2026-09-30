package server

import (
	"errors"
	"testing"

	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/metricstest"
	"github.com/huangyuCN/atlas/metrics"
	"github.com/huangyuCN/atlas/transport"
)

// TestMeterRequest 验证请求计数打点的标签口径与 nil/noop 短路。
func TestMeterRequest(t *testing.T) {
	rec := metricstest.New()
	meterRequest(rec, "/x", nil)
	meterRequest(rec, "/x", errors.New("boom"))
	if got := rec.CounterValue(MetricGatewayRequests, "op", "/x", "result", resultSuccess); got != 1 {
		t.Fatalf("success 计数 = %v, 期望 1", got)
	}
	if got := rec.CounterValue(MetricGatewayRequests, "op", "/x", "result", resultError); got != 1 {
		t.Fatalf("error 计数 = %v, 期望 1", got)
	}
	// 未配置后端（nil/noop）：短路且不得 panic。
	meterRequest(nil, "/y", nil)
	meterRequest(metrics.Noop(), "/y", nil)
}

// TestMeteredSessionMetrics 验证自留会话接口装饰器按 op/result 打点。
func TestMeteredSessionMetrics(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	rec := metricstest.New()
	env := newGWEnvMeter(t, "gw-a", mr.Addr(), natsURL, rec)
	ms := newMeteredSession(env.g, rec)

	loginCtx := connCtx(transport.KindTCP, gatewayv1.OperationSessionLoginTCP, 1, "")
	if _, err := ms.Login(loginCtx, &gatewayv1.LoginRequest{PlayerId: "p-1", Password: "x"}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	if got := rec.CounterValue(MetricGatewayRequests, "op", opGatewayLogin, "result", resultSuccess); got != 1 {
		t.Fatalf("Login success 计数 = %v, 期望 1", got)
	}

	// 无本地会话的心跳：错误路径记 error。
	hbCtx := connCtx(transport.KindTCP, gatewayv1.OperationSessionHeartbeatTCP, 9, "")
	if _, err := ms.Heartbeat(hbCtx, &gatewayv1.HeartbeatRequest{}); err == nil {
		t.Fatal("无会话心跳应报错")
	}
	if got := rec.CounterValue(MetricGatewayRequests, "op", opGatewayHeartbeat, "result", resultError); got != 1 {
		t.Fatalf("Heartbeat error 计数 = %v, 期望 1", got)
	}
}

// TestRelayForwardMetrics 验证透传引擎按路由 op 打点（成功/失败分别计入 result）。
func TestRelayForwardMetrics(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	rec := metricstest.New()
	env := newGWEnvMeter(t, "gw-a", mr.Addr(), natsURL, rec)
	env.login(t, 1, "p-1")

	env.forward(t, connCtx(transport.KindTCP, opEnterMatch, 1, ""), opEnterMatch,
		&gamev1.EnterMatchQueueReq{Ruleset: "casual"})
	if got := rec.CounterValue(MetricGatewayRequests, "op", opEnterMatch, "result", resultSuccess); got != 1 {
		t.Fatalf("Forward success 计数 = %v, 期望 1", got)
	}

	// 未绑定身份：同一 op 记 error。
	badCtx := connCtx(transport.KindTCP, opEnterMatch, 5, "")
	if _, err := env.g.Relay().Forward(badCtx, env.relayEntry(t, opEnterMatch),
		&gamev1.EnterMatchQueueReq{Ruleset: "casual"}); err == nil {
		t.Fatal("未绑定身份应报错")
	}
	if got := rec.CounterValue(MetricGatewayRequests, "op", opEnterMatch, "result", resultError); got != 1 {
		t.Fatalf("Forward error 计数 = %v, 期望 1", got)
	}
}
