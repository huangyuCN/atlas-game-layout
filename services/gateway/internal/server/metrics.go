// server 包内指标接线与业务打点：请求计数（自留会话接口 + 透传引擎共用一个
// counter，面板按 op 汇总全部入口 QPS）。指标名/标签遵循有界基数约定——
// op 取注解路由表 op 与自留接口方法（集合有界），result 仅 success/error；
// meter 为 nil 时全部打点短路（noop 零开销）。
package server

import (
	"context"

	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	"github.com/huangyuCN/atlas/metrics"
)

// MetricGatewayRequests 是 gateway 请求计数（labels: op, result）：
// 自留会话接口（meteredSession 装饰器）与透传引擎（Relay.Forward）共用。
const MetricGatewayRequests = "gateway_requests_total"

// requestResult 标签取值（有界枚举）。
const (
	resultSuccess = "success"
	resultError   = "error"
)

// meterRequest 打点一次请求（op 为协议 op 路径；err 非 nil 记 error）。
func meterRequest(c metrics.Collector, op string, err error) {
	if c == nil {
		return
	}
	result := resultSuccess
	if err != nil {
		result = resultError
	}
	c.Counter(MetricGatewayRequests, "op", op, "result", result).Add(1)
}

// 自留会话接口 op（与生成注册的 op 路径形态一致）。
const (
	opGatewayRegister  = "/gateway.v1.Session/Register"
	opGatewayLogin     = "/gateway.v1.Session/Login"
	opGatewayResume    = "/gateway.v1.Session/Resume"
	opGatewayLogout    = "/gateway.v1.Session/Logout"
	opGatewayHeartbeat = "/gateway.v1.Session/Heartbeat"
)

// meteredSession 是自留会话接口（*Gateway）的打点装饰器：
// Register/Login/Resume/Logout/Heartbeat 的统一拦截点，每方法结束后打
// gateway_requests_total{op,result}。生成代码按协议提供独立接口
// （SessionTCPServer 等，方法集一致），装饰器以具体类型转发并断言全部满足。
type meteredSession struct {
	inner *Gateway
	meter metrics.Collector
}

// 编译期断言：装饰器满足四协议的会话 Server 接口（方法集与 Gateway 一致）。
var (
	_ gatewayv1.SessionTCPServer = (*meteredSession)(nil)
	_ gatewayv1.SessionWSServer  = (*meteredSession)(nil)
	_ gatewayv1.SessionKCPServer = (*meteredSession)(nil)
	_ gatewayv1.SessionUDPServer = (*meteredSession)(nil)
)

// newMeteredSession 包装会话接口（meter 为 nil 时打点自动短路）。
func newMeteredSession(inner *Gateway, meter metrics.Collector) *meteredSession {
	return &meteredSession{inner: inner, meter: meter}
}

// Register 打点装饰（透传业务逻辑给被装饰者）。
func (m *meteredSession) Register(ctx context.Context, req *gatewayv1.RegisterRequest) (*gatewayv1.RegisterReply, error) {
	rep, err := m.inner.Register(ctx, req)
	meterRequest(m.meter, opGatewayRegister, err)
	return rep, err
}

// Login 打点装饰（透传业务逻辑给被装饰者）。
func (m *meteredSession) Login(ctx context.Context, req *gatewayv1.LoginRequest) (*gatewayv1.LoginReply, error) {
	rep, err := m.inner.Login(ctx, req)
	meterRequest(m.meter, opGatewayLogin, err)
	return rep, err
}

// Resume 打点装饰（透传业务逻辑给被装饰者）。
func (m *meteredSession) Resume(ctx context.Context, req *gatewayv1.ResumeRequest) (*gatewayv1.ResumeReply, error) {
	rep, err := m.inner.Resume(ctx, req)
	meterRequest(m.meter, opGatewayResume, err)
	return rep, err
}

// Logout 打点装饰（透传业务逻辑给被装饰者）。
func (m *meteredSession) Logout(ctx context.Context, req *gatewayv1.LogoutRequest) (*gatewayv1.LogoutReply, error) {
	rep, err := m.inner.Logout(ctx, req)
	meterRequest(m.meter, opGatewayLogout, err)
	return rep, err
}

// Heartbeat 打点装饰（透传业务逻辑给被装饰者）。
func (m *meteredSession) Heartbeat(ctx context.Context, req *gatewayv1.HeartbeatRequest) (*gatewayv1.HeartbeatReply, error) {
	rep, err := m.inner.Heartbeat(ctx, req)
	meterRequest(m.meter, opGatewayHeartbeat, err)
	return rep, err
}
