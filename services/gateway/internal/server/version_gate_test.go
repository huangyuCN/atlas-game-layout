package server

import (
	"context"
	"testing"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas/transport"
)

// TestVersionGateCheck 验证模式语义：OFF 放行；无门槛放行；NEGOTIATE 只记录；
// ENFORCE 下「低版本 / 未上报 / 版本串非法」一律拒（失败即不满足，不静默放行）。
func TestVersionGateCheck(t *testing.T) {
	const (
		off       = configspb.MinClientVersionMode_MIN_CLIENT_VERSION_MODE_OFF
		negotiate = configspb.MinClientVersionMode_MIN_CLIENT_VERSION_MODE_NEGOTIATE
		enforce   = configspb.MinClientVersionMode_MIN_CLIENT_VERSION_MODE_ENFORCE
	)
	cases := []struct {
		name    string
		gate    VersionGate
		client  string
		wantErr bool
	}{
		{"OFF 不校验", VersionGate{min: "1.4.0", mode: off}, "1.0.0", false},
		{"无门槛放行", VersionGate{min: "", mode: enforce}, "1.0.0", false},
		{"NEGOTIATE 低版本放行", VersionGate{min: "1.4.0", mode: negotiate}, "1.0.0", false},
		{"NEGOTIATE 未上报放行", VersionGate{min: "1.4.0", mode: negotiate}, "", false},
		{"ENFORCE 低版本拒绝", VersionGate{min: "1.4.0", mode: enforce}, "1.3.9", true},
		{"ENFORCE 等于门槛放行", VersionGate{min: "1.4.0", mode: enforce}, "1.4.0", false},
		{"ENFORCE 高于门槛放行", VersionGate{min: "1.4.0", mode: enforce}, "1.5.0", false},
		{"ENFORCE 未上报拒绝", VersionGate{min: "1.4.0", mode: enforce}, "", true},
		{"ENFORCE 版本串非法拒绝", VersionGate{min: "1.4.0", mode: enforce}, "not-a-version", true},
	}
	for _, c := range cases {
		err := c.gate.Check(context.Background(), c.client)
		if c.wantErr {
			if !errorv1.IsClientVersionTooLow(err) {
				t.Errorf("%s: 期望 CLIENT_VERSION_TOO_LOW，实际 %v", c.name, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: 期望放行，实际 %v", c.name, err)
		}
	}
}

// TestVersionGateFromConfig 验证门槛直接从运行时配置构造（字段与枚举同名同义）。
func TestVersionGateFromConfig(t *testing.T) {
	cfg := &configspb.Runtime{
		MinClientVersion:     "2.0.0",
		MinClientVersionMode: configspb.MinClientVersionMode_MIN_CLIENT_VERSION_MODE_ENFORCE,
	}
	gate := NewVersionGate(cfg)
	if err := gate.Check(context.Background(), "1.9.9"); !errorv1.IsClientVersionTooLow(err) {
		t.Fatalf("配置门槛 2.0.0 + ENFORCE：1.9.9 应被拒，实际 %v", err)
	}
	if err := gate.Check(context.Background(), "2.0.0"); err != nil {
		t.Fatalf("等于门槛应放行，实际 %v", err)
	}
	if err := NewVersionGate(nil).Check(context.Background(), ""); err != nil {
		t.Fatalf("空配置应放行（不设门槛），实际 %v", err)
	}
}

// TestGatewayLoginRejectsLowVersion 验证登录链路上的门槛生效点：
// ENFORCE 下低版本登录被拒，且**不产生任何 actor 调用**（门槛先于业务裁决）。
func TestGatewayLoginRejectsLowVersion(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)
	env.g.gate = VersionGate{min: "1.4.0", mode: configspb.MinClientVersionMode_MIN_CLIENT_VERSION_MODE_ENFORCE}

	ctx := connCtx(transport.KindTCP, gatewayv1.OperationSessionLoginTCP, 1, "")
	_, err := env.g.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p-1", Password: "x", ClientVersion: "1.3.0"})
	if !errorv1.IsClientVersionTooLow(err) {
		t.Fatalf("低版本登录应 CLIENT_VERSION_TOO_LOW，实际 %v", err)
	}
	if got := len(env.mock.loginReqs); got != 0 {
		t.Fatalf("被拒请求不应触达 actor，实际 %d 次", got)
	}

	// 同连接升级后放行（门槛只拦低版本，不影响正常登录）。
	if _, err := env.g.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p-1", Password: "x", ClientVersion: "1.4.0"}); err != nil {
		t.Fatalf("达标版本登录应放行，实际 %v", err)
	}
	if got := len(env.mock.loginReqs); got != 1 {
		t.Fatalf("达标版本应触达 actor 一次，实际 %d 次", got)
	}
}
