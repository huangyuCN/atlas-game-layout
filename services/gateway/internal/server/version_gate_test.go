package server

import (
	"bytes"
	"context"
	"strings"
	"testing"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	atlaslog "github.com/huangyuCN/atlas/log"
	"github.com/huangyuCN/atlas/transport"
)

// TestVersionGateCheck 验证门槛语义（阶段 3 战斗帧直连版）：
// 门槛为空 ⇒ 仅记录放行（不改变未设门槛部署的既有行为）；门槛非空 ⇒ **登录期强制**
// （无需显式 mode）：低于门槛 / 未上报 / 版本串非法一律拒绝，等于与高于门槛放行；
// 显式 NEGOTIATE 是灰度逃生门（只记录不拒绝）。
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
		{"空门槛仅记录：低版本放行", VersionGate{}, "0.0.1", false},
		{"空门槛仅记录：非法串放行", VersionGate{mode: enforce}, "not-a-version", false},
		{"设置门槛（未配 mode）低于门槛拒绝", VersionGate{min: "0.9.0"}, "0.8.9", true},
		{"设置门槛（未配 mode）等于门槛放行", VersionGate{min: "0.9.0"}, "0.9.0", false},
		{"设置门槛（未配 mode）高于门槛放行", VersionGate{min: "0.9.0"}, "0.10.0", false},
		{"逐段比较：门槛 0.10.0 拒绝 0.9.0（非字符串比较）", VersionGate{min: "0.10.0"}, "0.9.0", true},
		{"段数不足按 0 补齐：1.2 == 1.2.0", VersionGate{min: "1.2.0"}, "1.2", false},
		{"预发布低于同号正式版", VersionGate{min: "1.2.0"}, "1.2.0-rc.1", true},
		{"设置门槛未上报拒绝", VersionGate{min: "0.9.0"}, "", true},
		{"设置门槛版本串非法拒绝", VersionGate{min: "0.9.0"}, "not-a-version", true},
		{"ENFORCE 强制拒绝", VersionGate{min: "1.4.0", mode: enforce}, "1.3.9", true},
		{"OFF 与缺省同义（有门槛即强制）", VersionGate{min: "1.4.0", mode: off}, "1.3.9", true},
		{"NEGOTIATE 只记录不拒绝", VersionGate{min: "1.4.0", mode: negotiate}, "1.3.9", false},
		{"NEGOTIATE 未上报放行", VersionGate{min: "1.4.0", mode: negotiate}, "", false},
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

// TestVersionGateRecordOnlyLogs 验证「仅记录」确实记录：门槛为空与显式 NEGOTIATE 都放行，
// 且日志里带上客户端上报版本与门槛值（灰度期只能靠日志判断老客户端占比）。
func TestVersionGateRecordOnlyLogs(t *testing.T) {
	var buf bytes.Buffer
	old := atlaslog.GetLogger()
	atlaslog.SetLogger(atlaslog.New(atlaslog.WithWriter(&buf)))
	t.Cleanup(func() { atlaslog.SetLogger(old) })

	if err := (VersionGate{}).Check(context.Background(), "0.3.0"); err != nil {
		t.Fatalf("空门槛应放行（仅记录），实际 %v", err)
	}
	negotiate := VersionGate{min: "1.4.0", mode: configspb.MinClientVersionMode_MIN_CLIENT_VERSION_MODE_NEGOTIATE}
	if err := negotiate.Check(context.Background(), "1.0.0"); err != nil {
		t.Fatalf("NEGOTIATE 应放行（仅记录），实际 %v", err)
	}
	logged := buf.String()
	for _, want := range []string{"0.3.0", "1.0.0", "仅记录"} {
		if !strings.Contains(logged, want) {
			t.Fatalf("仅记录模式应把客户端版本与门槛写进日志，缺少 %q：%s", want, logged)
		}
	}
}

// TestVersionGateRejectMessage 验证拒绝回执可判定：reason 取常量（客户端按 reason 分支，
// 不比对字面量），message 写出「当前版本 + 最低要求版本 + 升级提示」，
// 且「未上报 / 无法识别 / 过低」三种成因措辞可区分。
func TestVersionGateRejectMessage(t *testing.T) {
	gate := VersionGate{min: "0.7.0"}
	cases := []struct{ name, client, wantCause string }{
		{"低于门槛", "0.6.9", "过低"},
		{"未上报", "", "未上报"},
		{"版本串非法", "not-a-version", "无法识别"},
	}
	for _, c := range cases {
		err := gate.Check(context.Background(), c.client)
		if !errorv1.IsClientVersionTooLow(err) {
			t.Fatalf("%s: 期望 CLIENT_VERSION_TOO_LOW，实际 %v", c.name, err)
		}
		if got := atlaserrors.Reason(err); got != errorv1.ReasonClientVersionTooLow() {
			t.Fatalf("%s: reason = %q，期望常量 %q", c.name, got, errorv1.ReasonClientVersionTooLow())
		}
		msg := atlaserrors.FromError(err).Message
		if !strings.Contains(msg, "0.7.0") || !strings.Contains(msg, "升级") || !strings.Contains(msg, c.wantCause) {
			t.Fatalf("%s: message 应含最低版本、升级提示与 %q，实际 %q", c.name, c.wantCause, msg)
		}
	}
}

// TestVersionGateFromConfig 验证门槛直接从运行时配置构造（字段与枚举同名同义）：
// 配置里设了门槛即登录期强制（未配 mode 也强制）。
func TestVersionGateFromConfig(t *testing.T) {
	cfg := &configspb.Runtime{MinClientVersion: "2.0.0"}
	gate := NewVersionGate(cfg)
	if err := gate.Check(context.Background(), "1.9.9"); !errorv1.IsClientVersionTooLow(err) {
		t.Fatalf("配置门槛 2.0.0：1.9.9 应被拒，实际 %v", err)
	}
	if err := gate.Check(context.Background(), "2.0.0"); err != nil {
		t.Fatalf("等于门槛应放行，实际 %v", err)
	}
	if err := NewVersionGate(nil).Check(context.Background(), ""); err != nil {
		t.Fatalf("空配置应放行（仅记录），实际 %v", err)
	}
}

// TestGatewayLoginRejectsLowVersion 验证登录链路上的强制点：门槛非空即强制（未配 mode）——
// 低版本 / 非法版本 / 未上报登录被拒，且**不建立会话、不触达域、不进匹配**
// （后续业务 op 因无会话身份被拒）；达标版本登录建立会话并恢复业务 op 可用。
func TestGatewayLoginRejectsLowVersion(t *testing.T) {
	mr, natsURL, _ := newSharedBackends(t)
	env := newGWEnv(t, "gw-a", mr, natsURL)
	env.g.gate = VersionGate{min: "0.7.0"} // 未配 mode：设置门槛即登录期强制

	ctx := connCtx(transport.KindTCP, gatewayv1.OperationSessionLoginTCP, 1, "")
	for _, bad := range []string{"0.6.9", "not-a-version", ""} {
		_, err := env.g.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p-1", Password: "x", ClientVersion: bad})
		if !errorv1.IsClientVersionTooLow(err) {
			t.Fatalf("客户端版本 %q 登录应被拒，实际 %v", bad, err)
		}
		if _, ok := env.sess.LocalSession("p-1"); ok {
			t.Fatalf("客户端版本 %q 被拒后不应建立会话", bad)
		}
		if got := len(env.mock.loginReqs); got != 0 {
			t.Fatalf("客户端版本 %q 被拒后不应触达域，实际 %d 次", bad, got)
		}
		// 无会话身份 ⇒ 匹配入口不可用（业务 op 被拒），且未触达匹配域。
		_, ferr := env.g.Relay().Forward(ctx, env.relayEntry(t, opEnterMatch), &gamev1.EnterMatchQueueReq{Ruleset: "casual"})
		if !errorv1.IsInvalidToken(ferr) {
			t.Fatalf("客户端版本 %q 被拒后不应能进入匹配，实际 %v", bad, ferr)
		}
		if len(env.mock.matchEnters) != 0 {
			t.Fatalf("客户端版本 %q 被拒后不应触达匹配域，实际 %d 次", bad, len(env.mock.matchEnters))
		}
	}

	// 达标版本放行：建立会话、触达域一次，业务 op 恢复可用。
	rep, err := env.g.Login(ctx, &gatewayv1.LoginRequest{PlayerId: "p-1", Password: "x", ClientVersion: "0.7.0"})
	if err != nil || rep.GetToken() == "" {
		t.Fatalf("达标版本登录应放行，实际 rep=%+v err=%v", rep, err)
	}
	if _, ok := env.sess.LocalSession("p-1"); !ok {
		t.Fatal("达标版本登录应建立会话")
	}
	if got := len(env.mock.loginReqs); got != 1 {
		t.Fatalf("达标版本应触达域一次，实际 %d 次", got)
	}
	if _, err := env.g.Relay().Forward(ctx, env.relayEntry(t, opEnterMatch), &gamev1.EnterMatchQueueReq{Ruleset: "casual"}); err != nil {
		t.Fatalf("会话建立后业务 op 应可用，实际 %v", err)
	}
	if len(env.mock.matchEnters) != 1 {
		t.Fatalf("会话建立后应触达匹配域一次，实际 %d 次", len(env.mock.matchEnters))
	}
}
