package e2e

import (
	"context"
	"testing"
	"time"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	gwassemble "github.com/huangyuCN/atlas-game-layout/services/gateway/assemble"
	tcpt "github.com/huangyuCN/atlas/transport/tcp"
)

// newGatewayWithGate 起一个带客户端版本门槛的 gateway 实例（M1 用例专用）。
func newGatewayWithGate(t *testing.T, id, min string, mode configspb.MinClientVersionMode) *gwassemble.Gateway {
	t.Helper()
	gw, err := gwassemble.New(context.Background(), gwassemble.Options{
		ID:                   id,
		EtcdEndpoints:        []string{itEtcdEndpoints},
		NatsURL:              itNatsURL,
		RedisAddrs:           []string{itRedisAddr},
		Namespace:            itNS,
		MinClientVersion:     min,
		MinClientVersionMode: mode,
	})
	if err != nil {
		t.Fatalf("gateway 装配（门槛 %s/%v）: %v", min, mode, err)
	}
	t.Cleanup(func() { _ = gw.Stop(context.Background()) })
	return gw
}

// dialSessionStub 拨 TCP 业务通道并返回会话 stub（可显式上报 client_version——
// SDK 侧上报归 P4，故 M1 用例直接走生成的传输桩）。
func dialSessionStub(t *testing.T, tcpURL string) (*tcpt.Client, gatewayv1.SessionTCPClient) {
	t.Helper()
	cli, err := tcpt.NewClient(tcpURL)
	if err != nil {
		t.Fatalf("tcp client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli, gatewayv1.NewSessionTCPClient(cli)
}

// TestE2EVersionGate 验证 M1 客户端版本门槛的三条路径（真 etcd/redis/nats + 进程内装配）：
// ENFORCE 下低版本与未上报版本被拒（reason 可判定）、等于门槛放行；NEGOTIATE 下低版本放行（只协商）。
func TestE2EVersionGate(t *testing.T) {
	if reason := probeCore(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_ = newGame(t)
	enforce := newGatewayWithGate(t, "gw-enforce", "1.4.0",
		configspb.MinClientVersionMode_MIN_CLIENT_VERSION_MODE_ENFORCE)
	_, sess := dialSessionStub(t, enforce.TCPURL)

	// 先注册建档（注册不受门槛约束：门槛只管登录/重连）；登录用回执 player_id，不用账号。
	reg, err := sess.Register(ctx, &gatewayv1.RegisterRequest{
		Account: testAccount(t, "gate"), Password: "pw", Nickname: "门槛",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	playerID := reg.GetPlayerId()
	if playerID == "" {
		t.Fatal("注册回执缺少 player_id")
	}

	// ① 低版本登录被拒，reason 为枚举名。
	if _, err := sess.Login(ctx, &gatewayv1.LoginRequest{
		PlayerId: playerID, Password: "pw", ClientVersion: "1.3.9",
	}); !errorv1.IsClientVersionTooLow(err) {
		t.Fatalf("低版本登录应 CLIENT_VERSION_TOO_LOW，实际 %v", err)
	}
	// ② 未上报版本在 ENFORCE 下一并拒绝（失败即不满足，不静默放行）。
	if _, err := sess.Login(ctx, &gatewayv1.LoginRequest{
		PlayerId: playerID, Password: "pw",
	}); !errorv1.IsClientVersionTooLow(err) {
		t.Fatalf("未上报版本应 CLIENT_VERSION_TOO_LOW，实际 %v", err)
	}
	// ③ 等于门槛放行（门槛只拦低版本，不影响正常登录）。
	if rep, err := sess.Login(ctx, &gatewayv1.LoginRequest{
		PlayerId: playerID, Password: "pw", ClientVersion: "1.4.0",
	}); err != nil || rep.GetToken() == "" {
		t.Fatalf("达标版本登录应放行，实际 rep=%+v err=%v", rep, err)
	}

	// ④ NEGOTIATE：低版本放行（灰度期只协商不拒）。
	negotiate := newGatewayWithGate(t, "gw-negotiate", "1.4.0",
		configspb.MinClientVersionMode_MIN_CLIENT_VERSION_MODE_NEGOTIATE)
	_, nsess := dialSessionStub(t, negotiate.TCPURL)
	if rep, err := nsess.Login(ctx, &gatewayv1.LoginRequest{
		PlayerId: playerID, Password: "pw", ClientVersion: "1.0.0",
	}); err != nil || rep.GetToken() == "" {
		t.Fatalf("NEGOTIATE 下低版本应放行，实际 rep=%+v err=%v", rep, err)
	}
}
