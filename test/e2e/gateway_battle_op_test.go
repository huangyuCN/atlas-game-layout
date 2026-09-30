package e2e

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-game-layout/api/battle/v1/opclient"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gamev1opclient "github.com/huangyuCN/atlas-game-layout/api/game/v1/opclient"
	sdkclient "github.com/huangyuCN/atlas-sdk-go/client"
	atlaserrors "github.com/huangyuCN/atlas/errors"
)

// reasonOpNotRegistered 是战斗 op 经网关被拒时客户端看到的稳定 reason：
// 框架把「op 未注册」的裸错误按出站投影补为 ReasonMissingReason
// （唯一来源是框架常量；此处引用而非抄字面量）。
var reasonOpNotRegistered = atlaserrors.ReasonOpNotFound

// rejectedOpMarker 是框架「op 未注册」错误 sentinel 的文本片段（引擎侧
// `engine: not registered: "<op>"`）：reason 之外再核对 message 指到具体 op——
// 失败必须可定位，不能只有一句"服务端出错"。
const rejectedOpMarker = "not registered"

// assertGatewayRejectsBattleOp 断言经网关发战斗 op 的**明确失败**形态：
// 服务端结构化业务拒绝（reason=ReasonMissingReason，message 指明该 op 未注册），
// 而不是静默丢弃（那会表现为 SDK 超时 *TimeoutError）或成功（err=nil）。
func assertGatewayRejectsBattleOp(t *testing.T, err error, op string) {
	t.Helper()
	if err == nil {
		t.Fatalf("经网关发战斗 op %s 竟然成功：旧承载路径未删净", op)
	}
	var be *sdkclient.BusinessError
	if !errors.As(err, &be) {
		t.Fatalf("战斗 op %s 的失败不是服务端业务拒绝（疑似连接层/超时，可能是静默丢弃）: %v", op, err)
	}
	if be.Reason != reasonOpNotRegistered {
		t.Fatalf("战斗 op %s 拒绝 reason = %q, 期望 %q", op, be.Reason, reasonOpNotRegistered)
	}
	if !strings.Contains(be.Message, rejectedOpMarker) || !strings.Contains(be.Message, op) {
		t.Fatalf("战斗 op %s 拒绝 message 未指明 op 未注册: %q", op, be.Message)
	}
}

// TestE2EGatewayRejectsBattleOp 是阶段 3 批次 5 的破坏性断言①：经网关发战斗 op = **明确失败**。
//
// 断言前提：battle 服务在线（排除"对端不在"这种假失败），客户端已登录并绑定连接身份。
// 失败形态：服务端结构化业务拒绝（op 不在网关注册表）——不是静默丢弃、不是连接断开、
// 不是超时；同一条连接上的业务 op 照常可达（失败是 op 级）。
func TestE2EGatewayRejectsBattleOp(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	newGame(t)
	gw := newGateway(t, "a")
	battleSvc := newBattle(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sess, cli := dialBizSession(t, gw.TCPURL)
	loginFlow(t, ctx, sess)
	seedBattle(t, ctx, battleSvc, "b-1", sess.PlayerID())

	op := battlev1opclient.BattleServiceProtocolOps.JoinBattle
	var join battlev1.JoinBattleReply
	err := cli.Invoke(ctx, op, &battlev1.JoinBattleReq{BattleId: "b-1"}, &join)
	assertGatewayRejectsBattleOp(t, err, op)

	// 正对照：同一连接上业务 op 仍可达（拒绝是 op 级，不是连接坏了）。
	var data gamev1.PlayerDataReply
	if err := cli.Invoke(ctx, gamev1opclient.PlayerServiceProtocolOps.GetPlayerData, &gamev1.GetPlayerDataReq{}, &data); err != nil {
		t.Fatalf("同一连接上业务 op 应可达: %v", err)
	}
	if data.GetPlayer().GetPlayerId() != sess.PlayerID() {
		t.Fatalf("业务 op 回执不符: %+v", data.GetPlayer())
	}
}

// TestE2EGatewayNoBattleFrameListeners 是阶段 3 批次 5 的破坏性断言②：
// **网关不再监听 KCP/UDP 端口**。ListenSchemes 取自 bootstrap.Boot 的就绪端点表
// （运行时的真实监听面，不是配置抄写），故"网关还在 9003/9004 上监听"会让本用例失败。
func TestE2EGatewayNoBattleFrameListeners(t *testing.T) {
	if reason := probeMiddlewares(t); reason != "" {
		t.Skipf("集成环境不可用: %s", reason)
	}
	gw := newGateway(t, "a")
	got := gw.ListenSchemes()
	want := []string{"grpc-edge", "http", "tcp", "ws"}
	if !slices.Equal(got, want) {
		t.Fatalf("网关监听面 = %v, 期望 %v（业务 + 健康 + edge，无战斗帧面）", got, want)
	}
	for _, scheme := range got {
		if scheme == "kcp" || scheme == "udp" {
			t.Fatalf("网关仍在监听战斗帧面 %s（监听面 = %v）", scheme, got)
		}
	}
}
