// 关键帧请求与载荷版本协商在 battle actor 侧的用例：协商结果必须写进 JoinBattle 回执
// （客户端只认回执，不认自己声明的），请求必须落到达成的推送端口（下一 tick 补全量）。

package actor

import (
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
)

// waitKeyframeRequests 等到推送端口收到 n 条关键帧请求（本地 Tell 走 actor 邮箱，异步）。
func waitKeyframeRequests(t *testing.T, env *battleEnv, n int) []keyframeRecord {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		reqs := env.pusher.keyframeRequests()
		if len(reqs) >= n {
			return reqs
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待 %d 条关键帧请求超时（当前 %d 条）", n, len(reqs))
		}
		time.Sleep(time.Millisecond)
	}
}

// TestJoinBattleReplyCarriesNegotiatedVersion 验证协商结果随读回执下发。
func TestJoinBattleReplyCarriesNegotiatedVersion(t *testing.T) {
	env := newBattleEnvWith(t, testBattleConfig())
	if _, err := env.ask(newTestContext(), &battlev1.CreateBattleRequest{
		MatchId: "m-1", PlayerIds: []string{"p-a", "p-b"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	env.pusher.wantVersion = battlev1.FramePayloadVersion_FRAME_PAYLOAD_VERSION_PROTOBUF
	raw, err := env.askAs(newTestContext(), "p-a", &battlev1.JoinBattleReq{
		BattleId:              "b-test01",
		AcceptPayloadVersions: []battlev1.FramePayloadVersion{1, 2, 3},
	})
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	reply, ok := raw.(*battlev1.JoinBattleReply)
	if !ok {
		t.Fatalf("回执类型 %T 不符", raw)
	}
	if reply.GetPayloadVersion() != battlev1.FramePayloadVersion_FRAME_PAYLOAD_VERSION_PROTOBUF {
		t.Fatalf("回执版本 = %v, 期望 PROTOBUF（以协商结果为准）", reply.GetPayloadVersion())
	}
	if len(env.pusher.negotiated) != 1 {
		t.Fatalf("协商调用次数 = %d, 期望 1", len(env.pusher.negotiated))
	}
	if got := env.pusher.negotiated[0]; got.playerID != "p-a" || len(got.accepted) != 3 {
		t.Fatalf("协商入参 = %+v, 期望 p-a/[1 2 3]", got)
	}
}

// TestRequestFrameKeyframeReachesPusher 验证关键帧请求经 actor 落到推送端口，
// 且不在参战名单的发起者被拒绝（与补帧同一鉴权口径）。
func TestRequestFrameKeyframeReachesPusher(t *testing.T) {
	env := newBattleEnvWith(t, testBattleConfig())
	if _, err := env.ask(newTestContext(), &battlev1.CreateBattleRequest{
		MatchId: "m-1", PlayerIds: []string{"p-a", "p-b"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := env.tellAs(newTestContext(), "p-a", &battlev1.RequestFrameKeyframeReq{
		BattleId: "b-test01", LastSeq: 41,
	}); err != nil {
		t.Fatalf("关键帧请求应成功（Tell）: %v", err)
	}
	reqs := waitKeyframeRequests(t, env, 1)
	if got := reqs[0]; got.playerID != "p-a" || got.lastSeq != 41 {
		t.Fatalf("请求记录 = %+v, 期望 p-a/41", got)
	}
	if err := env.tellAs(newTestContext(), "p-ghost", &battlev1.RequestFrameKeyframeReq{
		BattleId: "b-test01", LastSeq: 41,
	}); err != nil {
		t.Logf("名单外的请求经本地投递返回错误（符合预期）: %v", err)
	}
	if got := env.pusher.keyframeRequests(); len(got) != 1 {
		t.Fatalf("名单外的请求不得落到推送端口（当前 %d 条）", len(got))
	}
}
