package server

import (
	"testing"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-game-layout/api/battle/v1/opclient"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas/contrib/actor/frameops"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	"google.golang.org/protobuf/proto"
)

// targetEntry 取一条真实的 battle 帧 op 路由条目（Actor/UidSource/UidOf 都是生成物原样）。
func targetEntry(t *testing.T) relay.RouteEntry {
	t.Helper()
	entry, ok := battlev1.BattleServiceRouteTable[battlev1opclient.BattleServiceProtocolOps.SendFrameInput]
	if !ok {
		t.Fatal("路由表缺少 SendFrameInput")
	}
	return entry
}

// TestFrameGuardRejectsMismatchAndEndedTarget 验证帧面投递前校验的两条拒绝口径：
// 票面 battle 与正文 battle_id 不一致 → 403（FRAME_TARGET_MISMATCH，框架标准校验）；
// 两者一致但目标对局已结束（身份解析到投递之间恰好结算）→ BATTLE_ENDED（补判一次）。
func TestFrameGuardRejectsMismatchAndEndedTarget(t *testing.T) {
	entry := targetEntry(t)
	cases := []struct {
		name     string
		ticket   string // 票面 battle（Identity.BattleID）
		ended    bool   // 目标对局是否已留档
		wantFunc func(error) bool
	}{
		{"票面与正文不一致", "b-2", false, func(err error) bool {
			return atlaserrors.FromError(err).Reason == frameops.ReasonTargetMismatch
		}},
		{"一致且未结束", "b-1", false, nil},
		{"一致但目标已结束", "b-1", true, errorv1.IsBattleEnded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := newFakeFrameConn()
			if tc.ended {
				conn.markEnded("b-1")
			}
			err := NewFrameGuard(conn)(entry,
				frameops.Identity{PlayerID: "p-1", BattleID: tc.ticket},
				&battlev1.FrameInputReq{BattleId: "b-1"})
			if tc.wantFunc == nil {
				if err != nil {
					t.Fatalf("应放行，实际被拒: %v", err)
				}
				return
			}
			if err == nil || !tc.wantFunc(err) {
				t.Fatalf("错误 = %v（reason=%s），期望另一类拒绝", err, atlaserrors.Reason(err))
			}
		})
	}
}

// TestFrameGuardRejectsMissingPayloadTarget 验证载荷取不到 battle_id 时按既有字段寻址口径拒绝
// （400/ACTOR_NO_UID）：目标校验不得对「无目标可校」的请求静默放行。
func TestFrameGuardRejectsMissingPayloadTarget(t *testing.T) {
	err := NewFrameGuard(newFakeFrameConn())(targetEntry(t),
		frameops.Identity{PlayerID: "p-1", BattleID: "b-1"}, &battlev1.FrameInputReq{})
	if se := atlaserrors.FromError(err); se.Code != 400 {
		t.Fatalf("code = %d（err=%v），期望 400", se.Code, err)
	}
}

// TestFrameGuardSkipsForeignEntry 验证只对 battle 的字段寻址条目生效：别的 actor 与
// 会话寻址条目不受影响（不误伤非战斗帧 op）。
func TestFrameGuardSkipsForeignEntry(t *testing.T) {
	entry := targetEntry(t)
	entry.Actor = "player"
	entry.UidSource = relay.UidSession
	entry.UidOf = nil
	conn := newFakeFrameConn()
	conn.markEnded("b-1")
	if err := NewFrameGuard(conn)(entry,
		frameops.Identity{PlayerID: "p-1"}, proto.Clone(&battlev1.FrameInputReq{BattleId: "b-1"})); err != nil {
		t.Fatalf("非 battle 字段寻址条目应跳过校验: %v", err)
	}
}
