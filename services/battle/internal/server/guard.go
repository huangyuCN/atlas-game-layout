// 帧面投递前的目标校验（P0-1）：帧票面 battle 与正文 battle_id 对齐 + 目标对局结束补判。
//
// 存在的意义：帧槽票据只对**票面那一局**有效，而投递目标是正文的 battle_id（UID_SOURCE_FIELD）。
// 两者不对齐时若照旧投递，一张合法票就能拉起任意 battle actor（已结束的对局也会被复活成
// 空名单实例，恢复快照后立刻再结算并反复关闭直连）。校验发生在 frameops 投递之前，故不触发
// 目标 actor 的懒激活。

package server

import (
	battlev1actor "github.com/huangyuCN/atlas-game-layout/api/battle/v1/actor"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas/contrib/actor/frameops"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	"google.golang.org/protobuf/proto"
)

// NewFrameGuard 组装 battle 帧面的投递前目标校验：
//  1. 票面 battle 与正文 battle_id 对齐（框架标准校验 frameops.TargetMatch，不一致即 403
//     FRAME_TARGET_MISMATCH 拒绝且不投递）；
//  2. 目标对局补一次 Ended 判定——身份解析（已判票面对局）到投递之间存在时间窗，目标局可能
//     恰好在这期间结算；投递前再判一次，已结束即以稳定 reason 拒绝（BATTLE_ENDED，SDK 据此
//     停止发送而不是重试到超时）。
func NewFrameGuard(conn FrameConn) frameops.TargetGuard {
	match := frameops.TargetMatch(battlev1actor.BattleServiceActorType)
	return func(entry relay.RouteEntry, id frameops.Identity, req proto.Message) error {
		if err := match(entry, id, req); err != nil {
			return err
		}
		return rejectEndedTarget(conn, entry, req)
	}
}

// rejectEndedTarget 对**载荷目标**补一次结束判定（目标取自路由条目的字段寻址闭包，与寻址
// 同一份取值，不另行解析协议正文）；目标对局未留档、条目不适用或取不到目标即放行
// （取不到目标的请求已由 TargetMatch 按既有口径拒绝，这里不重复判定）。
func rejectEndedTarget(conn FrameConn, entry relay.RouteEntry, req proto.Message) error {
	if conn == nil || entry.UidSource != relay.UidField || entry.UidOf == nil {
		return nil
	}
	battleID, ok := entry.UidOf(req)
	if !ok || battleID == "" {
		return nil
	}
	if !conn.Ended(battleID) {
		return nil
	}
	return errorv1.ErrBattleEnded("对局 %s 已结束（帧面投递前复核）", battleID)
}
