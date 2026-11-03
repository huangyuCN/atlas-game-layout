// 关键帧请求（battle.v1.BattleService/RequestFrameKeyframe）：ver=3 增量链失步时的快速修复。
//
// 设计（spec §4.3）：客户端只在「序号跳变 / base_seq 与自己的基线不符」时发一次，服务端下一
// tick 给**这条连接**补一帧全量（ver=2），其余连接照发增量——补关键帧不是 per-client 差分。
// 本方法不解释 last_seq（只作排障关联），是否补帧只看「这条连接请求过」：
// 请求是幂等 Tell，重复到达至多触发一次，不放大、不改对局状态。

package actor

import (
	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas/contrib/actor/core"
)

// RequestFrameKeyframe 实现 battlev1actor.BattleService：记录发起者的关键帧请求。
// 不在参战名单即拒绝（与补帧同一鉴权口径）；未开增量的节点静默忽略（请求无意义，不报错——
// 客户端无法知道服务端开关，回错只会诱发重试风暴）。
func (b *BattleActor) RequestFrameKeyframe(ctx core.ActorContext, req *battlev1.RequestFrameKeyframeReq) error {
	playerID, err := b.senderPlayer(ctx, "请求关键帧")
	if err != nil {
		return err
	}
	if _, ok := b.players[playerID]; !ok {
		return errorv1.ErrInvalidToken("关键帧请求被拒绝：不在参战名单")
	}
	b.pusher.RequestKeyframe(playerID, req.GetLastSeq())
	return nil
}
