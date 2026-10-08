package stream

import (
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz"
	atlaslog "github.com/huangyuCN/atlas/log"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas/transport/frame/engine"
)

// TellFunc 是连接生命周期消息到战斗 actor 的投递端口（**必须非阻塞**）：帧引擎在读循环 /
// peer 表维护协程里同步回调，回调内只允许「查表 + 入队」，不得阻塞、不得 Ask
// （阻塞会拖慢该连接的帧处理，数据报面还会拖慢 peer 表维护）。
type TellFunc func(battleID string, msg any) error

// Bridge 把帧引擎的连接生命周期事件翻译成战斗 actor 的本地消息（规格 §9.1）：
//   - 上线：帧槽验票登记成功（含重连接管）时上报一次；逐帧重复登记同一连接不上报；
//   - 下线：端点断开事件按注册表反查玩家后上报；未登记端点（匿名连接）直接忽略。
//
// 断开事件不在这里判「是否过期」——同一玩家的新旧连接事件会交错（数据报面同一对端键、
// 流式面接管旧连接），过期判定放在 actor 侧复核注册表（规格 §9.2 硬约束②）。
type Bridge struct {
	reg  *Registry
	tell TellFunc
}

// NewBridge 构造连接生命周期桥（tell 为 nil 时只登记不上报，供单测裸用）。
func NewBridge(reg *Registry, tell TellFunc) *Bridge {
	return &Bridge{reg: reg, tell: tell}
}

// Record 登记玩家的直连并上报上线（帧槽验票通过时调用，规格 §9.1）：
// 连接未变化（逐帧重复登记）不上报；首次登记与重连接管各上报一次（接管即回座，规格 §9.4）。
func (b *Bridge) Record(playerID string, c Conn) {
	if !b.reg.Register(playerID, c) {
		return
	}
	b.notify(c.BattleID, playerID, biz.PlayerOnline{PlayerID: playerID})
}

// Ended 实现 server.FrameConn：判定该对局是否已结束（委托直连注册表的结束留档）。
// 为真时身份解析会**在投递之前**拒绝迟到 op，不触发 SpawnAuto 重建。
func (b *Bridge) Ended(battleID string) bool { return b.reg.Ended(battleID) }

// ReplayEnd 实现 server.FrameConn：向重连后的新连接补投留档的结算结果（委托注册表；
// 幂等、有界，不关闭连接）。
func (b *Bridge) ReplayEnd(playerID string, c Conn) bool { return b.reg.ReplayEnd(playerID, c) }

// ForFace 返回指定帧面的生命周期回调（装配期挂到帧传输服务端）：
// 只处理断开事件——建立事件此刻票还没验（身份未知），上线一律由 Record 上报。
func (b *Bridge) ForFace(kind transport.Kind) func(engine.ConnEvent) {
	return func(ev engine.ConnEvent) {
		if ev.Kind != engine.ConnEventDisconnected {
			return
		}
		playerID, battleID, ok := b.reg.Disconnect(Endpoint{Kind: kind, ID: ev.ID})
		if !ok {
			return
		}
		b.notify(battleID, playerID, biz.PlayerOffline{PlayerID: playerID})
	}
}

// notify 非阻塞投递一条连接生命周期消息：投递失败只记日志（帧引擎回调的契约是快速返回，
// 不做重试也不回压——掉线事件丢失由掉线窗口后的行为覆盖，不在此处阻塞补救）。
func (b *Bridge) notify(battleID, playerID string, msg any) {
	if b.tell == nil {
		return
	}
	if err := b.tell(battleID, msg); err != nil {
		atlaslog.Warnw("stream: 连接生命周期消息投递失败", "battle", battleID, "player", playerID, "err", err)
	}
}
