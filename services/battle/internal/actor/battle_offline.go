// 掉线与重连策略（规格 §9，最小可配置版）：连接生命周期消息 → 掉线打点与计时 →
// 超时后果（判负 + 出局广播；只剩一人判胜走既有结算路径）→ 窗口内回座取消计时。
//
// 计时走 actor 定时器（ctx.After：时间轮上的一次性自消息），不新起 goroutine 池；
// 所有入口都是本地消息（core.WithLocalTell），故打点、判负、回座全在 actor 线程里串行，
// 不需要额外加锁。
//
// 边界（本轮口径）：
//   - 顶号/业务会话失效走业务链路，触达不到直连，也**不**断本局直连（规格 §9.7）：
//     本文件只消费帧引擎的直连事件，业务面事件一概不在此处理；
//     强制断流（运营/风控踢出对局）需要 battle 的 INTERNAL 接口，列为下一轮，本轮只留接缝。

package actor

import (
	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz"
	"github.com/huangyuCN/atlas/contrib/actor/core"
)

// offlineTimeout 是掉线计时到点消息（actor 定时器自发自收，不经连接生命周期桥）。
type offlineTimeout struct {
	// PlayerID 是打点玩家。
	PlayerID string
}

// 迁移窗口的**唯一入口**是迁移 op（battle_migrate.go 的 Prepare/ResumeBattleMigration →
// enterMigrationWindow / exitMigrationWindow）：窗口内断开不算掉线（本文件只提供计时挂起与
// 重新起算两个原语），且会话帧推进一并暂停。
//
// 裁定（P1-8）：这里原有一个本地消息接缝 `MigrationPause{Paused}`（无生产发送方，批次 7 的
// 属主切换走迁移 op），它与迁移 op **语义不一致**——只挂起掉线计时、不暂停会话推进，
// 误用会让客户端在迁移窗口内看到帧回退。为免两条入口漂移，接缝已删除：窗口只能由迁移 op
// 打开/关闭，计时开关由 enterMigrationWindow / exitMigrationWindow 调用本文件的两个原语。

// onPlayerOnline 实现本地消息：掉线窗口内回座（规格 §9.4）——取消计时、清除打点；
// 补帧仍走既有 SyncFrames 语义（帧协议零改动，只是承载路径改成直连）。
// 已出局/未知玩家的上线消息忽略：判负后重新取票只对新一局有意义（规格 §9.5）。
func (b *BattleActor) onPlayerOnline(_ core.ActorContext, msg biz.PlayerOnline) error {
	if _, ok := b.players[msg.PlayerID]; !ok {
		return nil
	}
	if _, marked := b.offline[msg.PlayerID]; !marked {
		return nil // 本就在线：逐帧重复登记已被桥去重，此处无计时可取消
	}
	b.clearOffline(msg.PlayerID)
	b.metrics.Counter(MetricReconnects).Add(1)
	return nil
}

// onPlayerOffline 实现本地消息：打点并启动掉线计时（规格 §9.2）。三道闸门：
//  1. 应用前复核注册表——玩家仍有存活直连即忽略这条过期事件（硬约束②：同一身份的新旧
//     连接事件会交错，不复核就会把刚回座的人误判成掉线）；
//  2. 迁移窗口内只打点不计时（规格 §9.6：拆流断开不算掉线，窗口关闭后才起算）；
//  3. 重复断开只认第一次打点（连发两条不重复判负）。
func (b *BattleActor) onPlayerOffline(ctx core.ActorContext, msg biz.PlayerOffline) error {
	if b.settled {
		return nil
	}
	if _, ok := b.players[msg.PlayerID]; !ok {
		return nil
	}
	if _, marked := b.offline[msg.PlayerID]; marked {
		return nil
	}
	if b.presenceOnline(msg.PlayerID) {
		return nil
	}
	if b.migration {
		b.offline[msg.PlayerID] = nil // 只打点：窗口关闭时由 onMigrationPause 起算
		return nil
	}
	b.armOffline(ctx, msg.PlayerID)
	return nil
}

// onOfflineTimeout 实现本地消息：掉线超时的后果（规格 §9.3）。
// 仍处于打点状态才执行——窗口内回座已清打点，定时器与取消的竞态在这里兜底；
// 执行前再复核一次注册表（注册表说还在场就不判负）。
func (b *BattleActor) onOfflineTimeout(ctx core.ActorContext, msg offlineTimeout) error {
	if b.settled {
		return nil
	}
	if _, marked := b.offline[msg.PlayerID]; !marked {
		return nil
	}
	if _, ok := b.players[msg.PlayerID]; !ok {
		b.clearOffline(msg.PlayerID)
		return nil
	}
	if b.presenceOnline(msg.PlayerID) {
		b.clearOffline(msg.PlayerID)
		return nil
	}
	b.metrics.Counter(MetricOfflineTimeouts).Add(1)
	b.eliminate(ctx, msg.PlayerID)
	return nil
}

// holdOfflineTimers 挂起全部掉线计时但保留打点（迁移窗口内不计时，规格 §9.6）。
// 只由 enterMigrationWindow 调用：迁移窗口的唯一入口是迁移 op（见本文件顶部裁定）。
func (b *BattleActor) holdOfflineTimers() {
	for player, cancel := range b.offline {
		if cancel == nil {
			continue
		}
		cancel()
		b.offline[player] = nil
	}
}

// rearmOfflineTimers 给所有仍打点的玩家重新起算掉线计时（迁移窗口关闭/迁移完成后调用）。
func (b *BattleActor) rearmOfflineTimers(ctx core.ActorContext) {
	for player := range b.offline {
		b.armOffline(ctx, player)
	}
}

// armOffline 打点并启动掉线计时（关闭判定（窗口 ≤ 0）时只打点不计时）。
func (b *BattleActor) armOffline(ctx core.ActorContext, playerID string) {
	b.clearOffline(playerID)
	if b.cfg.OfflineTimeout <= 0 {
		b.offline[playerID] = nil
		return
	}
	b.offline[playerID] = ctx.After(b.cfg.OfflineTimeout, offlineTimeout{PlayerID: playerID})
}

// clearOffline 清除打点并取消未触发的计时（回座/出局/迁移窗口进入）。
func (b *BattleActor) clearOffline(playerID string) {
	cancel, ok := b.offline[playerID]
	if !ok {
		return
	}
	delete(b.offline, playerID)
	if cancel != nil {
		cancel()
	}
}

// presenceOnline 复核注册表：玩家当前是否仍有存活直连（未注入复核端口时一律视为不在场）。
func (b *BattleActor) presenceOnline(playerID string) bool {
	return b.presence != nil && b.presence.Online(playerID)
}

// eliminate 执行掉线超时的后果（规格 §9.3）：出局广播（发给当前名单全部玩家，含掉线者本人）
// + 移出参战名单；移出后只剩一人即该玩家判胜，走既有结算路径。
//
// 判胜在移出名单**之前**结算：结算记录与结算事件保留完整参战名单（掉线者记 Win=false）。
// 名单被清空（全员掉线）时无人可判胜，本局继续到帧数上限按既有路径自然结算。
func (b *BattleActor) eliminate(ctx core.ActorContext, playerID string) {
	for player := range b.players {
		_ = b.pusher.PublishOut(ctx.Context(), player, b.battleID, playerID,
			battlev1.PlayerOutReason_PLAYER_OUT_REASON_OFFLINE_TIMEOUT)
	}
	winner, last := lastStanding(b.players, playerID)
	if last {
		b.clearOffline(playerID)
		b.settle(ctx, settleOutcome{Winner: winner, Frames: b.lastFrame})
		return
	}
	delete(b.players, playerID)
	b.clearOffline(playerID)
}

// lastStanding 返回移出 playerID 后名单里仅剩的那名玩家；
// 剩余人数不是恰好一人（0 人或 ≥2 人）时返回空串与 false。
func lastStanding(players map[string]struct{}, playerID string) (string, bool) {
	rest := ""
	for player := range players {
		if player == playerID {
			continue
		}
		if rest != "" {
			return "", false
		}
		rest = player
	}
	return rest, rest != ""
}
