// Package actor 提供 game 服务的 actor 装配：PlayerActor（集群懒激活）承载
// 玩家在线态：内存聚合根（cow 写）+ 统一落盘（Redis 主路径 / Mongo 降级）+
// 下线流程反转（落库成功才停止，双库失败进入在线冻结）。
// 会话裁决单点收敛到 Gateway（会话管理器）：game 侧不持 token 副本。
//
// 分发由 protoc-gen-atlas-actor 生成的桩接管（gamev1.NewPlayerServiceServer）：
// 业务 actor 只实现 PlayerServiceServer 业务接口与可选生命周期（OnStart/OnStop）；
// 本地消息（tickSnapshot）经 WithLocalTell 类型路由注册，无 switch 分发。
package actor

import (
	"time"

	atlaslog "github.com/huangyuCN/atlas/log"

	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/models"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/repo"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// tickSnapshot 是定时落盘自消息（OnStart 注册 Repeat，经本地类型路由分发；
// 冻结期重试也复用它，见 freeze）。
type tickSnapshot struct{}

// 停止前落库的短重试参数（同步执行；无论成败都退出，丢已冻结窗口数据）。
const (
	stopRetryTimes = 3
	stopRetryDelay = 500 * time.Millisecond
	// frozenRetryInterval 是在线冻结期间的重试周期（每分钟）。
	frozenRetryInterval = time.Minute
)

// NewProps 构造 PlayerActor 注册规格（SpawnAuto 懒激活 + 默认中间件链）。
// snapTick 是统一落盘周期（≤0 关闭定时落盘）；match 是匹配队列客户端
// （nil 降级：匹配方法返回内部错误，单测可注入 fake）。
// 在线玩家数由拉取式指标按本节点活跃实例统计（见 app.registerActor），
// 不在生命周期里成对增减——避免急停/重启等不回调 OnStop 的路径造成漂移。
func NewProps(svc biz.PlayerService, store repo.PlayerRepo, match biz.MatchmakerClient, snapTick time.Duration) core.Props {
	return core.Props{
		Type: consts.ActorTypePlayer,
		NewHandler: func(pid types.PID) core.Handler {
			p := &PlayerActor{
				pid: pid, svc: svc, store: store, match: match, snapTick: snapTick,
			}
			return gamev1.NewPlayerServiceServer(p, core.WithLocalTell(p.onTickSnapshot))
		},
		SpawnMode:     core.SpawnAuto,
		Tell:          pkgactor.DefaultTellChain(),
		Ask:           pkgactor.DefaultAskChain(),
		DecodeInbound: gamev1.NewPlayerServiceDecodeInbound(),
	}
}

// MetricPlayersOnline 是在线玩家数 gauge（拉取式：采集时统计本节点活跃 PlayerActor）。
const MetricPlayersOnline = "game_players_online"

// PlayerActor 是玩家在线态 actor：同一玩家全局唯一实例（Locator 注册 player:<id>）。
// 实现 gamev1.PlayerServiceServer 业务接口；OnStart/OnStop 为可选生命周期接口
// （生成桩断言转发）；OnAsk/OnTell 分发由生成桩全权接管。
//
// 耐久性编排（对齐第一期设计）：
//   - 定时落盘走统一 FlushPlayer（Redis 主路径 / Mongo 降级）；
//   - 双库失败进入在线冻结（拒绝改档、保连接、每分钟重试，不 Kick 不 Terminate）；
//   - 下线流程反转：先落库（短重试）成功才 ctx.Stop，失败转 Redis PERSIST 兜底；
//   - OnStop 退化为零失败收尾（清定时器、断聚合根引用）。
type PlayerActor struct {
	gamev1.UnimplementedPlayerServiceServer // 兜底：service 加新 rpc 未实现也能编译
	pid                                     types.PID
	svc                                     biz.PlayerService
	store                                   repo.PlayerRepo
	match                                   biz.MatchmakerClient

	player            *models.Player  // 内存聚合根（在线缓存；登录后持有）
	partyID           string          // 我所在的队伍（纯在线态：下线即离队，名册权威在撮合域）
	snapTick          time.Duration   // 统一落盘周期（≤0 关闭定时落盘）
	lastCRC           uint32          // 上次成功落盘的数据指纹（CRC 跳过用）
	frozen            bool            // 在线冻结标记：双库失败后拒绝改档（数据不再堆积）
	freezeRetryCancel core.CancelFunc // 冻结期重试定时器的取消句柄
}

// OnStart 实现 core.Handler 生命周期：注册定时落盘 Repeat（3 分钟节奏）。
func (p *PlayerActor) OnStart(ctx core.ActorContext) error {
	p.pid = ctx.Self()
	if p.snapTick > 0 {
		ctx.Repeat(p.snapTick, p.snapTick, tickSnapshot{})
	}
	return nil
}

// OnStop 实现 core.Handler 生命周期：零失败收尾（幂等、错误仅记录不阻断）。
// 落库已在停止前的下线流程完成（流程反转：落库成功才发起 Stop）；
// 这里不再有可失败的写路径。
func (p *PlayerActor) OnStop(ctx core.ActorContext, reason types.ExitReason) error {
	if p.match != nil {
		_, _ = p.match.Cancel(ctx.Context(), p.pid.UID())
		if p.partyID != "" {
			_ = p.match.Leave(ctx.Context(), p.partyID, p.pid.UID())
		}
	}
	p.player = nil
	return nil
}

// beginStopFlow 发起下线流程（流程反转的统一入口）：撮合域联动先行（幂等），
// 再进入「落库短重试 → 成功才真正 Stop；双失败 PERSIST 兜底后强制退出」。
func (p *PlayerActor) beginStopFlow(ctx core.ActorContext, reason types.ExitReason) {
	if p.player == nil {
		ctx.Stop(reason)
		return
	}
	// 撮合域联动先行（幂等；失败仅忽略——下线落库才是数据屏障）。
	if p.match != nil {
		_, _ = p.match.Cancel(ctx.Context(), p.pid.UID())
		if p.partyID != "" {
			_ = p.match.Leave(ctx.Context(), p.partyID, p.pid.UID())
			p.partyID = ""
		}
	}
	if p.stopFlush(ctx) {
		// 落库成功：数据安全，真正退出（OnStop 仅零失败收尾）。
		p.player = nil
		ctx.Stop(reason)
		return
	}
	// 双失败短重试后仍失败：redis PERSIST 兜底——最新态转未落库权威副本（零丢失），
	// 强制退出（接受 mongo 缺档；Mongo 恢复后登录选源以 Redis 为准）。
	if perr := p.store.PersistSnapshot(ctx.Context(), p.player.PlayerID); perr != nil {
		atlaslog.Ctx(ctx.Context()).Error("player actor 下线 PERSIST 兜底失败", "uid", p.pid.UID(), "err", perr)
	}
	atlaslog.Ctx(ctx.Context()).Error("player actor 下线双库落盘失败（已 PERSIST 兜底，退出）", "uid", p.pid.UID())
	p.player = nil
	ctx.Stop(reason)
}

// stopFlush 同步短重试落盘（下线路径专用）：3 次 × 500ms。
// 下线必须双写尝试：FlushPlayer 只保证 Redis（成功即止），因此无论 Redis
// 成败都**无条件补写 Mongo**——下线后玩家不再在线，mongo 是唯一长期存储
// （与在线 tick 的「Redis 成功即止」语义不同，设计 §7.1 四组合表）。
// 返回：数据是否已在某一层持久化（决定是否 PERSIST 兜底）。
func (p *PlayerActor) stopFlush(ctx core.ActorContext) bool {
	for i := 0; i < stopRetryTimes; i++ {
		// Redis 写（首选；失败不影响 Mongo 尝试）。
		result, crc, err := p.store.FlushPlayer(ctx.Context(), p.player, p.lastCRC)
		if err == nil && result != repo.FlushBothFailed {
			p.lastCRC = crc
		}
		// Mongo 写（无条件尝试：下线是最后一次落库机会）。
		if err := p.store.SavePlayer(ctx.Context(), p.player); err == nil {
			return true
		}
		time.Sleep(stopRetryDelay)
	}
	return false
}

// flushOrRetry 执行一次统一落盘并按结果编排：
//   - Redis 成功 / Skip：数据已安全 → 解冻（如有）；
//   - 仅 Mongo 成功：数据已落 mongo（零丢失），但在线热路径依赖 Redis
//     （设计 §6.2）→ 强制下线（与「redis 不可用拒登」的降级姿态一致，
//     避免 mongo 变成全局在线热路径）；
//   - 双失败 → 进入在线冻结（拒绝改档 + 每分钟重试），不 Kick 不 Terminate。
func (p *PlayerActor) flushOrRetry(ctx core.ActorContext) {
	if p.player == nil {
		return
	}
	result, crc, err := p.store.FlushPlayer(ctx.Context(), p.player, p.lastCRC)
	if err != nil {
		result = repo.FlushBothFailed // FlushPlayer 返回 err 时按双失败编排
	}
	switch result {
	case repo.FlushRedisOK, repo.FlushSkipped:
		p.lastCRC = crc
		p.unfreeze(ctx)
	case repo.FlushMongoOK:
		p.lastCRC = crc
		atlaslog.Ctx(ctx.Context()).Error("player actor 仅 Mongo 落盘成功（Redis 不可用），强制下线", "uid", p.pid.UID())
		p.beginStopFlow(ctx, types.ExitKilled("redis 不可用，仅 mongo 落盘成功，强制下线"))
	case repo.FlushBothFailed:
		p.freeze(ctx)
	}
}

// freeze 进入在线冻结：拒绝改档（玩法冻结），数据不再堆积。
// 不 Kick、不 Terminate——网关踢人会触发下线落盘链路，冻结态会丢。
// 注册每分钟重试定时器（ctx.Repeat，解冻时取消）。
func (p *PlayerActor) freeze(ctx core.ActorContext) {
	p.frozen = true
	atlaslog.Ctx(ctx.Context()).Error("player actor 冻结改档（双库落盘失败，每分钟重试）", "uid", p.pid.UID())
	if p.freezeRetryCancel != nil {
		p.freezeRetryCancel() // 幂等：先取消旧的重试（防重复注册）
	}
	p.freezeRetryCancel = ctx.After(frozenRetryInterval, tickSnapshot{})
}

// unfreeze 解除在线冻结（取消冻结期重试定时器）。
func (p *PlayerActor) unfreeze(ctx core.ActorContext) {
	if p.freezeRetryCancel != nil {
		p.freezeRetryCancel()
		p.freezeRetryCancel = nil
	}
	if p.frozen {
		atlaslog.Ctx(ctx.Context()).Info("player actor 解冻", "uid", p.pid.UID())
	}
	p.frozen = false
}

// OnBeforeAsk 实现 core.BeforeAskHook：冻结期统一拒绝改档请求（SERVER_FROZEN，
// HTTP 503 语义——客户端退避重试）；只读查询与重新登录放行（重登复用内存
// 权威态，不触存储写）。白名单是 fail-safe 方向：新增改档 rpc 忘记登记 →
// 冻结期被拒（安全）；新增只读 rpc 忘记登记 → 冻结期也被拒（体验损失，登记即恢复）。
func (p *PlayerActor) OnBeforeAsk(_ core.ActorContext, req any) error {
	if !p.frozen {
		return nil
	}
	switch req.(type) {
	case *gamev1.GetPlayerDataReq,
		*gamev1.GetBackpackReq,
		*gamev1.GetPlayerReq,
		*gamev1.GetMatchStatusReq,
		*gamev1.GetPartyReq,
		*gamev1.LoginReq: // 冻结期重登：复用内存权威态
		return nil
	default:
		return errorv1.ErrServerFrozen("服务暂时不可写，请稍后重试")
	}
}

// OnBeforeTell 实现 core.BeforeTellHook：冻结期拒绝 Tell 类玩法消息（玩法冻结
// 语义）；白名单放行下线流程与落盘重试自消息（编排机制本身必须可用）。
// 系统消息（停止控制、生命周期）走邮箱系统通道，不经本钩子。
func (p *PlayerActor) OnBeforeTell(_ core.ActorContext, msg any) error {
	if !p.frozen {
		return nil
	}
	switch msg.(type) {
	case *gamev1.LogoutMsg: // 下线流程：必须通（PERSIST 兜底后退出）
		return nil
	case tickSnapshot: // 落盘重试编排：必须通
		return nil
	default:
		return errorv1.ErrServerFrozen("服务暂时不可写，请稍后重试")
	}
}

// onTickSnapshot 定时统一落盘（在线缓存刷盘，本地路由注册项）。
func (p *PlayerActor) onTickSnapshot(ctx core.ActorContext, _ tickSnapshot) error {
	if p.player == nil {
		return nil
	}
	p.flushOrRetry(ctx)
	return nil
}
