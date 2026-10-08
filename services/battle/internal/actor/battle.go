// Package actor 提供 battle 服务的 actor 装配：BattleActor（集群懒激活）承载
// 一局战斗：内嵌 lockstep 会话（帧引擎）+ 帧广播下发 + 结算落库与事件。
//
// 分发由 protoc-gen-atlas-actor 生成的桩接管（battlev1actor.NewBattleService），
// 客户端 op 与集群内部调用共用同一份 service 契约。发起者玩家身份不来自消息体，
// 统一经投递 sender 注入（ActorContext.Sender，Gateway 侧组装）。
//
// BattleActor 按业务域分文件：
//   - battle.go         装配/配置/生命周期（本文件）
//   - battle_session.go 会话域（Create/JoinBattle/SyncFrames/GetState）
//   - battle_frame.go   帧同步/结算域（SendFrameInput/onFrameResult/checkSettle）
//   - battle_ping.go    直连保活探针（Ping：只刷新帧面活跃，不动对局状态）
//   - battle_offline.go 掉线/重连域（打点计时、判负出局、回座取消）
package actor

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1actor "github.com/huangyuCN/atlas-game-layout/api/battle/v1/actor"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz/simulator"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/data/repo"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/pubsub"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	lockstepimpl "github.com/huangyuCN/atlas/contrib/lockstep"
	"github.com/huangyuCN/atlas/lockstep"
	"github.com/huangyuCN/atlas/metrics"
)

// Config 是战斗装配参数（Props 工厂用）。
type Config struct {
	TickInterval  int64                    // 帧间隔（纳秒）
	TrackLen      int32                    // 赛道长度（胜负判定）
	MaxFrames     uint64                   // 帧数上限
	SnapshotEvery uint64                   // 快照周期（帧）
	MaxPlayers    int                      // 每局会话容量（0 = 不限）
	TicketTTL     time.Duration            // 入场票据有效期（默认 DefaultTicketTTL）
	EdgeEndpoints []*battlev1.EdgeEndpoint // 接入层「传输面 → 地址」列表（回填进 IssueEntryTicketReply）
	TicketKey     []byte                   // 入场票据 AEAD 密钥（32 字节；装配期由配置 base64 解码校验）
	// OfflineTimeout 是掉线判定窗口（规格 §9.2；≤0 = 关闭掉线判定，仅供测试/特例）。
	// 数据报面帧面的空闲读超时按它推导（1/3）并在装配期校验，见 server.FramePolicy。
	OfflineTimeout time.Duration
}

// 默认战斗参数（示例竞速对局：10fps、5 格赛道、60 帧上限）。
const (
	DefaultTickInterval  = int64(100 * time.Millisecond)
	DefaultTrackLen      = int32(5)
	DefaultMaxFrames     = uint64(60)
	DefaultSnapshotEvery = uint64(10)
	// DefaultMaxPlayers 是每局会话容量的缺省值（示例竞速为双人对局）。
	DefaultMaxPlayers = 2
	// DefaultOfflineTimeout 是掉线判定窗口的缺省值（规格 §9.2：默认 15s，不写死在判定点）。
	DefaultOfflineTimeout = 15 * time.Second
)

// 掉线策略指标名（规格 §10：掉线/重连可见）。
const (
	// MetricOfflineTimeouts 是掉线超时判负的累计次数。
	MetricOfflineTimeouts = "battle_offline_timeouts_total"
	// MetricReconnects 是掉线窗口内回座的累计次数。
	MetricReconnects = "battle_reconnects_total"
)

// DefaultConfig 返回默认战斗参数（含出票默认 TTL 与掉线窗口；密钥与接入层面列表无默认值，须由配置注入）。
func DefaultConfig() Config {
	return Config{
		TickInterval:   DefaultTickInterval,
		TrackLen:       DefaultTrackLen,
		MaxFrames:      DefaultMaxFrames,
		SnapshotEvery:  DefaultSnapshotEvery,
		MaxPlayers:     DefaultMaxPlayers,
		TicketTTL:      DefaultTicketTTL,
		OfflineTimeout: DefaultOfflineTimeout,
	}
}

// Runtime 是 BattleActor 依赖的最小 actor 运行时接口：
// pkg/actor.Runtime（集群）与 core.LocalRuntime（单测）均满足。
type Runtime interface {
	Register(props core.Props) error
	Spawn(ctx context.Context, pid types.PID) (core.Ref, error)
	Stop(ctx context.Context, pid types.PID) error
	Tell(ctx context.Context, pid types.PID, msg any, opts ...core.SendOption) error
	Ask(ctx context.Context, pid types.PID, req any, opts ...core.SendOption) (any, error)
}

// Props 是 BattleActor 的注册规格（SpawnAuto 懒激活，matcher 开局拉起）。
type Props struct {
	Rt         Runtime
	Registry   *pubsub.Registry
	Storage    lockstep.Storage // 帧数据持久化（按 sessionID 分片，可共享）
	ResultRepo repo.ResultRepo
	// Pusher 是直连推送端口（帧广播/战斗结束/出局经帧引擎直发客户端，不回网关）。
	Pusher    biz.BattlePusher
	Publisher biz.SettlePublisher
	// Presence 是直连在场复核端口（规格 §9.2 硬约束②；nil = 一律视为不在场，仅供单测）。
	Presence biz.ConnPresence
	// Metrics 是指标采集器（掉线/重连计数；nil = noop）。
	Metrics metrics.Collector
	Cfg     Config
}

// DefaultDeps 是默认战斗装配依赖（server fx 装配与 assemble 可编程装配共用）。
type DefaultDeps struct {
	Rt         Runtime
	Registry   *pubsub.Registry
	ResultRepo repo.ResultRepo
	Pusher     biz.BattlePusher
	Publisher  biz.SettlePublisher
	Presence   biz.ConnPresence
	Metrics    metrics.Collector
}

// NewRuntimeProps 以给定战斗参数组装 Props：
// MemoryStorage 按 sessionID 分片可共享，参数默认值由调用方给出（DefaultConfig）。
func NewRuntimeProps(d DefaultDeps, cfg Config) core.Props {
	return NewProps(Props{
		Rt:         d.Rt,
		Registry:   d.Registry,
		Storage:    lockstepimpl.NewMemoryStorage(),
		ResultRepo: d.ResultRepo,
		Pusher:     d.Pusher,
		Publisher:  d.Publisher,
		Presence:   d.Presence,
		Metrics:    d.Metrics,
		Cfg:        cfg,
	})
}

// NewProps 构造 BattleActor 注册规格。
func NewProps(p Props) core.Props {
	if p.Metrics == nil {
		p.Metrics = metrics.Noop()
	}
	return core.Props{
		Type: battlev1actor.BattleServiceActorType,
		NewHandler: func(pid types.PID) core.Handler {
			b := &BattleActor{
				pid:        pid,
				rt:         p.Rt,
				registry:   p.Registry,
				storage:    p.Storage,
				resultRepo: p.ResultRepo,
				pusher:     p.Pusher,
				publisher:  p.Publisher,
				presence:   p.Presence,
				metrics:    p.Metrics,
				cfg:        p.Cfg,
				players:    make(map[string]struct{}),
				offline:    make(map[string]core.CancelFunc),
			}
			return battlev1actor.NewBattleService(b,
				core.WithLocalTell(b.onFrameResult),
				// 连接生命周期消息（帧引擎钩子 → 注册表映射 → 本地投递，规格 §9.1）。
				core.WithLocalTell(b.onPlayerOnline),
				core.WithLocalTell(b.onPlayerOffline),
				core.WithLocalTell(b.onOfflineTimeout),
				// 迁移窗口开关（批次 7 在属主切换窗口调用，规格 §9.6）。
				core.WithLocalTell(b.onMigrationPause),
				// 迁移状态导出/恢复（迁移控制面消息，非服务 op，见 battle_migrate.go）。
				core.WithLocalAsk(b.onPrepareMigration),
				core.WithLocalAsk(b.onResumeMigration),
			)
		},
		SpawnMode: core.SpawnAuto,
		Tell:      pkgactor.DefaultTellChain(),
		Ask:       pkgactor.DefaultAskChain(),
		// 入站解码：迁移控制面消息 + 生成的服务消息（一份钩子，两条链路共用）。
		DecodeInbound: DecodeMigrationInbound,
	}
}

// BattleActor 是一局战斗的业务外壳：
// 帧引擎由内嵌 lockstep 会话（lockstep:<battleID>）承担。
type BattleActor struct {
	pid        types.PID
	cctx       core.ActorContext
	rt         Runtime
	registry   *pubsub.Registry
	resultRepo repo.ResultRepo
	pusher     biz.BattlePusher
	publisher  biz.SettlePublisher
	presence   biz.ConnPresence
	metrics    metrics.Collector
	cfg        Config

	battleID   string
	matchID    string
	storage    lockstep.Storage
	players    map[string]struct{} // 参战玩家
	sessionPID types.PID           // lockstep 会话 PID（每局唯一 type 保证 sim 隔离）
	settled    bool

	offline   map[string]core.CancelFunc // 参战者 → 掉线计时取消句柄（nil = 已打点但无计时，规格 §9.2）
	migration bool                       // 迁移窗口：暂停掉线计时（规格 §9.6）
	restored  bool                       // 本实例由迁移恢复拉起（需把名单重新登记进新会话）
	lastFrame uint64                     // 最近一次帧广播的帧号（判负结算的总帧数）
}

// OnStart 实现 core.Handler：拉起 lockstep 会话（每局独立 type 保证 sim 状态隔离）并订阅帧广播。
func (b *BattleActor) OnStart(ctx core.ActorContext) error {
	b.cctx = ctx
	b.battleID = ctx.Self().UID()
	// 迁移恢复必须在建立会话 actor **之前**：会话的 OnStart 会读 LatestSnapshot 还原模拟器，
	// 状态晚到就只能从第 0 帧重新开始（规格 §8「新节点恢复」）。
	if err := b.restoreIfMigrated(ctx); err != nil {
		return fmt.Errorf("actor: 恢复迁移状态失败: %w", err)
	}
	// contrib 的 NewSessionProps 按 type 注册且 sim 为共享单例：
	// 多局并发时同 type 会共享 sim 状态，故每局注册唯一 type（lockstep+短哈希）。
	// 代价是进程内 type 注册表随对局数增长（示例取舍；生产建议为 contrib 增加 sim 工厂）。
	sessionType := lockstepType(b.battleID)
	sessionCfg := lockstepimpl.SessionConfig{
		SessionID:      b.battleID,
		Mode:           lockstep.ModeServerAuthoritative,
		TickInterval:   time.Duration(b.cfg.TickInterval),
		MaxPlayers:     b.cfg.MaxPlayers,
		SnapshotEvery:  lockstep.FrameID(b.cfg.SnapshotEvery),
		EnableRollback: false,
		// 恢复失败即启动失败（fail-closed）：迁移恢复的唯一依据是快照，
		// 静默从第 0 帧重开会让「迁移后帧号一致」变成无声的状态丢失。
		RestoreRequired: true,
	}
	props := lockstepimpl.NewSessionProps(sessionCfg, simulator.New(b.cfg.TrackLen, b.cfg.MaxFrames), b.storage, b.registry)
	props.Type = sessionType
	if err := b.rt.Register(props); err != nil {
		return fmt.Errorf("actor: 注册 lockstep 会话失败: %w", err)
	}
	b.sessionPID, _ = types.NewPID(sessionType, b.battleID)
	if _, err := b.rt.Spawn(ctx.Context(), b.sessionPID); err != nil {
		return fmt.Errorf("actor: 拉起 lockstep 会话失败: %w", err)
	}
	// 迁移恢复：会话 actor 是新建实例，参战成员必须重新登记，否则帧输入会被会话拒收
	//（会话按自己的成员表校验 PlayerInput），且状态查询的人数会归零。
	if b.restored {
		if err := b.rejoinSession(ctx); err != nil {
			return err
		}
	}
	return b.registry.Subscribe(frameTopic(b.battleID), ctx.Self())
}

// lockstepType 从战斗 ID 派生每局唯一的 lockstep 类型名：
// 取 FNV-1a 哈希的 8 位十六进制后缀，保证任意战斗 ID 都映射到合法类型段（[a-z][a-z0-9_]*）。
func lockstepType(battleID string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(battleID))
	return fmt.Sprintf("lockstep%08x", h.Sum32())
}

// OnStop 实现 core.Handler：停 lockstep 会话（目录归属释放）。
func (b *BattleActor) OnStop(ctx core.ActorContext, _ types.ExitReason) error {
	_ = b.rt.Stop(ctx.Context(), b.sessionPID)
	return nil
}
