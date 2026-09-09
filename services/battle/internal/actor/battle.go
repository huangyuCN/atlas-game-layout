// Package actor 提供 battle 服务的 actor 装配：BattleActor（集群懒激活）承载
// 一局战斗：内嵌 lockstep 会话（帧引擎）+ 帧广播下发 + 结算落库与事件（M7）。
//
// BattleActor 按业务域分文件：
//   - battle.go         装配/配置/生命周期（本文件）
//   - battle_session.go 会话域（Create/Join/Reconnect/GetState）
//   - battle_frame.go   帧同步/结算域（FrameInput/onFrameResult/checkSettle）
package actor

import (
	"context"
	"fmt"
	"hash/fnv"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz/simulator"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/data/repo"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/pubsub"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	lockstepimpl "github.com/huangyuCN/atlas/contrib/lockstep"
	"github.com/huangyuCN/atlas/lockstep"
)

// Config 是战斗装配参数（Props 工厂用）。
type Config struct {
	TickInterval  int64  // 帧间隔（纳秒）
	TrackLen      int32  // 赛道长度（胜负判定）
	MaxFrames     uint64 // 帧数上限
	SnapshotEvery uint64 // 快照周期（帧）
}

// 默认战斗参数（示例竞速对局：10fps、5 格赛道、60 帧上限）。
const (
	DefaultTickInterval  = int64(100 * time.Millisecond)
	DefaultTrackLen      = int32(5)
	DefaultMaxFrames     = uint64(60)
	DefaultSnapshotEvery = uint64(10)
)

// DefaultConfig 返回默认战斗参数。
func DefaultConfig() Config {
	return Config{
		TickInterval:  DefaultTickInterval,
		TrackLen:      DefaultTrackLen,
		MaxFrames:     DefaultMaxFrames,
		SnapshotEvery: DefaultSnapshotEvery,
	}
}

// Runtime 是 BattleActor 依赖的最小 actor 运行时接口：
// pkg/actor.Runtime（集群）与 core.LocalRuntime（单测）均满足。
type Runtime interface {
	Register(props core.Props) error
	Spawn(ctx context.Context, pid types.PID) (core.Ref, error)
	Stop(ctx context.Context, pid types.PID) error
	Tell(ctx context.Context, pid types.PID, msg any) error
	Ask(ctx context.Context, pid types.PID, req any, opts ...core.SendOption) (any, error)
}

// Props 是 BattleActor 的注册规格（SpawnAuto 懒激活，matcher 开局拉起）。
type Props struct {
	Rt         Runtime
	Registry   *pubsub.Registry
	Storage    lockstep.Storage // 帧数据持久化（按 sessionID 分片，可共享）
	ResultRepo repo.ResultRepo
	Notifier   biz.BattleNotifier
	Publisher  biz.SettlePublisher
	Cfg        Config
}

// DefaultDeps 是默认战斗装配依赖（server fx 装配与 assemble 可编程装配共用）。
type DefaultDeps struct {
	Rt         Runtime
	Registry   *pubsub.Registry
	ResultRepo repo.ResultRepo
	Notifier   biz.BattleNotifier
	Publisher  biz.SettlePublisher
}

// NewRuntimeProps 以给定战斗参数组装 Props：
// MemoryStorage 按 sessionID 分片可共享，参数默认值由调用方给出（DefaultConfig）。
func NewRuntimeProps(d DefaultDeps, cfg Config) core.Props {
	return NewProps(Props{
		Rt:         d.Rt,
		Registry:   d.Registry,
		Storage:    lockstepimpl.NewMemoryStorage(),
		ResultRepo: d.ResultRepo,
		Notifier:   d.Notifier,
		Publisher:  d.Publisher,
		Cfg:        cfg,
	})
}

// NewProps 构造 BattleActor 注册规格。
func NewProps(p Props) core.Props {
	return core.Props{
		Type: consts.ActorTypeBattle,
		NewHandler: func(pid types.PID) core.Handler {
			b := &BattleActor{
				pid:        pid,
				rt:         p.Rt,
				registry:   p.Registry,
				storage:    p.Storage,
				resultRepo: p.ResultRepo,
				notifier:   p.Notifier,
				publisher:  p.Publisher,
				cfg:        p.Cfg,
				players:    make(map[string]struct{}),
			}
			return battlev1.NewBattleActorServer(b, core.WithLocalTell(b.onFrameResult))
		},
		SpawnMode:     core.SpawnAuto,
		Tell:          pkgactor.DefaultTellChain(),
		Ask:           pkgactor.DefaultAskChain(),
		DecodeInbound: battlev1.NewBattleActorDecodeInbound(),
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
	notifier   biz.BattleNotifier
	publisher  biz.SettlePublisher
	cfg        Config

	battleID   string
	matchID    string
	storage    lockstep.Storage
	players    map[string]struct{} // 参战玩家
	sessionPID types.PID           // lockstep 会话 PID（每局唯一 type 保证 sim 隔离）
	settled    bool
}

// OnStart 实现 core.Handler：拉起 lockstep 会话（每局独立 type 保证 sim 状态隔离）并订阅帧广播。
func (b *BattleActor) OnStart(ctx core.ActorContext) error {
	b.cctx = ctx
	b.battleID = ctx.Self().UID()
	// contrib 的 NewSessionProps 按 type 注册且 sim 为共享单例：
	// 多局并发时同 type 会共享 sim 状态，故每局注册唯一 type（lockstep+短哈希）。
	// 代价是进程内 type 注册表随对局数增长（示例取舍；生产建议为 contrib 增加 sim 工厂）。
	sessionType := lockstepType(b.battleID)
	sessionCfg := lockstepimpl.SessionConfig{
		SessionID:      b.battleID,
		Mode:           lockstep.ModeServerAuthoritative,
		TickInterval:   time.Duration(b.cfg.TickInterval),
		MaxPlayers:     2,
		SnapshotEvery:  lockstep.FrameID(b.cfg.SnapshotEvery),
		EnableRollback: false,
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
