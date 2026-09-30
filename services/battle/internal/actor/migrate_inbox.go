// 迁移收件箱/编排 actor（每节点一个，PID = battlemigrate:<node_id>，规格 §8）。
//
// 它承担两件事，都必须「在目标节点本机」完成：
//  1. **预置待恢复状态**：迁移编排器把旧属主导出的状态投递到这里暂存；新属主的战斗 actor
//     在 OnStart 里取走它（必须先于会话 actor 的建立，见 battle_migrate.go 的恢复路径）。
//  2. **触发迁移**：管理面按 PID 寻址到**当前属主节点**的收件箱，由它驱动
//     services/battle/internal/migrate 的编排（drain → activate → verify）。
//
// 消息是「迁移控制面」的独立 proto（api/battle/v1/migration.proto），不是服务 op：
// 故本 actor 自带分发与入站解码（DecodeMigrationInbound），战斗 actor 的入站解码
// 也复用同一份钩子（两条链路共用一套迁移消息词表）。

package actor

import (
	"context"
	"fmt"
	"sync"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1actor "github.com/huangyuCN/atlas-game-layout/api/battle/v1/actor"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/migrate"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/log"
	"google.golang.org/protobuf/proto"
)

// migrationMessageTypes 是迁移控制面消息的「消息全名 → 新建实例」表，
// 供入站解码把 wire payload 归一为具体 Go 消息（键与 types.FullName 的归一结果一致）。
var migrationMessageTypes = map[string]func() proto.Message{
	"battle.v1.PrepareBattleMigrationRequest": func() proto.Message { return &battlev1.PrepareBattleMigrationRequest{} },
	"battle.v1.ResumeBattleMigrationRequest":  func() proto.Message { return &battlev1.ResumeBattleMigrationRequest{} },
	"battle.v1.StageBattleMigrationRequest":   func() proto.Message { return &battlev1.StageBattleMigrationRequest{} },
	"battle.v1.TakeBattleMigrationRequest":    func() proto.Message { return &battlev1.TakeBattleMigrationRequest{} },
	"battle.v1.MigrateBattleRequest":          func() proto.Message { return &battlev1.MigrateBattleRequest{} },
}

// battleDecodeInbound 是生成的服务消息解码表（迁移消息未命中时的回落目标）。
var battleDecodeInbound = battlev1actor.NewBattleServiceDecodeInbound()

// DecodeMigrationInbound 是迁移控制面消息的入站解码钩子：命中迁移消息自行反序列化，
// 未命中回落生成的服务消息解码表；两条链路都会走到这里，故不可只认一边。
func DecodeMigrationInbound(typeURL string, payload []byte) (any, error) {
	if build, ok := migrationMessageTypes[types.FullName(typeURL)]; ok {
		msg := build()
		if err := proto.Unmarshal(payload, msg); err != nil {
			return nil, fmt.Errorf("actor: 解码迁移消息 %q 失败: %w", typeURL, err)
		}
		return msg, nil
	}
	return battleDecodeInbound(typeURL, payload)
}

// MigrateRunner 是迁移编排端口：生产实现是 migrate.Ops（带状态搬运的 rollout.ClusterOps
// 装饰器），单测可注入内存实现。
type MigrateRunner interface {
	// Migrate 把指定战斗迁移到目标节点；targetNode 为空时由编排器选点。
	Migrate(ctx context.Context, battleID, targetNode string) (migrate.Outcome, error)
}

// MigrationInbox 是迁移收件箱 actor 的业务实现：状态表在本 actor 线程内读写，
// 加锁只为防御「单测直接并发调用同一实例」。
type MigrationInbox struct {
	node   string
	runner MigrateRunner
	log    log.Logger

	mu     sync.Mutex
	staged map[string]*battlev1.BattleMigrationState
}

// NewMigrationInboxProps 构造迁移收件箱 actor 的注册规格（SpawnAuto：被投递时本机懒激活）。
func NewMigrationInboxProps(node string, runner MigrateRunner, logger log.Logger) core.Props {
	if logger == nil {
		logger = log.DefaultLogger
	}
	return core.Props{
		Type: consts.ActorTypeBattleMigrate,
		NewHandler: func(types.PID) core.Handler {
			return newInboxDispatch(&MigrationInbox{node: node, runner: runner, log: logger,
				staged: make(map[string]*battlev1.BattleMigrationState)})
		},
		SpawnMode:     core.SpawnAuto,
		Tell:          pkgactor.DefaultTellChain(),
		Ask:           pkgactor.DefaultAskChain(),
		DecodeInbound: DecodeMigrationInbound,
	}
}

// inboxDispatch 是本 actor 的分发桩：迁移控制面消息走静态 switch，未命中回落本地路由。
type inboxDispatch struct {
	*core.DispatchBase
	impl *MigrationInbox
}

// newInboxDispatch 组装分发桩（生命周期与前置钩子经 DispatchBase 断言转发）。
func newInboxDispatch(impl *MigrationInbox) *inboxDispatch {
	return &inboxDispatch{DispatchBase: core.NewDispatchBase(impl), impl: impl}
}

// OnTell 分发单向消息。
func (d *inboxDispatch) OnTell(ctx core.ActorContext, msg any) error {
	if m, ok := msg.(*battlev1.StageBattleMigrationRequest); ok {
		return d.impl.onStage(ctx, m)
	}
	return d.FallbackTell(ctx, msg)
}

// OnAsk 分发请求消息。
func (d *inboxDispatch) OnAsk(ctx core.ActorContext, req any) (any, error) {
	switch r := req.(type) {
	case *battlev1.TakeBattleMigrationRequest:
		return d.impl.onTake(ctx, r)
	case *battlev1.MigrateBattleRequest:
		return d.impl.onMigrate(ctx, r)
	default:
		return d.FallbackAsk(ctx, req)
	}
}

// onStage 预置一份待恢复状态（按 battle_id 覆盖：同一局的重复预置以最后一次为准）。
func (m *MigrationInbox) onStage(ctx core.ActorContext, req *battlev1.StageBattleMigrationRequest) error {
	state := req.GetState()
	if state.GetBattleId() == "" {
		return fmt.Errorf("actor: 预置迁移状态缺少 battle_id")
	}
	m.mu.Lock()
	m.staged[state.GetBattleId()] = state
	m.mu.Unlock()
	ctx.Logger().Info("battle: 预置迁移状态",
		"battle", state.GetBattleId(), "node", m.node,
		"snapshot_frame", state.GetSnapshotFrame(), "source_node", state.GetSourceNode())
	return nil
}

// onTake 取走一份待恢复状态（取出即消费：新属主只可能恢复一次）。
func (m *MigrationInbox) onTake(_ core.ActorContext, req *battlev1.TakeBattleMigrationRequest) (any, error) {
	m.mu.Lock()
	state, ok := m.staged[req.GetBattleId()]
	if ok {
		delete(m.staged, req.GetBattleId())
	}
	m.mu.Unlock()
	if !ok {
		return &battlev1.TakeBattleMigrationReply{Found: false}, nil
	}
	return &battlev1.TakeBattleMigrationReply{State: state, Found: true}, nil
}

// onMigrate 触发一次迁移（寻址到当前属主节点的收件箱）。
func (m *MigrationInbox) onMigrate(ctx core.ActorContext, req *battlev1.MigrateBattleRequest) (any, error) {
	if m.runner == nil {
		return nil, fmt.Errorf("actor: 本节点未装配迁移编排器（battle.migration 未启用）")
	}
	out, err := m.runner.Migrate(ctx.Context(), req.GetBattleId(), req.GetTargetNode())
	if err != nil {
		return nil, err
	}
	return &battlev1.MigrateBattleReply{FromNode: out.FromNode, ToNode: out.ToNode, Epoch: out.Epoch}, nil
}
