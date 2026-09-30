package app

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/migrate"
	"github.com/huangyuCN/atlas/contrib/actor/rebalance"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/registry"
	"go.uber.org/fx"
)

// migratePort 把集群运行时适配为迁移投递端口（剥离可选投递参数）。
//
// 本文件是战斗 actor 迁移能力的装配之家（规格 §8）：
//   - migrate.Ops：框架 rollout.ClusterOps 的装饰器，在 drain 与 activate 之间搬运战斗状态；
//   - migrate.Quarantiner：drain 成功后旧属主进入隔离窗口（窗口内拒绝懒激活本 PID，
//     盖住 Release↔新属主 Claim 的空窗，杜绝双 actor）；
//   - migrate.AutoChooser：未显式指定目标节点时，用 rebalance 的负载快照与迁移计划选点；
//   - MigrationInbox：每节点一个的收件箱/编排 actor（预置待恢复状态 + 触发迁移）。
type migratePort struct{ rt *pkgactor.Runtime }

// Ask 实现 migrate.Port：按 PID 寻址的同步请求（自动跨节点路由）。
func (p migratePort) Ask(ctx context.Context, pid types.PID, req any) (any, error) {
	return p.rt.Ask(ctx, pid, req)
}

// Tell 实现 migrate.Port：按 PID 寻址的单向投递。
func (p migratePort) Tell(ctx context.Context, pid types.PID, msg any) error {
	return p.rt.Tell(ctx, pid, msg)
}

// QuarantinePID 实现 migrate.Quarantiner：把 PID 加入**本节点**的隔离窗口
// （框架 cluster.Runtime.QuarantinePID；窗口到期自动失效，无需显式解除）。
// 迁移由当前属主节点的收件箱驱动，故本机就是执行了 Release 的旧属主（见 actor.MigrationInbox）。
func (p migratePort) QuarantinePID(pid types.PID, window time.Duration) error {
	return p.rt.Raw().QuarantinePID(pid, window)
}

// NewMigrateOps 装配迁移编排面：框架 ClusterOps（drain/activate/verify + 目录属主与 epoch）
// + 战斗状态搬运装饰 + rebalance 选点 + 旧属主隔离窗口（drain 成功后拒绝懒激活本 PID）。
func NewMigrateOps(rt *pkgactor.Runtime, discovery registry.Discovery) *migrate.Ops {
	return migrate.NewOps(rt.Ops(), migratePort{rt: rt}, rt.NodeID(), nil).
		WithQuarantine(migratePort{rt: rt}).
		WithChooser(newMigrateChooser(rt, discovery))
}

// newMigrateChooser 组装自动选点：负载快照按需同步刷新（不常驻后台循环，只在触发迁移时扫一轮），
// 候选节点由服务发现给出，可迁移类型白名单只放战斗 actor（别的类型没有状态搬运语义）。
func newMigrateChooser(rt *pkgactor.Runtime, discovery registry.Discovery) *migrate.AutoChooser {
	if discovery == nil {
		return nil
	}
	cfg := rebalance.DefaultConfig()
	cfg.MigratableTypes = []string{consts.ActorTypeBattle}
	snap := rebalance.NewSnapshotter(rt.Directory(), cfg, nil, nil, nil)
	return migrate.NewAutoChooser(snap, nodeLister{discovery: discovery})
}

// nodeLister 用服务发现列出候选 battle 节点（负载计数由快照覆盖，此处只给静态属性）。
type nodeLister struct{ discovery registry.Discovery }

// List 实现 migrate.NodeLister。
func (l nodeLister) List(ctx context.Context) ([]rebalance.Node, error) {
	insts, err := l.discovery.GetService(ctx, consts.ServiceBattle)
	if err != nil {
		return nil, fmt.Errorf("app: 查询 battle 节点失败: %w", err)
	}
	out := make([]rebalance.Node, 0, len(insts))
	for _, ins := range insts {
		if ins == nil {
			continue
		}
		out = append(out, rebalance.Node{
			ID:      ins.ID,
			Version: ins.Version,
			Weight:  1,
			// 静态计数：真实计数由快照按目录扫描覆盖，缺元数据时为 0。
			ProcCount: procCountOf(ins),
		})
	}
	return out, nil
}

// procCountOf 读取实例元数据里的进程计数（与 cluster 的放置适配同一键）。
func procCountOf(ins *registry.ServiceInstance) int {
	n, err := strconv.Atoi(ins.Metadata["proc_count"])
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// registerMigration 注册迁移收件箱 actor 并在运行时就绪后**本机自领**：
// 收件箱必须落在 PID 名字所指的节点上（新属主在 OnStart 里按 ctx.Node() 定位它），
// 而 SpawnAuto 的懒激活由投递侧选点——先把归属钉在本节点，跨节点预置状态才不会投错机器。
// runner 为 nil（未装配编排器）时收件箱只做预置，触发迁移会明确报错（不静默降级）。
func registerMigration(lc fx.Lifecycle, rt *pkgactor.Runtime, runner actor.MigrateRunner) error {
	if err := rt.Register(actor.NewMigrationInboxProps(rt.NodeID(), runner, nil)); err != nil {
		return err
	}
	pid, err := types.NewPID(consts.ActorTypeBattleMigrate, rt.NodeID())
	if err != nil {
		return fmt.Errorf("app: 迁移收件箱 PID 非法: %w", err)
	}
	lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
		if _, err := rt.Raw().Spawn(ctx, pid); err != nil {
			return fmt.Errorf("app: 本机领取迁移收件箱失败: %w", err)
		}
		return nil
	}})
	return nil
}
