package app

import (
	"context"
	"fmt"
	gamev1actor "github.com/huangyuCN/atlas-game-layout/api/game/v1/actor"

	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/observability"
	gameactor "github.com/huangyuCN/atlas-game-layout/services/game/internal/actor"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/repo"
	"github.com/huangyuCN/atlas/metrics"
	"go.uber.org/fx"
)

// newPlayerService 装配玩家业务实现（注册/登录，供 PlayerActor 回调）。
// 会话裁决单点收敛到 Gateway：game 侧不装配会话存储。
func newPlayerService(store *repo.PlayerStore) biz.PlayerService {
	return handler.NewPlayerHandler(store, biz.PlayerServiceOptions{})
}

// newPlayerStateAccess 装配玩家状态访问（经 PlayerActor 转发的聚合根单写者客户端）。
func newPlayerStateAccess(rt *pkgactor.Runtime) biz.PlayerStateAccess {
	return gameactor.NewPlayerClient(rt)
}

// newAdminService 装配管理面实现（GM 发道具 + 查玩家/背包/审计）。
// 单次发放上限来自 game.conf.Admin.max_grant_count：缺省/0 由 handler 回退默认值（R12，不可关闭）。
// 审计仓储用具体类型 *repo.MongoAuditRepo 注入：fx 按类型解析不做接口隐式匹配，
// 且它一被消费即在启动期建好幂等键唯一稀疏索引。
// meter 用于审计收尾失败计数（未配置后端时为 noop，打点自动短路）。
func newAdminService(audit *repo.MongoAuditRepo, state biz.PlayerStateAccess, cfg *conf.Bootstrap, meter metrics.Collector) biz.AdminService {
	return handler.NewAdminHandler(audit, state, biz.AdminOptions{
		MaxGrantCount: cfg.GetAdmin().GetMaxGrantCount(),
		Meter:         meter,
	})
}

// matchmakerClientOf 把 matcher gRPC 客户端实现绑定为 biz 接口（供 fx 按接口注入）。
func matchmakerClientOf(m *repo.MatchQueueGRPC) biz.MatchmakerClient { return m }

// registerActor 注册 PlayerActor 并把集群运行时接入 fx 生命周期
// （OnStart 启动懒激活监听、OnStop 触发在持聚合根下线落库）。
// 在线玩家数用拉取式指标按本节点活跃 PlayerActor 统计：值来自运行时真相源，
// 不随急停/重启等不回调 OnStop 的路径漂移。
func registerActor(lc fx.Lifecycle, rt *pkgactor.Runtime, svc biz.PlayerService, store *repo.PlayerStore, match biz.MatchmakerClient, meter metrics.Collector, t Tuning) error {
	if err := rt.Register(gameactor.NewProps(svc, store, match, t.SnapshotTick)); err != nil {
		return fmt.Errorf("app: 注册 PlayerActor 失败: %w", err)
	}
	observability.RegisterObservableGauge(meter, gameactor.MetricPlayersOnline, func() float64 {
		return float64(rt.CountByType(gamev1actor.PlayerServiceActorType))
	})
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return rt.Start(ctx) },
		OnStop:  func(ctx context.Context) error { return rt.Shutdown(ctx) },
	})
	return nil
}
