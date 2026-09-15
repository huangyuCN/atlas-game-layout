package app

import (
	"context"
	"fmt"

	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	gameactor "github.com/huangyuCN/atlas-game-layout/services/game/internal/actor"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/repo"
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

// matchmakerClientOf 把 matcher gRPC 客户端实现绑定为 biz 接口（供 fx 按接口注入）。
func matchmakerClientOf(m *repo.MatchQueueGRPC) biz.MatchmakerClient { return m }

// registerActor 注册 PlayerActor 并把集群运行时接入 fx 生命周期
// （OnStart 启动懒激活监听、OnStop 触发在持聚合根下线落库）。
func registerActor(lc fx.Lifecycle, rt *pkgactor.Runtime, svc biz.PlayerService, store *repo.PlayerStore, match biz.MatchmakerClient, t Tuning) error {
	if err := rt.Register(gameactor.NewProps(svc, store, match, t.SnapshotTTL, t.SnapshotTick)); err != nil {
		return fmt.Errorf("app: 注册 PlayerActor 失败: %w", err)
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return rt.Start(ctx) },
		OnStop:  func(ctx context.Context) error { return rt.Shutdown(ctx) },
	})
	return nil
}
