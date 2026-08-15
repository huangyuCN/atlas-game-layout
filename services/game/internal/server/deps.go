// Package server 负责 game 服务的传输层组装：
// gRPC（玩家业务）+ HTTP（健康/管理）+ 依赖装配（infra）与 PlayerActor 注册。
package server

import (
	"fmt"
	"time"

	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	gameactor "github.com/huangyuCN/atlas-game-layout/services/game/internal/actor"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/repo"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/infra"
	"go.uber.org/fx"
)

// 时间参数（与 gateway 会话租期对齐）。
const (
	// sessionTTL 是玩家在线会话租期。
	sessionTTL = 30 * time.Second
	// snapshotTTL 是玩家快照缓存租期。
	snapshotTTL = 5 * time.Minute
	// snapshotTick 是定时快照周期。
	snapshotTick = 10 * time.Second
)

// newPlayerStore 装配玩家仓储（redis 快照 + mongo 持久化组合）。
func newPlayerStore(cache *repo.RedisPlayerCache, persist *repo.MongoPlayerRepo) *repo.PlayerStore {
	return repo.NewPlayerStore(cache, persist)
}

// newPlayerService 装配玩家业务实现（biz.PlayerService 接口）。
func newPlayerService(store *repo.PlayerStore, cli *pkredis.Client) biz.PlayerService {
	return handler.NewPlayerHandler(store, repo.NewRedisSessionStore(cli), biz.PlayerServiceOptions{
		SessionTTL: sessionTTL,
	})
}

// newPlayerActorClient 装配玩家状态访问（PlayerActor 转发，grpc 用）。
func newPlayerActorClient(rt *pkgactor.Runtime) biz.PlayerStateAccess {
	return gameactor.NewPlayerClient(rt)
}

// registerActor 注册 PlayerActor 并接入生命周期（含定时快照与下线落库）。
func registerActor(lc fx.Lifecycle, rt *pkgactor.Runtime, svc biz.PlayerService, store *repo.PlayerStore) error {
	if err := rt.Register(gameactor.NewProps(svc, store, snapshotTTL, snapshotTick)); err != nil {
		return fmt.Errorf("server: 注册 PlayerActor 失败: %w", err)
	}
	infra.RegisterRuntimeLifecycle(lc, rt)
	return nil
}
