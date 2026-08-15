// Package server 负责 game 服务的传输层组装：
// gRPC（玩家业务）+ HTTP（健康/管理）+ 依赖装配（infra）与 PlayerActor 注册。
package server

import (
	"context"
	"fmt"
	"net/http"

	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/data/repo"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/infra"
	"github.com/huangyuCN/atlas/transport"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
	"go.uber.org/fx"
)

// NewHTTPServer 构造 HTTP 服务端（健康检查 + 玩家业务管理接口）。
func NewHTTPServer(cfg *conf.Bootstrap, svc *handler.GameHandler) (transport.Server, error) {
	srv, err := atlashttp.NewServer(atlashttp.WithAddress(cfg.GetServer().GetHttp().GetAddr()))
	if err != nil {
		return nil, fmt.Errorf("server: 构造 HTTP 服务端失败: %w", err)
	}
	srv.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","service":"game"}`))
	})
	gamev1.RegisterPlayerHTTPServer(srv, svc)
	return srv, nil
}

// NewGRPCServer 构造 gRPC 服务端并注册玩家业务服务。
func NewGRPCServer(cfg *conf.Bootstrap, svc *handler.GameHandler) (transport.Server, error) {
	srv, err := atlasgrpc.NewServer(atlasgrpc.WithAddress(cfg.GetServer().GetGrpc().GetAddr()))
	if err != nil {
		return nil, fmt.Errorf("server: 构造 gRPC 服务端失败: %w", err)
	}
	gamev1.RegisterPlayerServer(srv, svc)
	return srv, nil
}

// newMongoPlayerRepo 装配 mongo 玩家持久化：
// fx 无法注入裸 context.Context，这里以进程级上下文构造（仅索引初始化用）。
func newMongoPlayerRepo(cli *mongo.Client) (*repo.MongoPlayerRepo, error) {
	return repo.NewMongoPlayerRepo(context.Background(), cli)
}

// Module 是 game 服务的传输层装配模块（grpc/http + infra + 仓储 + 业务 + actor）。
var Module = fx.Module("server",
	fx.Provide(
		fx.Annotate(NewHTTPServer, fx.ResultTags(`group:"servers"`)),
		fx.Annotate(NewGRPCServer, fx.ResultTags(`group:"servers"`)),
		infra.NewRedisClient,
		infra.NewNatsConn,
		infra.NewMongoClient,
		infra.NewActorRuntime,
		repo.NewRedisPlayerCache,
		newMongoPlayerRepo,
		newPlayerStore,
		newPlayerService,
		newPlayerActorClient,
		handler.NewGameHandler,
	),
	fx.Invoke(registerActor),
)
