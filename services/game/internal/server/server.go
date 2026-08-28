// Package server 负责 game 服务的传输层构造：
// gRPC（玩家业务）+ HTTP（健康检查 + 玩家 REST 管理接口）。
// 依赖装配见 internal/app；服务启停归属驱动方
// （进程形态 atlas.App / 进程内形态 serverutil.ServeAsync）。
package server

import (
	"fmt"
	"net/http"

	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/conf"
	"github.com/huangyuCN/atlas/transport"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
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
