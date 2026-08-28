// Package server 负责 battle 服务的传输层构造：
// gRPC（开局/查询/帧上行）+ HTTP（健康检查）。
// 依赖装配见 internal/app；服务启停归属驱动方
// （进程形态 atlas.App / 进程内形态 serverutil.ServeAsync）。
package server

import (
	"fmt"
	"net/http"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"github.com/huangyuCN/atlas/transport"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
)

// NewHTTPServer 构造 HTTP 服务端（健康检查）。
func NewHTTPServer(cfg *conf.Bootstrap) (transport.Server, error) {
	srv, err := atlashttp.NewServer(atlashttp.WithAddress(cfg.GetServer().GetHttp().GetAddr()))
	if err != nil {
		return nil, fmt.Errorf("server: 构造 HTTP 服务端失败: %w", err)
	}
	srv.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","service":"battle"}`))
	})
	return srv, nil
}

// NewGRPCServer 构造 gRPC 服务端并注册战斗管理服务。
func NewGRPCServer(cfg *conf.Bootstrap, svc *handler.BattleHandler) (transport.Server, error) {
	srv, err := atlasgrpc.NewServer(atlasgrpc.WithAddress(cfg.GetServer().GetGrpc().GetAddr()))
	if err != nil {
		return nil, fmt.Errorf("server: 构造 gRPC 服务端失败: %w", err)
	}
	battlev1.RegisterBattleServer(srv, svc)
	return srv, nil
}
