// Package server 负责 battle 服务的传输层组装：
// gRPC 服务端（开局/查询）+ HTTP 健康检查，供 bootstrap 收集。
package server

import (
	"fmt"
	"net/http"

	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
	"github.com/huangyuCN/atlas/transport"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"go.uber.org/fx"
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

// NewGRPCServer 构造 gRPC 服务端（服务注册在协议里程碑后追加）。
func NewGRPCServer(cfg *conf.Bootstrap) (transport.Server, error) {
	srv, err := atlasgrpc.NewServer(atlasgrpc.WithAddress(cfg.GetServer().GetGrpc().GetAddr()))
	if err != nil {
		return nil, fmt.Errorf("server: 构造 gRPC 服务端失败: %w", err)
	}
	return srv, nil
}

// Module 是 battle 服务的传输层装配模块。
var Module = fx.Module("server",
	fx.Provide(
		fx.Annotate(NewHTTPServer, fx.ResultTags(`group:"servers"`)),
		fx.Annotate(NewGRPCServer, fx.ResultTags(`group:"servers"`)),
	),
)
