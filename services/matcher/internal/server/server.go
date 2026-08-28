// Package server 负责 matcher 服务的传输层构造：
// gRPC（入队/取消/查询）+ HTTP（健康/规则查询）。
// 依赖装配见 internal/app；服务启停归属驱动方
// （进程形态 atlas.App / 进程内形态 serverutil.ServeAsync）。
package server

import (
	"fmt"
	"net/http"

	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz/handler"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/infra"
	"github.com/huangyuCN/atlas/transport"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
)

// NewHTTPServer 构造 HTTP 服务端（健康检查 + 规则查询）。
func NewHTTPServer(cfg *conf.Bootstrap) (transport.Server, error) {
	srv, err := atlashttp.NewServer(atlashttp.WithAddress(cfg.GetServer().GetHttp().GetAddr()))
	if err != nil {
		return nil, fmt.Errorf("server: 构造 HTTP 服务端失败: %w", err)
	}
	srv.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","service":"matcher"}`))
	})
	srv.HandleFunc("/v1/matcher/rules", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"rulesets":["%s"],"rule":%q}`, biz.DefaultMatchmakerName, infra.RuleDescription)
	})
	return srv, nil
}

// NewGRPCServer 构造 gRPC 服务端并注册撮合服务。
func NewGRPCServer(cfg *conf.Bootstrap, svc *handler.MatcherHandler) (transport.Server, error) {
	srv, err := atlasgrpc.NewServer(atlasgrpc.WithAddress(cfg.GetServer().GetGrpc().GetAddr()))
	if err != nil {
		return nil, fmt.Errorf("server: 构造 gRPC 服务端失败: %w", err)
	}
	matcherv1.RegisterMatcherServer(srv, svc)
	return srv, nil
}
