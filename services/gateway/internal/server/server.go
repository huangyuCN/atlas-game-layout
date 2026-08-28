// Package server 负责 gateway 服务的传输层构造与统一 handler：
// 五协议 Server（tcp/ws/kcp/udp/http）+ 会话绑定 + 挤下线 + 下行推送。
// 依赖装配见 internal/app；服务启停归属驱动方
// （进程形态 atlas.App / 进程内形态 serverutil.ServeAsync + WS 的 /ws 包装）。
package server

import (
	"fmt"
	"net/http"

	"github.com/huangyuCN/atlas-game-layout/services/gateway/internal/conf"
	"github.com/huangyuCN/atlas/transport"
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
		_, _ = w.Write([]byte(`{"status":"ok","service":"gateway"}`))
	})
	return srv, nil
}
