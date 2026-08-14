// game 服务入口：装配通用模块（配置/日志/注册）与传输层，启动 Atlas App。
package main

import (
	"github.com/huangyuCN/atlas-game-layout/pkg/bootstrap"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/game/internal/server"
	"go.uber.org/fx"
)

func main() {
	var cfg conf.Config
	opts, err := bootstrap.Assemble("game", &cfg, server.Module)
	if err != nil {
		panic(err)
	}
	// fx.App.Run：启动、注册信号监听（SIGINT/SIGTERM）、优雅停止。
	fx.New(opts...).Run()
}
