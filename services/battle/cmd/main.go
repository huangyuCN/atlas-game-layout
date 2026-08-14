// battle 服务入口：装配通用模块与传输层，启动 Atlas App。
package main

import (
	"github.com/huangyuCN/atlas-game-layout/pkg/bootstrap"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/server"
	"go.uber.org/fx"
)

func main() {
	var cfg conf.Bootstrap
	opts, err := bootstrap.Assemble("battle", &cfg, server.Module)
	if err != nil {
		panic(err)
	}
	fx.New(opts...).Run()
}
