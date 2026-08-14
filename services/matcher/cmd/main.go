// matcher 服务入口：装配通用模块与传输层，启动 Atlas App。
package main

import (
	"github.com/huangyuCN/atlas-game-layout/pkg/bootstrap"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/server"
	"go.uber.org/fx"
)

func main() {
	var cfg conf.Bootstrap
	opts, err := bootstrap.Assemble("matcher", &cfg, server.Module)
	if err != nil {
		panic(err)
	}
	fx.New(opts...).Run()
}
