// edge 服务入口：装配通用模块与接入层（裸 L4 转发 + HTTP 健康），启动 Atlas App。
package main

import (
	"github.com/huangyuCN/atlas-game-layout/pkg/bootstrap"
	"github.com/huangyuCN/atlas-game-layout/services/edge/internal/app"
	"github.com/huangyuCN/atlas-game-layout/services/edge/internal/conf"
	"go.uber.org/fx"
)

func main() {
	var cfg conf.Bootstrap
	opts, err := bootstrap.Assemble("edge", &cfg, app.Module)
	if err != nil {
		panic(err)
	}
	fx.New(opts...).Run()
}
