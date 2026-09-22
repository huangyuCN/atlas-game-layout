// Package assemble 提供 battle 服务的进程内（嵌入式）装配入口：
// 与进程形态共用 internal/app 的同一张 fx 依赖图与同一套启停路径
// （bootstrap.Boot → atlas.App：启动服务端、注册实例、注销与停机），
// 仅不注册进程信号——宿主/测试进程的信号不能被本实例拦下。
// 供集成测试与嵌入式部署复用。
package assemble

import (
	"context"

	"github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/bootstrap"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	battleactor "github.com/huangyuCN/atlas-game-layout/services/battle/internal/actor"
	battleapp "github.com/huangyuCN/atlas-game-layout/services/battle/internal/app"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"go.uber.org/fx"
)

// BattleConfig 是战斗参数覆盖（供 e2e 等外部包注入；镜像 internal/actor.Config——
// Go internal 规则禁止外部包直接引用 actor.Config，故以本包类型暴露，toActor 单点映射）。
type BattleConfig struct {
	TickInterval  int64  // 帧间隔（纳秒）
	TrackLen      int32  // 赛道长度（胜负判定）
	MaxFrames     uint64 // 帧数上限
	SnapshotEvery uint64 // 快照周期（帧）
}

// toActor 映射为 actor.Config（字段镜像的唯一转换点）。
func (c BattleConfig) toActor() battleactor.Config {
	return battleactor.Config{
		TickInterval:  c.TickInterval,
		TrackLen:      c.TrackLen,
		MaxFrames:     c.MaxFrames,
		SnapshotEvery: c.SnapshotEvery,
	}
}

// DefaultBattleConfig 返回默认战斗参数（完整填充，可局部覆盖）。
func DefaultBattleConfig() BattleConfig {
	cfg := battleactor.DefaultConfig()
	return BattleConfig{
		TickInterval:  cfg.TickInterval,
		TrackLen:      cfg.TrackLen,
		MaxFrames:     cfg.MaxFrames,
		SnapshotEvery: cfg.SnapshotEvery,
	}
}

// Options 是进程内装配参数。
type Options struct {
	NodeID        string
	EtcdEndpoints []string
	NatsURL       string
	MongoURI      string
	MongoDB       string
	// BattleCfg 覆盖战斗默认参数（测试注入：短帧间隔/长赛道等）；nil 用默认。
	BattleCfg *BattleConfig
	// Namespace 是注册中心键前缀（可选）：嵌入式/测试形态用它把实例与常驻进程隔离，
	// 并避免上一次运行残留的实例键（租约未过期）导致注册冲突；
	// 缺省按 runtime.env 派生（/atlas/services/<env>）。
	Namespace string
}

// Battle 是装配完成的 battle 服务句柄。
type Battle struct {
	Runtime *actor.Runtime // 战斗 actor 宿主（测试/观测可直达）
	GRPCURL string         // host:port（matcher 开局调用与测试直连用）
	stop    func(ctx context.Context) error
}

// graphHandles 从依赖图回捞句柄所需组件。
type graphHandles struct {
	fx.In

	bootstrap.Servers
	Runtime *actor.Runtime
}

// New 装配并启动 battle 服务：映射配置 → bootstrap.Boot 启动依赖图（含 actor 运行时），
// 由 atlas.App 统一启动服务端并注册实例（返回时注册已完成）。
// 实例 ID 必须 == actor NodeID（matcher 懒激活按服务实例选 battle 节点的硬约束）。
func New(ctx context.Context, o Options) (*Battle, error) {
	var h graphHandles
	inst, urls, err := bootstrap.Boot(ctx, newBootstrap(o),
		fx.Options(battleapp.Module, overrideBattleConfig(o.BattleCfg)), &h, serverutil.SchemeGRPC)
	if err != nil {
		return nil, err
	}
	return &Battle{Runtime: h.Runtime, GRPCURL: urls[serverutil.SchemeGRPC].Host, stop: inst.Stop}, nil
}

// Stop 停止 battle 服务并释放全部资源
// （注销实例 → 停服务端 → 组件逆序回收，均由 atlas.App 驱动）。
func (b *Battle) Stop(ctx context.Context) error {
	if b.stop == nil {
		return nil
	}
	return b.stop(ctx)
}

// overrideBattleConfig 以 fx.Decorate 覆盖依赖图中的战斗参数默认值；
// override 为 nil 时透传基础值（装饰器恒挂载、行为零差异）。
func overrideBattleConfig(override *BattleConfig) fx.Option {
	return fx.Decorate(func(base battleactor.Config) battleactor.Config {
		if override != nil {
			return override.toActor()
		}
		return base
	})
}

// newBootstrap 把进程内装配参数映射为服务配置：
// 装配图只认 *conf.Bootstrap 一种输入，两种驱动形态因此共享全部构造函数。
// 监听地址固定随机端口（进程内形态不做端口管理）。
func newBootstrap(o Options) *conf.Bootstrap {
	const randomPort = "127.0.0.1:0"
	return &conf.Bootstrap{
		Runtime: &configspb.Runtime{Name: "battle", Id: o.NodeID},
		Registry: &configspb.Registry{
			Etcd:      &configspb.Registry_Etcd{Endpoints: o.EtcdEndpoints},
			Namespace: o.Namespace,
		},
		Server: &configspb.Server{
			Grpc: &configspb.Server_GRPC{Addr: randomPort},
			Http: &configspb.Server_HTTP{Addr: randomPort},
		},
		Data: &configspb.Data{
			Nats:  &configspb.Data_Nats{Url: o.NatsURL},
			Mongo: &configspb.Data_Mongo{Uri: o.MongoURI, Database: o.MongoDB},
		},
	}
}
