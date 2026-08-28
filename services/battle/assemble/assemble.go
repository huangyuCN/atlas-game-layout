// Package assemble 提供 battle 服务的进程内（嵌入式）装配入口：
// 与生产形态共用 internal/app 的同一张 fx 依赖图，仅驱动方式不同
// （进程形态由 atlas.App 管信号与启停；本形态以 fx 编程式 Start/Stop
// + serverutil.ServeAsync 驱动），供集成测试与嵌入式部署复用，
// 装配逻辑只此一份、不再手工重复接线。
package assemble

import (
	"context"
	"fmt"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/actor"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	battleactor "github.com/huangyuCN/atlas-game-layout/services/battle/internal/actor"
	battleapp "github.com/huangyuCN/atlas-game-layout/services/battle/internal/app"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"github.com/huangyuCN/atlas/registry"
	"github.com/huangyuCN/atlas/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/fx"
)

// ServiceVersion 是注册到注册中心的服务版本（进程内形态固定值）。
const ServiceVersion = "0.1.0"

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
}

// Battle 是装配完成的 battle 服务句柄。
type Battle struct {
	Runtime *actor.Runtime // 战斗 actor 宿主（测试/观测可直达）
	GRPCURL string         // host:port（matcher 开局调用与测试直连用）
	stop    func(ctx context.Context) error
}

// graphHandles 从依赖图回捞句柄所需组件（含 servers 值组）。
type graphHandles struct {
	fx.In

	Runtime *actor.Runtime
	Etcd    *clientv3.Client
	Servers []transport.Server `group:"servers"`
}

// New 装配并启动 battle 服务：映射配置 → 启动 fx 依赖图（含 actor 运行时）
// → 后台起传输层 → 注册服务实例。实例 ID 必须 == actor NodeID
// （matcher 懒激活按服务实例选 battle 节点的硬约束）。
func New(ctx context.Context, o Options) (*Battle, error) {
	var h graphHandles
	root := fx.New(
		fx.NopLogger,
		fx.Supply(newBootstrap(o)),
		overrideBattleConfig(o.BattleCfg),
		battleapp.Module,
		fx.Populate(&h),
	)
	if err := root.Err(); err != nil {
		return nil, fmt.Errorf("assemble: 依赖图校验失败: %w", err)
	}
	if err := root.Start(ctx); err != nil {
		return nil, fmt.Errorf("assemble: 启动组件失败: %w", err)
	}

	urls, stopServers, err := startServers(h.Servers)
	if err != nil {
		_ = root.Stop(context.Background())
		return nil, err
	}
	reg, err := registerInstance(ctx, o.NodeID, h.Etcd, urls.grpc)
	if err != nil {
		_ = stopServers(context.Background())
		_ = root.Stop(context.Background())
		return nil, err
	}

	b := &Battle{Runtime: h.Runtime, GRPCURL: urls.grpc}
	b.stop = func(ctx context.Context) error {
		firstErr := reg.Deregister(ctx, instanceOf(o.NodeID, urls.grpc))
		if err := stopServers(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := root.Stop(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	return b, nil
}

// Stop 停止 battle 服务并释放全部资源（注销实例 → 停服务器 → 组件逆序回收）。
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

// serverURLs 是传输层的就绪端点（host:port，按 scheme 归集）。
type serverURLs struct {
	grpc string
	http string
}

// startServers 以进程内形态后台启动全部传输层并按 scheme 分派端点
// （fx 值组不保证提供顺序，不能依赖下标）。
func startServers(servers []transport.Server) (serverURLs, func(context.Context) error, error) {
	eps, stop, err := serverutil.ServeAsync(5*time.Second, servers...)
	if err != nil {
		return serverURLs{}, nil, fmt.Errorf("assemble: 启动传输层: %w", err)
	}
	var urls serverURLs
	for _, ep := range eps {
		switch ep.Scheme {
		case "grpc":
			urls.grpc = ep.Host
		case "http":
			urls.http = ep.Host
		}
	}
	if urls.grpc == "" {
		_ = stop(context.Background())
		return serverURLs{}, nil, fmt.Errorf("assemble: 未找到 gRPC 端点: %v", eps)
	}
	return urls, stop, nil
}

// registerInstance 构造注册中心并注册服务实例。
func registerInstance(ctx context.Context, nodeID string, ec *clientv3.Client, grpcHost string) (registry.Registrar, error) {
	reg, err := pkgregistry.NewEtcd(ec, pkgregistry.Options{})
	if err != nil {
		return nil, fmt.Errorf("assemble: 构造注册中心: %w", err)
	}
	if err := reg.Register(ctx, instanceOf(nodeID, grpcHost)); err != nil {
		return nil, fmt.Errorf("assemble: 注册服务实例: %w", err)
	}
	return reg, nil
}

// instanceOf 构造注册实例（etcd 注册端点带 grpc:// scheme，供发现方解析拨号）。
func instanceOf(nodeID, grpcHost string) *registry.ServiceInstance {
	return &registry.ServiceInstance{
		ID:        nodeID,
		Name:      "battle",
		Version:   ServiceVersion,
		Endpoints: []string{"grpc://" + grpcHost},
	}
}

// newBootstrap 把进程内装配参数映射为服务配置：
// 装配图只认 *conf.Bootstrap 一种输入，两种驱动形态因此共享全部构造函数。
// 监听地址固定随机端口（进程内形态不做端口管理）。
func newBootstrap(o Options) *conf.Bootstrap {
	const randomPort = "127.0.0.1:0"
	return &conf.Bootstrap{
		Runtime: &configspb.Runtime{Name: "battle", Id: o.NodeID},
		Registry: &configspb.Registry{
			Etcd: &configspb.Registry_Etcd{Endpoints: o.EtcdEndpoints},
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
