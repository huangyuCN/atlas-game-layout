// Package actor 提供游戏模板的 Atlas actor 集群统一装配：
// Locator（etcd）+ NATS 传输 + 集群运行时（懒激活）+ 默认中间件链。
// 业务通过 Register 注册 actor 类型（SpawnAuto 默认懒激活），
// 通过 Tell/Ask 向任意节点 actor 发消息（目录路由自动寻址）。
package actor

import (
	"context"
	"fmt"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/etcd"
	pkgnats "github.com/huangyuCN/atlas-game-layout/pkg/nats"
	"github.com/huangyuCN/atlas/contrib/actor/cluster"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/rollout"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	etcdlocator "github.com/huangyuCN/atlas/contrib/locator/etcd"
	"github.com/huangyuCN/atlas/metrics"
	"github.com/huangyuCN/atlas/namespace"
	"github.com/huangyuCN/atlas/registry"
	"github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Options 是 actor 集群装配选项。
type Options struct {
	// NodeID 是本节点 ID（服务实例 ID）。
	NodeID string
	// ServiceName 是服务发现中的服务名（懒激活候选节点）。
	ServiceName string
	// EtcdEndpoints 是 Locator 后端 etcd 地址。
	EtcdEndpoints []string
	// NatsURL 是集群传输 NATS 地址。
	NatsURL string
	// Namespace 是命名空间 token（配置字段 runtime.namespace 经框架 namespace.Derive 归一后的值）：
	// actor NATS subject 前缀（atlas_actor.<ns>）与 etcd 目录前缀（/atlas/actors/<ns>）同由它派生。
	// **必填**——两套共用同一 NATS/etcd 的部署若不隔离会静默串台（互相收到对方的节点消息与
	// 广播主题）；缺失或非法即构造失败（R9：不回落 env/default）。
	Namespace namespace.Namespace
	// Discovery 是可选的服务发现（懒激活选节点；nil 时懒激活退化到本机）。
	Discovery registry.Discovery
	// Tracer 是可选的链路追踪器（core.Tracer 适配，如 contrib/actor/observe/otel
	// 的 otel 实现；nil 时 Ask/Tell 不产生 span，noop 热路径零开销）。
	// 模板装配传入 otel.New(otel.GetTracerProvider().Tracer("atlas-actor"))，
	// 开发者配置 OTel SDK（如 otlp/jaeger exporter）后自动生效。
	Tracer core.Tracer
	// Meter 是可选的指标采集器（atlas metrics.Collector，如 contrib/metrics/otel
	// 的 OTel+Prometheus 实现；nil 时为 noop，打点零开销）。注入后 core/cluster
	// 运行时自动导出 actor 指标（投递计数、handler 耗时、邮箱深度、重启等）。
	Meter metrics.Collector
	// ActivationGate 是可选的激活闸门（框架 cluster.ActivationGate）：本节点即将为一个 PID
	// **创建** cell 之前询问一次，消费方据此拒绝不应再被激活的 PID（battle 用它拒绝复活
	// 已结束的对局，见 services/battle/internal/ledger.Gate）。未注入即不询问；
	// 只影响激活路径——cell 已在本节点存活时照旧投递。
	ActivationGate cluster.ActivationGate
	// claimer 声明节点 ID 归属（默认走 etcd 租约键）；包内测试注入桩，业务不设置。
	claimer nodeClaimer
}

// Runtime 是 actor 集群运行时封装。
type Runtime struct {
	inner   *cluster.Runtime
	nc      *nats.Conn
	ec      *clientv3.Client
	nodeID  string // actor 节点 ID（= 注册实例 ID，见 Options.NodeID）
	claimer nodeClaimer
	// dir/tr 是本节点持有的目录与集群传输：迁移编排（drain / activate / 属主校验）
	// 需要它们直接对接框架的 rollout.ClusterOps（见 Ops）。
	dir cluster.Directory
	tr  *cluster.NATSTransport
	// releaseClaim 是节点归属的释放函数（Start 成功时设置，Shutdown 调用并置空）。
	releaseClaim func(context.Context)
}

// NewRuntime 装配集群运行时（惰性：Start 前不建任何连接）。
func NewRuntime(opts Options) (*Runtime, error) {
	derived, err := deriveOptions(opts)
	if err != nil {
		return nil, err
	}
	nc, err := pkgnats.Connect(pkgnats.Options{URL: opts.NatsURL, Name: "actor-" + opts.NodeID})
	if err != nil {
		return nil, err
	}
	ec, loc, err := newLocator(opts, derived)
	if err != nil {
		nc.Close()
		return nil, err
	}
	inner, dir, tr, err := newClusterRuntime(opts, derived, nc, loc)
	if err != nil {
		nc.Close()
		_ = ec.Close()
		return nil, err
	}
	claimer := opts.claimer
	if claimer == nil {
		claimer = etcdNodeClaimer{ec: ec, prefix: derived.EtcdDirectory, ttl: nodeLeaseTTL}
	}
	return &Runtime{inner: inner, nc: nc, ec: ec, nodeID: opts.NodeID, claimer: claimer,
		dir: dir, tr: tr}, nil
}

// deriveOptions 校验必填项并派生命名空间五面：命名空间缺失或非法即报错
// （R9 严格模式：不回落 env/default；唯一派生点是框架 namespace.Derive）。
func deriveOptions(opts Options) (namespace.Derived, error) {
	if opts.NodeID == "" {
		return namespace.Derived{}, fmt.Errorf("actor: NodeID 不能为空")
	}
	if len(opts.EtcdEndpoints) == 0 {
		return namespace.Derived{}, fmt.Errorf("actor: EtcdEndpoints 不能为空")
	}
	if opts.NatsURL == "" {
		return namespace.Derived{}, fmt.Errorf("actor: NatsURL 不能为空")
	}
	derived, err := namespace.Derive(opts.Namespace.String())
	if err != nil {
		return namespace.Derived{}, fmt.Errorf("actor: %w", err)
	}
	return derived, nil
}

// newLocator 构造 etcd 客户端与 locator：目录前缀取 Derived.EtcdDirectory（/atlas/actors/<ns>），
// 使共用同一 etcd 的两套部署不争同一条 PID 归属记录（节点归属键复用同一前缀）。
func newLocator(opts Options, derived namespace.Derived) (*clientv3.Client, *etcdlocator.Locator, error) {
	ec, err := etcd.NewClient(etcd.Options{Endpoints: opts.EtcdEndpoints})
	if err != nil {
		return nil, nil, err
	}
	loc, err := etcdlocator.NewLocator(ec, etcdlocator.WithPrefix(derived.EtcdDirectory))
	if err != nil {
		_ = ec.Close()
		return nil, nil, fmt.Errorf("actor: 创建 locator 失败: %w", err)
	}
	return ec, loc, nil
}

// newClusterRuntime 组装集群运行时（目录 + NATS 传输 + 可选发现/追踪/指标）。
// 日志不显式注入：cluster 默认取 atlas 全局 Logger（bootstrap 已把本服务配置好的 Logger
// 装进全局），再捕获一次只会在将来二次 SetLogger 时固化为旧实例。
func newClusterRuntime(opts Options, derived namespace.Derived, nc *nats.Conn,
	loc *etcdlocator.Locator) (*cluster.Runtime, cluster.Directory, *cluster.NATSTransport, error) {
	transport, err := cluster.NewNATSTransport(nc,
		cluster.WithLocalNodeID(opts.NodeID), cluster.WithNamespace(derived.Namespace.String()))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("actor: 创建 NATS 传输失败: %w", err)
	}
	cfg := cluster.Config{
		NodeID: opts.NodeID,
		Mode:   cluster.ModeCluster,
		Lease:  cluster.DefaultLease(),
	}
	dir := cluster.NewDirectory(loc, opts.NodeID, 10*time.Second)
	rtOpts := []cluster.Option{
		cluster.WithDirectory(dir),
		cluster.WithTransport(transport),
	}
	if opts.Discovery != nil && opts.ServiceName != "" {
		rtOpts = append(rtOpts, cluster.WithDiscovery(opts.Discovery, opts.ServiceName))
	}
	if opts.Tracer != nil {
		rtOpts = append(rtOpts, cluster.WithTracer(opts.Tracer))
	}
	if opts.Meter != nil {
		rtOpts = append(rtOpts, cluster.WithMetrics(opts.Meter))
	}
	if opts.ActivationGate != nil {
		rtOpts = append(rtOpts, cluster.WithActivationGate(opts.ActivationGate))
	}
	rt, err := cluster.NewRuntime(cfg, rtOpts...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("actor: 创建集群运行时失败: %w", err)
	}
	return rt, dir, transport, nil
}

// Start 启动集群运行时：先声明节点 ID 归属（同 ID 冲突即启动失败，防静默串台），
// 再启动目录/租约与节点订阅；声明失败不启动运行时。
func (r *Runtime) Start(ctx context.Context) error {
	release, err := r.claimer.Claim(ctx, r.nodeID)
	if err != nil {
		return err
	}
	if err := r.inner.Start(ctx); err != nil {
		release(ctx)
		return err
	}
	r.releaseClaim = release
	return nil
}

// Shutdown 停止集群运行时（kill 语义：立即退出并释放目录归属）并释放节点归属租约。
func (r *Runtime) Shutdown(ctx context.Context) error {
	err := r.inner.Shutdown(ctx, cluster.ShutdownKill)
	release := r.releaseClaim
	r.releaseClaim = nil
	if release != nil {
		release(ctx)
	}
	_ = r.ec.Close()
	r.nc.Close()
	return err
}

// Register 注册 actor 类型（Props.SpawnMode 为空时按 SpawnManual，业务懒激活需 SpawnAuto）。
func (r *Runtime) Register(props core.Props) error {
	return r.inner.Register(props)
}

// Spawn 在本节点手动拉起指定 PID（须已 Register 对应 Props；lockstep 会话等 SpawnManual 类型使用）。
func (r *Runtime) Spawn(ctx context.Context, pid types.PID) (core.Ref, error) {
	return r.inner.Local().Spawn(ctx, pid)
}

// Stop 停止指定 PID（幂等；lockstep 会话等子 actor 的回收）。
func (r *Runtime) Stop(ctx context.Context, pid types.PID) error {
	return r.inner.Local().Stop(ctx, pid)
}

// Tell 向任意 actor 发送消息（目录路由自动寻址，支持跨节点与懒激活）。
// 满足 core.ActorInvoker（protoc-gen-atlas-actor 生成 client stub 的发送端依赖）。
func (r *Runtime) Tell(ctx context.Context, pid types.PID, msg any, opts ...core.SendOption) error {
	return r.inner.Local().Tell(ctx, pid, msg, opts...)
}

// Ask 向任意 actor 请求响应（目录路由自动寻址）。
// 满足 core.ActorInvoker（protoc-gen-atlas-actor 生成 client stub 的发送端依赖）。
// 业务错误直接以 error 返回：跨节点经集群 error 通道往返（code/reason 保留）。
func (r *Runtime) Ask(ctx context.Context, pid types.PID, req any, opts ...core.SendOption) (any, error) {
	return r.inner.Local().Ask(ctx, pid, req, opts...)
}

// ParsePID 解析 PID 字符串（如 "player:p-xxx"）。
func ParsePID(s string) (types.PID, error) { return types.ParsePID(s) }

// Raw 返回底层集群运行时（高级场景）。
func (r *Runtime) Raw() *cluster.Runtime { return r.inner }

// NodeID 返回本节点 ID（= 注册实例 ID，actor 目录里的属主标识）。
func (r *Runtime) NodeID() string { return r.nodeID }

// Directory 返回本节点持有的 actor 目录（只读查询与扫描；迁移编排据此读属主与 epoch）。
func (r *Runtime) Directory() cluster.Directory { return r.dir }

// Ops 返回框架的集群操作面（drain / activate / verify，目录属主与 epoch 语义）。
// 迁移编排以它为底座：战斗侧只在外面套一层「状态搬运」装饰（见 services/battle/internal/migrate）。
func (r *Runtime) Ops() rollout.ClusterOps {
	return cluster.NewClusterOps(r.dir, r.inner, "", cluster.WithDrainRemote(r.tr))
}

// CountByType 返回本节点指定 actor 类型的活跃实例数（供拉取式指标按需统计）。
func (r *Runtime) CountByType(typ string) int {
	return countByType(r.inner.Local().Cells(), typ)
}

// countByType 从 PID 快照统计指定类型的数量（纯函数，便于单测）。
func countByType(pids []types.PID, typ string) int {
	n := 0
	for _, pid := range pids {
		if pid.Type() == typ {
			n++
		}
	}
	return n
}
