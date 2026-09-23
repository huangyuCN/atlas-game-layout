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
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas/contrib/actor/cluster"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	etcdlocator "github.com/huangyuCN/atlas/contrib/locator/etcd"
	"github.com/huangyuCN/atlas/metrics"
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
	// Namespace 是 actor 平面命名空间（NATS subject 前缀与 etcd 目录前缀；空 = default）。
	// 与注册中心的环境隔离同源（装配层传 runtime.env）：两套共用同一 NATS/etcd 的部署
	// 若不隔离会静默串台——互相收到对方的节点消息与广播主题。
	Namespace string
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
	// claimer 声明节点 ID 归属（默认走 etcd 租约键）；包内测试注入桩，业务不设置。
	claimer nodeClaimer
}

// NamespaceOf 返回生效的 actor 平面命名空间：`runtime.actor_namespace` 优先，
// 缺省取 `runtime.env`（再空则 default，由 types.NormalizeNamespace 归一）。
// 四服务装配与进程内形态都经它取值，避免两处规则漂移。
func NamespaceOf(r *configspb.Runtime) string {
	if r == nil {
		return ""
	}
	if ns := r.GetActorNamespace(); ns != "" {
		return ns
	}
	return r.GetEnv()
}

// Runtime 是 actor 集群运行时封装。
type Runtime struct {
	inner   *cluster.Runtime
	nc      *nats.Conn
	ec      *clientv3.Client
	nodeID  string // actor 节点 ID（= 注册实例 ID，见 Options.NodeID）
	claimer nodeClaimer
	// releaseClaim 是节点归属的释放函数（Start 成功时设置，Shutdown 调用并置空）。
	releaseClaim func(context.Context)
}

// NewRuntime 装配集群运行时（惰性：Start 前不建任何连接）。
func NewRuntime(opts Options) (*Runtime, error) {
	ns, err := runtimeNamespace(opts)
	if err != nil {
		return nil, err
	}
	nc, err := pkgnats.Connect(pkgnats.Options{URL: opts.NatsURL, Name: "actor-" + opts.NodeID})
	if err != nil {
		return nil, err
	}
	ec, loc, err := newLocator(opts, ns)
	if err != nil {
		nc.Close()
		return nil, err
	}
	inner, err := newClusterRuntime(opts, ns, nc, loc)
	if err != nil {
		nc.Close()
		_ = ec.Close()
		return nil, err
	}
	claimer := opts.claimer
	if claimer == nil {
		claimer = etcdNodeClaimer{ec: ec, prefix: "/atlas/actors/" + ns, ttl: nodeLeaseTTL}
	}
	return &Runtime{inner: inner, nc: nc, ec: ec, nodeID: opts.NodeID, claimer: claimer}, nil
}

// runtimeNamespace 校验必填项并归一 actor 平面命名空间（空 = default；非法值启动期报错）。
func runtimeNamespace(opts Options) (string, error) {
	if opts.NodeID == "" {
		return "", fmt.Errorf("actor: NodeID 不能为空")
	}
	if len(opts.EtcdEndpoints) == 0 {
		return "", fmt.Errorf("actor: EtcdEndpoints 不能为空")
	}
	if opts.NatsURL == "" {
		return "", fmt.Errorf("actor: NatsURL 不能为空")
	}
	ns, err := types.NormalizeNamespace(opts.Namespace)
	if err != nil {
		return "", fmt.Errorf("actor: 命名空间非法: %w", err)
	}
	return ns, nil
}

// newLocator 构造 etcd 客户端与 locator：目录前缀按命名空间分层，
// 使共用同一 etcd 的两套部署不争同一条 PID 归属记录（节点归属键复用同一前缀）。
func newLocator(opts Options, ns string) (*clientv3.Client, *etcdlocator.Locator, error) {
	ec, err := etcd.NewClient(etcd.Options{Endpoints: opts.EtcdEndpoints})
	if err != nil {
		return nil, nil, err
	}
	loc, err := etcdlocator.NewLocator(ec, etcdlocator.WithPrefix("/atlas/actors/"+ns))
	if err != nil {
		_ = ec.Close()
		return nil, nil, fmt.Errorf("actor: 创建 locator 失败: %w", err)
	}
	return ec, loc, nil
}

// newClusterRuntime 组装集群运行时（目录 + NATS 传输 + 可选发现/追踪/指标）。
// 日志不显式注入：cluster 默认取 atlas 全局 Logger（bootstrap 已把本服务配置好的 Logger
// 装进全局），再捕获一次只会在将来二次 SetLogger 时固化为旧实例。
func newClusterRuntime(opts Options, ns string, nc *nats.Conn, loc *etcdlocator.Locator) (*cluster.Runtime, error) {
	cfg := cluster.Config{
		NodeID: opts.NodeID,
		Mode:   cluster.ModeCluster,
		Lease:  cluster.DefaultLease(),
	}
	rtOpts := []cluster.Option{
		cluster.WithDirectory(cluster.NewDirectory(loc, opts.NodeID, 10*time.Second)),
		cluster.WithTransport(cluster.NewNATSTransport(nc,
			cluster.WithLocalNodeID(opts.NodeID), cluster.WithNamespace(ns))),
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
	rt, err := cluster.NewRuntime(cfg, rtOpts...)
	if err != nil {
		return nil, fmt.Errorf("actor: 创建集群运行时失败: %w", err)
	}
	return rt, nil
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
