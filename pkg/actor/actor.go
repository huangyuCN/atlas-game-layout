// Package actor 提供游戏模板的 Atlas actor 集群统一装配：
// Locator（etcd）+ NATS 传输 + 集群运行时（懒激活）+ 默认中间件链。
// 业务通过 Register 注册 actor 类型（SpawnAuto 默认懒激活），
// 通过 Tell/Ask 向任意节点 actor 发消息（目录路由自动寻址）。
package actor

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/etcd"
	pkgnats "github.com/huangyuCN/atlas-game-layout/pkg/nats"
	"github.com/huangyuCN/atlas/contrib/actor/cluster"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	etcdlocator "github.com/huangyuCN/atlas/contrib/locator/etcd"
	atlaslog "github.com/huangyuCN/atlas/log"
	"github.com/huangyuCN/atlas/registry"
	"github.com/nats-io/nats.go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
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
	// Discovery 是可选的服务发现（懒激活选节点；nil 时懒激活退化到本机）。
	Discovery registry.Discovery
}

// Runtime 是 actor 集群运行时封装。
type Runtime struct {
	inner *cluster.Runtime
	nc    *nats.Conn
	ec    *clientv3.Client
}

// NewRuntime 装配集群运行时（惰性：Start 前不建任何连接）。
func NewRuntime(opts Options) (*Runtime, error) {
	if opts.NodeID == "" {
		return nil, fmt.Errorf("actor: NodeID 不能为空")
	}
	if len(opts.EtcdEndpoints) == 0 {
		return nil, fmt.Errorf("actor: EtcdEndpoints 不能为空")
	}
	if opts.NatsURL == "" {
		return nil, fmt.Errorf("actor: NatsURL 不能为空")
	}

	nc, err := pkgnats.Connect(pkgnats.Options{URL: opts.NatsURL, Name: "actor-" + opts.NodeID})
	if err != nil {
		return nil, err
	}
	ec, err := etcd.NewClient(etcd.Options{Endpoints: opts.EtcdEndpoints})
	if err != nil {
		nc.Close()
		return nil, err
	}
	// Locator 使用独立前缀，与注册中心（/atlas/services）隔离。
	loc, err := etcdlocator.NewLocator(ec, etcdlocator.WithPrefix("/atlas/actors"))
	if err != nil {
		nc.Close()
		_ = ec.Close()
		return nil, fmt.Errorf("actor: 创建 locator 失败: %w", err)
	}

	dir := cluster.NewDirectory(loc, opts.NodeID, 10*time.Second)
	cfg := cluster.Config{
		NodeID: opts.NodeID,
		Mode:   cluster.ModeCluster,
		Lease:  cluster.DefaultLease(),
	}
	rtOpts := []cluster.Option{
		cluster.WithDirectory(dir),
		cluster.WithTransport(cluster.NewNATSTransport(nc, cluster.WithLocalNodeID(opts.NodeID))),
		cluster.WithLogger(atlaslog.GetLogger()),
	}
	if opts.Discovery != nil && opts.ServiceName != "" {
		rtOpts = append(rtOpts, cluster.WithDiscovery(opts.Discovery, opts.ServiceName))
	}
	rt, err := cluster.NewRuntime(cfg, rtOpts...)
	if err != nil {
		nc.Close()
		_ = ec.Close()
		return nil, fmt.Errorf("actor: 创建集群运行时失败: %w", err)
	}
	return &Runtime{inner: rt, nc: nc, ec: ec}, nil
}

// Start 启动集群运行时（注册目录、起租约续期）。
func (r *Runtime) Start(ctx context.Context) error { return r.inner.Start(ctx) }

// Shutdown 停止集群运行时（kill 语义：立即退出并释放目录归属）。
func (r *Runtime) Shutdown(ctx context.Context) error {
	err := r.inner.Shutdown(ctx, cluster.ShutdownKill)
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
func (r *Runtime) Tell(ctx context.Context, pid types.PID, msg any) error {
	return r.inner.Local().Tell(ctx, pid, msg)
}

// Ask 向任意 actor 请求响应（目录路由自动寻址）。
func (r *Runtime) Ask(ctx context.Context, pid types.PID, req any) (any, error) {
	return r.inner.Local().Ask(ctx, pid, req)
}

// AskProto 以 proto 消息请求并解析 proto 响应：
// 跨节点响应为序列化字节（集群约定），同节点为对象直传，两者均兼容。
func (r *Runtime) AskProto(ctx context.Context, pid types.PID, req, out proto.Message) error {
	reply, err := r.Ask(ctx, pid, req)
	if err != nil {
		return err
	}
	switch v := reply.(type) {
	case []byte:
		if err := proto.Unmarshal(v, out); err != nil {
			return fmt.Errorf("actor: 响应解码失败: %w", err)
		}
		return nil
	case proto.Message:
		// 同节点对象直传：类型一致时反射赋值。
		src := reflect.ValueOf(v)
		dst := reflect.ValueOf(out)
		if src.Type() != dst.Type() {
			return fmt.Errorf("actor: 响应类型 %T 与期望 %T 不符", v, out)
		}
		dst.Elem().Set(src.Elem())
		return nil
	default:
		return fmt.Errorf("actor: 不支持的响应类型 %T", reply)
	}
}

// TellProto 以 proto 消息投递（跨节点自动序列化 + 类型标识）。
func (r *Runtime) TellProto(ctx context.Context, pid types.PID, msg proto.Message) error {
	return r.Tell(ctx, pid, msg)
}

// ParsePID 解析 PID 字符串（如 "player:p-xxx"）。
func ParsePID(s string) (types.PID, error) { return types.ParsePID(s) }

// Raw 返回底层集群运行时（高级场景）。
func (r *Runtime) Raw() *cluster.Runtime { return r.inner }
