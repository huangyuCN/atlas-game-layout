package resolver

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/locator"
	"github.com/huangyuCN/atlas/registry"
)

// LocatorDirectory 把框架 locator（只读 actor 目录，etcd/redis 实现）适配为 Directory。
// 目录监听在进程内共享：同类型 PID 只有一个前缀级 Watch（见 watchHub），
// 消费者注册/注销不触发扫描，避免 Redis 后端「每局一次 SCAN」被规模放大。
type LocatorDirectory struct {
	loc locator.Locator
	hub *watchHub
}

// NewLocatorDirectory 构造目录适配器；loc 必须已带 /atlas/actors/<ns> 前缀
// （前缀由框架 namespace.Derive 派生，见装配层）。
func NewLocatorDirectory(loc locator.Locator) (*LocatorDirectory, error) {
	if loc == nil {
		return nil, errors.New("resolver: actor 目录 locator 不能为空")
	}
	return newLocatorDirectory(loc, DefaultWatchIdleTTL), nil
}

// newLocatorDirectory 构造目录适配器并指定共享监听的空闲回收时长（测试注入短时长）。
func newLocatorDirectory(loc locator.Locator, idleTTL time.Duration) *LocatorDirectory {
	return &LocatorDirectory{loc: loc, hub: newWatchHub(loc, idleTTL)}
}

// Close 停止共享目录监听并等待扇出 goroutine 退出（幂等；接入层停机时由装配层调用）。
func (d *LocatorDirectory) Close() { d.hub.Close() }

// OwnerNode 读取 PID 归属记录里的 owner_node（记录格式即 contrib/actor 的 ActorLocation）。
func (d *LocatorDirectory) OwnerNode(ctx context.Context, pid string) (string, error) {
	parsed, err := types.ParsePID(pid)
	if err != nil {
		return "", err
	}
	cur := types.NewLocationBuilder()()
	if err := d.loc.Lookup(ctx, parsed.Key(), cur); err != nil {
		return "", err
	}
	rec, ok := cur.(*types.ActorLocation)
	if !ok || rec.Location == nil {
		return "", fmt.Errorf("resolver: 目录记录 %s 类型不符", pid)
	}
	return rec.GetOwnerNode(), nil
}

// WatchOwner 订阅该 PID 的属主变化：同类型的所有订阅共享一条前缀级监听，
// 事件按 key 精确分发（battle:b1 不收 battle:b10 的事件，两者前缀相同）。
// 目录记录在租约续期时会被重写，故回调会重复触发；比较属主是否变化由调用方（guard）负责。
//
// 返回的 cancel 幂等：只注销本消费者（该局无人订阅即丢弃条目），不按局关监听——
// 共享监听在整类订阅归零且空闲超时后才关闭（DefaultWatchIdleTTL），再订阅时按需重建。
func (d *LocatorDirectory) WatchOwner(ctx context.Context, pid string, onChange func(owner string)) (func(), error) {
	return d.hub.subscribe(ctx, pid, onChange)
}

// ownerOfLocation 从目录记录解出属主节点（类型不符或删除事件返回空串）。
func ownerOfLocation(loc locator.Location) string {
	rec, ok := loc.(*types.ActorLocation)
	if !ok || rec == nil || rec.Location == nil {
		return ""
	}
	return rec.GetOwnerNode()
}

// DiscoveryRegistry 把框架服务发现适配为帧面实例发现。
type DiscoveryRegistry struct {
	discovery registry.Discovery
}

// NewDiscoveryRegistry 构造帧面实例发现适配器（键前缀与注册端同源由装配层保证）。
func NewDiscoveryRegistry(discovery registry.Discovery) (*DiscoveryRegistry, error) {
	if discovery == nil {
		return nil, errors.New("resolver: 服务发现不能为空")
	}
	return &DiscoveryRegistry{discovery: discovery}, nil
}

// Instances 返回指定服务名的实例，映射为帧面实例的中间表示。
func (r *DiscoveryRegistry) Instances(ctx context.Context, service string) ([]Instance, error) {
	list, err := r.discovery.GetService(ctx, service)
	if err != nil {
		return nil, err
	}
	return instancesOf(list), nil
}

// WatchInstances 监听帧面实例变化并回调全量快照（规格 §2.1：实例上下线即时更新）。
// 服务发现的 Next 本身即「有变化就返回最新列表」，故每次回调都是一份可直接替换的快照。
//
// cancel 幂等且两件事一起做（与 WatchOwner 同款约束）：取消派生 ctx（关掉客户端侧 watch 流）
// ＋停掉 watcher（唤醒阻塞在 Next() 的监听 goroutine）——只关停止信号会按监听泄漏 goroutine。
func (r *DiscoveryRegistry) WatchInstances(ctx context.Context, service string,
	onChange func([]Instance)) (func(), error) {
	if onChange == nil {
		return func() {}, nil
	}
	ctx, cancelWatch := context.WithCancel(ctx)
	w, err := r.discovery.Watch(ctx, service)
	if err != nil {
		cancelWatch()
		return nil, fmt.Errorf("resolver: 监听帧面实例（%s）失败: %w", service, err)
	}
	stop := make(chan struct{})
	var stopOnce sync.Once
	stopWatch := func() { stopOnce.Do(func() { _ = w.Stop() }) }
	go func() {
		defer stopWatch()
		for {
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			default:
			}
			list, err := w.Next()
			if err != nil {
				return // 监听终止：调用方退回每次 Instances 查询（保守兜底）
			}
			onChange(instancesOf(list))
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancelWatch()
			stopWatch()
			close(stop)
		})
	}, nil
}

// instancesOf 把注册实例列表映射为帧面实例列表（nil 实例跳过）。
func instancesOf(list []*registry.ServiceInstance) []Instance {
	out := make([]Instance, 0, len(list))
	for _, si := range list {
		if si != nil {
			out = append(out, instanceOf(si))
		}
	}
	return out
}

// instanceOf 把注册实例映射为帧面实例：元数据键契约见 lib/consts
// （node_id / host / ws / kcp / udp，由 battle 注册帧面实例时写入）。
func instanceOf(si *registry.ServiceInstance) Instance {
	md := si.Metadata
	inst := Instance{
		ID:     si.ID,
		NodeID: md[consts.FrameMetaNodeID],
		Host:   md[consts.FrameMetaHost],
		Ports:  make(map[string]string, 3),
	}
	for _, face := range []string{consts.FrameMetaPortWS, consts.FrameMetaPortKCP, consts.FrameMetaPortUDP} {
		if port := md[face]; port != "" {
			inst.Ports[face] = port
		}
	}
	inst.EndpointHost = endpointHost(si.Endpoints)
	return inst
}

// endpointHost 取实例端点里的主机（端点形如 tcp://10.0.0.7:9401）。
func endpointHost(endpoints []string) string {
	for _, ep := range endpoints {
		u, err := url.Parse(ep)
		if err != nil || u.Host == "" {
			continue
		}
		return u.Hostname()
	}
	return ""
}
