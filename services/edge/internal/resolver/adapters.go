package resolver

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/locator"
	"github.com/huangyuCN/atlas/registry"
)

// LocatorDirectory 把框架 locator（只读 actor 目录，etcd 实现）适配为 Directory。
type LocatorDirectory struct {
	loc locator.Locator
}

// NewLocatorDirectory 构造目录适配器；loc 必须已带 /atlas/actors/<ns> 前缀
// （前缀由框架 namespace.Derive 派生，见装配层）。
func NewLocatorDirectory(loc locator.Locator) (*LocatorDirectory, error) {
	if loc == nil {
		return nil, errors.New("resolver: actor 目录 locator 不能为空")
	}
	return &LocatorDirectory{loc: loc}, nil
}

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

// WatchOwner 监听目录记录变化并回调当前属主（规格 §8：属主变更 → 接入层拆流）。
// 目录记录在租约续期时会被重写，故回调会重复触发；比较属主是否变化由调用方（guard）负责。
// 监听按前缀建立，回调前按 key 精确过滤——PID 形如 battle:b1 与 battle:b10 前缀相同，
// 不过滤会把别的对局的属主变化误当成自己的。
func (d *LocatorDirectory) WatchOwner(ctx context.Context, pid string, onChange func(owner string)) (func(), error) {
	parsed, err := types.ParsePID(pid)
	if err != nil {
		return nil, err
	}
	key := parsed.Key().String()
	w, err := d.loc.Watch(ctx, locator.WatchOptions{
		KeyPrefix: key, LocationBuilder: types.NewLocationBuilder(),
	})
	if err != nil {
		return nil, fmt.Errorf("resolver: 监听目录 %s 失败: %w", pid, err)
	}
	stop := make(chan struct{})
	go func() {
		defer func() { _ = w.Stop() }()
		for {
			events, err := w.Next()
			if err != nil {
				return // 上下文取消或监听终止：静默退出（接入层不再拆该局的流）
			}
			for _, ev := range events {
				if ev.Key == nil || ev.Key.String() != key {
					continue
				}
				onChange(ownerOfLocation(ev.Loc))
			}
			select {
			case <-stop:
				return
			default:
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(stop) }) }, nil
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
	out := make([]Instance, 0, len(list))
	for _, si := range list {
		if si != nil {
			out = append(out, instanceOf(si))
		}
	}
	return out, nil
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
