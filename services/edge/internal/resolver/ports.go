package resolver

import (
	"context"
	"net"
)

// Directory 是 actor 目录的只读查询端口（PID → 属主节点）。
// 接入层不持有 actor 运行时，只读目录；实现见 LocatorDirectory。
type Directory interface {
	// OwnerNode 返回 PID 的属主节点 ID；查不到返回错误。
	OwnerNode(ctx context.Context, pid string) (string, error)
	// WatchOwner 监听 PID 的属主变化并回调**当前**属主（记录消失时为空串）；
	// 返回的 cancel 幂等（接入层在该局最后一条流结束时调用）。
	WatchOwner(ctx context.Context, pid string, onChange func(owner string)) (cancel func(), err error)
}

// FrameRegistry 是帧面实例发现端口（服务名 → 实例列表），实现见 DiscoveryRegistry。
type FrameRegistry interface {
	// Instances 返回服务名的实例列表（含元数据里的 node_id 与传输面端口）。
	Instances(ctx context.Context, service string) ([]Instance, error)
}

// Instance 是注册中心里一个帧面实例的中间表示：把框架 registry.ServiceInstance
// 归一成便于用内存桩测试 Resolver 的形状。
type Instance struct {
	// ID 是实例 ID（日志与排障用）。
	ID string
	// NodeID 是实例所属 actor 节点（与目录属主 node_id 比对）。
	NodeID string
	// Host 是该节点对客户端可达的主机（元数据 host；空则用 EndpointHost）。
	Host string
	// Ports 是「面名 → 端口」映射（元数据 ws/kcp/udp）。
	Ports map[string]string
	// EndpointHost 是实例端点里的主机（未配 host 元数据时的兜底）。
	EndpointHost string
}

// Address 返回该实例在指定传输面上的后端地址（host:port）。
// 端口缺失时返回 "host:"，由调用方通过 hasPort 判定后拒绝。
func (i Instance) Address(face string) string {
	return net.JoinHostPort(i.host(), i.port(face))
}

// host 返回该实例的主机：优先元数据 host，其次端点主机。
func (i Instance) host() string {
	if i.Host != "" {
		return i.Host
	}
	return i.EndpointHost
}

// port 返回该实例在指定面上的端口（缺失返回空串）。
func (i Instance) port(face string) string {
	if i.Ports == nil {
		return ""
	}
	return i.Ports[face]
}
