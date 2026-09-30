// 帧面实例注册：battle 在注册中心额外注册 battle-frame 实例（规格 §2.1）——接入层
// Resolver 先查 actor 目录得属主 node_id，再按本实例的 node_id 元数据选中它，并读该实例的
// 传输面端口（ws/kcp/udp）拨后端；端口不写死、不进票据。
//
// 注册内容的事实来源（不新造配置项）：
//   - 节点 ID：runtime.id，与 actor NodeID、注册实例 ID 三处同源（见 pkg/actor.Options.NodeID）；
//   - 端口：帧面服务端**已绑定**的真实地址——配置 addr 为 0 时端口由内核分配，取配置字符串会注册出端口 0；
//   - 主机：battle.frame_advertise_host（本节点帧端口对外的可达主机）；未配置时回落
//     battle.edge_endpoints 的主机（**仅适合同机部署**，跨机部署必须显式配置）。

package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas-game-layout/lib/version"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/server"
	"github.com/huangyuCN/atlas/registry"
	"github.com/huangyuCN/atlas/transport"
)

// frameRegistrar 是 battle 帧面实例的注册器：启动注册、停机注销（不留残留实例）。
type frameRegistrar struct {
	reg      registry.Registrar
	instance *registry.ServiceInstance
}

// frameFace 描述一个已启用的直连帧面：元数据端口键、注册端点 scheme 与端点来源。
// 端口键必须与接入层 Resolver 读的元数据契约（lib/consts）逐个对齐。
type frameFace struct {
	key    string
	scheme string
	server transport.Endpointer
}

// frameFacesOf 汇总已启用的帧面（未启用的面为 nil，与帧监听启停同口径）。
func frameFacesOf(f server.FrameFaces) []frameFace {
	out := make([]frameFace, 0, 3)
	if f.KCP != nil {
		out = append(out, frameFace{consts.FrameMetaPortKCP, serverutil.SchemeKCP, f.KCP})
	}
	if f.UDP != nil {
		out = append(out, frameFace{consts.FrameMetaPortUDP, serverutil.SchemeUDP, f.UDP})
	}
	if f.WS != nil {
		out = append(out, frameFace{consts.FrameMetaPortWS, serverutil.SchemeWS, f.WS})
	}
	return out
}

// bound 返回该帧面已绑定的端点（尚未监听时先建监听，故端口必然是真实可拨的）。
func (f frameFace) bound() (*url.URL, error) {
	ep, err := f.server.Endpoint()
	if err != nil {
		return nil, fmt.Errorf("app: 取 %s 帧面端点失败: %w", f.key, err)
	}
	if ep.Port() == "" {
		return nil, fmt.Errorf("app: %s 帧面端点 %s 缺少端口", f.key, ep)
	}
	return ep, nil
}

// NewFrameRegistrar 组装帧面实例的注册内容：服务名 battle-frame、元数据 node_id + 三面端口、
// 端点为本节点帧面的可达地址。注册器/节点 ID/帧面主机/帧端口任一缺失或非法即构造失败
// （启动期暴露，而不是等接入层解析不到后端才发现）。
func NewFrameRegistrar(reg registry.Registrar, faces server.FrameFaces, cfg *conf.Bootstrap) (*frameRegistrar, error) {
	if reg == nil {
		return nil, errors.New("app: 注册帧面实例需要注册器（registry.Registrar）")
	}
	nodeID := cfg.GetRuntime().GetId()
	if nodeID == "" {
		return nil, errors.New("app: 注册帧面实例需要 runtime.id（与 actor NodeID 同源）")
	}
	host, err := frameAdvertiseHostOf(cfg.GetBattle())
	if err != nil {
		return nil, err
	}
	list := frameFacesOf(faces)
	if len(list) == 0 {
		return nil, errors.New("app: 三个直连帧面都未启用，无法注册帧面实例")
	}
	md := make(map[string]string, len(list)+1)
	md[consts.FrameMetaNodeID] = nodeID
	eps := make([]string, 0, len(list))
	for _, face := range list {
		ep, err := face.bound()
		if err != nil {
			return nil, err
		}
		md[face.key] = ep.Port()
		eps = append(eps, face.scheme+"://"+net.JoinHostPort(host, ep.Port()))
	}
	return &frameRegistrar{
		reg: reg,
		instance: &registry.ServiceInstance{
			ID:        nodeID,
			Name:      consts.ServiceBattleFrame,
			Version:   version.String(),
			Metadata:  md,
			Endpoints: eps,
		},
	}, nil
}

// frameAdvertiseHostOf 取帧面实例的对外主机（接入层从哪个主机能拨到本节点帧端口）：
//   - 字段**缺失**：回落 edge_endpoints 的主机——该回落**仅适合同机部署**（接入层与 battle
//     同一主机）；跨机部署必须显式配置，否则注册出去的端点会指向接入层主机，接入层会拿
//     自己的地址去拨后端（表现为连不上后端，且极难定位）；
//   - 字段**显式配置**：必须是非空、不含端口的主机部分；空白、带端口（host:port）、带方括号
//     的 IPv6 字面量一律报错——不静默截断、不静默回落（「配了没生效」比启动失败更贵）。
func frameAdvertiseHostOf(b *conf.BattleConf) (string, error) {
	if b.FrameAdvertiseHost == nil {
		return edgeHost(b.GetEdgeEndpoints())
	}
	raw := strings.TrimSpace(*b.FrameAdvertiseHost)
	if raw == "" {
		return "", errors.New("app: battle.frame_advertise_host 配了空白值；同机部署请删除该字段（回落 edge_endpoints 主机），跨机部署请填本节点可达主机")
	}
	if _, _, err := net.SplitHostPort(raw); err == nil {
		return "", fmt.Errorf("app: battle.frame_advertise_host 只能填主机部分，不含端口（实际 %q；端口由帧面监听地址决定）", raw)
	}
	if strings.HasPrefix(raw, "[") {
		return "", fmt.Errorf("app: battle.frame_advertise_host 的 IPv6 字面量不要带方括号（实际 %q）", raw)
	}
	return raw, nil
}

// edgeHost 是帧面主机的**回落**来源：取接入层地址列表里的主机。仅在
// frame_advertise_host 未配置时使用，语义前提是「接入层与帧面同机部署」。
// 逐项取首个「主机 + 端口」合法的地址；一项都没有即报错（接入层地址必须带端口，否则客户端也连不上）。
func edgeHost(list []*battlev1.EdgeEndpoint) (string, error) {
	for _, ep := range list {
		if host, _, err := net.SplitHostPort(ep.GetAddress()); err == nil && host != "" {
			return host, nil
		}
	}
	return "", fmt.Errorf("app: battle.edge_endpoints 里没有形如 host:port 的地址（%d 项），无法取帧面实例主机", len(list))
}

// Register 注册帧面实例（启动期；失败即进程启动失败，不静默降级）。
func (r *frameRegistrar) Register(ctx context.Context) error {
	if err := r.reg.Register(ctx, r.instance); err != nil {
		return fmt.Errorf("app: 注册帧面实例（%s）失败: %w", r.instance.Name, err)
	}
	return nil
}

// Deregister 注销帧面实例（停机期）：不让接入层再解析到已停节点的帧端口。
// 注销失败只上报错误（租约 TTL 会兜底回收残留实例），不阻断停机流程。
func (r *frameRegistrar) Deregister(ctx context.Context) error {
	if err := r.reg.Deregister(ctx, r.instance); err != nil {
		return fmt.Errorf("app: 注销帧面实例（%s）失败: %w", r.instance.Name, err)
	}
	return nil
}
