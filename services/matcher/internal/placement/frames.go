package placement

import (
	"context"
	"net/url"

	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas/registry"
)

// 本文件实现「只选有帧面端点的 battle」：battle 把直连帧面另注册成 battle-frame 实例
// （ID 与 battle 实例同源 = runtime.id，见 services/battle/internal/app/framereg.go），
// 接入层按该实例声明的帧面端点拨后端。没有帧面端点的实例接得住 Create，却接不住客户端
// 帧连接——选到它等于这局永远连不上。
//
// 过滤是**软**的：帧面发现不可用、没有帧面实例、或过滤后一个候选都不剩时，一律退回不过滤
// （选点只是分摊手段，绝不能因为多看了一眼帧面就把选点整体打成空集）。

// frameSchemes 是 battle-frame 实例上算「帧面端点」的 scheme（与注册端同源）。
var frameSchemes = map[string]struct{}{
	serverutil.SchemeWS:  {},
	serverutil.SchemeKCP: {},
	serverutil.SchemeUDP: {},
}

// frameReady 返回「有帧面端点」的 battle 实例 ID 集合；nil 表示**不做过滤**。
//
// 三种情况返回 nil（都属「过滤依据不可得，退回旧口径」）：
//   - 帧面服务名未配置或发现失败：注册中心抖动时不得把候选缩成空集；
//   - 没有任何帧面实例：battle 侧可能还没升级到「额外注册帧面实例」的形态；
//   - 帧面实例都没声明帧面端点（只有 grpc:// 等非帧面端点）。
func (p *Planner) frameReady(ctx context.Context) map[string]struct{} {
	if p == nil || p.disc == nil || p.frameService == "" {
		return nil
	}
	list, err := p.disc.GetService(ctx, p.frameService)
	if err != nil || len(list) == 0 {
		return nil
	}
	ready := make(map[string]struct{}, len(list))
	for _, si := range list {
		if si == nil || !hasFrameFace(si) {
			continue
		}
		ready[si.ID] = struct{}{}
	}
	if len(ready) == 0 {
		return nil
	}
	return ready
}

// hasFrameFace 报告该实例是否声明了至少一个帧面端点（ws/kcp/udp，且带主机）。
func hasFrameFace(si *registry.ServiceInstance) bool {
	for _, ep := range si.Endpoints {
		u, err := url.Parse(ep)
		if err != nil || u.Host == "" {
			continue
		}
		if _, ok := frameSchemes[u.Scheme]; ok {
			return true
		}
	}
	return false
}
