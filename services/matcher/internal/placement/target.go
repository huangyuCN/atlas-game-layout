package placement

import (
	"context"

	"github.com/huangyuCN/atlas/selector"
)

// targetKey 是 ctx 上「本局目标 battle 端点」的键（不导出：避免别处自造同名键而语义不同）。
type targetKey struct{}

// WithTarget 把本局目标 battle 端点挂到 ctx 上（预热与入场共用同一 ctx 即落到同一台）。
func WithTarget(ctx context.Context, addr string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, targetKey{}, addr)
}

// Target 返回 ctx 上挂的目标端点；未选点时 ok=false。
func Target(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	addr, ok := ctx.Value(targetKey{}).(string)
	if !ok || addr == "" {
		return "", false
	}
	return addr, true
}

// TargetFilter 返回「钉住本局目标端点」的节点过滤器：
// 候选里有目标就只留目标；目标不在候选里（实例下线/端点变了）就**原样返回候选**——
// 选点失效不得让本局失败，由既有负载均衡兜底（与 PreferLocal 的跨机兜底同一条原则）。
//
// 顺序上有要求：挂在 PreferLocal 之后（先按主机收窄，再钉），否则跨机候选里的同名地址会把
// 本机优先语义盖掉；本过滤器自身不改变「选哪台」，只把已选的那台固定下来。
func TargetFilter() selector.NodeFilter {
	return func(ctx context.Context, nodes []selector.Node) []selector.Node {
		addr, ok := Target(ctx)
		if !ok {
			return nodes
		}
		for _, n := range nodes {
			if n.Address() == addr {
				return []selector.Node{n}
			}
		}
		return nodes // 目标不在候选里：兜底（不缩成空集）
	}
}
