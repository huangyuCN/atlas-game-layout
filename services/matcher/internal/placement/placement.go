// Package placement 为**每一局**选择一台 battle 落点（本机优先 + 轮询），并把该决定挂在
// ctx 上，让同一局的预热与入场跳到同一台。
//
// 为什么要「每局一次决定」：修前选点完全交给 gRPC 每次 RPC 的均衡器（wrr 在两台等权实例上
// 严格交替），而成局处置每局要发**两次** battle 调用——预热 Create 与入场 Admit。于是同一局
// 的两跳必然劈到两个进程：预热的 actor 长在 A、入场的 actor 长在 B，谁先到谁成为属主
// （预热是异步 goroutine，见 handler.admitMatch），落点变成两跳之间的竞态而不是一次决定；
// 竞态一旦系统性偏向一侧，所有对局就都堆在同一台（实测 battle1 1.37 核 / battle2 0.03 核）。
//
// 取舍（为什么是轮询而不是按在途加权）：选点的粒度是**一局**，而一局的两次 RPC 都是毫秒级
// 一元调用——在途 RPC 数完全不反映「这台已经跑了几局」（一局持续数分钟）。真正贴合负载的
// 信号是「各 battle 的在跑对局数」，但它不在 matcher 手里，要拿到就得新增注册元数据或协议
// 字段（超出本次范围，也会把 matcher 的成局路径绑到 battle 的上报节奏上）。故取轮询：
// 决策不依赖任何跨进程状态、分布精确（100 局 = 50/50）、并发安全、可确定性测试。
package placement

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas/registry"
	"github.com/huangyuCN/atlas/selector"
)

// schemeGRPC 是 battle internal 面端点的 scheme（与注册端/拨号端同口径，见 pkg/serverutil）。
const schemeGRPC = "grpc"

// DefaultCacheTTL 是候选快照的复用窗口：压测下单 tick 能成上百局，逐局查注册中心会把
// 成局速率绑到 etcd 的 QPS 上；1s 的陈旧代价只是「新 battle 上线后最多 1s 才分到对局」。
const DefaultCacheTTL = time.Second

// Options 是 Planner 构造选项。
type Options struct {
	// Service 是 battle internal 面注册的服务名（空 = consts.ServiceBattle）。
	Service string
	// Local 是本机优先过滤器（nil = 不区分主机，候选集为全部实例）。
	Local selector.NodeFilter
	// CacheTTL 是候选快照复用窗口（≤0 取 DefaultCacheTTL）。
	CacheTTL time.Duration
	// FrameService 是 battle 帧面实例的注册中心服务名（空 = consts.ServiceBattleFrame；
	// 选点据此排除「没有帧面端点」的实例，见 frames.go）。
	FrameService string
	// Now 是时钟（测试可注入；nil = time.Now）。
	Now func() time.Time
}

// Planner 按「本机优先 + 轮询」为每局选一台 battle。
type Planner struct {
	disc         registry.Discovery
	service      string
	frameService string
	local        selector.NodeFilter
	cacheTTL     time.Duration
	now          func() time.Time

	mu     sync.Mutex
	nodes  []selector.Node // 候选快照（本机优先过滤后的结果）
	loaded time.Time       // 快照建立时刻

	turn uint64 // 轮询游标（锁内自增：与快照读取同域，避免两次读取跨越一次快照切换）
}

// New 构造选点器；disc 为空时 Place 恒返回空串（等于不钉选点，不 panic）。
func New(disc registry.Discovery, o Options) *Planner {
	p := &Planner{
		disc:         disc,
		service:      o.Service,
		frameService: o.FrameService,
		local:        o.Local,
		cacheTTL:     o.CacheTTL,
		now:          o.Now,
	}
	if p.service == "" {
		p.service = consts.ServiceBattle
	}
	if p.frameService == "" {
		p.frameService = consts.ServiceBattleFrame
	}
	if p.cacheTTL <= 0 {
		p.cacheTTL = DefaultCacheTTL
	}
	if p.now == nil {
		p.now = time.Now
	}
	return p
}

// Pin 为本局选点并把目标端点挂到 ctx 上；无候选/发现失败时**原样返回 ctx**
// （选点失效不得让本局失败，落点退回既有每 RPC 负载均衡）。
func (p *Planner) Pin(ctx context.Context) context.Context {
	addr := p.Place(ctx)
	if addr == "" {
		return ctx
	}
	return WithTarget(ctx, addr)
}

// Retarget 为本局换一个候选端点并挂回 ctx；没有其它候选时返回原 ctx 与 false。
//
// 语义：Admit 的传输层失败（实例刚下线、端口不通）换一台可能成功；只有一台候选时换点
// 等于原样重试，没有价值——宁可把失败按既有路径处置，也不制造一次无意义的重试。
func (p *Planner) Retarget(ctx context.Context) (context.Context, bool) {
	if p == nil {
		return ctx, false
	}
	cur, _ := Target(ctx)
	addr := p.placeExcept(ctx, cur)
	if addr == "" || addr == cur {
		return ctx, false
	}
	return WithTarget(ctx, addr), true
}

// placeExcept 返回一个与 exclude 不同的候选端点（无其它候选时返回空串）。
func (p *Planner) placeExcept(ctx context.Context, exclude string) string {
	nodes := p.candidates(ctx)
	if len(nodes) < 2 {
		return ""
	}
	idx := 0
	for i, n := range nodes {
		if n.Address() == exclude {
			idx = i
			break
		}
	}
	return nodes[(idx+1)%len(nodes)].Address()
}

// Place 返回本局目标 battle 的端点地址（host:port）；无候选或发现失败返回空串。
func (p *Planner) Place(ctx context.Context) string {
	if p == nil || p.disc == nil {
		return ""
	}
	nodes := p.candidates(ctx)
	if len(nodes) == 0 {
		return ""
	}
	return nodes[p.next(len(nodes))].Address()
}

// next 返回本次轮询的候选下标（第一局取 0，之后严格轮转）。
func (p *Planner) next(n int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	idx := int(p.turn % uint64(n))
	p.turn++
	return idx
}

// candidates 返回候选快照：TTL 内复用；过期则重查注册中心并套用本机优先过滤。
// 发现失败时**保留原快照**（自愈交给下一次查询），无快照则返回空。
func (p *Planner) candidates(ctx context.Context) []selector.Node {
	p.mu.Lock()
	nodes, loaded := p.nodes, p.loaded
	p.mu.Unlock()
	if len(nodes) > 0 && p.now().Sub(loaded) < p.cacheTTL {
		return nodes
	}
	list, err := p.disc.GetService(ctx, p.service)
	if err != nil {
		return nodes // 发现失败：沿用上次快照（可能为空），不把错误甩给成局路径
	}
	ready := p.frameReady(ctx)
	nodes = localFirst(ctx, p.local, nodesOf(list, ready))
	if len(nodes) == 0 && ready != nil {
		// 过滤后无候选：退回不过滤（选点不得因帧面过滤而整体失效）。
		nodes = localFirst(ctx, p.local, nodesOf(list, nil))
	}
	p.mu.Lock()
	p.nodes, p.loaded = nodes, p.now()
	p.mu.Unlock()
	return nodes
}

// nodesOf 把注册实例映射为选点节点：地址取 grpc 面端点主机（host:port），
// 没有该面端点的实例（只注册 edge 面/帧面端口）直接跳过；
// ready 非空时再要求实例 ID 在「有帧面端点」集合里（nil = 不做帧面过滤）。
func nodesOf(list []*registry.ServiceInstance, ready map[string]struct{}) []selector.Node {
	out := make([]selector.Node, 0, len(list))
	for _, si := range list {
		if si == nil {
			continue
		}
		if ready != nil {
			if _, ok := ready[si.ID]; !ok {
				continue
			}
		}
		addr, ok := grpcAddr(si.Endpoints)
		if !ok {
			continue
		}
		out = append(out, selector.NewNode(schemeGRPC, addr, si))
	}
	return out
}

// grpcAddr 返回实例端点的 grpc 面主机；无该面端点返回 false（不静默取别的面）。
func grpcAddr(endpoints []string) (string, bool) {
	for _, ep := range endpoints {
		u, err := url.Parse(ep)
		if err != nil || u.Scheme != schemeGRPC || u.Host == "" {
			continue
		}
		return u.Host, true
	}
	return "", false
}

// localFirst 套用本机优先过滤器（nil = 不过滤，保持跨机候选全集）。
func localFirst(ctx context.Context, f selector.NodeFilter, nodes []selector.Node) []selector.Node {
	if f == nil || len(nodes) == 0 {
		return nodes
	}
	return f(ctx, nodes)
}
