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
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	getlog "github.com/huangyuCN/atlas/log"
	"github.com/huangyuCN/atlas/registry"
	"github.com/huangyuCN/atlas/selector"
)

// schemeGRPC 是 battle internal 面端点的 scheme（与注册端/拨号端同口径，见 pkg/serverutil）。
const schemeGRPC = "grpc"

// DefaultCacheTTL 是候选快照的复用窗口：压测下单 tick 能成上百局，逐局查注册中心会把
// 成局速率绑到 etcd 的 QPS 上；1s 的陈旧代价只是「新 battle 上线后最多 1s 才分到对局」。
const DefaultCacheTTL = time.Second

// DefaultReadyTimeout 是「候选集未就绪」的最长等待：重启后实例逐个注册进注册中心，
// 帧面实例晚到的那一台会被帧面过滤摘掉，候选集随之塌成 1 台。等到齐再出票只多花注册延迟
// （实测百毫秒级）；有了上限，等不齐时仍照常出票，绝不把成局路径挂在注册中心上。
const DefaultReadyTimeout = 2 * time.Second

// DefaultReadyPoll 是等待候选集就绪时的重试间隔。
const DefaultReadyPoll = 20 * time.Millisecond

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
	// ReadyTimeout 是候选集未就绪（少于注册的 battle 实例数）时的**最长等待**（≤0 取
	// DefaultReadyTimeout）；给个极小值等于关掉等待，退回旧口径。
	ReadyTimeout time.Duration
	// ReadyPoll 是未就绪时的重试间隔（≤0 取 DefaultReadyPoll）。
	ReadyPoll time.Duration
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
	readyTimeout time.Duration
	readyPoll    time.Duration
	now          func() time.Time

	mu       sync.Mutex
	nodes    []selector.Node // 候选快照（本机优先过滤后的结果）
	loaded   time.Time       // 快照建立时刻
	warned   int             // 上次快照的候选数（候选集塌成 1 时只告警一次，见 warnSole）
	knownSig string          // 上次快照的候选指纹（同一份缺口只等一次，见 refresh）

	// refreshMu 串行化快照重建：冷启动时上百局并发选点只让一个 goroutine 去等注册中心，
	// 其余在锁上短暂排队后直接读新快照（否则等待窗口会被放大成「每局各等一轮」）。
	refreshMu sync.Mutex

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
		readyTimeout: o.ReadyTimeout,
		readyPoll:    o.ReadyPoll,
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
	if p.readyTimeout <= 0 {
		p.readyTimeout = DefaultReadyTimeout
	}
	if p.readyPoll <= 0 {
		p.readyPoll = DefaultReadyPoll
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

// candidates 返回候选快照：TTL 内复用；过期则重建（含「候选集未就绪即等待」，见 refresh）。
// 发现失败时**保留原快照**（自愈交给下一次查询），无快照则返回空。
func (p *Planner) candidates(ctx context.Context) []selector.Node {
	if nodes, ok := p.fresh(); ok {
		return nodes
	}
	p.refreshMu.Lock()
	defer p.refreshMu.Unlock()
	if nodes, ok := p.fresh(); ok {
		return nodes // 等锁期间别人已重建（冷启动时并发选点不各等一轮）
	}
	return p.refresh(ctx)
}

// fresh 返回 TTL 内可用的候选快照；无可用快照时 ok=false。
func (p *Planner) fresh() ([]selector.Node, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.nodes) > 0 && p.now().Sub(p.loaded) < p.cacheTTL {
		return p.nodes, true
	}
	return nil, false
}

// candidateSet 是一次快照重建的产物：候选端点 + 注册的 battle 实例数（候选的理论上限）。
type candidateSet struct {
	nodes      []selector.Node
	registered int
}

// ready 报告候选集是否已就绪：候选数不少于注册实例数（注册为 0 时无从比较，视为就绪）。
func (c candidateSet) ready() bool { return len(c.nodes) >= c.registered }

// sig 返回候选集的地址指纹（用于判断「同一份缺口是不是已经见过」）。
func (c candidateSet) sig() string {
	addrs := make([]string, 0, len(c.nodes))
	for _, n := range c.nodes {
		addrs = append(addrs, n.Address())
	}
	return strings.Join(addrs, ",")
}

// refresh 查注册中心重建快照。候选数少于**注册的 battle 实例数**（帧面实例晚于 battle 实例
// 注册进注册中心，即冷启动窗口）时先等：这段时间出的票必然全落先就绪的那台，而 1s 快照复用
// 会把窗口放大成整批偏斜（实测重启后首轮 113:297，复测 199:211）。
//
// 三条边界：等到上限仍不齐 ⇒ WARN（带注册数 vs 候选数）并照常出票，绝不永久卡住成局路径；
// 同一份缺口只等一次（已在此前快照出现过 ⇒ 不是启动竞态，不再为它加延迟）；发现失败 ⇒ 沿用
// 上次快照。
func (p *Planner) refresh(ctx context.Context) []selector.Node {
	deadline := p.now().Add(p.readyTimeout)
	var set candidateSet
	for {
		got, ok := p.query(ctx)
		if !ok {
			return p.lastSnapshot() // 发现失败：不把错误甩给成局路径
		}
		set = got
		if set.ready() || p.known(set.sig()) || ctx.Err() != nil || !p.now().Before(deadline) {
			break
		}
		p.pause(ctx)
	}
	if !set.ready() && !p.known(set.sig()) && ctx.Err() == nil {
		getlog.GetLogger().Warn(fmt.Sprintf(
			"matcher: 候选集未就绪——等待 %s 仍不足（候选 %d / 注册 %d），按现有候选继续出票",
			p.readyTimeout, len(set.nodes), set.registered), "service", p.service)
	}
	p.store(set)
	p.warnSole(set)
	return set.nodes
}

// query 查一次注册中心并构造候选集：registered 是套用本机优先后可作候选的实例数
// （即候选的理论上限），nodes 是再叠加帧面过滤后的结果（过滤不可得时退回不过滤）。
func (p *Planner) query(ctx context.Context) (candidateSet, bool) {
	list, err := p.disc.GetService(ctx, p.service)
	if err != nil {
		return candidateSet{}, false
	}
	eligible := localFirst(ctx, p.local, nodesOf(list, nil))
	nodes := eligible
	if ready := p.frameReady(ctx); ready != nil {
		if filtered := localFirst(ctx, p.local, nodesOf(list, ready)); len(filtered) > 0 {
			nodes = filtered // 过滤后为空：退回不过滤（选点不得因帧面过滤而整体失效）
		}
	}
	return candidateSet{nodes: nodes, registered: len(eligible)}, true
}

// store 落盘新快照（候选集 + 指纹 + 建立时刻）。
func (p *Planner) store(set candidateSet) {
	p.mu.Lock()
	p.nodes, p.loaded, p.knownSig = set.nodes, p.now(), set.sig()
	p.mu.Unlock()
}

// lastSnapshot 返回上一次成功建立的候选快照（无快照时为空）。
func (p *Planner) lastSnapshot() []selector.Node {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.nodes
}

// known 报告该指纹的候选集是否已经出现过（同一份缺口只等一次）。
func (p *Planner) known(sig string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return sig != "" && p.knownSig == sig
}

// pause 等一个重试间隔；ctx 结束即提前返回（选点不得超出调用方预算）。
func (p *Planner) pause(ctx context.Context) {
	t := time.NewTimer(p.readyPoll)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// warnSole 在候选集**只剩一台**时打一条 WARN（只在候选数变化时打，避免每 TTL 刷一行）。
//
// 为什么值得一条告警：轮询在单候选上退化成「本批对局全落这一台」，而选点代码本身完全
// 看不出错——500 局里 486 局同台这种偏斜，根因几乎总是候选集塌了（实例 id 缺省导致同名键
// 被顶掉、注册冲突、帧面过滤把某台摘掉）。把候选数/注册数做成可观测信号，下次直接看 WARN。
func (p *Planner) warnSole(set candidateSet) {
	p.mu.Lock()
	changed := p.warned != len(set.nodes)
	p.warned = len(set.nodes)
	p.mu.Unlock()
	if !changed || len(set.nodes) != 1 {
		return
	}
	getlog.GetLogger().Warn(fmt.Sprintf(
		"matcher: battle 候选集只有 1 台（候选 %d / 注册 %d）——轮询退化为本批对局全落这一台",
		len(set.nodes), set.registered),
		"service", p.service, "instance", set.nodes[0].Address())
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
