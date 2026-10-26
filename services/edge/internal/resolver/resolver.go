// Package resolver 实现接入层的后端解析：先查 actor 目录得战斗属主节点，
// 再按节点选 battle-frame 帧面实例并取其元数据里的传输面端口（规格 §2.1）。
//
// 端口不写死、不进票据：actor 目录与注册中心是唯一事实来源；解析失败一律按
// backend_unavailable 拒绝（接入层断开 + 指标），不做应用层回执。
//
// 解析走两层缓存，目的都是让「解析」几乎不产生网络往返：
//  1. 本包的 watch 驱动本地映射（长期有效、事件更新）：battle_id → 属主节点、节点 → 帧面实例；
//  2. 接入层的 TTL 短缓存（contrib/edge）：命中即连本包都不进。
//
// 监听不可用、尚未就绪或条目过期时退回原来的两跳查询——缓存是增强能力，不引入新的失败路径。
package resolver

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/contrib/edge"
	"github.com/huangyuCN/atlas/log"
	"github.com/huangyuCN/atlas/metrics"
)

// Options 是 Resolver 构造选项。
type Options struct {
	// Metrics 是可选指标采集器（nil = noop）：解析命中率记在 edge_resolve_cache_total。
	Metrics metrics.Collector
	// Logger 是可选日志器（nil = 框架默认日志器）：监听建立失败按 Debug 记录（退回两跳查询）。
	Logger log.Logger
	// ActorType 是战斗 actor 的类型段（空 = consts.ActorTypeBattle，PID 形如 battle:<battle_id>）。
	ActorType string
	// FrameService 是帧面实例的注册中心服务名（空 = consts.ServiceBattleFrame）。
	FrameService string
}

// Resolver 实现 edge.Resolver：目录属主 + 帧面实例端口 → 后端地址，并维护 watch 驱动的本地映射。
type Resolver struct {
	dir          Directory
	frames       FrameRegistry
	actorType    string
	frameService string
	stats        *edge.ResolveCacheCounters
	log          log.Logger
	now          func() time.Time

	// ctx 是全部监听的父 ctx（Close 时取消，让适配层的监听 goroutine 退出）。
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu            sync.Mutex
	owners        map[string]*ownerEntry // battle_id → 属主条目（每局一条目录监听）
	snapshot      *frameSnapshot         // 帧面实例全量快照（全局一条监听维护）
	frameWatch    func()                 // 实例监听的取消函数（nil = 未建立）
	frameStarting bool                   // 正在建立实例监听（防并发重复建）
	frameRetry    time.Time              // 实例监听建立失败后的下次尝试时间
	closed        bool                   // 是否已 Close（再来的请求不再建监听）
	onChange      func(string)           // 「该局解析结果已变」回调（接入层据此失效 TTL 条目）
}

// New 构造 Resolver；依赖缺失即失败（启动期暴露，而不是每局连接时才发现）。
func New(dir Directory, frames FrameRegistry, opts Options) (*Resolver, error) {
	if dir == nil {
		return nil, errors.New("resolver: actor 目录不能为空")
	}
	if frames == nil {
		return nil, errors.New("resolver: 帧面实例发现不能为空")
	}
	logger := opts.Logger
	if logger == nil {
		logger = log.DefaultLogger
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Resolver{
		dir: dir, frames: frames,
		actorType: opts.ActorType, frameService: opts.FrameService,
		stats: edge.NewResolveCacheCounters(opts.Metrics),
		log:   logger,
		now:   time.Now,
		ctx:   ctx, cancel: cancel,
		owners: make(map[string]*ownerEntry),
	}
	if r.actorType == "" {
		r.actorType = consts.ActorTypeBattle
	}
	if r.frameService == "" {
		r.frameService = consts.ServiceBattleFrame
	}
	r.startSweeper()
	return r, nil
}

// Resolve 解析战斗属主节点上的帧面后端地址；任何失败都按 backend_unavailable 拒绝。
func (r *Resolver) Resolve(ctx context.Context, req edge.ResolveRequest) (edge.Backend, error) {
	pid, err := r.pidOf(req.Ticket.BattleID)
	if err != nil {
		return edge.Backend{}, err
	}
	nodeID, ownerLookup, err := r.ownerNode(ctx, pid, req.Ticket.BattleID)
	if err != nil {
		return edge.Backend{}, err
	}
	inst, frameLookup, err := r.pickInstance(ctx, nodeID, req.Listener)
	if err != nil {
		return edge.Backend{}, err
	}
	r.report(ownerLookup.merge(frameLookup))
	// Owner 一并交给接入层：属主变更拆流要按「接通时的属主」比对（规格 §8）。
	return edge.Backend{Address: inst.Address(req.Listener), Owner: nodeID}, nil
}

// ownerNode 返回该局的属主节点：本地映射命中即用；未命中（或过期）才查一次目录并顺带回填。
// 监听建立失败时不回填（保守兜底：下次仍走目录查询），避免「没有事件来源的条目」永久陈旧。
func (r *Resolver) ownerNode(ctx context.Context, pid types.PID, battleID string) (string, cacheLookup, error) {
	started := r.now()
	owner, lookup := r.lookupOwner(battleID, started)
	if lookup == lookupHit {
		return owner, lookupHit, nil
	}
	r.ensureOwnerWatch(pid, battleID)
	nodeID, err := r.dir.OwnerNode(ctx, pid.String())
	if err != nil {
		return "", lookup, reject(fmt.Errorf("查询 actor 目录 %s 失败: %w", pid, err))
	}
	if nodeID == "" {
		return "", lookup, reject(fmt.Errorf("actor 目录 %s 无属主节点（租约可能已过期）", pid))
	}
	r.storeOwner(battleID, nodeID, started)
	return nodeID, lookup, nil
}

// pidOf 由 battle_id 组装并校验战斗 PID（非法直接拒绝，不去查空键）。
func (r *Resolver) pidOf(battleID string) (types.PID, error) {
	if battleID == "" {
		return types.PID{}, reject(errors.New("票据缺少 battle_id"))
	}
	pid, err := types.NewPID(r.actorType, battleID)
	if err != nil {
		return types.PID{}, reject(fmt.Errorf("battle_id %q 无法组成战斗 PID: %w", battleID, err))
	}
	return pid, nil
}

// reject 把解析失败包装为「后端不可用」拒绝（接入层据此计数并断开）。
func reject(err error) error { return edge.Reject(edge.ReasonBackendUnavailable, err) }

// hasPort 报告该实例是否声明了指定面的合法端口。
func (i Instance) hasPort(face string) bool {
	port, err := strconv.Atoi(i.port(face))
	return err == nil && port > 0
}
