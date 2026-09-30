// Package resolver 实现接入层的后端解析：先查 actor 目录得战斗属主节点，
// 再按节点选 battle-frame 帧面实例并取其元数据里的传输面端口（规格 §2.1）。
//
// 端口不写死、不进票据：actor 目录与注册中心是唯一事实来源；解析失败一律按
// backend_unavailable 拒绝（接入层断开 + 指标），不做应用层回执。
package resolver

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/contrib/edge"
)

// Options 是 Resolver 构造选项。
type Options struct {
	// ActorType 是战斗 actor 的类型段（空 = consts.ActorTypeBattle，PID 形如 battle:<battle_id>）。
	ActorType string
	// FrameService 是帧面实例的注册中心服务名（空 = consts.ServiceBattleFrame）。
	FrameService string
}

// Resolver 实现 edge.Resolver：目录属主 + 帧面实例端口 → 后端地址。
type Resolver struct {
	dir          Directory
	frames       FrameRegistry
	actorType    string
	frameService string
}

// New 构造 Resolver；依赖缺失即失败（启动期暴露，而不是每局连接时才发现）。
func New(dir Directory, frames FrameRegistry, opts Options) (*Resolver, error) {
	if dir == nil {
		return nil, errors.New("resolver: actor 目录不能为空")
	}
	if frames == nil {
		return nil, errors.New("resolver: 帧面实例发现不能为空")
	}
	r := &Resolver{dir: dir, frames: frames, actorType: opts.ActorType, frameService: opts.FrameService}
	if r.actorType == "" {
		r.actorType = consts.ActorTypeBattle
	}
	if r.frameService == "" {
		r.frameService = consts.ServiceBattleFrame
	}
	return r, nil
}

// Resolve 解析战斗属主节点上的帧面后端地址；任何失败都按 backend_unavailable 拒绝。
func (r *Resolver) Resolve(ctx context.Context, req edge.ResolveRequest) (edge.Backend, error) {
	pid, err := r.pidOf(req.Ticket.BattleID)
	if err != nil {
		return edge.Backend{}, err
	}
	nodeID, err := r.dir.OwnerNode(ctx, pid.String())
	if err != nil {
		return edge.Backend{}, reject(fmt.Errorf("查询 actor 目录 %s 失败: %w", pid, err))
	}
	if nodeID == "" {
		return edge.Backend{}, reject(fmt.Errorf("actor 目录 %s 无属主节点（租约可能已过期）", pid))
	}
	inst, err := r.pickInstance(ctx, nodeID, req.Listener)
	if err != nil {
		return edge.Backend{}, err
	}
	// Owner 一并交给接入层：属主变更拆流要按「接通时的属主」比对（规格 §8）。
	return edge.Backend{Address: inst.Address(req.Listener), Owner: nodeID}, nil
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

// pickInstance 选属主节点上承载该传输面的帧面实例（同节点多面 → 按客户端传输面选）。
func (r *Resolver) pickInstance(ctx context.Context, nodeID, face string) (Instance, error) {
	instances, err := r.frames.Instances(ctx, r.frameService)
	if err != nil {
		return Instance{}, reject(fmt.Errorf("查询帧面实例（%s）失败: %w", r.frameService, err))
	}
	for _, inst := range instances {
		if inst.NodeID == nodeID && inst.hasPort(face) {
			return inst, nil
		}
	}
	return Instance{}, reject(fmt.Errorf("节点 %s 上没有承载 %q 面的 %s 实例", nodeID, face, r.frameService))
}

// reject 把解析失败包装为「后端不可用」拒绝（接入层据此计数并断开）。
func reject(err error) error { return edge.Reject(edge.ReasonBackendUnavailable, err) }

// hasPort 报告该实例是否声明了指定面的合法端口。
func (i Instance) hasPort(face string) bool {
	port, err := strconv.Atoi(i.port(face))
	return err == nil && port > 0
}
