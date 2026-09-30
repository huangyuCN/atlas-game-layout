// 迁入节点的选点：复用 rebalance 的负载快照与计划（规格 §8「把 contrib/actor/rebalance
// 接到 battle」）。选点是**纯函数**：输入快照 + 候选节点 + 目标 PID，输出目标节点，
// 因此可以在单测里用内存构造的 rebalance.Snapshot 直接验证，不需要 etcd 与真实集群。

package migrate

import (
	"context"
	"errors"
	"fmt"
	"sort"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas/contrib/actor/rebalance"
	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// ErrNoTarget 表示没有可用的迁入节点（候选为空或全被排除）。
var ErrNoTarget = errors.New("migrate: 没有可用的迁入节点")

// ErrNoPlan 表示 rebalance 计划里没有该 PID（负载未失衡），需要回落选点。
var ErrNoPlan = errors.New("migrate: rebalance 计划未覆盖该 PID")

// Picker 是 rebalance.NodePicker 的模板实现：在候选里选负载最低的节点（同负载取节点 ID 字典序，
// 保证多次选点结果稳定、便于断言）。框架的 Placement 适配器在 cluster 包内不可导出，
// 故模板自带一个等价语义的最小实现。
type Picker struct{}

// Select 实现 rebalance.NodePicker：排除 exclude 后取 ProcCount 最小者。
func (Picker) Select(_ context.Context, nodes []rebalance.Node, exclude string) (string, error) {
	cands := make([]rebalance.Node, 0, len(nodes))
	for _, n := range nodes {
		if n.ID != exclude && n.ID != "" {
			cands = append(cands, n)
		}
	}
	if len(cands) == 0 {
		return "", ErrNoTarget
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].ProcCount != cands[j].ProcCount {
			return cands[i].ProcCount < cands[j].ProcCount
		}
		return cands[i].ID < cands[j].ID
	})
	return cands[0].ID, nil
}

// ChooseTarget 从 rebalance 计划里取该 PID 的迁入节点；
// 计划为空（负载未失衡）时回落到 Picker 选点——迁移是运维显式触发，
// 不能因为「恰好均衡」而拒绝执行。
func ChooseTarget(ctx context.Context, snap *rebalance.Snapshot, nodes []rebalance.Node,
	pid types.PID, fromNode string, picker rebalance.NodePicker) (string, error) {
	if picker == nil {
		picker = Picker{}
	}
	cfg := rebalance.DefaultConfig()
	plans := rebalance.ComputePlans(ctx, snap, nodes, picker, cfg)
	for _, plan := range plans {
		if plan.PID.String() == pid.String() {
			return plan.ToNode, nil
		}
	}
	target, err := picker.Select(ctx, nodes, fromNode)
	if err != nil {
		return "", fmt.Errorf("%w（计划 %d 条，源 %s）", ErrNoTarget, len(plans), fromNode)
	}
	return target, nil
}

// Snapshotter 是负载快照端口（实现为 rebalance.Snapshotter：Refresh 同步扫一轮目录）。
type Snapshotter interface {
	// Refresh 同步刷新一轮负载快照。
	Refresh(ctx context.Context)
	// Snapshot 返回最近一轮快照（可能为 nil）。
	Snapshot() *rebalance.Snapshot
}

// NodeLister 是候选节点端口（实现由装配层用服务发现适配）。
type NodeLister interface {
	// List 返回当前候选节点列表。
	List(ctx context.Context) ([]rebalance.Node, error)
}

// AutoChooser 是自动选点：负载快照 + 候选节点 → rebalance 计划 → 目标节点。
// 快照按需同步刷新（不常驻后台循环）：运维显式触发一次迁移才扫一轮目录。
type AutoChooser struct {
	snap  Snapshotter
	nodes NodeLister
}

// NewAutoChooser 构造自动选点；snap/nodes 任一为 nil 时选点不可用（返回 ErrNoTarget）。
func NewAutoChooser(snap Snapshotter, nodes NodeLister) *AutoChooser {
	return &AutoChooser{snap: snap, nodes: nodes}
}

// Choose 选出一个非源节点的迁入目标。
func (c *AutoChooser) Choose(ctx context.Context, pid types.PID, fromNode string) (string, error) {
	if c == nil || c.snap == nil || c.nodes == nil {
		return "", ErrNoTarget
	}
	c.snap.Refresh(ctx)
	nodes, err := c.nodes.List(ctx)
	if err != nil {
		return "", err
	}
	return ChooseTarget(ctx, c.snap.Snapshot(), nodes, pid, fromNode, Picker{})
}

// Target 是管理面请求里解析出的目标节点（空 = 自动选点）。
type Target struct {
	// Node 是解析结果（一定非空）。
	Node string
	// From 是迁出节点（自动选点时的排除项）。
	From string
}

// TargetOf 解析迁移目标：显式指定则原样返回；为空时按 rebalance 计划选点。
// nodes/snap 由调用方（装配层）从服务发现与负载快照取得。
func TargetOf(ctx context.Context, req *battlev1.MigrateBattleRequest, fromNode string,
	snap *rebalance.Snapshot, nodes []rebalance.Node) (Target, error) {
	if target := req.GetTargetNode(); target != "" {
		return Target{Node: target, From: fromNode}, nil
	}
	pid, err := types.NewPID("battle", req.GetBattleId())
	if err != nil {
		return Target{}, err
	}
	target, err := ChooseTarget(ctx, snap, nodes, pid, fromNode, Picker{})
	if err != nil {
		return Target{}, err
	}
	return Target{Node: target, From: fromNode}, nil
}
