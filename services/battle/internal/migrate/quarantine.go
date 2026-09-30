package migrate

import (
	"time"

	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// QuarantineWindow 是迁移 drain 之后旧属主节点的隔离窗口时长。取值 10s 的依据：
//
//  1. 必须盖住归属空窗：drain 的 Release 之后目录里没有该 PID 的归属记录，直到新属主
//     Claim 成功；这段窗口内旧节点被在途帧 op 或重投的激活请求命中就会重新 Claim。
//     健康路径下新属主 Claim 只花一次跨节点激活往返（毫秒级），但一次跨节点激活的
//     传输超时预算是框架的 defaultNATSTimeout=5s（contrib/actor/cluster/transport_nats.go），
//     取 10s = 「一次超时 + 一次编排重试」，编排器 tick 为 100ms 级
//     （rebalance.DefaultConfig().Tick），足够覆盖首次激活失败后的重试。
//
//  2. 无需叠加租约时长：新属主 Claim 成功后目录里就有归属了，旧节点在他节点
//     租约 + QuiesceGrace（框架默认 TTL 10s + grace 5s，见 cluster.defaultLease）
//     之内再 Claim 本就会被 directory 拒抢（ACTOR_NOT_OWNER），隔离只需盖住无归属的前半段。
//
//  3. 不宜更长：窗口内回滚/回迁到旧属主同样被拒（编排器按 tick 重试，到期后自然恢复），
//     窗口越长越容易把正常的回迁、重启激活挡在门外。
const QuarantineWindow = 10 * time.Second

// Quarantiner 是旧属主节点的隔离能力：窗口内拒绝本节点对该 PID 的懒激活/重建。
// 生产实现是框架 cluster.Runtime.QuarantinePID（窗口到期自动失效，无需显式解除）。
// 未注入时迁移只做 drain/activate/verify，不安装隔离窗口（保持既有行为）。
type Quarantiner interface {
	// QuarantinePID 在**本节点**把 pid 加入隔离窗口；window 必须为正。
	QuarantinePID(pid types.PID, window time.Duration) error
}

// WithQuarantine 注入隔离能力（不注入时迁移不安装隔离窗口；见 Quarantiner）。
func (o *Ops) WithQuarantine(q Quarantiner) *Ops {
	o.quarantiner = q
	return o
}

// quarantineOldOwner 在旧属主 drain 成功后安装隔离窗口（窗口内该节点拒绝懒激活本 PID）。
// 隔离必须落在**执行了 Release 的那个节点**上，因此跨节点 drain（fromNode 非本机）不由
// 本编排器安装——本模板的迁移由当前属主节点的收件箱驱动（fromNode == o.node，见
// services/battle/internal/actor/migrate_inbox.go），该分支只作为显式边界告警，
// 不做一次无意义的跨节点调用（框架远端 drain 落点也未提供隔离控制面）。
// 安装失败不阻断迁移（drain 已完成），但必须告警：这正是双 actor 的空窗。
func (o *Ops) quarantineOldOwner(pid types.PID, fromNode string) {
	if o.quarantiner == nil {
		return
	}
	if fromNode != "" && fromNode != o.node {
		o.log.Warn("migrate: 跨节点 drain 未安装隔离窗口（需由旧属主节点自行安装）",
			"pid", pid.String(), "from", fromNode, "local", o.node)
		return
	}
	if err := o.quarantiner.QuarantinePID(pid, QuarantineWindow); err != nil {
		o.log.Warn("migrate: 安装隔离窗口失败，旧属主在归属空窗内可能懒激活同一 PID",
			"pid", pid.String(), "window", QuarantineWindow, "err", err)
		return
	}
	o.log.Info("migrate: 旧属主已进入隔离窗口（窗口内拒绝懒激活本 PID）",
		"pid", pid.String(), "node", o.node, "window", QuarantineWindow)
}
