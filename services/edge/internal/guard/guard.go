// Package guard 实现接入层的「属主变更即拆流」（规格 §8）：
// 每条已接通流按 battle_id 订阅 actor 目录的属主变化，目录说属主换人（或属主记录消失）
// 就通知接入层拆流——旧后端不再持有该局，留着流只会让客户端收不到帧广播。
//
// 分层：本包只做「订阅 + 扇出」，不碰 etcd；目录监听由注入的 OwnerWatcher 提供
// （实现见 resolver.LocatorDirectory.WatchOwner）。
package guard

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas/contrib/edge"
	"github.com/huangyuCN/atlas/log"
)

// OwnerWatcher 是属主变更监听端口：onChange 收到该 PID **当前**的属主节点（记录消失时为空串）。
// 实现方保证 onChange 可被监听 goroutine 反复调用；返回的 cancel 必须幂等。
type OwnerWatcher interface {
	WatchOwner(ctx context.Context, pid string, onChange func(owner string)) (cancel func(), err error)
}

// OwnerGuard 实现 edge.BackendGuard：按 battle_id 维护一条目录监听，属主变化即拆该局全部流。
type OwnerGuard struct {
	watcher   OwnerWatcher
	actorType string
	log       log.Logger
	ctx       context.Context
	cancel    context.CancelFunc

	mu      sync.Mutex
	battles map[string]*battleWatch
}

// battleWatch 是一局战斗的监听与订阅者集合。
type battleWatch struct {
	cancel func()
	subs   map[edge.StreamID]*subscriber
}

// subscriber 是一条流的拆流回调与它接通时的属主。
type subscriber struct {
	owner string
	evict func(edge.TeardownReason)
}

// New 构造属主守卫；watcher 必填（缺失即装配失败，不静默降级成「永不拆流」）。
func New(w OwnerWatcher, logger log.Logger) (*OwnerGuard, error) {
	if w == nil {
		return nil, errors.New("guard: 属主监听端口不能为空")
	}
	if logger == nil {
		logger = log.DefaultLogger
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &OwnerGuard{watcher: w, actorType: consts.ActorTypeBattle, log: logger,
		ctx: ctx, cancel: cancel, battles: make(map[string]*battleWatch)}, nil
}

// Close 停止全部目录监听（接入层停机时调用）。
func (g *OwnerGuard) Close() {
	g.cancel()
	g.mu.Lock()
	for _, bw := range g.battles {
		if bw.cancel != nil {
			bw.cancel()
		}
	}
	g.battles = make(map[string]*battleWatch)
	g.mu.Unlock()
}

// Register 实现 edge.BackendGuard：登记一条流并确保该局的目录监听已启动。
// 首次注册某局时才建监听（同一局的多条流共享一条 watch）。
func (g *OwnerGuard) Register(info edge.StreamInfo, evict func(edge.TeardownReason)) func() {
	if info.BattleID == "" || evict == nil {
		return func() {}
	}
	g.mu.Lock()
	bw := g.battles[info.BattleID]
	fresh := bw == nil
	if fresh {
		bw = &battleWatch{subs: make(map[edge.StreamID]*subscriber)}
		g.battles[info.BattleID] = bw
	}
	bw.subs[info.ID] = &subscriber{owner: info.Owner, evict: evict}
	g.mu.Unlock()
	if fresh {
		g.startWatch(info.BattleID)
	}
	var once sync.Once
	return func() { once.Do(func() { g.unregister(info.BattleID, info.ID) }) }
}

// startWatch 为一局启动目录监听（失败只记日志：拆流是增强能力，不该阻断接入层）。
func (g *OwnerGuard) startWatch(battleID string) {
	pid := fmt.Sprintf("%s:%s", g.actorType, battleID)
	cancel, err := g.watcher.WatchOwner(g.ctx, pid, func(owner string) {
		g.onOwnerChange(battleID, owner)
	})
	if err != nil {
		g.log.Warn("edge: 属主变更监听启动失败（该局不拆流）", "battle", battleID, "err", err)
		return
	}
	g.mu.Lock()
	if bw := g.battles[battleID]; bw != nil {
		bw.cancel = cancel
	} else {
		cancel() // 监听建立期间该局最后一条流已结束
	}
	g.mu.Unlock()
}

// onOwnerChange 处理目录属主变化：与每条流接通时的属主比对，不一致即拆该流。
// 记录消失（owner 为空）同样视为「旧后端不再是属主」——迁移的 drain 与 activate 之间
// 正是这个窗口，此时不拆流客户端会一直收不到帧广播（连接还挂在旧节点上）。
func (g *OwnerGuard) onOwnerChange(battleID, owner string) {
	g.mu.Lock()
	bw := g.battles[battleID]
	if bw == nil {
		g.mu.Unlock()
		return
	}
	targets := make([]*subscriber, 0, len(bw.subs))
	for _, sub := range bw.subs {
		if sub.owner != owner {
			targets = append(targets, sub)
		}
	}
	g.mu.Unlock()
	for _, sub := range targets {
		g.log.Info("edge: 属主变更，拆除该局直连流",
			"battle", battleID, "from", sub.owner, "to", owner)
		sub.evict(edge.TeardownOwnerChanged)
	}
}

// unregister 注销一条流；该局已无订阅者时停止监听并回收表项。
func (g *OwnerGuard) unregister(battleID string, id edge.StreamID) {
	g.mu.Lock()
	bw := g.battles[battleID]
	if bw == nil {
		g.mu.Unlock()
		return
	}
	delete(bw.subs, id)
	if len(bw.subs) > 0 {
		g.mu.Unlock()
		return
	}
	cancel := bw.cancel
	delete(g.battles, battleID)
	g.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}
