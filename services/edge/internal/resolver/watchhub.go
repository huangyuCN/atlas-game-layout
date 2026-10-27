package resolver

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/locator"
)

// DefaultWatchIdleTTL 是共享目录监听的空闲回收时长：某 PID 类型在进程内一个消费者都不剩时开始
// 计时，超过即关掉底层 Watch（下次订阅按需重建，重建会触发后端全量扫描——可接受）。
// 注意它回收的是「整个类型前缀的监听」，不是每局订阅：每局条目在消费者归零时立即丢弃。
const DefaultWatchIdleTTL = time.Minute

// errDirectoryClosed 表示目录适配器已停机：不再接受新订阅。
var errDirectoryClosed = errors.New("resolver: 目录适配器已关闭")

// watchHub 是进程内唯一的目录监听扇出器：每个 PID 类型只建一条前缀级 Watch（如 battle:），
// 把事件按 key 分发给订阅了该 battle_id 的消费者。
//
// 为什么必须共享：Redis 后端的 locator.Watch 每次建立都会先全量 SCAN（启动时显式 rescan 一次，
// 订阅确认再 rescan 一次；重订阅同理）。旧实现「每局一条 watch」在 4096 人（约 410 局）规模下，
// 解析器与拆流守卫各建一条 ⇒ 820 条 watch ⇒ 约 1640 次全键空间 SCAN（还随接入层实例数倍增）。
// 共享后只剩 1 条 watch（启动 2 次 SCAN，与对局数无关）；消费者的注册/注销只增删内存条目，
// 一次 SCAN 都不触发。
type watchHub struct {
	loc     locator.Locator
	idleTTL time.Duration

	ctx    context.Context // 全部共享监听的父 ctx（Close 取消）
	cancel context.CancelFunc

	mu      sync.Mutex
	streams map[string]*hubStream // PID 类型前缀 → 前缀级监听
	seq     uint64                // 订阅 ID 发号器
	closed  bool
	wg      sync.WaitGroup
}

// hubStream 是一个类型前缀上的共享监听与它服务着的对局条目。
type hubStream struct {
	prefix string
	topics map[string]*hubTopic // 完整 key（type:uid）→ 该局的消费者集合
	stop   func()               // 底层 Watch 的停止函数（nil = 尚未建立/已终止）
	idle   *time.Timer          // 无消费者时的空闲关闭计时（nil = 不在计时）
	gen    uint64               // 代次：重建后让上一代 goroutine 的迟到事件失效
}

// hubTopic 是一个 battle_id 的消费者集合与最近一次属主快照（受 watchHub.mu 保护）。
type hubTopic struct {
	// deliver 串行化「事件投递」与「新订阅补快照」，保证消费者看到的属主不倒退。
	// 锁序：deliver → watchHub.mu；投递路径先释放 mu 再拿 deliver，不会成环。
	deliver sync.Mutex
	subs    map[uint64]func(owner string)
	owner   string // 最近一次已知属主（known = false 时无意义）
	known   bool
}

// newWatchHub 构造扇出器；idleTTL 传 DefaultWatchIdleTTL（测试可注入短时长）。
func newWatchHub(loc locator.Locator, idleTTL time.Duration) *watchHub {
	ctx, cancel := context.WithCancel(context.Background())
	return &watchHub{
		loc: loc, idleTTL: idleTTL, ctx: ctx, cancel: cancel,
		streams: make(map[string]*hubStream),
	}
}

// subscribe 订阅一个 PID 的属主变化：同类型的所有订阅共享一条前缀级监听。
// onChange 可能在本函数返回前被**同步**调用一次（补当前快照）；返回的 cancel 幂等，
// 调用方 ctx 取消也会自动注销（与旧实现同口径）。
func (h *watchHub) subscribe(ctx context.Context, pid string, onChange func(owner string)) (func(), error) {
	parsed, err := types.ParsePID(pid)
	if err != nil {
		return nil, err
	}
	if onChange == nil {
		return nil, fmt.Errorf("resolver: 监听目录 %s 的回调不能为空", pid)
	}
	prefix, key := parsed.Type()+":", parsed.Key().String()
	id, err := h.attach(prefix, key, onChange)
	if err != nil {
		return nil, err
	}
	release := h.releaseFunc(prefix, key, id)
	if ctx == nil {
		return release, nil
	}
	stopCtx := context.AfterFunc(ctx, release)
	return func() { stopCtx(); release() }, nil
}

// releaseFunc 返回幂等的注销函数（显式 cancel 与 ctx 取消共用）。
func (h *watchHub) releaseFunc(prefix, key string, id uint64) func() {
	var once sync.Once
	return func() { once.Do(func() { h.unsubscribe(prefix, key, id) }) }
}

// attach 登记一个消费者并返回订阅 ID：若该局已有当前属主快照，登记后**同步**回调一次
// （新消费者不必等下一次事件）。事件投递与补快照用 topic.deliver 串行，避免旧快照覆盖新属主。
func (h *watchHub) attach(prefix, key string, onChange func(owner string)) (uint64, error) {
	for {
		h.mu.Lock()
		if h.closed {
			h.mu.Unlock()
			return 0, errDirectoryClosed
		}
		s, err := h.ensureStream(prefix)
		if err != nil {
			h.mu.Unlock()
			return 0, err
		}
		t := s.topics[key]
		if t == nil {
			t = &hubTopic{subs: make(map[uint64]func(string))}
			s.topics[key] = t
		}
		h.mu.Unlock()

		t.deliver.Lock()
		h.mu.Lock()
		if h.closed {
			h.mu.Unlock()
			t.deliver.Unlock()
			return 0, errDirectoryClosed
		}
		s = h.streams[prefix]
		if s == nil || s.topics[key] != t {
			h.mu.Unlock()
			t.deliver.Unlock()
			continue // 条目在等待期间被注销/重建：重新登记（快照以新条目为准）
		}
		h.seq++
		t.subs[h.seq] = onChange
		owner, known := t.owner, t.known
		h.mu.Unlock()
		if known {
			onChange(owner)
		}
		t.deliver.Unlock()
		return h.seq, nil
	}
}

// ensureStream 返回类型前缀的共享监听（必要时建立并启动消费 goroutine）。调用方持有 h.mu。
func (h *watchHub) ensureStream(prefix string) (*hubStream, error) {
	s := h.streams[prefix]
	if s == nil {
		s = &hubStream{prefix: prefix, topics: make(map[string]*hubTopic)}
		h.streams[prefix] = s
	}
	if s.stop == nil {
		stop, err := h.start(s)
		if err != nil {
			if len(s.topics) == 0 {
				delete(h.streams, prefix)
			}
			return nil, err
		}
		s.stop = stop
	}
	if s.idle != nil {
		s.idle.Stop()
		s.idle = nil
	}
	return s, nil
}

// start 建立唯一的前缀级监听并起消费 goroutine；返回的停止函数恰好一次。调用方持有 h.mu。
// 各 locator 实现的 Watch 都只做「建立」（读取在后台 goroutine），不会在锁内等 IO。
func (h *watchHub) start(s *hubStream) (func(), error) {
	ctx, cancel := context.WithCancel(h.ctx)
	watcher, err := h.loc.Watch(ctx, locator.WatchOptions{
		KeyPrefix:       s.prefix,
		LocationBuilder: types.NewLocationBuilder(),
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("resolver: 监听目录前缀 %q 失败: %w", s.prefix, err)
	}
	s.gen++
	gen := s.gen
	h.wg.Add(1)
	go h.consume(s, gen, watcher)
	var once sync.Once
	stop := func() { once.Do(func() { cancel(); _ = watcher.Stop() }) }
	return stop, nil
}

// consume 消费共享监听的事件并扇出；监听终止（Stop/ctx/后端断流）即退出。
func (h *watchHub) consume(s *hubStream, gen uint64, watcher locator.Watcher) {
	defer h.wg.Done()
	for {
		events, err := watcher.Next()
		if err != nil {
			h.streamEnded(s, gen)
			return
		}
		h.dispatch(s, gen, events)
	}
}

// dispatch 把一批目录事件按 key 扇出给订阅了该 battle_id 的消费者：只更新快照与拷贝回调在锁内，
// 回调在锁外串行投递（回调里注销订阅不会自锁）。
func (h *watchHub) dispatch(s *hubStream, gen uint64, events []locator.WatchEvent) {
	h.mu.Lock()
	if h.closed || h.streams[s.prefix] != s || s.gen != gen {
		h.mu.Unlock()
		return
	}
	batches := make([]hubBatch, 0, len(events))
	for _, ev := range events {
		if ev.Key == nil {
			continue
		}
		t := s.topics[ev.Key.String()]
		if t == nil {
			continue // 没人订阅该 battle：不缓存（条目只服务在册消费者）
		}
		owner := ownerOfLocation(ev.Loc)
		t.owner, t.known = owner, owner != ""
		subs := make([]func(string), 0, len(t.subs))
		for _, fn := range t.subs {
			subs = append(subs, fn)
		}
		batches = append(batches, hubBatch{topic: t, owner: owner, subs: subs})
	}
	h.mu.Unlock()
	for _, b := range batches {
		b.topic.deliver.Lock()
		for _, fn := range b.subs {
			fn(b.owner)
		}
		b.topic.deliver.Unlock()
	}
}

// hubBatch 是一次待投递的事件（事件已在锁内更新到条目快照）。
type hubBatch struct {
	topic *hubTopic
	owner string
	subs  []func(owner string)
}

// streamEnded 处理监听终止：允许下一次订阅重建，并丢弃上一代的快照知识——断流期间的变化未知，
// 新订阅不能拿旧值冒充当前快照（重建后的首轮事件就是后端的全量快照）。
func (h *watchHub) streamEnded(s *hubStream, gen uint64) {
	h.mu.Lock()
	if h.streams[s.prefix] != s || s.gen != gen {
		h.mu.Unlock()
		return
	}
	stop := s.stop
	s.stop = nil
	for _, t := range s.topics {
		t.owner, t.known = "", false
	}
	h.mu.Unlock()
	if stop != nil {
		stop()
	}
}

// unsubscribe 注销消费者：该局无人订阅即丢弃条目；整个类型无人订阅时开始空闲计时。
func (h *watchHub) unsubscribe(prefix, key string, id uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.streams[prefix]
	if s == nil {
		return
	}
	if t := s.topics[key]; t != nil {
		delete(t.subs, id)
		if len(t.subs) == 0 {
			delete(s.topics, key)
		}
	}
	if len(s.topics) == 0 && s.stop == nil {
		delete(h.streams, prefix) // 监听已终止且无条目：类型条目本身也没必要留着
		return
	}
	if len(s.topics) == 0 && s.idle == nil {
		s.idle = time.AfterFunc(h.idleTTL, func() { h.closeIdle(s) })
	}
}

// closeIdle 空闲超时：关掉共享监听并丢弃该类型的全部缓存（下次订阅按需重建，重建 SCAN 一次）。
func (h *watchHub) closeIdle(s *hubStream) {
	h.mu.Lock()
	if h.closed || h.streams[s.prefix] != s || len(s.topics) > 0 {
		h.mu.Unlock()
		return
	}
	stop := s.stop
	s.stop, s.idle = nil, nil
	s.gen++ // 让上一代消费 goroutine 的迟到事件失效
	delete(h.streams, s.prefix)
	h.mu.Unlock()
	if stop != nil {
		stop()
	}
}

// Close 停止全部共享监听并等待消费 goroutine 退出（幂等；不得在消费者回调里调用）。
func (h *watchHub) Close() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	stops := make([]func(), 0, len(h.streams))
	for prefix, s := range h.streams {
		if s.idle != nil {
			s.idle.Stop()
		}
		if s.stop != nil {
			stops = append(stops, s.stop)
		}
		s.stop, s.idle = nil, nil
		s.gen++
		delete(h.streams, prefix)
	}
	h.cancel()
	h.mu.Unlock()
	for _, stop := range stops {
		stop()
	}
	h.wg.Wait()
}

// topicCount 返回某类型前缀下在册的对局条目数（用例断言「消费者归零即丢弃条目」）。
func (h *watchHub) topicCount(prefix string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s := h.streams[prefix]; s != nil {
		return len(s.topics)
	}
	return 0
}
