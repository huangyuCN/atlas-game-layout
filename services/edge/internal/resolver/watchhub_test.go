package resolver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/services/edge/internal/testlocator"
	"github.com/huangyuCN/atlas/locator"
)

// watchTestIdleTTL 是短空闲回收时长：用例等它触发「空闲关 watcher」，不必等生产值（1 分钟）。
const watchTestIdleTTL = 20 * time.Millisecond

// newTestDirectory 构造带短空闲回收的目录适配器，用例结束停机（停掉共享监听）。
func newTestDirectory(t *testing.T, loc locator.Locator) *LocatorDirectory {
	t.Helper()
	dir := newLocatorDirectory(loc, watchTestIdleTTL)
	t.Cleanup(dir.Close)
	return dir
}

// TestDirectorySharesOneWatchAcrossBattles 覆盖「进程内单 watcher」：N 个 battle 订阅共用同一条
// 前缀级目录监听。Redis 后端的 Watch 每次建立（含重订阅）都会触发全量扫描，旧实现「每局一条
// watch」在 4096 人规模下会放大成 SCAN 风暴；本用例断言 100 个订阅只建 1 条监听——全量扫描
// 次数因此与对局数无关（旧实现 100 条 ⇒ 200 次 SCAN；新实现 1 条 ⇒ 启动时 2 次）。
func TestDirectorySharesOneWatchAcrossBattles(t *testing.T) {
	loc := &testlocator.Locator{}
	dir := newTestDirectory(t, loc)

	const battles = 100
	for i := 0; i < battles; i++ {
		pid := fmt.Sprintf("battle:b-%d", i)
		cancel, err := dir.WatchOwner(context.Background(), pid, func(string) {})
		if err != nil {
			t.Fatalf("WatchOwner(%s): %v", pid, err)
		}
		defer cancel()
	}
	if got := loc.Count(); got != 1 {
		t.Fatalf("%d 个 battle 订阅应只建 1 条目录监听（全量扫描次数与对局数无关），实际 %d 条（旧实现 %d 条）",
			battles, got, battles)
	}
	if got := loc.Prefix(0); got != "battle:" {
		t.Fatalf("共享监听应为 PID 类型前缀级（battle:），实际 %q", got)
	}
}

// waitForEvent 等一次属主回调（超时即失败），返回回调值。
func waitForEvent(t *testing.T, ch <-chan string, what string) string {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-time.After(testlocator.WaitTimeout):
		t.Fatalf("等待%s超时", what)
		return ""
	}
}

// requireNoEvent 断言一刻钟内没有额外回调（扇出不得串台、注销后不得再投递）。
func requireNoEvent(t *testing.T, ch <-chan string, what string) {
	t.Helper()
	select {
	case got := <-ch:
		t.Fatalf("%s不该收到回调 %q", what, got)
	case <-time.After(20 * time.Millisecond):
	}
}

// TestDirectoryFansOutEventsByKey 覆盖按 key 扇出：共享监听的同一批事件只到订阅了该 battle_id
// 的消费者（battle:b1 不收 battle:b10）；删除事件按「属主为空」投递。
func TestDirectoryFansOutEventsByKey(t *testing.T) {
	loc := &testlocator.Locator{}
	dir := newTestDirectory(t, loc)

	got1 := make(chan string, 4)
	got2 := make(chan string, 4)
	cancel1, err := dir.WatchOwner(context.Background(), "battle:b1", func(owner string) { got1 <- owner })
	if err != nil {
		t.Fatalf("订阅 battle:b1: %v", err)
	}
	defer cancel1()
	cancel2, err := dir.WatchOwner(context.Background(), "battle:b2", func(owner string) { got2 <- owner })
	if err != nil {
		t.Fatalf("订阅 battle:b2: %v", err)
	}
	defer cancel2()

	shared := loc.Watcher(0)
	shared.Emit(locEvent(t, "battle:b10", "node-x"), locEvent(t, "battle:b1", "node-1"),
		locEvent(t, "battle:b2", "node-2"))

	if got := waitForEvent(t, got1, "battle:b1 的属主"); got != "node-1" {
		t.Fatalf("battle:b1 应收到 node-1，实际 %q", got)
	}
	if got := waitForEvent(t, got2, "battle:b2 的属主"); got != "node-2" {
		t.Fatalf("battle:b2 应收到 node-2，实际 %q", got)
	}
	requireNoEvent(t, got1, "battle:b1 消费者")
	requireNoEvent(t, got2, "battle:b2 消费者")

	shared.Emit(locator.WatchEvent{Key: keyOf(t, "battle:b1")}) // 记录消失
	if got := waitForEvent(t, got1, "battle:b1 的删除事件"); got != "" {
		t.Fatalf("删除事件应回调空属主，实际 %q", got)
	}
	requireNoEvent(t, got2, "battle:b2 消费者")
}

// TestDirectoryReplaysCurrentOwnerToLateSubscriber 覆盖「新消费者补当前快照」：同一个 battle_id
// 的第二个消费者加入时先收到当前属主（不能等下一次事件才补），之后两者都跟随后续变化。
func TestDirectoryReplaysCurrentOwnerToLateSubscriber(t *testing.T) {
	loc := &testlocator.Locator{}
	dir := newTestDirectory(t, loc)

	got1 := make(chan string, 4)
	cancel1, err := dir.WatchOwner(context.Background(), "battle:b1", func(owner string) { got1 <- owner })
	if err != nil {
		t.Fatalf("首个订阅: %v", err)
	}
	defer cancel1()
	loc.Watcher(0).Emit(locEvent(t, "battle:b1", "node-1"))
	if got := waitForEvent(t, got1, "首个消费者的属主"); got != "node-1" {
		t.Fatalf("首个消费者应收到 node-1，实际 %q", got)
	}

	got2 := make(chan string, 4)
	cancel2, err := dir.WatchOwner(context.Background(), "battle:b1", func(owner string) { got2 <- owner })
	if err != nil {
		t.Fatalf("第二个订阅: %v", err)
	}
	defer cancel2()
	if got := waitForEvent(t, got2, "新订阅补的快照"); got != "node-1" {
		t.Fatalf("新订阅应先补当前属主 node-1，实际 %q", got)
	}

	loc.Watcher(0).Emit(locEvent(t, "battle:b1", "node-2"))
	for _, tc := range []struct {
		what string
		ch   <-chan string
	}{{"首个消费者", got1}, {"新订阅", got2}} {
		if got := waitForEvent(t, tc.ch, tc.what+"的属主变化"); got != "node-2" {
			t.Fatalf("%s应收到 node-2，实际 %q", tc.what, got)
		}
	}

	cancel1() // 注销一个消费者不影响另一个
	loc.Watcher(0).Emit(locEvent(t, "battle:b1", "node-3"))
	if got := waitForEvent(t, got2, "留存消费者的属主变化"); got != "node-3" {
		t.Fatalf("注销一个消费者后另一个应继续收到 node-3，实际 %q", got)
	}
	requireNoEvent(t, got1, "已注销的消费者")
}

// TestDirectoryReclaimsTopicsOnLastConsumer 覆盖引用计数：同一 battle 的消费者逐个退出时条目
// 保留到最后一人；最后一个消费者退出即丢弃该局条目（不同局互不影响）。
func TestDirectoryReclaimsTopicsOnLastConsumer(t *testing.T) {
	loc := &testlocator.Locator{}
	dir := newTestDirectory(t, loc)
	ctx := context.Background()
	noop := func(string) {}

	cancel1, err := dir.WatchOwner(ctx, "battle:b1", noop)
	if err != nil {
		t.Fatalf("订阅 battle:b1: %v", err)
	}
	cancel2, err := dir.WatchOwner(ctx, "battle:b2", noop)
	if err != nil {
		t.Fatalf("订阅 battle:b2: %v", err)
	}
	cancel3, err := dir.WatchOwner(ctx, "battle:b1", noop)
	if err != nil {
		t.Fatalf("重复订阅 battle:b1: %v", err)
	}
	if got := dir.hub.topicCount("battle:"); got != 2 {
		t.Fatalf("两个 battle 应只有 2 个条目，实际 %d", got)
	}
	if got := loc.Count(); got != 1 {
		t.Fatalf("三个消费者应共用 1 条监听，实际 %d", got)
	}

	cancel1() // battle:b1 还有第二个消费者：条目必须保留
	if got := dir.hub.topicCount("battle:"); got != 2 {
		t.Fatalf("同局仍有消费者时不得丢弃条目，实际 %d 个", got)
	}
	cancel3() // 最后一个 battle:b1 消费者退出：条目立即回收
	if got := dir.hub.topicCount("battle:"); got != 1 {
		t.Fatalf("battle:b1 消费者全退后应丢弃其条目（只剩 b2），实际 %d 个", got)
	}
	cancel2()
	if got := dir.hub.topicCount("battle:"); got != 0 {
		t.Fatalf("消费者全退后应丢弃全部条目，实际 %d 个", got)
	}
}

// TestDirectoryClosesIdleWatchAndRebuilds 覆盖空闲回收与按需重建：整类订阅归零且空闲超时后
// 关掉共享 watcher（goroutine 泄漏门）；再订阅时重建一条（重建的那一次 SCAN 可接受）。
func TestDirectoryClosesIdleWatchAndRebuilds(t *testing.T) {
	loc := &testlocator.Locator{}
	dir := newTestDirectory(t, loc)
	cancel, err := dir.WatchOwner(context.Background(), "battle:b1", func(string) {})
	if err != nil {
		t.Fatalf("订阅 battle:b1: %v", err)
	}
	cancel()

	if !loc.Watcher(0).Stopped(testlocator.WaitTimeout) {
		t.Fatal("空闲超时后未关闭共享 watcher：按监听泄漏 goroutine")
	}
	select {
	case <-loc.Ctx(0).Done():
	default:
		t.Fatal("空闲关闭未取消传给 locator.Watch 的 ctx")
	}
	if n := loc.Watcher(0).Stops(); n != 1 {
		t.Fatalf("共享 watcher 应恰好停止一次，实际 %d", n)
	}

	cancelRebuilt, err := dir.WatchOwner(context.Background(), "battle:b3", func(string) {}) // 按需重建
	if err != nil {
		t.Fatalf("空闲后重新订阅: %v", err)
	}
	defer cancelRebuilt()
	if got := loc.Count(); got != 2 {
		t.Fatalf("空闲关闭后新订阅应重建 1 条监听，实际累计 %d 条", got)
	}
}

// TestDirectoryReplaysFullSnapshotAfterIdleRebuild 覆盖「重建补快照」：共享 watcher 关闭期间
// 属主换人，新订阅触发的重建会先全量扫一次——消费者的首个事件必须是新快照（不得漏、不得补旧值）。
func TestDirectoryReplaysFullSnapshotAfterIdleRebuild(t *testing.T) {
	loc := &testlocator.Locator{}
	loc.SetSnapshot(locEvent(t, "battle:b1", "node-1"))
	dir := newTestDirectory(t, loc)

	got := make(chan string, 4)
	cancel, err := dir.WatchOwner(context.Background(), "battle:b1", func(owner string) { got <- owner })
	if err != nil {
		t.Fatalf("首个订阅: %v", err)
	}
	if first := waitForEvent(t, got, "首个快照"); first != "node-1" {
		t.Fatalf("首个监听应收到快照 node-1，实际 %q", first)
	}
	cancel()
	if !loc.Watcher(0).Stopped(testlocator.WaitTimeout) {
		t.Fatal("空闲超时后未关闭共享 watcher")
	}

	loc.SetSnapshot(locEvent(t, "battle:b1", "node-2")) // watcher 关闭期间属主换人
	rebuilt, err := dir.WatchOwner(context.Background(), "battle:b1", func(owner string) { got <- owner })
	if err != nil {
		t.Fatalf("重建后订阅: %v", err)
	}
	defer rebuilt()
	if first := waitForEvent(t, got, "重建后的首个快照"); first != "node-2" {
		t.Fatalf("重建后的首个事件应为新快照 node-2（不得漏），实际 %q", first)
	}
}
