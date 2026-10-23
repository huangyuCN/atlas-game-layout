package infra

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	matchredis "github.com/huangyuCN/atlas/contrib/matchmaker/redis"
	"github.com/huangyuCN/atlas/matchmaker"
	goredis "github.com/redis/go-redis/v9"
)

// TestMatchmakerRuntime_OneTickFormsMultipleMatches 端到端锁定「一个 tick 成多局」：
// 满池 200 张单人票 + fetch_size=100 + 规则集（10 人一局）⟹ 一个 tick 应成 ≥5 局
// （期望 10 局：认领 100 票 / 每局 10 票）。
//
// 口径是「**一个** tick」：tick 周期设 1s，只观测第一次 tick 的成局（第二次 tick 在 2s 之后），
// 这样「每 tick 只成 1 局」的实现会被判红，而「靠多跑几个 tick 凑够局数」不会被误判为通过。
func TestMatchmakerRuntime_OneTickFormsMultipleMatches(t *testing.T) {
	rt := newTickRuntime(t)
	events := enqueueTickets(t, rt, 200)
	if err := rt.Start(context.Background()); err != nil {
		t.Fatalf("启动 tick 循环失败: %v", err)
	}
	t.Cleanup(func() { _ = rt.Stop(context.Background()) })

	byTicket, spent := collectOneTick(t, events, 3*time.Second)
	assertOneTickMatches(t, byTicket, 5)
	t.Logf("一个 tick 成局覆盖 %d 张票（收齐耗时 %v）", len(byTicket), spent.Round(time.Millisecond))
}

// newTickRuntime 起 miniredis 并用**生产注册表**（fetch_size=100、tick 周期 1s）装配撮合运行时。
func newTickRuntime(t *testing.T) *matchredis.Runtime {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("启动 miniredis 失败: %v", err)
	}
	t.Cleanup(mr.Close)
	client := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	reg := MatchmakerRegistry(30*time.Second, time.Second, 100)
	rt, err := matchredis.NewRuntime(client, matchredis.Registry(reg), matchredis.WithPrefix("tick:"))
	if err != nil {
		t.Fatalf("装配撮合运行时失败: %v", err)
	}
	return rt
}

// enqueueTickets 入池 n 张单人票（等级一致，规则集必定可成局）并订阅各自的票事件。
func enqueueTickets(t *testing.T, rt *matchredis.Runtime, n int) <-chan matchmaker.TicketEvent {
	t.Helper()
	ctx := context.Background()
	events := make(chan matchmaker.TicketEvent, n)
	for i := 0; i < n; i++ {
		id, err := rt.Service.StartMatchmaking(ctx, matchmaker.StartRequest{
			Matchmaker: "casual",
			Players: []matchmaker.Player{{
				ID:         "p-" + strconv.Itoa(i),
				Attributes: map[string]matchmaker.Attribute{"level": {Type: matchmaker.AttributeNumber, Number: 1}},
			}},
		})
		if err != nil {
			t.Fatalf("入池第 %d 张票失败: %v", i, err)
		}
		ch, err := rt.Service.Watch(ctx, id)
		if err != nil {
			t.Fatalf("订阅第 %d 张票失败: %v", i, err)
		}
		go func() {
			for ev := range ch {
				events <- ev
			}
		}()
	}
	return events
}

// assertOneTickMatches 断言一个 tick 的成局覆盖：局数 ≥ wantMin、每局 10 票、票不重复不遗漏。
func assertOneTickMatches(t *testing.T, byTicket map[string]string, wantMin int) {
	t.Helper()
	matches := make(map[string]struct{}, len(byTicket))
	for ticketID, matchID := range byTicket {
		if matchID == "" {
			t.Fatalf("票 %s 的成局事件缺 match_id", ticketID)
		}
		matches[matchID] = struct{}{}
	}
	if len(matches) < wantMin {
		t.Fatalf("一个 tick 成局数 = %d，期望 ≥%d（200 票、fetch_size=100、每局 10 票 ⟹ 10 局）；"+
			"恒 1 局说明成型没有在余票上继续", len(matches), wantMin)
	}
	if len(byTicket) != len(matches)*10 {
		t.Fatalf("成局票数 = %d，期望 %d（%d 局 × 10 票，不重复配对）",
			len(byTicket), len(matches)*10, len(matches))
	}
}

// collectOneTick 收集一次 tick 的成局事件：先等第一条成局（最多 wait），
// 再多收 settle 时长把同一批事件收齐，返回「票 → match_id」与本次耗时。
func collectOneTick(t *testing.T, events <-chan matchmaker.TicketEvent, wait time.Duration) (map[string]string, time.Duration) {
	t.Helper()
	const settle = 300 * time.Millisecond
	byTicket := make(map[string]string, 128)
	start := time.Now()
	first := time.After(wait)
	var quiet <-chan time.Time
	for {
		select {
		case ev := <-events:
			if ev.State != matchmaker.TicketCompleted || ev.Match == nil {
				continue
			}
			if _, seen := byTicket[ev.TicketID]; seen {
				continue
			}
			byTicket[ev.TicketID] = ev.Match.ID
			if quiet == nil {
				first, quiet = nil, time.After(settle) // 已收到本 tick 事件：转入收齐窗口
			}
		case <-quiet:
			return byTicket, time.Since(start)
		case <-first:
			t.Fatalf("%v 内没有任何成局事件（撮合没跑起来？）", wait)
		}
	}
}
