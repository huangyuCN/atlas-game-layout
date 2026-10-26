// 成局处置流水线：有界扇出 → 开局+出票（单跳 RPC）→ 对局关联 → 发布成局事件。
// 任一步失败都走**显式**失败处置（日志 + 打点 + 失败事件 + 回池），不静默丢局。

package handler

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/services/matcher/internal/biz"
	getlog "github.com/huangyuCN/atlas/log"
	"github.com/huangyuCN/atlas/matchmaker"
)

// metricAdmissionFailed 是入场失败计数（label stage 取 admit/publish/empty_ticket/gate_wait）。
const metricAdmissionFailed = "matcher_admission_failures_total"

// 预热口径的指标：失败次数与耗时（耗时按结果分 label，便于热/冷对比与回归观测）。
//
// 为什么耗时也要导出：预热的价值就是「把懒激活冷启动挪出关键路径」，
// 只有把预热自身的耗时与入场是否命中热态分开看，才能判断它是否真的省下了时间。
const (
	metricWarmupFailed = "matcher_battle_warmup_failures_total"
	metricWarmupMillis = "matcher_battle_warmup_millis"
)

// requeueExtKey 是「已回池次数」在票据扩展里的键：引擎把 StartRequest.Extensions 原样带进
// 新票，故同一票血统的重排次数可跨轮累计（无需新增存储）。
const requeueExtKey = "matcher_requeue"

// maxRequeue 是同一票血统的最大回池次数。为什么必须有上限：每次回池都是一张**新票**
// （带新的等待阈值），若 battle 持续不可用，「成局→失败→回池→再成局」会永不终止并
// 与有界并发闸门互相挤压；到上限即只发失败回执（客户端可显式重排）。
const maxRequeue = 2

// 入场失败的两个自造阶段错误（哨兵包装：指标维度按 errors.Is 判定，不靠字符串匹配）。
var (
	// errEmptyTickets 表示入场回执没有票（battle 侧异常）：空票绝不能下发给客户端。
	errEmptyTickets = errors.New("入场回执未带票")
	// errPublishStarted 表示成局事件发布失败（票已签发但客户端收不到，等同本局作废）。
	errPublishStarted = errors.New("成局事件发布失败")
)

// admitMatch 处置一局成局：有界并发内完成开局+出票与成局事件发布。
// 失败返回 false，并已由 onAdmissionFailed 完成回池、失败事件与打点。
func (h *MatcherHandler) admitMatch(ctx context.Context, ev matchmaker.TicketEvent, battleID string, playerIDs []string) bool {
	// 预热先于并发槽：懒激活的冷启动在排队等槽期间就能被消费掉，等槽结束后入场落在热态 actor 上。
	// 预热失败/超时只记日志与打点（见 warm），本局处置照旧。
	h.warm(ctx, battleID, ev.Match.ID, playerIDs)
	err := h.gate.do(ctx, func(ctx context.Context) error {
		return h.admitAndPublish(ctx, ev, battleID, playerIDs)
	})
	if err != nil {
		h.onAdmissionFailed(ev, battleID, err)
		return false
	}
	return true
}

// warm 异步预热目标对局的 battle actor（成局后、入场前，独立 goroutine）。
//
// 三条约束（缺一即成为新的故障源）：
//   - **不阻塞**：调用方不等待预热结果，入场不因预热慢/失败而延后或失败；
//   - **不继承取消**：成局处置的 ctx 会随 tick/请求结束被取消，预热用 WithoutCancel 续命，
//     再套自己的预算（缺省 biz.DefaultWarmupTimeout），避免「预热刚开始就被上游取消」；
//   - **可观测**：失败记日志 + 计数，耗时进直方图（按 result 分 label）。
func (h *MatcherHandler) warm(ctx context.Context, battleID, matchID string, playerIDs []string) {
	if h.warmer == nil || h.warmup.Disabled {
		return
	}
	budget := h.warmup.Timeout
	if budget <= 0 {
		budget = biz.DefaultWarmupTimeout
	}
	roster := append([]string(nil), playerIDs...)
	go func() {
		wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), budget)
		defer cancel()
		begin := time.Now()
		err := h.warmer.Warm(wctx, battleID, matchID, roster)
		elapsed := time.Since(begin)
		result := "ok"
		if err != nil {
			result = "error"
			h.meter.Counter(metricWarmupFailed).Add(1)
			getlog.GetLogger().Warn("matcher: 预热 battle actor 失败（不影响入场）",
				"battle", battleID, "match", matchID, "cost", elapsed.String(), "err", err)
		}
		h.meter.Histogram(metricWarmupMillis, "result", result).Observe(float64(elapsed.Milliseconds()))
	}()
}

// admitAndPublish 在并发槽内完成一次入场：开局+出票（一次 RPC）→ 对局关联 → 成局事件。
func (h *MatcherHandler) admitAndPublish(ctx context.Context, ev matchmaker.TicketEvent, battleID string, playerIDs []string) error {
	rpcCtx, cancel := context.WithTimeout(ctx, h.admissionTimeout())
	defer cancel()
	tickets, err := h.sink.Admit(rpcCtx, battleID, ev.Match.ID, playerIDs)
	if err != nil {
		return err
	}
	if len(tickets.GetTickets()) == 0 {
		return errEmptyTickets
	}
	h.linkPlayers(ctx, ev.Match.ID, battleID, playerIDs)
	if err := h.sink.PublishStarted(rpcCtx, battleID, ev.Match.ID, playerIDs, tickets); err != nil {
		// 票签了但事件没发出：客户端拿不到票，等同本局作废——关联也要撤掉，
		// 否则玩家会停在「关联到一局永远收不到推送的战斗」的假成局里。
		h.unlinkPlayers(ctx, playerIDs)
		return fmt.Errorf("%w: %w", errPublishStarted, err)
	}
	return nil
}

// onAdmissionFailed 处置入场失败（显式，不静默）：
// 日志 + 打点 → 逐票发失败事件（确定性回执）→ 逐票回池（玩家继续等下一轮成局）。
func (h *MatcherHandler) onAdmissionFailed(ev matchmaker.TicketEvent, battleID string, cause error) {
	getlog.GetLogger().Error("matcher: 开局+出票失败，本轮成局作废并回池",
		"battle", battleID, "match", ev.Match.ID, "err", cause)
	h.meter.Counter(metricAdmissionFailed, "stage", admissionStageOf(cause)).Add(1)
	for _, tk := range ev.Match.Tickets {
		_ = h.sink.PublishFailed(context.Background(), tk.ID, tk.PlayerIDs(),
			matcherv1.MatchFailReason_MATCH_FAIL_REASON_FAILED)
		h.requeue(tk)
	}
}

// admissionStageOf 把失败原因归到处置阶段（指标 label 取值有限，便于告警细分）。
func admissionStageOf(cause error) string {
	switch {
	case errors.Is(cause, ErrAdmissionBusy):
		return "gate_wait"
	case errors.Is(cause, errEmptyTickets):
		return "empty_ticket"
	case errors.Is(cause, errPublishStarted):
		return "publish"
	default:
		return "admit"
	}
}

// requeue 把一张已被消费的票按原名册重新入池：保留匹配属性、队伍归属与回池计数，
// 玩家→票映射跟随新票，并继续监听新票事件——玩家不会因一次入场失败就无声消失。
// 回池次数达上限（maxRequeue）后只留失败回执，不再重建票据（避免自激）。
func (h *MatcherHandler) requeue(tk matchmaker.Ticket) {
	ext := cloneExt(tk.Extensions)
	count := requeueCount(ext)
	if count >= maxRequeue {
		getlog.GetLogger().Error("matcher: 回池次数达上限，只发失败回执（不再重建票据）",
			"ticket", tk.ID, "requeue", count)
		return
	}
	ext[requeueExtKey] = strconv.Itoa(count + 1)
	ctx := context.Background()
	name := tk.Matchmaker
	if name == "" {
		name = biz.DefaultMatchmakerName
	}
	ticketID, err := h.svc.StartMatchmaking(ctx, matchmaker.StartRequest{
		Matchmaker: name,
		Players:    tk.Players,
		PartyID:    tk.PartyID,
		Extensions: ext,
	})
	if err != nil {
		getlog.GetLogger().Error("matcher: 入场失败后回池失败", "ticket", tk.ID, "matchmaker", name, "err", err)
		return
	}
	for _, playerID := range tk.PlayerIDs() {
		_ = h.mapper.Set(ctx, playerID, ticketID, ticketTTL)
	}
	if tk.PartyID != "" && h.partyMapper != nil {
		_ = h.partyMapper.SetPartyTicket(ctx, tk.PartyID, ticketID, ticketTTL)
	}
	go h.watchTicket(firstPlayerID(tk), ticketID, tk.PartyID)
}

// requeueCount 读取票据扩展里的回池次数（缺失/非法按 0）。
func requeueCount(ext map[string]string) int {
	n, err := strconv.Atoi(ext[requeueExtKey])
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// cloneExt 复制票据扩展（不修改引擎侧的票据对象）。
func cloneExt(ext map[string]string) map[string]string {
	out := make(map[string]string, len(ext)+1)
	for k, v := range ext {
		out[k] = v
	}
	return out
}

// firstPlayerID 返回票面首位玩家（单人票即本人；队伍票的失败推送名单按 partyID 取名册）。
func firstPlayerID(tk matchmaker.Ticket) string {
	if ids := tk.PlayerIDs(); len(ids) > 0 {
		return ids[0]
	}
	return ""
}

// linkPlayers 写入玩家→对局/战斗关联（开局推送丢失后，玩家重登可经查询接口恢复加入）。
func (h *MatcherHandler) linkPlayers(ctx context.Context, matchID, battleID string, playerIDs []string) {
	for _, playerID := range playerIDs {
		_ = h.mapper.SetMatch(ctx, playerID, matchID, ticketTTL)
		_ = h.mapper.SetBattle(ctx, playerID, battleID, ticketTTL)
	}
}

// unlinkPlayers 撤掉未成立的关联（成局事件没发出时，玩家不得停留在假成局里）。
func (h *MatcherHandler) unlinkPlayers(ctx context.Context, playerIDs []string) {
	for _, playerID := range playerIDs {
		_ = h.mapper.DelMatch(ctx, playerID)
		_ = h.mapper.DelBattle(ctx, playerID)
	}
}

// admissionTimeout 返回单局入场预算（零值取缺省，见 biz.DefaultAdmitTimeout）。
func (h *MatcherHandler) admissionTimeout() time.Duration {
	if h.admission.Timeout > 0 {
		return h.admission.Timeout
	}
	return biz.DefaultAdmitTimeout
}
