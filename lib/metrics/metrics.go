// Package metrics 定义游戏模板的业务指标名常量：
// 指标实现由 pkg/observability（OTel）装配，业务按名创建仪器。
package metrics

// 玩家与登录类指标。
const (
	// LoginTotal 登录次数（labels: result）。
	LoginTotal = "game_login_total"
	// OnlineGauge 在线玩家数。
	OnlineGauge = "game_online_players"
)

// 匹配类指标。
const (
	// MatchQueueGauge 匹配队列长度。
	MatchQueueGauge = "matcher_queue_length"
	// MatchTotal 成局次数（labels: ruleset）。
	MatchTotal = "matcher_match_total"
	// MatchDuration 匹配耗时（labels: ruleset）。
	MatchDuration = "matcher_match_duration_seconds"
)

// 战斗类指标。
const (
	// BattleTotal 开局次数。
	BattleTotal = "battle_start_total"
	// BattleFrameDelay 帧延迟（labels: transport）。
	BattleFrameDelay = "battle_frame_delay_seconds"
)
