// Package consts 定义游戏模板的全局常量：服务名、actor 类型、主题与上下文键。
package consts

// 服务名（注册中心中的服务名）。
const (
	ServiceGateway = "gateway"
	ServiceGame    = "game"
	ServiceMatcher = "matcher"
	ServiceBattle  = "battle"
)

// Actor 类型名（集群懒激活的 group 名，PID 形如 <Type>:<Key>）。
const (
	ActorTypePlayer = "player"
	ActorTypeBattle = "battle"
)

// Nats 主题约定。
const (
	// TopicPush 玩家推送：atlas.push.<playerID>。
	TopicPush = "atlas.push."
	// TopicEvent 业务事件：atlas.event.<kind>。
	TopicEvent = "atlas.event."
	// TopicGatewayControl gateway 实例控制通道：atlas.gw.<instanceID>。
	TopicGatewayControl = "atlas.gw."
)

// 上下文键（日志基础字段注入使用，见 pkg/middleware 与 pkg/log）。
const (
	CtxKeyPlayerID  = "player_id"
	CtxKeySessionID = "session_id"
	CtxKeyBattleID  = "battle_id"
)

// 服务端推送 operation（消息 protobuf 完整名，客户端 OnNotify 按此分发；
// gateway 下发与 battle 上行通知共用同一套约定，ADR-0002）。
const (
	PushOpKickedOffline  = "gateway.v1.KickedNotify"
	PushOpMatchStarted   = "gateway.v1.MatchStartedNotify"
	PushOpFrameBroadcast = "gateway.v1.FrameBroadcast"
	PushOpBattleEnd      = "gateway.v1.BattleEndNotify"
)

// MatchStartedTopic 返回成局事件主题：atlas.event.match.started。
func MatchStartedTopic() string { return TopicEvent + "match.started" }

// MatchFailedTopic 返回失败事件主题：atlas.event.match.failed。
func MatchFailedTopic() string { return TopicEvent + "match.failed" }

// PushTopic 拼接玩家推送主题：atlas.push.<playerID>。
func PushTopic(playerID string) string { return TopicPush + playerID }

// EventTopic 拼接业务事件主题：atlas.event.<kind>。
func EventTopic(kind string) string { return TopicEvent + kind }

// GatewayTopic 拼接 Gateway 控制主题：atlas.gw.<instanceID>。
func GatewayTopic(instanceID string) string { return TopicGatewayControl + instanceID }
