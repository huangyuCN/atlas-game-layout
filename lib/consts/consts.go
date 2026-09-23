// Package consts 定义游戏模板的全局常量：服务名、actor 类型、主题与上下文键。
package consts

// 服务名（注册中心中的服务名）。
const (
	ServiceGateway = "gateway"
	ServiceGame    = "game"
	ServiceMatcher = "matcher"
	ServiceBattle  = "battle"
)

// 运行环境（注册中心键前缀按环境隔离，见 pkg/registry.NamespaceOf）。
const (
	// EnvDefault 是 runtime.env 未配置时的缺省环境名。
	EnvDefault = "default"
)

// Actor 类型名（集群懒激活的 group 名，PID 形如 <Type>:<Key>）。
const (
	ActorTypePlayer = "player"
	ActorTypeBattle = "battle"
)

// Topics 按命名空间（通常取 runtime.env）构造业务 topic。
// 两套共用同一 NATS 的部署若不隔离会串台——玩家推送互相下发、业务事件互相收到、
// 网关控制通道串号，且都是静默的（不报错，只是消息走错环境）。命名空间与注册中心
// 前缀（pkg/registry.NamespaceOf）和 actor 平面（pkg/actor 的 NATS subject）同源。
//
// topic 形态：
//   - 玩家推送：atlas.<ns>.push.<playerID>
//   - 业务事件：atlas.<ns>.event.<kind>
//   - 网关控制：atlas.<ns>.gw.<instanceID>
type Topics struct{ ns string }

// NewTopics 构造 topic 构造器（ns 空 = EnvDefault）。
func NewTopics(ns string) Topics {
	if ns == "" {
		ns = EnvDefault
	}
	return Topics{ns: ns}
}

// Namespace 返回命名空间（日志与断言用）。
func (t Topics) Namespace() string { return t.ns }

// PushPrefix 返回玩家推送前缀（atlas.<ns>.push.）。
func (t Topics) PushPrefix() string { return "atlas." + t.ns + ".push." }

// PushWildcard 返回玩家推送的全量订阅通配 subject（atlas.<ns>.push.>）。
func (t Topics) PushWildcard() string { return t.PushPrefix() + ">" }

// Push 拼接玩家推送 subject（atlas.<ns>.push.<playerID>）。
func (t Topics) Push(playerID string) string { return t.PushPrefix() + playerID }

// Event 拼接业务事件 subject（atlas.<ns>.event.<kind>）。
func (t Topics) Event(kind string) string { return "atlas." + t.ns + ".event." + kind }

// MatchStarted 返回成局事件 subject（atlas.<ns>.event.match.started）。
func (t Topics) MatchStarted() string { return t.Event("match.started") }

// MatchFailed 返回撮合失败/超时事件 subject（atlas.<ns>.event.match.failed）。
func (t Topics) MatchFailed() string { return t.Event("match.failed") }

// PartyRoster 返回队伍名册变更事件 subject（atlas.<ns>.event.party.roster）。
func (t Topics) PartyRoster() string { return t.Event("party.roster") }

// GatewayControl 返回网关实例控制通道 subject（atlas.<ns>.gw.<instanceID>）。
func (t Topics) GatewayControl(instanceID string) string {
	return "atlas." + t.ns + ".gw." + instanceID
}

// HeaderKeyRequestID 是投递头中的客户端请求幂等键键名（gateway 透传注入，
// actor 日志经 ctx.Header 记录，与客户端 SDK 调试日志一一对应）。
const HeaderKeyRequestID = "x-atlas-request-id"

// 上下文键（日志基础字段注入使用，见 pkg/middleware 与 pkg/log）。
const (
	CtxKeyPlayerID  = "player_id"
	CtxKeySessionID = "session_id"
	CtxKeyBattleID  = "battle_id"
)

// 服务端推送 operation（消息 protobuf 完整名，客户端 OnNotify 按此分发；
// gateway 下发与 battle 上行通知共用同一套约定，ADR-0002）。
const (
	PushOpKickedOffline  = "/gateway.v1.KickedNotify"
	PushOpMatchStarted   = "/game.v1.MatchStartedNotify"
	PushOpMatchFailed    = "/game.v1.MatchFailedNotify"
	PushOpPartyRoster    = "/game.v1.PartyRosterNotify"
	PushOpFrameBroadcast = "/battle.v1.FrameBroadcast"
	PushOpBattleEnd      = "/battle.v1.BattleEndNotify"
)

// 链路追踪 instrumentation scope 名（span 归属的库标识，集中管理便于统一改名）。
const (
	// TracerNameActor 是 actor 运行时 span 的 scope 名（pkg/actor.DefaultTracer）。
	TracerNameActor = "atlas-actor"
	// TracerNameBiz 是 biz 层业务 span 的 scope 名（pkg/observability.StartSpan）。
	TracerNameBiz = "atlas-biz"
	// TracerNameTransport 是传输层中间件 span 的 scope 名（pkg/middleware 默认链）。
	TracerNameTransport = "atlas-transport"
	// MeterNameTransport 是传输层中间件指标的 scope 名（otel_scope_name 标签）。
	MeterNameTransport = "atlas-transport"
)
