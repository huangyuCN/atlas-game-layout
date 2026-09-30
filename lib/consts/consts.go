// Package consts 定义游戏模板的全局常量：服务名、actor 类型、主题与上下文键。
package consts

import (
	"fmt"

	"github.com/huangyuCN/atlas/namespace"
)

// 服务名（注册中心中的服务名）。
const (
	ServiceGateway = "gateway"
	ServiceGame    = "game"
	ServiceMatcher = "matcher"
	ServiceBattle  = "battle"
	// ServiceEdge 是接入层服务名（裸 L4 转发，规格 §2：成局后客户端直连它）。
	ServiceEdge = "edge"
	// ServiceBattleFrame 是 battle 额外注册的**帧面实例**服务名（规格 §2.1）：
	// 接入层 Resolver 先查 actor 目录得属主 node_id，再按该服务名选同节点的帧面实例与端口。
	ServiceBattleFrame = "battle-frame"
)

// 帧面实例的元数据契约（规格 §2.1「实例元数据含 node_id 与 kcp/udp/ws 端口」）：
// battle 注册 battle-frame 实例时写入，接入层 Resolver 读取；端口不写死、不进票据。
const (
	// FrameMetaNodeID 是帧面实例所属的 actor 节点 ID（与目录里的属主 node_id 比对）。
	FrameMetaNodeID = "node_id"
	// FrameMetaHost 是该节点对客户端可达的主机（可省略，缺省取实例端点里的主机）。
	FrameMetaHost = "host"
	// FrameMetaPortWS 是 WS 帧面端口。
	FrameMetaPortWS = "ws"
	// FrameMetaPortKCP 是 KCP 帧面端口。
	FrameMetaPortKCP = "kcp"
	// FrameMetaPortUDP 是 UDP 帧面端口。
	FrameMetaPortUDP = "udp"
	// ActorTypeBattle 是战斗 actor 的类型段：PID 形如 battle:<battle_id>。
	ActorTypeBattle = "battle"
	// ActorTypeBattleMigrate 是战斗迁移收件箱/编排 actor 的类型段：PID 形如
	// battlemigrate:<node_id>（每节点一个，负责预置待恢复状态并触发迁移，规格 §8）。
	// 收件箱必须**本机**：新属主的战斗 actor 在 OnStart 里向它取状态，
	// 且状态要在会话 actor 建立之前写进会话存储。
	ActorTypeBattleMigrate = "battlemigrate"
)

// 运行环境（runtime.env **仅作标签**：链路资源 deployment.environment.name 与指标 env 标签，
// 不参与任何前缀派生；命名空间的唯一来源是 runtime.namespace）。
const (
	// EnvDefault 是 runtime.env 未配置时的缺省环境名（仅标签缺省，见 pkg/bootstrap.fillRuntimeIdentity）。
	EnvDefault = "default"
)

// Topics 按命名空间构造业务 topic：前缀取 namespace.Derived.TopicPrefix（atlas.<ns>）。
// 两套共用同一 NATS 的部署若不隔离会串台——玩家推送互相下发、业务事件互相收到、
// 网关控制通道串号，且都是静默的（不报错，只是消息走错环境）。命名空间与注册中心
// 前缀、actor subject、redis 键**同源**（唯一来源是配置字段 runtime.namespace 经
// 框架 namespace.Derive 派生），本包只拼后缀、不再自行归一或兜底。
//
// topic 形态：
//   - 玩家推送：atlas.<ns>.push.<playerID>
//   - 业务事件：atlas.<ns>.event.<kind>
//   - 网关控制：atlas.<ns>.gw.<instanceID>
type Topics struct {
	ns     namespace.Namespace
	prefix string // = namespace.Derived.TopicPrefix（atlas.<ns>）
}

// NewTopics 构造 topic 构造器：入参必须是 namespace.Derive 的产物
// （零值即报错，R9 严格模式：不回落 default/env）。
func NewTopics(d namespace.Derived) (Topics, error) {
	if d.TopicPrefix == "" {
		return Topics{}, fmt.Errorf("%w: 业务 topic 命名空间缺失（必须显式配置 runtime.namespace）", namespace.ErrInvalid)
	}
	return Topics{ns: d.Namespace, prefix: d.TopicPrefix}, nil
}

// Namespace 返回命名空间 token（日志与断言用）。
func (t Topics) Namespace() string { return t.ns.String() }

// PushPrefix 返回玩家推送前缀（atlas.<ns>.push.）。
func (t Topics) PushPrefix() string { return t.prefix + ".push." }

// PushWildcard 返回玩家推送的全量订阅通配 subject（atlas.<ns>.push.>）。
func (t Topics) PushWildcard() string { return t.PushPrefix() + ">" }

// Push 拼接玩家推送 subject（atlas.<ns>.push.<playerID>）。
func (t Topics) Push(playerID string) string { return t.PushPrefix() + playerID }

// Event 拼接业务事件 subject（atlas.<ns>.event.<kind>）。
func (t Topics) Event(kind string) string { return t.prefix + ".event." + kind }

// MatchStarted 返回成局事件 subject（atlas.<ns>.event.match.started）。
func (t Topics) MatchStarted() string { return t.Event("match.started") }

// MatchFailed 返回撮合失败/超时事件 subject（atlas.<ns>.event.match.failed）。
func (t Topics) MatchFailed() string { return t.Event("match.failed") }

// PartyRoster 返回队伍名册变更事件 subject（atlas.<ns>.event.party.roster）。
func (t Topics) PartyRoster() string { return t.Event("party.roster") }

// GatewayControl 返回网关实例控制通道 subject（atlas.<ns>.gw.<instanceID>）。
func (t Topics) GatewayControl(instanceID string) string {
	return t.prefix + ".gw." + instanceID
}

// 上下文键（日志基础字段注入使用，见 pkg/middleware 与 pkg/log）。
const (
	CtxKeyPlayerID  = "player_id"
	CtxKeySessionID = "session_id"
	CtxKeyBattleID  = "battle_id"
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
