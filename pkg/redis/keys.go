package redis

import (
	"fmt"

	"github.com/huangyuCN/atlas/namespace"
)

// Keys 按命名空间构造业务键：前缀取 namespace.Derived.RedisKeyPrefix（atlas:<ns>:）。
// 共用同一 redis 的两套部署若不加命名空间，会话路由 / 玩家快照 / 撮合票据会互相覆盖
// ——静默串台（不报错，只是状态互相踩）。
//
// 命名空间与注册中心前缀、actor subject、业务 topic **同源**：唯一来源是配置字段
// runtime.namespace 经框架 namespace.Derive 派生（fxkit 构造 redis 客户端时写入
// Options.Namespace），本包只拼后缀、不再自行归一或兜底。
type Keys struct{ prefix string }

// NewKeys 构造键构造器：入参必须是 namespace.Derive 的产物
// （零值即报错，R9 严格模式：不回落任何缺省）。
func NewKeys(d namespace.Derived) (Keys, error) {
	if d.RedisKeyPrefix == "" {
		return Keys{}, fmt.Errorf("%w: redis 键命名空间缺失（必须显式配置 runtime.namespace）", namespace.ErrInvalid)
	}
	return Keys{prefix: d.RedisKeyPrefix}, nil
}

// base 返回键前缀：atlas:<ns>:
func (k Keys) base() string { return k.prefix }

// GatewayRoute 返回会话路由键（gateway 分布式路由表）。
func (k Keys) GatewayRoute(playerID string) string { return k.base() + "gw:" + playerID }

// PlayerSession 返回玩家会话令牌键。
func (k Keys) PlayerSession(playerID string) string { return k.base() + "session:" + playerID }

// PlayerSnapshot 返回玩家快照键（game 服务的 redis 持久化缓冲）。
func (k Keys) PlayerSnapshot(playerID string) string { return k.base() + "player:" + playerID }

// MatchPlayerTicket 返回玩家→ticket 映射键（matcher）。
func (k Keys) MatchPlayerTicket(playerID string) string { return k.base() + "match:player:" + playerID }

// MatchResult 返回玩家→match 映射键（matcher）。
func (k Keys) MatchResult(playerID string) string { return k.base() + "match:result:" + playerID }

// MatchBattle 返回玩家→battle 映射键（matcher）。
func (k Keys) MatchBattle(playerID string) string { return k.base() + "match:battle:" + playerID }

// MatchSettled 返回结算去重键（matcher，SETNX 占位）。
func (k Keys) MatchSettled(matchID string) string { return k.base() + "match:settled:" + matchID }

// MatchmakerPrefix 返回撮合后端（contrib/matchmaker/redis）的键前缀：
// 它自带一套键空间，同样必须按命名空间隔离，否则共用同一 redis 的两套部署会抢同一批票据。
func (k Keys) MatchmakerPrefix() string { return k.base() + "matchmaker:" }

// MatchParty 返回队伍→ticket 映射键（matcher）。
func (k Keys) MatchParty(partyID string) string { return k.base() + "match:party:" + partyID }
