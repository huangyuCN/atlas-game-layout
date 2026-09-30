package app

import (
	"encoding/base64"
	"fmt"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
)

// ticketKeyLen 是入场票据 AEAD 密钥的字节数（AES-256；与 contrib/edge/ticket 的
// keySize 一致，框架未导出该常量，故在此显式声明并在此处校验）。
const ticketKeyLen = 32

// newActorConfig 从 battle 配置装配战斗参数：默认战斗参数 + battle 段的出票三件与掉线窗口
// （ticket_key / ticket_ttl / edge_endpoints / offline_timeout）。
//
// 校验口径（缺一即启动失败，不提供默认值——「配了没生效」比启动失败更贵）：
//   - ticket_key 必须存在、base64 合法且解码后恰好 32 字节，错误可用
//     errors.Is(err, ticket.ErrKeySize) 判定；
//   - ticket_ttl 省略取 DefaultTicketTTL，显式值必须能被 time.ParseDuration 解析且为正；
//   - edge_endpoints 至少一面、面不重复、地址非空（见 edgeEndpointsOf）；
//   - offline_timeout 省略取 DefaultOfflineTimeout（15s），显式 0 表示关闭掉线判定，
//     负值/非法即失败（见 offlineTimeoutOf）。
func newActorConfig(cfg *conf.Bootstrap) (actor.Config, error) {
	out := actor.DefaultConfig()
	b := cfg.GetBattle()
	if b == nil {
		return out, fmt.Errorf("%w: 缺少 battle 配置段（ticket_key / edge_endpoints 必填）", ticket.ErrKeySize)
	}
	key, err := ticketKeyOf(b.GetTicketKey())
	if err != nil {
		return out, err
	}
	ttl, err := ticketTTLOf(b.GetTicketTtl())
	if err != nil {
		return out, err
	}
	offline, err := offlineTimeoutOf(b.GetOfflineTimeout())
	if err != nil {
		return out, err
	}
	endpoints, err := edgeEndpointsOf(b.GetEdgeEndpoints())
	if err != nil {
		return out, err
	}
	out.TicketKey = key
	out.TicketTTL = ttl
	out.OfflineTimeout = offline
	out.EdgeEndpoints = endpoints
	return out, nil
}

// ticketKeyOf 解析并校验票据密钥：必须存在、base64 合法且解码后恰好 32 字节，
// 错误可用 errors.Is(err, ticket.ErrKeySize) 判定。
func ticketKeyOf(raw string) ([]byte, error) {
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: battle.ticket_key 不是合法 base64: %v", ticket.ErrKeySize, err)
	}
	if len(key) != ticketKeyLen {
		return nil, fmt.Errorf("%w: battle.ticket_key 解码后 %d 字节", ticket.ErrKeySize, len(key))
	}
	return key, nil
}

// edgeEndpointsOf 校验接入层「传输面 → 地址」列表并拷贝成契约类型（回执直接用它，
// 与配置解耦）。强校验口径（缺一即启动失败）：
//   - 至少一面：一面都没有等于客户端无面可连（**不要求三面齐全**，只开 ws 也能启动）；
//   - 传输面不得未指定：未指定面 SDK 无法按面选地址；
//   - 地址不得为空：空地址会下发一张连不上的局；
//   - 同一面不得重复：重复即「两个地址都叫 ws」，SDK 只能瞎猜（比失败更难查）。
func edgeEndpointsOf(list []*battlev1.EdgeEndpoint) ([]*battlev1.EdgeEndpoint, error) {
	if len(list) == 0 {
		return nil, fmt.Errorf("app: battle.edge_endpoints 至少配置一面（ws/kcp/udp），否则客户端无面可连")
	}
	out := make([]*battlev1.EdgeEndpoint, 0, len(list))
	seen := make(map[battlev1.EdgeTransport]struct{}, len(list))
	for _, ep := range list {
		transport := ep.GetTransport()
		if transport == battlev1.EdgeTransport_EDGE_TRANSPORT_UNSPECIFIED {
			return nil, fmt.Errorf("app: battle.edge_endpoints 存在未指定传输面（transport 必填：ws/kcp/udp）")
		}
		if ep.GetAddress() == "" {
			return nil, fmt.Errorf("app: battle.edge_endpoints 的 %s 面地址不得为空", transport)
		}
		if _, dup := seen[transport]; dup {
			return nil, fmt.Errorf("app: battle.edge_endpoints 传输面重复: %s（同一面只能有一个地址）", transport)
		}
		seen[transport] = struct{}{}
		out = append(out, &battlev1.EdgeEndpoint{Transport: transport, Address: ep.GetAddress()})
	}
	return out, nil
}

// ticketTTLOf 解析出票有效期：空串取默认值，其余必须可解析且为正。
func ticketTTLOf(raw string) (time.Duration, error) {
	if raw == "" {
		return actor.DefaultTicketTTL, nil
	}
	ttl, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("app: battle.ticket_ttl 非法（time.ParseDuration 字符串，如 120s）: %w", err)
	}
	if ttl <= 0 {
		return 0, fmt.Errorf("app: battle.ticket_ttl 必须为正，实际 %s", raw)
	}
	return ttl, nil
}

// offlineTimeoutOf 解析掉线判定窗口（规格 §9.2）：空串取默认 15s；显式 0 表示关闭掉线判定
// （合法特例：此时不推导也不校验数据报面空闲超时）；负值或非法即启动失败。
func offlineTimeoutOf(raw string) (time.Duration, error) {
	if raw == "" {
		return actor.DefaultOfflineTimeout, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("app: battle.offline_timeout 非法（time.ParseDuration 字符串，如 15s）: %w", err)
	}
	if d < 0 {
		return 0, fmt.Errorf("app: battle.offline_timeout 不得为负，实际 %s（0 表示关闭掉线判定）", raw)
	}
	return d, nil
}
