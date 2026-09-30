package app

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/config"
	"github.com/huangyuCN/atlas-game-layout/services/edge/internal/conf"
	"github.com/huangyuCN/atlas/contrib/edge"
	"github.com/huangyuCN/atlas/contrib/edge/ticket"
	"github.com/huangyuCN/atlas/metrics"
)

// ticketKeyLen 是入场票据 AEAD 密钥的字节数（AES-256；与 battle 侧校验口径一致）。
const ticketKeyLen = 32

// proxyConfig 把服务配置映射为接入层配置：密钥解码、监听面、限流与探活参数。
//
// 校验口径（缺一即启动失败，不提供默认值——「配了没生效」比启动失败更贵）：
//   - ticket_key 必须存在、base64 合法且解码后恰好 32 字节，错误可用
//     errors.Is(err, ticket.ErrKeySize) 判定（与 battle 侧同一口径，同值才能互通）；
//   - listeners 至少一个，且面名 / 网络 / 载体 / 地址齐备（面名要与 battle 帧面实例的
//     元数据端口键一致，否则 Resolver 选不到端口）；
//   - 时长字段走 config.ParseDuration（空串 = 交给框架缺省，非法值启动期报错）。
func proxyConfig(cfg *conf.Bootstrap, res edge.Resolver, collector metrics.Collector,
	guard edge.BackendGuard) (edge.Config, error) {
	if res == nil {
		return edge.Config{}, fmt.Errorf("app: 缺少接入层 Resolver（装配期依赖缺失）")
	}
	ec := cfg.GetEdge()
	key, err := ticketKeyOf(ec)
	if err != nil {
		return edge.Config{}, err
	}
	listeners, err := listenersOf(ec.GetListeners())
	if err != nil {
		return edge.Config{}, err
	}
	if len(listeners) == 0 {
		return edge.Config{}, fmt.Errorf("app: edge.listeners 不能为空（至少一个接入面：ws/kcp/udp）")
	}
	out := edge.Config{
		Listeners:   listeners,
		TicketKey:   key,
		Resolver:    res,
		Guard:       guard,
		Metrics:     collector,
		MaxStreams:  int(ec.GetMaxStreams()),
		NewConnRate: int(ec.GetNewConnRate()),
		MaxPerIP:    int(ec.GetMaxPerIp()),
	}
	if err := applyTimings(&out, ec); err != nil {
		return edge.Config{}, err
	}
	return out, nil
}

// ticketKeyOf 解码并校验 edge.ticket_key（缺失/非法/长度不符都归为 ticket.ErrKeySize）。
func ticketKeyOf(ec *conf.Edge) ([]byte, error) {
	if ec == nil {
		return nil, fmt.Errorf("%w: 缺少 edge 配置段（ticket_key 必填）", ticket.ErrKeySize)
	}
	key, err := base64.StdEncoding.DecodeString(ec.GetTicketKey())
	if err != nil {
		return nil, fmt.Errorf("%w: edge.ticket_key 不是合法 base64: %v", ticket.ErrKeySize, err)
	}
	if len(key) != ticketKeyLen {
		return nil, fmt.Errorf("%w: edge.ticket_key 解码后 %d 字节（必须 %d 字节，且与 battle.ticket_key 同值）",
			ticket.ErrKeySize, len(key), ticketKeyLen)
	}
	return key, nil
}

// listenersOf 映射全部接入面；一个都不能少（空列表由框架再校验一次）。
func listenersOf(list []*conf.Edge_Listener) ([]edge.Listener, error) {
	out := make([]edge.Listener, 0, len(list))
	for _, l := range list {
		face, err := listenerOf(l)
		if err != nil {
			return nil, err
		}
		out = append(out, face)
	}
	return out, nil
}

// listenerOf 映射单个接入面：网络与 hello 载体按枚举映射，未知取值即启动失败。
func listenerOf(l *conf.Edge_Listener) (edge.Listener, error) {
	if l.GetName() == "" {
		return edge.Listener{}, fmt.Errorf("app: edge.listeners[].name 不能为空（面名须为 ws/kcp/udp）")
	}
	if l.GetAddr() == "" {
		return edge.Listener{}, fmt.Errorf("app: edge.listeners[%s].addr 不能为空", l.GetName())
	}
	network, err := networkOf(l.GetNetwork())
	if err != nil {
		return edge.Listener{}, err
	}
	carrier, err := carrierOf(l.GetCarrier())
	if err != nil {
		return edge.Listener{}, err
	}
	return edge.Listener{Name: l.GetName(), Network: network, Address: l.GetAddr(), Carrier: carrier}, nil
}

// networkOf 把配置里的监听网络映射为框架网络名。
func networkOf(n conf.Edge_Network) (string, error) {
	switch n {
	case conf.Edge_NETWORK_TCP:
		return edge.NetworkTCP, nil
	case conf.Edge_NETWORK_UDP:
		return edge.NetworkUDP, nil
	default:
		return "", fmt.Errorf("app: edge.listeners[].network 未配置或不支持（枚举值 %d）", int32(n))
	}
}

// carrierOf 把配置里的 hello 载体映射为框架载体。
func carrierOf(c conf.Edge_Carrier) (edge.Carrier, error) {
	switch c {
	case conf.Edge_CARRIER_WS_UPGRADE:
		return edge.CarrierWSUpgrade, nil
	case conf.Edge_CARRIER_STREAM_HELLO:
		return edge.CarrierStreamHello, nil
	case conf.Edge_CARRIER_DATAGRAM:
		return edge.CarrierDatagram, nil
	default:
		return "", fmt.Errorf("app: edge.listeners[].carrier 未配置或不支持（枚举值 %d）", int32(c))
	}
}

// applyTimings 回填时长参数（空串交给框架缺省；非法值启动期报错）。
func applyTimings(out *edge.Config, ec *conf.Edge) error {
	var err error
	if out.IdleTimeout, err = durationOf(ec.GetIdleTimeout(), "edge.idle_timeout"); err != nil {
		return err
	}
	if out.DialTimeout, err = durationOf(ec.GetDialTimeout(), "edge.dial_timeout"); err != nil {
		return err
	}
	if out.Probe.After, err = durationOf(ec.GetProbeAfter(), "edge.probe_after"); err != nil {
		return err
	}
	if out.Probe.Timeout, err = durationOf(ec.GetProbeTimeout(), "edge.probe_timeout"); err != nil {
		return err
	}
	return nil
}

// durationOf 解析时长字段（空 = 0 交给框架缺省）。
func durationOf(raw, field string) (time.Duration, error) {
	d, err := config.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("app: %s 非法: %w", field, err)
	}
	return d, nil
}
