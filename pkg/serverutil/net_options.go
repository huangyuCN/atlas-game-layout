package serverutil

import (
	"fmt"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/config"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	kcpt "github.com/huangyuCN/atlas/transport/kcp"
	tcpt "github.com/huangyuCN/atlas/transport/tcp"
	udpt "github.com/huangyuCN/atlas/transport/udp"
	wst "github.com/huangyuCN/atlas/transport/websocket"
)

// durationField 是一个时长配置字段及其选项构造器。
type durationField[T any] struct {
	field string
	value string
	mk    func(time.Duration) T
}

// intField 是一个数值配置字段及其选项构造器。
type intField[T any] struct {
	field string
	value int64
	mk    func(int) T
}

// durationOptions 批量解析时长字段：空 = 交给底层默认（不追加），非法或非正值报错。
func durationOptions[T any](fields []durationField[T], out []T) ([]T, error) {
	for _, f := range fields {
		d, err := config.ParseDuration(f.value)
		if err != nil {
			return nil, fmt.Errorf("serverutil: %s 无效: %w", f.field, err)
		}
		if d > 0 {
			out = append(out, f.mk(d))
		}
	}
	return out, nil
}

// intOptions 批量处理数值字段：0 = 交给底层默认（不追加），负数报错——
// 静默忽略负数等于「配了不生效」，与空值语义一样要避免。
func intOptions[T any](fields []intField[T], out []T) ([]T, error) {
	for _, f := range fields {
		if f.value < 0 {
			return nil, fmt.Errorf("serverutil: %s 不能为负（0 = 交给底层默认）", f.field)
		}
		if f.value > 0 {
			out = append(out, f.mk(int(f.value)))
		}
	}
	return out, nil
}

// pairOption 处理底层「一个选项吃两个值」的成对配置：
// 两侧都未配 = 不覆盖；只配一侧 = 配置错误（底层语义要求成对）。
func pairOption[T any](field string, a, b int64, mk func(int, int) T, out []T) ([]T, error) {
	if a == 0 && b == 0 {
		return out, nil
	}
	if a <= 0 || b <= 0 {
		return nil, fmt.Errorf("serverutil: %s 必须成对配置（两个值都需为正）", field)
	}
	return append(out, mk(int(a), int(b))), nil
}

// netServer 是客户端接入协议服务端的公共构造骨架：配置节缺失（absent）表示该协议未启用，
// 返回 nil 而不是错误——装配层按「nil = 未启用」决定不进启停组、不注册 handler；
// 其余情况映射选项后构造，错误统一包装。
func netServer[S any, O any](what string, absent bool, opts func() ([]O, error),
	ctor func(...O) (S, error)) (S, error) {
	var zero S
	if absent {
		return zero, nil
	}
	o, err := opts()
	if err != nil {
		return zero, err
	}
	return build(what, ctor, o...)
}

// TCPOptions 把 server.tcp 配置映射为 TCP 服务端选项（空值语义同 HTTPOptions）。
func TCPOptions(c *configspb.Server_TCP) ([]tcpt.ServerOption, error) {
	if c == nil {
		return nil, nil
	}
	opts := make([]tcpt.ServerOption, 0, 12)
	if c.GetAddr() != "" {
		opts = append(opts, tcpt.WithAddress(c.GetAddr()))
	}
	if c.NoDelay != nil {
		opts = append(opts, tcpt.WithNoDelay(c.GetNoDelay()))
	}
	var err error
	opts, err = intOptions([]intField[tcpt.ServerOption]{
		{"server.tcp.read_buffer", int64(c.GetReadBuffer()), tcpt.WithReadBuffer},
		{"server.tcp.write_buffer", int64(c.GetWriteBuffer()), tcpt.WithWriteBuffer},
		{"server.tcp.pool_size", int64(c.GetPoolSize()), tcpt.WithPoolSize},
		{"server.tcp.max_conns", int64(c.GetMaxConns()), tcpt.WithMaxConns},
		{"server.tcp.max_body_size", c.GetMaxBodySize(), func(n int) tcpt.ServerOption {
			return tcpt.WithMaxBodySize(n)
		}},
	}, opts)
	if err != nil {
		return nil, err
	}
	opts, err = durationOptions([]durationField[tcpt.ServerOption]{
		{"server.tcp.keep_alive", c.GetKeepAlive(), tcpt.WithKeepAlive},
		{"server.tcp.idle_timeout", c.GetIdleTimeout(), tcpt.WithIdleTimeout},
		{"server.tcp.write_timeout", c.GetWriteTimeout(), tcpt.WithWriteTimeout},
	}, opts)
	if err != nil {
		return nil, err
	}
	conf, err := TLSConfig(c.GetTls())
	if err != nil {
		return nil, err
	}
	if conf != nil {
		opts = append(opts, tcpt.WithTLSConfig(conf))
	}
	return opts, nil
}

// WSOptions 把 server.websocket 配置映射为 WebSocket 服务端选项。
func WSOptions(c *configspb.Server_WebSocket) ([]wst.ServerOption, error) {
	if c == nil {
		return nil, nil
	}
	opts := make([]wst.ServerOption, 0, 12)
	if c.GetAddr() != "" {
		opts = append(opts, wst.WithAddress(c.GetAddr()))
	}
	if c.GetPath() != "" {
		opts = append(opts, wst.WithPath(c.GetPath()))
	}
	// 底层读写缓冲按侧独立生效（各自 >0 才覆盖），故只配一侧也合法。
	if c.GetReadBuffer() < 0 || c.GetWriteBuffer() < 0 {
		return nil, fmt.Errorf("serverutil: server.websocket 的 read_buffer/write_buffer 不能为负（0 = 交给底层默认）")
	}
	if c.GetReadBuffer() > 0 || c.GetWriteBuffer() > 0 {
		opts = append(opts, wst.WithBufferSize(int(c.GetReadBuffer()), int(c.GetWriteBuffer())))
	}
	if len(c.GetSubprotocols()) > 0 {
		opts = append(opts, wst.WithSubprotocols(c.GetSubprotocols()...))
	}
	var err error
	opts, err = intOptions([]intField[wst.ServerOption]{
		{"server.websocket.pool_size", int64(c.GetPoolSize()), wst.WithPoolSize},
		{"server.websocket.read_limit", c.GetReadLimit(), func(n int) wst.ServerOption {
			return wst.WithReadLimit(int64(n))
		}},
		{"server.websocket.max_conns", int64(c.GetMaxConns()), wst.WithMaxConns},
		{"server.websocket.max_body_size", c.GetMaxBodySize(), func(n int) wst.ServerOption {
			return wst.WithMaxBodySize(n)
		}},
	}, opts)
	if err != nil {
		return nil, err
	}
	opts, err = durationOptions([]durationField[wst.ServerOption]{
		{"server.websocket.idle_timeout", c.GetIdleTimeout(), wst.WithIdleTimeout},
		{"server.websocket.write_timeout", c.GetWriteTimeout(), wst.WithWriteTimeout},
	}, opts)
	if err != nil {
		return nil, err
	}
	conf, err := TLSConfig(c.GetTls())
	if err != nil {
		return nil, err
	}
	if conf != nil {
		opts = append(opts, wst.WithTLSConfig(conf))
	}
	return opts, nil
}

// KCPOptions 把 server.kcp 配置映射为 KCP 服务端选项：
// 预设（fast/lan profile）先挂、显式参数随后追加——atlas 按选项顺序应用，同项以显式值为准。
func KCPOptions(c *configspb.Server_KCP) ([]kcpt.ServerOption, error) {
	if c == nil {
		return nil, nil
	}
	opts := make([]kcpt.ServerOption, 0, 12)
	if c.GetAddr() != "" {
		opts = append(opts, kcpt.WithAddress(c.GetAddr()))
	}
	if c.GetFastProfile() {
		opts = append(opts, kcpt.WithFastProfile())
	}
	if c.GetLanProfile() {
		opts = append(opts, kcpt.WithLANProfile())
	}
	if c.GetBlockCrypt() != "" {
		opts = append(opts, kcpt.WithBlockCrypt([]byte(c.GetBlockCrypt())))
	}
	var err error
	opts, err = pairOption("server.kcp.fec_data_shards/fec_parity_shards",
		int64(c.GetFecDataShards()), int64(c.GetFecParityShards()),
		func(a, b int) kcpt.ServerOption { return kcpt.WithFEC(a, b) }, opts)
	if err != nil {
		return nil, err
	}
	opts, err = pairOption("server.kcp.window_snd/window_rcv",
		int64(c.GetWindowSnd()), int64(c.GetWindowRcv()),
		func(a, b int) kcpt.ServerOption { return kcpt.WithWindowSize(a, b) }, opts)
	if err != nil {
		return nil, err
	}
	opts, err = intOptions([]intField[kcpt.ServerOption]{
		{"server.kcp.mtu", int64(c.GetMtu()), func(n int) kcpt.ServerOption { return kcpt.WithMTU(n) }},
		{"server.kcp.max_conns", int64(c.GetMaxConns()), kcpt.WithMaxConns},
		{"server.kcp.max_body_size", c.GetMaxBodySize(), func(n int) kcpt.ServerOption {
			return kcpt.WithMaxBodySize(n)
		}},
	}, opts)
	if err != nil {
		return nil, err
	}
	opts, err = durationOptions([]durationField[kcpt.ServerOption]{
		{"server.kcp.idle_timeout", c.GetIdleTimeout(), kcpt.WithIdleTimeout},
		{"server.kcp.write_timeout", c.GetWriteTimeout(), kcpt.WithWriteTimeout},
	}, opts)
	if err != nil {
		return nil, err
	}
	return opts, nil
}

// UDPOptions 把 server.udp 配置映射为 UDP 服务端选项。
func UDPOptions(c *configspb.Server_UDP) ([]udpt.ServerOption, error) {
	if c == nil {
		return nil, nil
	}
	opts := make([]udpt.ServerOption, 0, 6)
	if c.GetAddr() != "" {
		opts = append(opts, udpt.WithAddress(c.GetAddr()))
	}
	var err error
	opts, err = intOptions([]intField[udpt.ServerOption]{
		{"server.udp.pool_size", int64(c.GetPoolSize()), udpt.WithPoolSize},
		{"server.udp.read_buffer", int64(c.GetReadBuffer()), func(n int) udpt.ServerOption {
			return udpt.WithReadBuffer(n)
		}},
		{"server.udp.max_body_size", c.GetMaxBodySize(), func(n int) udpt.ServerOption {
			return udpt.WithMaxBodySize(n)
		}},
		{"server.udp.max_peers", int64(c.GetMaxPeers()), udpt.WithMaxPeers},
	}, opts)
	if err != nil {
		return nil, err
	}
	opts, err = durationOptions([]durationField[udpt.ServerOption]{
		{"server.udp.idle_timeout", c.GetIdleTimeout(), udpt.WithIdleTimeout},
	}, opts)
	if err != nil {
		return nil, err
	}
	return opts, nil
}

// TCPServer 按 server.tcp 配置构造 TCP 服务端（handler 注册由调用方完成）；
// 配置节缺失表示该协议未启用，返回 nil。
func TCPServer(c *configspb.Server_TCP) (*tcpt.Server, error) {
	return netServer("TCP 服务端", c == nil, func() ([]tcpt.ServerOption, error) {
		return TCPOptions(c)
	}, tcpt.NewServer)
}

// WSServer 按 server.websocket 配置构造 WebSocket 服务端（handler 注册由调用方完成）；
// 配置节缺失表示该协议未启用，返回 nil。
func WSServer(c *configspb.Server_WebSocket) (*wst.Server, error) {
	return netServer("WebSocket 服务端", c == nil, func() ([]wst.ServerOption, error) {
		return WSOptions(c)
	}, wst.NewServer)
}

// KCPServer 按 server.kcp 配置构造 KCP 服务端（handler 注册由调用方完成）；
// 配置节缺失表示该协议未启用，返回 nil。
func KCPServer(c *configspb.Server_KCP) (*kcpt.Server, error) {
	return netServer("KCP 服务端", c == nil, func() ([]kcpt.ServerOption, error) {
		return KCPOptions(c)
	}, kcpt.NewServer)
}

// UDPServer 按 server.udp 配置构造 UDP 服务端（handler 注册由调用方完成）；
// 配置节缺失表示该协议未启用，返回 nil。
func UDPServer(c *configspb.Server_UDP) (*udpt.Server, error) {
	return netServer("UDP 服务端", c == nil, func() ([]udpt.ServerOption, error) {
		return UDPOptions(c)
	}, udpt.NewServer)
}
