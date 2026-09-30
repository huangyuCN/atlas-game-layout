// stack.go 进程内装配压测所需的服务端（接入层 + game/gateway/battle/matcher），
// 返回压测客户端要用的地址集。全部服务在同一进程内起停，压测结束即回收。
package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	pkgetcd "github.com/huangyuCN/atlas-game-layout/pkg/etcd"
	pkgnats "github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkgredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	battleassemble "github.com/huangyuCN/atlas-game-layout/services/battle/assemble"
	edgeassemble "github.com/huangyuCN/atlas-game-layout/services/edge/assemble"
	gameassemble "github.com/huangyuCN/atlas-game-layout/services/game/assemble"
	gwassemble "github.com/huangyuCN/atlas-game-layout/services/gateway/assemble"
	matcherassemble "github.com/huangyuCN/atlas-game-layout/services/matcher/assemble"
	"github.com/huangyuCN/atlas/contrib/edge"
)

// 压测专用常量：票据密钥与 TTL 与 e2e 同口径（便于排障时对照）。
const (
	// ticketKey 是压测票据密钥（base64 的 32 字节；接入层与 battle 必须同值）。
	ticketKey = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA="
	// ticketTTL 是票据有效期：压测时长可能超过短 TTL，故取足够长的值。
	ticketTTL = "600s"
	// loadNamespacePrefix 是本次压测命名空间 token 的前缀（与常驻部署隔离开）。
	loadNamespacePrefix = "load"
)

// loadNS 是本次压测独占的命名空间 token：五面前缀（注册键/actor subject/topic/redis 键/etcd 目录）
// 全由它派生，故与常驻五服务、其它脚本互不可见。
var loadNS = fmt.Sprintf("%s-%d", loadNamespacePrefix, time.Now().UnixNano())

// middlewareAddrs 是中间件地址集（默认与 deploy/docker-compose 端口约定一致）。
type middlewareAddrs struct {
	etcdEndpoints []string
	redisAddr     string
	natsURL       string
	mongoURI      string
	mongoDB       string
}

// stack 是压测拉起的服务端句柄。
type stack struct {
	gw    *gwassemble.Gateway
	bat   *battleassemble.Battle
	edge  *edgeassemble.Edge
	stops []func(context.Context) error // 逆序停止（装配顺序的 LIFO）
}

// stop 逆序停止全部服务实例（幂等）。
func (s *stack) stop() {
	for i := len(s.stops) - 1; i >= 0; i-- {
		_ = s.stops[i](context.Background())
	}
	s.stops = nil
}

// proxy 返回接入层本体（读活跃流数等运行态）。
func (s *stack) proxy() *edge.Proxy { return s.edge.Proxy }

// listenerAddr 返回接入层第 i 个面的实际监听地址（面序与装配入参一致）。
func (s *stack) listenerAddr(i int) string {
	if s.edge == nil || i >= len(s.edge.ListenerAddrs) {
		return ""
	}
	return s.edge.ListenerAddrs[i]
}

// startStack 起接入层与四服务（接入层先起：battle 的出票配置要引用它的地址）。
func startStack(ctx context.Context, mw middlewareAddrs, edgeWS, edgeUDP string, limits battleLimits) (*stack, error) {
	key, err := base64.StdEncoding.DecodeString(ticketKey)
	if err != nil {
		return nil, fmt.Errorf("压测票据密钥不是合法 base64: %w", err)
	}
	st := &stack{}
	ed, err := startEdge(ctx, mw, key, edgeWS, edgeUDP)
	if err != nil {
		return nil, err
	}
	st.edge = ed
	st.stops = append(st.stops, ed.Stop)

	if err := st.startBase(ctx, mw, edgeEndpoints(edgeWS, edgeUDP), limits); err != nil {
		st.stop()
		return nil, err
	}
	return st, nil
}

// battleLimits 是压测用的对局参数覆盖。默认对局是「60 帧上限 + 5 步赛道」的示例竞速：
// 不覆盖的话每局约 6 秒即结算并**关闭该局全部直连**，压测后半程全是失败调用。
type battleLimits struct {
	maxFrames     uint64 // 帧数上限（压测取远大于时长内可产生的帧数）
	snapshotEvery uint64 // 快照周期（帧）；0 = 用默认
}

// battleCfg 把压测覆盖组装为装配参数（nil 表示不改）。
func (l battleLimits) battleCfg() *battleassemble.BattleConfig {
	if l.maxFrames == 0 {
		return nil
	}
	cfg := battleassemble.DefaultBattleConfig()
	cfg.MaxFrames = l.maxFrames
	if l.snapshotEvery > 0 {
		cfg.SnapshotEvery = l.snapshotEvery
	}
	return &cfg
}

// edgeEndpoints 构造 battle 配置里的「传输面 → 接入层地址」列表（出票时随成局通知下发）。
func edgeEndpoints(edgeWS, edgeUDP string) []*battlev1.EdgeEndpoint {
	return []*battlev1.EdgeEndpoint{
		{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: edgeWS},
		{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_UDP, Address: edgeUDP},
	}
}

// startEdge 起接入层（ws 走 TCP + 升级请求载体，udp 走数据报载体）。
func startEdge(ctx context.Context, mw middlewareAddrs, key []byte, edgeWS, edgeUDP string) (*edgeassemble.Edge, error) {
	ed, err := edgeassemble.New(ctx, edgeassemble.Options{
		Namespace: loadNS, EtcdEndpoints: mw.etcdEndpoints, TicketKey: key,
		Listeners: []edge.Listener{
			{Name: "ws", Network: edge.NetworkTCP, Address: edgeWS, Carrier: edge.CarrierWSUpgrade},
			{Name: "udp", Network: edge.NetworkUDP, Address: edgeUDP, Carrier: edge.CarrierDatagram},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("启动接入层失败（%s / %s 可能被占用）: %w", edgeWS, edgeUDP, err)
	}
	return ed, nil
}

// startBase 起 game/gateway/battle/matcher（battle 的出票配置指向接入层地址）。
func (s *stack) startBase(ctx context.Context, mw middlewareAddrs, endpoints []*battlev1.EdgeEndpoint, limits battleLimits) error {
	game, err := gameassemble.New(ctx, gameassemble.Options{
		NodeID: "game-load", EtcdEndpoints: mw.etcdEndpoints, NatsURL: mw.natsURL,
		RedisAddrs: []string{mw.redisAddr}, MongoURI: mw.mongoURI, MongoDB: mw.mongoDB,
		Namespace: loadNS,
	})
	if err != nil {
		return fmt.Errorf("启动 game 失败: %w", err)
	}
	s.stops = append(s.stops, game.Stop)

	gw, err := gwassemble.New(ctx, gwassemble.Options{
		ID: "load", EtcdEndpoints: mw.etcdEndpoints, NatsURL: mw.natsURL,
		RedisAddrs: []string{mw.redisAddr}, Namespace: loadNS,
	})
	if err != nil {
		return fmt.Errorf("启动 gateway 失败: %w", err)
	}
	s.gw = gw
	s.stops = append(s.stops, gw.Stop)

	bat, err := battleassemble.New(ctx, battleassemble.Options{
		NodeID: "battle-load", EtcdEndpoints: mw.etcdEndpoints, NatsURL: mw.natsURL,
		MongoURI: mw.mongoURI, MongoDB: mw.mongoDB, Namespace: loadNS,
		TicketKey: ticketKey, TicketTTL: ticketTTL, EdgeEndpoints: endpoints,
		BattleCfg: limits.battleCfg(),
	})
	if err != nil {
		return fmt.Errorf("启动 battle 失败: %w", err)
	}
	s.bat = bat
	s.stops = append(s.stops, bat.Stop)

	m, err := matcherassemble.New(ctx, matcherassemble.Options{
		NodeID: "matcher-load", EtcdEndpoints: mw.etcdEndpoints, NatsURL: mw.natsURL,
		RedisAddrs: []string{mw.redisAddr}, Namespace: loadNS,
	})
	if err != nil {
		return fmt.Errorf("启动 matcher 失败: %w", err)
	}
	s.stops = append(s.stops, m.Stop)
	return nil
}

// probeMiddlewares 探测中间件连通性（不可用给出明确指引，不静默降级）。
func probeMiddlewares(mw middlewareAddrs) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rc, err := pkgredis.NewClient(pkgredis.Options{Addrs: []string{mw.redisAddr}, Namespace: loadNS})
	if err != nil {
		return fmt.Errorf("redis 构造失败: %w", err)
	}
	if err := rc.Ping(ctx); err != nil {
		_ = rc.Close()
		return fmt.Errorf("redis 不可用: %w（请先起中间件，见 deploy/docker-compose）", err)
	}
	_ = rc.Close()

	nc, err := pkgnats.Connect(pkgnats.Options{URL: mw.natsURL, Name: "loadtest-probe"})
	if err != nil {
		return fmt.Errorf("nats 不可用: %w", err)
	}
	nc.Close()

	ec, err := pkgetcd.NewClient(pkgetcd.Options{Endpoints: mw.etcdEndpoints})
	if err != nil {
		return fmt.Errorf("etcd 不可用: %w", err)
	}
	_ = ec.Close()
	return nil
}

// splitAddrs 把逗号分隔的地址串拆成切片（忽略空白项）。
func splitAddrs(raw string) []string {
	var out []string
	for _, s := range strings.Split(raw, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}
