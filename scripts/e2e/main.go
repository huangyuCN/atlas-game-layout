// e2e 客户端脚本：atlas-sdk-go 客户端 SDK 驱动「注册 → 登录 → 匹配 → 战斗 → 结算」闭环。
//
// 脚本自身拉起四服务（game/gateway/battle/matcher 进程内装配，监听随机端口），
// 业务链路经 SDK 驱动（会话 Session + Invoke + 注解生成的强类型 stub），
// **战斗帧走直连**：成局通知携带逐人签发的 battle_ticket，客户端凭票直连 battle 帧面
// （框架传输客户端 + 帧槽票据），帧广播/结束通知由直连推送到达（阶段 3 批次 5：
// 战斗帧不再经网关，网关也不再监听 KCP/UDP）。
// 中间件（etcd/redis/nats/mongo）需先起（make compose）。
//
// 形态（-mode）：
//   - dual（默认）：TCP 业务通道 + KCP 直连帧面
//   - single：WS 业务通道 + WS 直连帧面
//   - party：4 客户端组队 2v2（A 建队拉 B 整队入队，C/D solo）+ 取消路径
//   - fault：异常下线容错（排队中断连 → 会话过期联动取消 → 重登状态归零）
//   - kick：顶号与断线恢复（B 同账号新登录挤下 A → A 收 KickedNotify → B 凭 token Resume 免密恢复）
//   - freeze：会话续租/过期恢复（中断心跳 → 路由 TTL 到期 → 清理联动）
//   - direct：不经接入层直连 battle 帧端口（-battle-frame 指定端口、-transport 选 kcp|udp|ws），
//     并跑票的负例（无票/篡改/过期）；用于帧面与验票本身的白盒验证
//
// 匹配经 gateway 业务通道 op 入队（gateway → game PlayerActor → matcher）；
// 成局/失败由 gateway 主动推送（/game.v1.MatchStartedNotify / MatchFailedNotify），
// 客户端据开局通知里的本人票据直连帧面 JoinBattle、发送帧输入，
// 直至收到双方一致的战斗结束通知。
//
// 用法：make compose && go run ./scripts/e2e -mode dual
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	pkgetcd "github.com/huangyuCN/atlas-game-layout/pkg/etcd"
	pkglog "github.com/huangyuCN/atlas-game-layout/pkg/log"
	pkgmongo "github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	pkgnats "github.com/huangyuCN/atlas-game-layout/pkg/nats"
	"github.com/huangyuCN/atlas-game-layout/pkg/observability"
	pkgredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	battleassemble "github.com/huangyuCN/atlas-game-layout/services/battle/assemble"
	edgeassemble "github.com/huangyuCN/atlas-game-layout/services/edge/assemble"
	gameassemble "github.com/huangyuCN/atlas-game-layout/services/game/assemble"
	gwassemble "github.com/huangyuCN/atlas-game-layout/services/gateway/assemble"
	matcherassemble "github.com/huangyuCN/atlas-game-layout/services/matcher/assemble"
	"github.com/huangyuCN/atlas/namespace"
	"go.opentelemetry.io/otel"
)

// e2eNS 是本次运行独占的命名空间 token（形如 e2e-<纳秒>）：与常驻进程
// （runtime.namespace: test）隔离，也不会命中上一次运行残留的实例键（租约未过期）
// 导致注册冲突。路径形态的前缀（/atlas/services/<ns> 等）一律由框架 namespace.Derive 拼，
// 代码里不再出现路径字面量。
var e2eNS = fmt.Sprintf("e2e-%d", time.Now().UnixNano())

// e2eDerived 是本次运行的五面派生结果（注册键前缀 / actor subject / 业务 topic /
// redis 键 / etcd 目录全由它取），与四服务进程内装配（assemble → newBootstrap）同源。
var e2eDerived = mustDerive(e2eNS)

// mustDerive 派生本次运行的命名空间；token 由本脚本生成，非法即 panic（夹具兜底）。
func mustDerive(ns string) namespace.Derived {
	derived, err := namespace.Derive(ns)
	if err != nil {
		panic(err)
	}
	return derived
}

// battle 出票配置（阶段 3 批次 2C 起 battle 装配的必填项，缺失即启动失败）：
// 密钥为 base64 的 32 字节（与 test/e2e 同值），direct 形态用它验票；接入层面列表在本脚本
// 只作配置占位（direct 形态不经接入层，edge 形态由批次 3 的脚本使用）。
const (
	e2eTicketKey = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA="
	e2eTicketTTL = "120s"
)

// e2eEdgeEndpoints 是接入层「传输面 → 地址」列表（migrate 形态据此起接入层并拨它）。
// 端口刻意避开常驻部署的 7100/7101/7102：e2e 与常驻五服务常在同一台机器上并存。
var e2eEdgeEndpoints = []*battlev1.EdgeEndpoint{
	{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: "127.0.0.1:7300"},
	{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_KCP, Address: "127.0.0.1:7301"},
	{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_UDP, Address: "127.0.0.1:7302"},
}

// middlewareAddrs 是中间件地址集（默认与 deploy/docker-compose 端口约定一致）。
type middlewareAddrs struct {
	etcdEndpoints []string
	redisAddr     string
	natsURL       string
	mongoURI      string
	mongoDB       string
}

// addrs 是 e2e 客户端要用的监听地址集（进程内装配为随机端口）。
type addrs struct {
	tcp string // 网关业务通道（tcp host:port）
	ws  string // 网关业务通道（ws://host:port/ws 完整 URL）
	// battle 三个直连帧面（阶段 3 批次 5：战斗帧不经网关，客户端凭票直连这里）。
	battleKCP string
	battleUDP string
	battleWS  string
}

// stack 是脚本拉起的服务句柄（进程内装配）：基础四服务，
// migrate 形态额外含第二个 battle 节点与接入层（batB/edge 非空）。
type stack struct {
	gw    *gwassemble.Gateway
	bat   *battleassemble.Battle
	batB  *battleassemble.Battle        // 迁移目标节点（仅 migrate 形态）
	edge  *edgeassemble.Edge            // 接入层（仅 migrate 形态）
	stops []func(context.Context) error // 逆序停止（装配顺序的 LIFO）
}

// addrs 归集客户端要用的监听地址（网关业务通道 + battle 直连帧面）。
func (s *stack) addrs() addrs {
	return addrs{
		tcp: s.gw.TCPURL, ws: s.gw.WSURL,
		battleKCP: s.bat.KCPURL, battleUDP: s.bat.UDPURL, battleWS: s.bat.WSURL,
	}
}

// stop 逆序停止全部服务实例（幂等）。
func (s *stack) stop() {
	for i := len(s.stops) - 1; i >= 0; i-- {
		_ = s.stops[i](context.Background())
	}
	s.stops = nil
}

// startStack 拉起四服务进程内装配（game → gateway → battle → matcher），
// 任一失败即逆序回滚已启动实例。
func startStack(mw middlewareAddrs, o directOpts) (*stack, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var stops []func(context.Context) error
	rollback := func(what string, err error) (*stack, error) {
		for i := len(stops) - 1; i >= 0; i-- {
			_ = stops[i](context.Background())
		}
		return nil, fmt.Errorf("启动 %s 失败: %w", what, err)
	}

	game, err := gameassemble.New(ctx, gameassemble.Options{
		NodeID: "game-e2e", EtcdEndpoints: mw.etcdEndpoints, NatsURL: mw.natsURL,
		RedisAddrs: []string{mw.redisAddr}, MongoURI: mw.mongoURI, MongoDB: mw.mongoDB,
		Namespace: e2eNS,
	})
	if err != nil {
		return rollback("game", err)
	}
	stops = append(stops, game.Stop)

	gw, err := gwassemble.New(ctx, gwassemble.Options{
		ID: "e2e", EtcdEndpoints: mw.etcdEndpoints, NatsURL: mw.natsURL, RedisAddrs: []string{mw.redisAddr},
		Namespace: e2eNS,
	})
	if err != nil {
		return rollback("gateway", err)
	}
	stops = append(stops, gw.Stop)

	bat, err := battleassemble.New(ctx, battleassemble.Options{
		NodeID: "battle-e2e", EtcdEndpoints: mw.etcdEndpoints, NatsURL: mw.natsURL,
		MongoURI: mw.mongoURI, MongoDB: mw.mongoDB,
		Namespace: e2eNS,
		// 出票三件（批次 2 起必填）：密钥供帧槽验票，TTL 决定票的有效期。
		TicketKey: e2eTicketKey, TicketTTL: e2eTicketTTL, EdgeEndpoints: e2eEdgeEndpoints,
		// 直连形态把所选帧面固定到 -battle-frame（客户端据此直连）；
		// 其余形态 nil = 三个帧面各起随机端口（与 gRPC 面同口径）。
		FrameAddrs: frameAddrsFor(o),
		// 损伤形态用长赛道：结算必须落在断网窗口之后，否则结算推送会被丢包吃掉，
		// 「双方结算一致」就无从断言（详见 damageBattleCfg）。
		BattleCfg: battleCfgFor(o),
	})
	if err != nil {
		return rollback("battle", err)
	}
	stops = append(stops, bat.Stop)

	m, err := matcherassemble.New(ctx, matcherassemble.Options{
		NodeID: "matcher-e2e", EtcdEndpoints: mw.etcdEndpoints, NatsURL: mw.natsURL, RedisAddrs: []string{mw.redisAddr},
		Namespace: e2eNS,
	})
	if err != nil {
		return rollback("matcher", err)
	}
	stops = append(stops, m.Stop)

	// matcher 的实例由 atlas.App 自行注册（game 的撮合链路经服务发现寻址），
	// 无需装置手工注册。

	return &stack{gw: gw, bat: bat, stops: stops}, nil
}

// frameAddrsFor 返回 direct 形态的帧面覆盖：只开所选传输面并把地址固定为 -battle-frame；
// 其余形态返回 nil（三面随机端口，不参与本形态断言）。
func frameAddrsFor(o directOpts) *battleassemble.FrameAddrs {
	if o.mode != "direct" {
		return nil
	}
	switch directTransport(o.transport) {
	case directUDP:
		return &battleassemble.FrameAddrs{UDP: o.frameAddr}
	case directWS:
		return &battleassemble.FrameAddrs{WS: o.frameAddr}
	default:
		return &battleassemble.FrameAddrs{KCP: o.frameAddr}
	}
}

// damageBattleCfg 返回损伤形态的战斗参数：长赛道 + 高帧上限。
// 断网窗口内双向全丢，若结算发生在窗口内，结束推送必然丢失——本形态要断言的是
// 「损伤下战斗可继续并最终一致」，故把结算推到窗口之后（赛道 40 步 ≈ 4s > 断网 3s）。
func damageBattleCfg() *battleassemble.BattleConfig {
	cfg := battleassemble.DefaultBattleConfig()
	cfg.TrackLen = 40
	cfg.MaxFrames = 300
	return &cfg
}

// battleCfgFor 按形态返回战斗参数覆盖（仅损伤形态需要）。
func battleCfgFor(o directOpts) *battleassemble.BattleConfig {
	if o.mode == "damage" {
		return damageBattleCfg()
	}
	return nil
}

// probeMiddlewares 探测四中间件连通性（不可用给出明确指引）。
func probeMiddlewares(mw middlewareAddrs) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rc, err := pkgredis.NewClient(pkgredis.Options{Addrs: []string{mw.redisAddr}, Namespace: e2eNS})
	if err != nil {
		return fmt.Errorf("redis 构造失败: %w", err)
	}
	if err := rc.Ping(ctx); err != nil {
		_ = rc.Close()
		return fmt.Errorf("redis 不可用: %w", err)
	}
	_ = rc.Close()

	nc, err := pkgnats.Connect(pkgnats.Options{URL: mw.natsURL, Name: "e2e-probe"})
	if err != nil {
		return fmt.Errorf("nats 不可用: %w", err)
	}
	nc.Close()

	ec, err := pkgetcd.NewClient(pkgetcd.Options{Endpoints: mw.etcdEndpoints})
	if err != nil {
		return fmt.Errorf("etcd 不可用: %w", err)
	}
	_ = ec.Close()

	mc, err := pkgmongo.NewClient(ctx, pkgmongo.Options{URI: mw.mongoURI, Database: mw.mongoDB})
	if err != nil {
		return fmt.Errorf("mongo 不可用: %w", err)
	}
	_ = mc.Close(ctx)
	return nil
}

// run 按形态执行闭环；返回错误则脚本非零退出。
func run(ctx context.Context, st *stack, mode string, a addrs, mw middlewareAddrs,
	o directOpts, frames uint64, mo migrateOpts) error {
	switch mode {
	case "dual", "single":
		return runClassic(ctx, mode, a, frames)
	case "direct":
		return runDirect(ctx, a, o, frames)
	case "party":
		return runParty(ctx, a, frames)
	case "fault":
		return runFault(ctx, a)
	case "freeze":
		return runDurability(ctx, a, mw, frames)
	case "kick":
		return runKick(ctx, a)
	case "migrate":
		return runMigrate(ctx, st, a, mo, frames)
	case "damage":
		return runDamage(ctx, a, o, mo, frames)
	default:
		return fmt.Errorf("未知形态 %q（dual|single|party|fault|kick|freeze|direct|migrate|damage）", mode)
	}
}

// runClassic 双端对战闭环：注册登录 → 入队撮合 → 成局 → 帧同步 → 结算一致。
func runClassic(ctx context.Context, mode string, a addrs, frames uint64) error {
	ps, err := connectPlayers(mode, a)
	if err != nil {
		return err
	}
	battleID, err := matchAndStart(ctx, ps)
	if err != nil {
		return err
	}
	if err := sendBattleInputs(ctx, ps, battleID, frames); err != nil {
		return err
	}
	return verifySettlement(ps)
}

// connectPlayers 按形态建立双客户端连接：single 走 WS 业务通道（战斗直连 WS 帧面），
// dual 走 TCP 业务通道（战斗直连 KCP 帧面）——阶段 3 批次 5 起战斗帧一律直连，
// 不再有"业务与战斗共用一条网关连接"的形态。
func connectPlayers(mode string, a addrs) (ps [2]*player, err error) {
	for i := range ps {
		if mode == "single" {
			ps[i], err = newSinglePlayer(a.ws, a.battleWS)
		} else {
			ps[i], err = newDualPlayer(a.tcp, a.battleKCP)
		}
		if err != nil {
			return ps, err
		}
	}
	return ps, nil
}

// matchAndStart 双玩家注册登录后入队，等待撮合成局并返回 battleID。
func matchAndStart(ctx context.Context, ps [2]*player) (string, error) {
	for i, p := range ps {
		if err := p.registerLogin(ctx, byte(i)); err != nil {
			return "", err
		}
	}
	fmt.Println("[匹配] 双玩家入队（等级相近）")
	if err := queueMatch(ctx, ps[0], ps[1]); err != nil {
		return "", err
	}
	battleID, err := waitStarted(ps[0], ps[1])
	if err != nil {
		return "", err
	}
	fmt.Printf("[开局] battle=%s\n", battleID)
	for _, p := range ps {
		if err := p.joinWithRetry(ctx, battleID); err != nil {
			return "", err
		}
	}
	return battleID, nil
}

// sendBattleInputs 双玩家向战斗发帧：A 每帧前进 1（率先到终点），B 原地不动。
func sendBattleInputs(ctx context.Context, ps [2]*player, battleID string, frames uint64) error {
	if err := ps[0].sendInputs(ctx, battleID, frames, 1); err != nil {
		return err
	}
	return ps[1].sendInputs(ctx, battleID, frames, 0)
}

// verifySettlement 校验双方结算一致：胜者相同且为 A。
func verifySettlement(ps [2]*player) error {
	winnerA, err := ps[0].waitEnd()
	if err != nil {
		return err
	}
	winnerB, err := ps[1].waitEnd()
	if err != nil {
		return err
	}
	fmt.Printf("[结算] 帧广播 A=%d B=%d\n", ps[0].frameCount(), ps[1].frameCount())
	if winnerA != ps[0].id || winnerB != ps[0].id {
		return fmt.Errorf("双方结局不一致: A=%q B=%q want %q", winnerA, winnerB, ps[0].id)
	}
	fmt.Printf("[结算] 胜者一致: %s\n", winnerA)
	return nil
}

func main() {
	// 可观测性：e2e 装置显式初始化（不读服务配置）——日志文件进 Loki（promtail 采集）、
	// span 经 OTLP 进 Tempo（127.0.0.1:4317，与 deploy/observability 的 compose 对齐），
	// 便于测试后在 Grafana 面板查询日志与链路。
	if err := pkglog.Init(pkglog.Options{Service: "e2e", Level: "info", File: "logs/e2e.log"}); err != nil {
		panic(err)
	}
	defer initTracing()() // 退出前 flush 未导出的 span
	mode := flag.String("mode", "dual", "客户端形态：dual（TCP+KCP）/ single（WS）/ party（组队 2v2）/ fault（异常下线联动）/ kick（顶号+断线恢复）/ direct（直连 battle 帧端口）/ migrate（两节点迁移）/ damage（网络损伤）")
	transportName := flag.String("transport", "kcp", "direct/damage 形态的直连传输：kcp|udp|ws（damage 固定 udp）")
	edgeAddr := flag.String("edge-addr", "127.0.0.1:7300", "migrate 形态的接入层 WS 面地址（与 battle 的 edge_endpoints 一致）")
	migrateTarget := flag.String("migrate-target", "battle-e2e-2", "migrate 形态的迁入节点 ID")
	preFrames := flag.Uint64("pre-frames", 3, "migrate 形态触发迁移前先发送的帧数")
	loss := flag.Int("loss", 0, "damage 形态：丢包百分比（0-100）")
	jitter := flag.Duration("jitter", 0, "damage 形态：抖动上限（如 20ms）")
	blackout := flag.Duration("blackout", 0, "damage 形态：断网时长（如 3s）")
	battleFrame := flag.String("battle-frame", "127.0.0.1:9401", "direct 形态的 battle 帧端口（host:port，battle 据此监听、脚本据此直连）")
	frames := flag.Uint64("frames", 20, "每客户端帧输入数")
	etcdFlag := flag.String("etcd", "127.0.0.1:12379", "etcd endpoints（逗号分隔）")
	redisFlag := flag.String("redis", "127.0.0.1:16379", "redis 地址")
	natsFlag := flag.String("nats", "nats://127.0.0.1:14222", "nats URL")
	mongoFlag := flag.String("mongo", "mongodb://127.0.0.1:27017", "mongo URI")
	mongoDBFlag := flag.String("mongo-db", "game_it", "mongo 数据库名")
	flag.Parse()

	mw := middlewareAddrs{
		etcdEndpoints: strings.Split(*etcdFlag, ","),
		redisAddr:     *redisFlag,
		natsURL:       *natsFlag,
		mongoURI:      *mongoFlag,
		mongoDB:       *mongoDBFlag,
	}
	if err := probeMiddlewares(mw); err != nil {
		fmt.Fprintf(os.Stderr, "e2e 失败: %v（请先 make compose 起中间件）\n", err)
		os.Exit(1)
	}
	o := directOpts{mode: *mode, transport: *transportName, frameAddr: *battleFrame}
	mo := migrateOpts{edgeAddr: *edgeAddr, targetNode: *migrateTarget,
		lossPercent: *loss, jitter: *jitter, blackout: *blackout, preFrames: *preFrames}
	st, err := startForMode(*mode, mw, o, mo)
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e 失败: %v\n", err)
		os.Exit(1)
	}
	defer st.stop()
	fmt.Printf("[环境] 四服务已就绪（gateway 业务面 tcp=%s ws=%s；battle 直连帧面 kcp=%s udp=%s ws=%s）\n",
		st.gw.TCPURL, st.gw.WSURL, st.bat.KCPURL, st.bat.UDPURL, st.bat.WSURL)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := run(ctx, st, *mode, st.addrs(), mw, o, *frames, mo); err != nil {
		fmt.Fprintf(os.Stderr, "e2e 失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("e2e 闭环通过（%s 形态）\n", *mode)
}

// initTracing 初始化 e2e 装置的链路导出并返回退出前调用的 flush 函数。
// 显式初始化（不读服务配置）：日志进 Loki、span 经 OTLP 进 Tempo，便于测试后在 Grafana 查询。
func initTracing() func() {
	shutdown, err := observability.InitTracing(context.Background(), observability.TracingOptions{
		Identity:    observability.ServiceIdentity{Name: "e2e", ID: "e2e-1", Env: "test"},
		Endpoint:    "grpc://127.0.0.1:4317",
		SampleRatio: 1, // 装置全量采集（库层零值 = 0 表示全丢，必须显式给）
	})
	if err != nil {
		panic(err)
	}
	// 探针 span：验证全局 provider 与 OTLP 导出通路（Tempo 面板应能查到）。
	_, probe := otel.Tracer("e2e-probe").Start(context.Background(), "e2e.probe")
	probe.End()
	return func() { _ = shutdown(context.Background()) }
}

// startForMode 按形态拉起服务栈：migrate 形态额外含第二个 battle 节点与接入层。
func startForMode(mode string, mw middlewareAddrs, o directOpts, mo migrateOpts) (*stack, error) {
	if mode == "migrate" {
		return startMigrateStack(context.Background(), mw, o, mo)
	}
	return startStack(mw, o)
}
