// e2e 客户端脚本：atlas-sdk-go 客户端 SDK 驱动「注册 → 登录 → 匹配 → 战斗 → 结算」闭环。
//
// 脚本自身拉起四服务（game/gateway/battle/matcher 进程内装配，监听随机端口），
// 客户端全部经 SDK 驱动：会话（Session）+ Invoke + 注解生成的强类型 stub；
// 中间件（etcd/redis/nats/mongo）需先起（make compose）。
//
// 双形态（-mode）：
//   - dual（默认）：TCP 业务通道 + KCP 战斗通道（SDK dual 双通道编排）
//   - single：WS 单通道（业务与战斗共用一条连接）
//   - party：4 客户端组队 2v2（A 建队拉 B 整队入队，C/D solo）+ 取消路径
//   - fault：异常下线容错（排队中断连 → 会话过期联动取消 → 重登状态归零）
//   - kick：顶号与断线恢复（B 同账号新登录挤下 A → A 收 KickedNotify → B 凭 token Resume 免密恢复）
//
// 匹配经 gateway 业务通道 op 入队（gateway → game PlayerActor → matcher）；
// 成局/失败由 gateway 主动推送（/game.v1.MatchStartedNotify / MatchFailedNotify），
// 客户端据开局通知在战斗通道 JoinBattle、发送帧输入，直至收到双方一致的战斗结束通知。
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

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgetcd "github.com/huangyuCN/atlas-game-layout/pkg/etcd"
	pkglog "github.com/huangyuCN/atlas-game-layout/pkg/log"
	pkgmongo "github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	pkgnats "github.com/huangyuCN/atlas-game-layout/pkg/nats"
	"github.com/huangyuCN/atlas-game-layout/pkg/observability"
	pkgredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	pkgregistry "github.com/huangyuCN/atlas-game-layout/pkg/registry"
	battleassemble "github.com/huangyuCN/atlas-game-layout/services/battle/assemble"
	gameassemble "github.com/huangyuCN/atlas-game-layout/services/game/assemble"
	gwassemble "github.com/huangyuCN/atlas-game-layout/services/gateway/assemble"
	matcherassemble "github.com/huangyuCN/atlas-game-layout/services/matcher/assemble"
	atlasregistry "github.com/huangyuCN/atlas/registry"
	"go.opentelemetry.io/otel"
)

// middlewareAddrs 是中间件地址集（默认与 deploy/docker-compose 端口约定一致）。
type middlewareAddrs struct {
	etcdEndpoints []string
	redisAddr     string
	natsURL       string
	mongoURI      string
	mongoDB       string
}

// addrs 是 e2e 客户端要用的 gateway 监听地址集（进程内装配为随机端口）。
type addrs struct {
	tcp string // 业务通道（tcp host:port）
	kcp string // 战斗通道（kcp host:port）
	ws  string // 单通道形态（ws://host:port/ws 完整 URL）
}

// stack 是脚本拉起的四服务句柄（进程内装配）。
type stack struct {
	gw    *gwassemble.Gateway
	stops []func(context.Context) error // 逆序停止（装配顺序的 LIFO）
}

// addrs 归集 gateway 监听地址。
func (s *stack) addrs() addrs {
	return addrs{tcp: s.gw.TCPURL, kcp: s.gw.KCPURL, ws: s.gw.WSURL}
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
func startStack(mw middlewareAddrs) (*stack, error) {
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
	})
	if err != nil {
		return rollback("game", err)
	}
	stops = append(stops, game.Stop)

	gw, err := gwassemble.New(ctx, gwassemble.Options{
		ID: "e2e", EtcdEndpoints: mw.etcdEndpoints, NatsURL: mw.natsURL, RedisAddrs: []string{mw.redisAddr},
	})
	if err != nil {
		return rollback("gateway", err)
	}
	stops = append(stops, gw.Stop)

	bat, err := battleassemble.New(ctx, battleassemble.Options{
		NodeID: "battle-e2e", EtcdEndpoints: mw.etcdEndpoints, NatsURL: mw.natsURL,
		MongoURI: mw.mongoURI, MongoDB: mw.mongoDB,
	})
	if err != nil {
		return rollback("battle", err)
	}
	stops = append(stops, bat.Stop)

	m, err := matcherassemble.New(ctx, matcherassemble.Options{
		NodeID: "matcher-e2e", EtcdEndpoints: mw.etcdEndpoints, NatsURL: mw.natsURL, RedisAddrs: []string{mw.redisAddr},
	})
	if err != nil {
		return rollback("matcher", err)
	}
	stops = append(stops, m.Stop)

	// 进程内形态没有 atlas.App 的自动注册，而 game 的撮合链路经服务发现
	//（discovery:///matcher）寻址——e2e 装置承担生产 App.Run 的注册职责。
	dereg, err := registerMatcher(ctx, mw.etcdEndpoints, m.GRPCURL)
	if err != nil {
		return rollback("matcher 注册", err)
	}
	stops = append(stops, dereg) // LIFO：先注销再停 matcher

	return &stack{gw: gw, stops: stops}, nil
}

// registerMatcher 把进程内 matcher 的 grpc endpoint 注册到 etcd，
// 返回停止时执行的注销函数。
func registerMatcher(ctx context.Context, etcdEndpoints []string, grpcURL string) (func(context.Context) error, error) {
	ec, err := pkgetcd.NewClient(pkgetcd.Options{Endpoints: etcdEndpoints})
	if err != nil {
		return nil, fmt.Errorf("e2e: 构造 etcd 客户端失败: %w", err)
	}
	reg, err := pkgregistry.NewEtcd(ec, pkgregistry.Options{})
	if err != nil {
		_ = ec.Close()
		return nil, fmt.Errorf("e2e: 构造注册器失败: %w", err)
	}
	inst := &atlasregistry.ServiceInstance{
		ID:        "matcher-e2e",
		Name:      consts.ServiceMatcher,
		Version:   "v1",
		Metadata:  map[string]string{},
		Endpoints: []string{"grpc://" + grpcURL + "?isSecure=false"},
	}
	if err := reg.Register(ctx, inst); err != nil {
		return nil, fmt.Errorf("e2e: 注册 matcher 失败: %w", err)
	}
	return func(c context.Context) error { return reg.Deregister(c, inst) }, nil
}

// probeMiddlewares 探测四中间件连通性（不可用给出明确指引）。
func probeMiddlewares(mw middlewareAddrs) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rc, err := pkgredis.NewClient(pkgredis.Options{Addrs: []string{mw.redisAddr}})
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
func run(ctx context.Context, mode string, a addrs, mw middlewareAddrs, frames uint64) error {
	switch mode {
	case "dual", "single":
		return runClassic(ctx, mode, a, frames)
	case "party":
		return runParty(ctx, a, frames)
	case "fault":
		return runFault(ctx, a)
	case "freeze":
		return runDurability(ctx, a, mw, frames)
	case "kick":
		return runKick(ctx, a)
	default:
		return fmt.Errorf("未知形态 %q（dual|single|party|fault|kick|freeze）", mode)
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

// connectPlayers 按形态建立双客户端连接：single 走 WS 单通道，否则走 TCP+KCP 双通道。
func connectPlayers(mode string, a addrs) (ps [2]*player, err error) {
	for i := range ps {
		if mode == "single" {
			ps[i], err = newSinglePlayer(a.ws)
		} else {
			ps[i], err = newDualPlayer(a.tcp, a.kcp)
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
	fmt.Printf("[结算] 帧广播 A=%d B=%d\n", ps[0].frames.Load(), ps[1].frames.Load())
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
	shutdown, err := observability.InitTracing(context.Background(), observability.TracingOptions{
		Endpoint:    "grpc://127.0.0.1:4317",
		ServiceName: "e2e",
		ServiceID:   "e2e-1",
		Env:         "test",
		SampleRatio: 1, // 装置全量采集（库层零值 = 0 表示全丢，必须显式给）
	})
	if err != nil {
		panic(err)
	}
	// 探针 span：验证全局 provider 与 OTLP 导出通路（Tempo 面板应能查到）。
	probeCtx, probe := otel.Tracer("e2e-probe").Start(context.Background(), "e2e.probe")
	_ = probeCtx
	probe.End()
	defer func() { _ = shutdown(context.Background()) }() // 退出前 flush 未导出的 span
	mode := flag.String("mode", "dual", "客户端形态：dual（TCP+KCP）/ single（WS）/ party（组队 2v2）/ fault（异常下线联动）/ kick（顶号+断线恢复）")
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
	st, err := startStack(mw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e 失败: %v\n", err)
		os.Exit(1)
	}
	defer st.stop()
	fmt.Printf("[环境] 四服务已就绪（gateway tcp=%s kcp=%s ws=%s）\n", st.gw.TCPURL, st.gw.KCPURL, st.gw.WSURL)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := run(ctx, *mode, st.addrs(), mw, *frames); err != nil {
		fmt.Fprintf(os.Stderr, "e2e 失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("e2e 闭环通过（%s 形态）\n", *mode)
}
