// loadtest 是「接入层 + battle 帧面」的连接数与包速率压测客户端（阶段 3 批次 8 改写）。
//
// 与旧版的区别：旧版按「网关 KCP 战斗通道」驱动（dual/single 双通道），阶段 3 批次 5A
// 删除网关的战斗通道后**已不可用**。本版改为**直连驱动**：业务链路（注册/登录/入队/收成局
// 通知取票）仍走网关，战斗帧则经**接入层**直连——用框架 `contrib/edge` 的 hello/flow-id
// 原语 + 框架传输客户端（`transport/websocket`、`transport/frame/engine`）+ 帧槽票据；
// `-via direct` 时不经接入层、直连 battle 帧端口，作为容量对照基线。
//
// 形态（-transport × -via）：
//   - ws：框架 WS 客户端（经接入层时票据走升级请求头 X-Atlas-Ticket，逐帧会话槽带同一张票）
//   - udp：框架帧引擎客户端跑在「接入层数据报载体」上（首包 hello 换 flow-id，此后双向带前缀）
//   - direct：直连 battle 帧端口（不经接入层），同传输面同口径，用于对照转发开销
//
// 用法：
//
//	go run ./scripts/loadtest -players 100 -transport ws -duration 30s -pps 20
//	go run ./scripts/loadtest -players 100 -transport udp -via direct -duration 30s
//
// 输出：人读表格 + 末尾一行 JSON 汇总（bench 归档用，见 docs/superpowers/benchmarks 约定）。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	pkglog "github.com/huangyuCN/atlas-game-layout/pkg/log"
)

// report 是压测汇总（JSON 输出，字段名进归档日志，勿随意改名）。
type report struct {
	Via            string  `json:"via"`
	Transport      string  `json:"transport"`
	PlayersWant    int     `json:"players_requested"`
	PlayersMatched int     `json:"players_matched"`
	Battles        int     `json:"battles"`
	Connected      int     `json:"connections_established"`
	ConnectFailed  int     `json:"connections_failed"`
	DurationS      float64 `json:"duration_s"`
	Inputs         int64   `json:"inputs_sent"`
	InputPPS       float64 `json:"input_pps"`
	Broadcasts     int64   `json:"broadcasts_received"`
	BroadcastPPS   float64 `json:"broadcast_pps"`
	Failures       int64   `json:"frame_failures"`
	OutNotifies    int64   `json:"player_out_notifies"`
	EdgeStreams    int     `json:"edge_active_streams"`

	Handshake latency `json:"handshake_rtt_ms"`
	Probe     latency `json:"frame_rtt_ms"`
	Downlink  latency `json:"broadcast_downlink_ms"`

	// 分段口径（p99 归因用）：建流窗口 = 建连/准入/入局全程；稳态窗口 = 负载末段。
	// DialProbe/SteadyProbe 是同一批 SyncFrames 探针按打点时刻切开的两段。
	DialS       float64 `json:"dial_phase_s"`
	SteadyS     float64 `json:"steady_window_s"`
	DialProbe   latency `json:"dial_rtt_ms"`
	SteadyProbe latency `json:"steady_rtt_ms"`
	DialDown    latency `json:"dial_broadcast_downlink_ms"`
	SteadyDown  latency `json:"steady_broadcast_downlink_ms"`
}

// windows 是一轮压测的分段边界（建流窗口 = [dialStart, dialEnd]；稳态 = 负载末 steady 秒）。
type windows struct {
	dialStart time.Time
	dialEnd   time.Time
	pumpEnd   time.Time
	steady    time.Duration
}

// loadRun 是一次压测的装配与负载参数。
type loadRun struct {
	players        int
	dialConc       int
	connectTimeout time.Duration
	totalTimeout   time.Duration
	via            string
	transport      string
	steady         time.Duration
	edgeWS         string
	edgeUDP        string
	battleWS       string
	battleUDP      string
	limits         battleLimits
	pump           pumpCfg
}

func main() {
	cfg := parseFlags()
	if err := pkglog.Init(pkglog.Options{Service: "loadtest", Level: "info", File: "logs/loadtest.log"}); err != nil {
		fatalf("日志初始化失败: %v", err)
	}
	mw := middlewareAddrs{
		etcdEndpoints: splitAddrs(*etcdFlag), redisAddr: *redisFlag,
		natsURL: *natsFlag, mongoURI: *mongoFlag, mongoDB: *mongoDBFlag,
	}
	if err := probeMiddlewares(mw); err != nil {
		fatalf("%v（请先起中间件，见 deploy/docker-compose）", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.totalTimeout)
	defer cancel()
	st, err := startStack(ctx, mw, cfg.edgeWS, cfg.edgeUDP, cfg.limits)
	if err != nil {
		fatalf("%v", err)
	}
	defer st.stop()
	cfg.battleWS, cfg.battleUDP = st.bat.WSURL, st.bat.UDPURL
	fmt.Printf("[环境] 接入层 ws=%s udp=%s；battle 帧面 ws=%s udp=%s；网关业务面 tcp=%s\n",
		cfg.edgeWS, cfg.edgeUDP, cfg.battleWS, cfg.battleUDP, st.gw.TCPURL)

	rep, err := runLoad(ctx, st, cfg)
	if err != nil {
		fatalf("%v", err)
	}
	printReport(rep)
}

// parseFlags 解析旗标并组装压测参数。
func parseFlags() loadRun {
	players := flag.Int("players", 50, "并发玩家数（= 帧连接数；每 2 人一局）")
	transport := flag.String("transport", "ws", "帧传输面：ws|udp")
	via := flag.String("via", "edge", "驱动路径：edge（经接入层）| direct（直连 battle 帧端口，对照基线）")
	pps := flag.Int("pps", 20, "每连接每秒帧输入数（上行包速率）")
	probe := flag.Duration("probe", time.Second, "每连接 SyncFrames 探针间隔（往返延迟样本来源）")
	duration := flag.Duration("duration", 20*time.Second, "稳定负载时长")
	steady := flag.Duration("steady-window", 5*time.Second, "稳态窗口长度（取负载末段该时长单独给分位数）")
	probeJitter := flag.Bool("probe-jitter", false,
		"探针相位抖动：每连接的探针随机错开一个间隔（诊断用：512 条连接同刻齐发会自造排队，抬高 p99）")
	connectTimeout := flag.Duration("connect-timeout", 60*time.Second, "建连与入局阶段的总超时")
	conc := flag.Int("dial-concurrency", 32, "建连/注册的并发度（压太高会打爆注册链路）")
	edgeWS := flag.String("edge-ws", "127.0.0.1:7400", "接入层 WS 面监听地址")
	edgeUDP := flag.String("edge-udp", "127.0.0.1:7401", "接入层 UDP 面监听地址")
	total := flag.Duration("total-timeout", 10*time.Minute, "整轮压测的兜底超时")
	maxFrames := flag.Uint64("max-frames", 100000, "对局帧数上限覆盖（默认 60 会在 6 秒后结算并关闭直连，压测必须拉高）")
	snapEvery := flag.Uint64("snapshot-every", 1000, "对局快照周期覆盖（帧；0 = 用默认 10）")
	flag.Parse()
	if *transport != "ws" && *transport != "udp" {
		fatalf("未知 -transport %q（ws|udp）", *transport)
	}
	if *via != "edge" && *via != "direct" {
		fatalf("未知 -via %q（edge|direct）", *via)
	}
	return loadRun{
		players: *players, dialConc: *conc, connectTimeout: *connectTimeout, totalTimeout: *total,
		via: *via, transport: *transport, steady: *steady, edgeWS: *edgeWS, edgeUDP: *edgeUDP,
		limits: battleLimits{maxFrames: *maxFrames, snapshotEvery: *snapEvery},
		pump:   pumpCfg{pps: *pps, probe: *probe, duration: *duration, jitter: *probeJitter},
	}
}

// runLoad 执行一轮压测：建客户端 → 入队成局 → 建帧连接并入局 → 稳定负载 → 汇总。
func runLoad(ctx context.Context, st *stack, cfg loadRun) (*report, error) {
	ps, err := createPlayers(ctx, st.gw.TCPURL, cfg)
	if err != nil {
		return nil, err
	}
	defer closePlayers(ps)
	fmt.Printf("[客户端] 已注册登录 %d 个压测账号\n", len(ps))

	matched, battles, err := matchAll(ctx, ps, cfg.connectTimeout)
	if err != nil {
		return nil, err
	}
	fmt.Printf("[匹配] 成局 %d 局（收到成局通知的玩家 %d/%d）\n", battles, matched, len(ps))

	active := matchedPlayers(ps)
	dialStart := time.Now()
	connected, failed := dialAndJoin(ctx, active, cfg)
	dialEnd := time.Now()
	fmt.Printf("[直连] via=%s transport=%s 建连成功 %d / 失败 %d（建流窗口 %.1fs）\n",
		cfg.via, cfg.transport, connected, failed, dialEnd.Sub(dialStart).Seconds())
	if connected == 0 {
		return nil, fmt.Errorf("没有任何帧连接建立成功（via=%s transport=%s）", cfg.via, cfg.transport)
	}

	start := time.Now()
	pumpAll(ctx, active, cfg.pump)
	elapsed := time.Since(start)
	win := windows{dialStart: dialStart, dialEnd: dialEnd, pumpEnd: time.Now(), steady: cfg.steady}
	rep := collect(active, cfg, battles, failed, elapsed, win)
	rep.EdgeStreams = st.proxy().ActiveStreams()
	return rep, nil
}

// createPlayers 并发建立压测客户端（业务通道 + 会话 + 注册登录）。
func createPlayers(ctx context.Context, gatewayTCP string, cfg loadRun) ([]*loadPlayer, error) {
	ps := make([]*loadPlayer, cfg.players)
	err := parallelFor(cfg.players, cfg.dialConc, func(i int) error {
		p, err := newLoadPlayer(ctx, gatewayTCP, fmt.Sprintf("p%d", i))
		if err != nil {
			return err
		}
		ps[i] = p
		return nil
	})
	if err != nil {
		closePlayers(ps)
		return nil, err
	}
	return ps, nil
}

// matchAll 让全部玩家入队并等待成局通知，返回已匹配玩家数与局数。
func matchAll(ctx context.Context, ps []*loadPlayer, timeout time.Duration) (int, int, error) {
	for _, p := range ps {
		if err := p.enterQueue(ctx); err != nil {
			return 0, 0, err
		}
	}
	deadline := time.Now().Add(timeout)
	var wg sync.WaitGroup
	var mu sync.Mutex
	matched, failed := 0, 0
	for _, p := range ps {
		wg.Add(1)
		go func(p *loadPlayer) {
			defer wg.Done()
			left := time.Until(deadline)
			if left <= 0 {
				left = time.Second
			}
			if err := p.waitStarted(ctx, left); err != nil {
				mu.Lock()
				failed++
				mu.Unlock()
				return
			}
			mu.Lock()
			matched++
			mu.Unlock()
		}(p)
	}
	wg.Wait()
	if matched == 0 {
		return 0, 0, fmt.Errorf("无任何玩家收到成局通知（入队未匹配 / 出票未接线）")
	}
	return matched, matched / 2, nil
}

// matchedPlayers 返回已拿到票据的玩家（未匹配者不参与帧面负载）。
func matchedPlayers(ps []*loadPlayer) []*loadPlayer {
	out := make([]*loadPlayer, 0, len(ps))
	for _, p := range ps {
		if p.battleID != "" && len(p.ticket) > 0 {
			out = append(out, p)
		}
	}
	return out
}

// dialAndJoin 并发建帧连接并入局，返回成功与失败连接数。
func dialAndJoin(ctx context.Context, ps []*loadPlayer, cfg loadRun) (int, int) {
	o := dialOpts{
		via: cfg.via, transport: cfg.transport,
		edgeWS: cfg.edgeWS, edgeUDP: cfg.edgeUDP, battleWS: cfg.battleWS, battleUDP: cfg.battleUDP,
	}
	var mu sync.Mutex
	ok, bad := 0, 0
	_ = parallelFor(len(ps), cfg.dialConc, func(i int) error {
		p := ps[i]
		if err := p.dialFrame(ctx, o); err != nil {
			mu.Lock()
			bad++
			mu.Unlock()
			fmt.Fprintf(os.Stderr, "[直连] %v\n", err)
			return nil
		}
		if err := p.join(ctx); err != nil {
			mu.Lock()
			bad++
			mu.Unlock()
			fmt.Fprintf(os.Stderr, "[入局] %v\n", err)
			return nil
		}
		mu.Lock()
		ok++
		mu.Unlock()
		return nil
	})
	return ok, bad
}

// pumpAll 对已入局的连接施加稳定负载，直到 duration 到点。
func pumpAll(ctx context.Context, ps []*loadPlayer, cfg pumpCfg) {
	var wg sync.WaitGroup
	for _, p := range ps {
		if p.frame == nil {
			continue
		}
		wg.Add(1)
		go func(p *loadPlayer) {
			defer wg.Done()
			p.pump(ctx, cfg)
		}(p)
	}
	wg.Wait()
}

// collect 汇总全部连接的样本与计数，并把探针/下行样本切成「建流窗口 vs 稳态窗口」两段。
func collect(ps []*loadPlayer, cfg loadRun, battles, failed int, elapsed time.Duration, win windows) *report {
	rep := &report{
		Via: cfg.via, Transport: cfg.transport, PlayersWant: cfg.players,
		Battles: battles, ConnectFailed: failed,
		DurationS: elapsed.Seconds(), EdgeStreams: 0,
	}
	hs, pr, dl := &samples{}, &samples{}, &samples{}
	for _, p := range ps {
		if p.frame == nil {
			continue
		}
		rep.Connected++
		rep.PlayersMatched++
		rep.Inputs += p.inputs.Load()
		rep.Broadcasts += p.broadcasts.Load()
		rep.Failures += p.failures.Load()
		rep.OutNotifies += p.outs.Load()
		hs.merge(p.handshake)
		pr.merge(p.probe)
		dl.merge(p.downlink)
	}
	if secs := elapsed.Seconds(); secs > 0 {
		rep.InputPPS = float64(rep.Inputs) / secs
		rep.BroadcastPPS = float64(rep.Broadcasts) / secs
	}
	rep.Handshake = summarize(hs.sorted())
	rep.Probe = summarize(pr.sorted())
	rep.Downlink = summarize(dl.sorted())
	rep.DialS = win.dialEnd.Sub(win.dialStart).Seconds()
	rep.SteadyS = win.steady.Seconds()
	rep.DialProbe = summarize(pr.window(win.dialStart, win.dialEnd))
	rep.DialDown = summarize(dl.window(win.dialStart, win.dialEnd))
	steadyFrom := win.pumpEnd.Add(-win.steady)
	rep.SteadyProbe = summarize(pr.window(steadyFrom, win.pumpEnd))
	rep.SteadyDown = summarize(dl.window(steadyFrom, win.pumpEnd))
	return rep
}

// printReport 打印人读表格与末尾一行 JSON 汇总。
func printReport(r *report) {
	fmt.Printf("\n=== 压测结果（via=%s transport=%s）===\n", r.Via, r.Transport)
	fmt.Printf("连接：请求 %d 玩家 → 成局 %d 局，建连成功 %d，失败 %d；负载时长 %.1fs\n",
		r.PlayersWant, r.Battles, r.Connected, r.ConnectFailed, r.DurationS)
	fmt.Printf("包速率：上行 %.0f pkt/s（共 %d），下行帧广播 %.0f pkt/s（共 %d）；帧面失败 %d，出局通知 %d\n",
		r.InputPPS, r.Inputs, r.BroadcastPPS, r.Broadcasts, r.Failures, r.OutNotifies)
	fmt.Printf("延迟：入局往返 p50=%.2fms p95=%.2fms p99=%.2fms（n=%d）\n",
		r.Handshake.P50, r.Handshake.P95, r.Handshake.P99, r.Handshake.Count)
	fmt.Printf("      SyncFrames 往返 p50=%.2fms p95=%.2fms p99=%.2fms（n=%d）\n",
		r.Probe.P50, r.Probe.P95, r.Probe.P99, r.Probe.Count)
	fmt.Printf("      帧广播下行 p50=%.2fms p95=%.2fms p99=%.2fms（n=%d）\n",
		r.Downlink.P50, r.Downlink.P95, r.Downlink.P99, r.Downlink.Count)
	fmt.Printf("分段 建流窗口 %.1fs：探针 p50=%.2f p95=%.2f p99=%.2f（n=%d）；下行 p99=%.2f（n=%d）\n",
		r.DialS, r.DialProbe.P50, r.DialProbe.P95, r.DialProbe.P99, r.DialProbe.Count,
		r.DialDown.P99, r.DialDown.Count)
	fmt.Printf("     稳态窗口 末 %.1fs：探针 p50=%.2f p95=%.2f p99=%.2f（n=%d）；下行 p99=%.2f（n=%d）\n",
		r.SteadyS, r.SteadyProbe.P50, r.SteadyProbe.P95, r.SteadyProbe.P99, r.SteadyProbe.Count,
		r.SteadyDown.P99, r.SteadyDown.Count)
	if r.Via == "edge" {
		fmt.Printf("接入层：负载结束时活跃流 %d\n", r.EdgeStreams)
	}
	out, err := json.Marshal(r)
	if err != nil {
		fatalf("汇总序列化失败: %v", err)
	}
	fmt.Printf("SUMMARY %s\n", out)
}

// parallelFor 以固定并发度执行 n 次 fn(i)；任意一次返回错误即停止派发并返回首个错误。
func parallelFor(n, conc int, fn func(i int) error) error {
	if conc <= 0 {
		conc = 1
	}
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for i := 0; i < n; i++ {
		mu.Lock()
		stop := firstErr != nil
		mu.Unlock()
		if stop {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := fn(i); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	return firstErr
}

// closePlayers 关闭全部客户端连接（幂等，含未建帧连接的账号）。
func closePlayers(ps []*loadPlayer) {
	for _, p := range ps {
		if p != nil {
			p.close()
		}
	}
}

// fatalf 打印错误并退出（压测脚本的失败路径统一出口）。
func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "loadtest 失败: "+format+"\n", args...)
	os.Exit(1)
}

var (
	etcdFlag    = flag.String("etcd", "127.0.0.1:12379", "etcd endpoints（逗号分隔）")
	redisFlag   = flag.String("redis", "127.0.0.1:16379", "redis 地址")
	natsFlag    = flag.String("nats", "nats://127.0.0.1:14222", "nats URL")
	mongoFlag   = flag.String("mongo", "mongodb://127.0.0.1:27017", "mongo URI")
	mongoDBFlag = flag.String("mongo-db", "game_it", "mongo 数据库名")
)
