// main.go 是「远程五服务」压测驱动：业务链路走远端网关（TCP SDK 会话），
// 战斗帧经远端接入层（WS/UDP）直连——client.go/edgeconn.go/stats.go 复用模板，逐字不动。
//
// 与模板 main.go 的唯一区别：不再进程内装配五服务（startStack），地址全部来自旗标。
//
// 用法：
//
//	ltdriver -players 4096 -duration 60s -pps 20 -probe 1s \
//	  -gateway 10.10.1.23:9001 -edge-ws 10.10.1.23:7100 -edge-udp 10.10.1.23:7102
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

// ratePoint 是一段 5 秒窗口的上行包速率（压测端视角，崩塌判据用）。
type ratePoint struct {
	T   float64 `json:"t_s"`
	PPS float64 `json:"pps"`
}

// report 是压测汇总（JSON 输出；字段名进归档日志，勿随意改名）。
type report struct {
	Via            string  `json:"via"`
	Transport      string  `json:"transport"`
	Gateway        string  `json:"gateway"`
	EdgeWS         string  `json:"edge_ws"`
	EdgeKCP        string  `json:"edge_kcp"`
	PlayersWant    int     `json:"players_requested"`
	PerBattleWant  int     `json:"players_per_battle_requested"`
	PlayersMatched int     `json:"players_matched"`
	Battles        int     `json:"battles"`
	PerBattleReal  float64 `json:"players_per_battle_actual"`
	Connected      int     `json:"connections_established"`
	ConnectFailed  int     `json:"connections_failed"`
	DurationS      float64 `json:"duration_s"`
	Inputs         int64   `json:"inputs_sent"`
	InputPPS       float64 `json:"input_pps"`
	Broadcasts     int64   `json:"broadcasts_received"`
	BroadcastPPS   float64 `json:"broadcast_pps"`
	Failures       int64   `json:"frame_failures"`
	OutNotifies    int64   `json:"player_out_notifies"`

	Rates []ratePoint `json:"rate_samples"`

	Handshake latency `json:"handshake_rtt_ms"`
	Probe     latency `json:"frame_rtt_ms"`
	Downlink  latency `json:"broadcast_downlink_ms"`

	DialS       float64 `json:"dial_phase_s"`
	SteadyS     float64 `json:"steady_window_s"`
	DialProbe   latency `json:"dial_rtt_ms"`
	SteadyProbe latency `json:"steady_rtt_ms"`
	DialDown    latency `json:"dial_broadcast_downlink_ms"`
	SteadyDown  latency `json:"steady_broadcast_downlink_ms"`

	// 入场窗口细分（入池阶段 vs 成局阶段）：上一轮两段混在一起，看不出哪段慢。
	QueueConc        int     `json:"queue_concurrency"`
	PrefillWant      int     `json:"prefill_requested"`
	PrefillEntered   int     `json:"prefill_entered"`
	PrefillFailed    int     `json:"prefill_failed"`
	PrefillS         float64 `json:"prefill_phase_s"`
	PrefillQPS       float64 `json:"prefill_qps"`
	QueueEntered     int     `json:"queue_entered"`
	QueueFailed      int     `json:"queue_failed"`
	QueueS           float64 `json:"queue_phase_s"`
	QueueQPS         float64 `json:"queue_qps"`
	QueueRTT         latency `json:"queue_rtt_ms"`
	Matched          int     `json:"players_matched_notify"`
	MatchS           float64 `json:"match_phase_s"`
	MatchPlayersPerS float64 `json:"match_players_per_s"`
	EntryWindowS     float64 `json:"entry_window_s"`
	EntryPlayersPerS float64 `json:"entry_players_per_s"`
}

// windows 是一轮压测的分段边界。
type windows struct {
	dialStart time.Time
	dialEnd   time.Time
	pumpEnd   time.Time
	steady    time.Duration
}

// loadRun 是一次压测的装配与负载参数。
type loadRun struct {
	players        int
	perBattle      int
	dialConc       int
	connectTimeout time.Duration
	matchTimeout   time.Duration
	matchQuiet     time.Duration
	totalTimeout   time.Duration
	via            string
	transport      string
	steady         time.Duration
	rateEvery      time.Duration
	gateway        string
	edgeWS         string
	edgeUDP        string
	edgeKCP        string
	battleWS       string
	battleUDP      string
	queue          queueCfg
	pump           pumpCfg
}

func main() {
	cfg := parseFlags()
	if err := pkglog.Init(pkglog.Options{Service: "ltdriver", Level: "info", File: "logs/ltdriver.log"}); err != nil {
		fmt.Fprintf(os.Stderr, "日志初始化失败（继续）: %v\n", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.totalTimeout)
	defer cancel()
	fmt.Printf("[环境] 远程驱动：网关业务面 tcp=%s；接入层 ws=%s udp=%s kcp=%s\n", cfg.gateway, cfg.edgeWS, cfg.edgeUDP, cfg.edgeKCP)
	rep, err := runLoad(ctx, cfg)
	if err != nil {
		fatalf("%v", err)
	}
	printReport(rep)
}

// parseFlags 解析旗标并组装压测参数。
func parseFlags() loadRun {
	players := flag.Int("players", 50, "并发玩家数（= 帧连接数）")
	perBattle := flag.Int("per-battle", 2, "期望每局人数（仅记录：真实人数由成局通知的 match_id 统计）")
	transport := flag.String("transport", "ws", "帧传输面：ws|udp|kcp")
	via := flag.String("via", "edge", "驱动路径：edge（经接入层）| direct（直连 battle 帧端口）")
	pps := flag.Int("pps", 20, "每连接每秒帧输入数（上行包速率）")
	probe := flag.Duration("probe", time.Second, "每连接 SyncFrames 探针间隔（往返延迟样本来源）")
	duration := flag.Duration("duration", 20*time.Second, "稳定负载时长")
	steady := flag.Duration("steady-window", 5*time.Second, "稳态窗口长度")
	probeJitter := flag.Bool("probe-jitter", false, "探针相位抖动（诊断用）")
	connectTimeout := flag.Duration("connect-timeout", 120*time.Second, "建连与入局阶段的总超时")
	matchTimeout := flag.Duration("match-timeout", 60*time.Second, "成局阶段的总超时（到点即用已匹配的玩家开打）")
	matchQuiet := flag.Duration("match-quiet", 3*time.Second, "成局静默期：连续这么久无新成局即认为成局收尾，立刻开打")
	conc := flag.Int("dial-concurrency", 64, "建连/注册的并发度")
	queueConc := flag.Int("queue-concurrency", 512, "入池（EnterMatchQueue）并发度：单票 RTT 110–200ms，靠并发盖出速率（上限 2048）")
	prefill := flag.Int("prefill", 0, "预灌池票数：先以 -queue-concurrency 把 N 张票灌进池、再开匹配（0 = 不预灌）")
	prefillSettle := flag.Duration("prefill-settle", 2*time.Second, "预灌后静默时长：让池子积起来再开匹配")
	gateway := flag.String("gateway", "10.10.1.23:9001", "远端网关业务面地址（host:port）")
	edgeWS := flag.String("edge-ws", "10.10.1.23:7100", "远端接入层 WS 面地址")
	edgeUDP := flag.String("edge-udp", "10.10.1.23:7102", "远端接入层 UDP 面地址")
	edgeKCP := flag.String("edge-kcp", "10.10.1.23:7101", "远端接入层 KCP 面地址（后端指向 battle 的 KCP 帧端口）")
	battleWS := flag.String("battle-ws", "", "battle WS 帧端口（-via direct 用）")
	battleUDP := flag.String("battle-udp", "", "battle UDP 帧端口（-via direct 用）")
	rateEvery := flag.Duration("rate-every", 5*time.Second, "上行包速率采样间隔（压测端 pps 时间线）")
	total := flag.Duration("total-timeout", 10*time.Minute, "整轮压测的兜底超时")
	flag.Parse()
	if *transport != "ws" && *transport != "udp" && *transport != "kcp" {
		fatalf("未知 -transport %q（ws|udp|kcp）", *transport)
	}
	if *via != "edge" && *via != "direct" {
		fatalf("未知 -via %q（edge|direct）", *via)
	}
	if *perBattle != 2 {
		fmt.Fprintf(os.Stderr, "[提示] -per-battle=%d 仅记录：真实每局人数由成局通知的 match_id 统计\n", *perBattle)
	}
	qc := queueCfg{conc: *queueConc, prefill: *prefill, settle: *prefillSettle}
	if err := qc.check(*players); err != nil {
		fatalf("%v", err)
	}
	return loadRun{
		players: *players, perBattle: *perBattle, dialConc: *conc, connectTimeout: *connectTimeout,
		matchTimeout: *matchTimeout, matchQuiet: *matchQuiet,
		totalTimeout: *total, via: *via, transport: *transport, steady: *steady, rateEvery: *rateEvery,
		gateway: *gateway, edgeWS: *edgeWS, edgeUDP: *edgeUDP, edgeKCP: *edgeKCP,
		battleWS: *battleWS, battleUDP: *battleUDP,
		queue: qc,
		pump:  pumpCfg{pps: *pps, probe: *probe, duration: *duration, jitter: *probeJitter},
	}
}

// runLoad 执行一轮压测：建客户端 → 入队成局 → 建帧连接并入局 → 稳定负载 → 汇总。
func runLoad(ctx context.Context, cfg loadRun) (*report, error) {
	ps, err := createPlayers(ctx, cfg.gateway, cfg)
	if err != nil {
		return nil, err
	}
	defer closePlayers(ps)
	fmt.Printf("[客户端] 已注册登录 %d 个压测账号\n", len(ps))

	out, err := matchAll(ctx, ps, cfg.matchTimeout, cfg.matchQuiet, cfg.queue)
	if err != nil {
		return nil, err
	}
	fmt.Printf("[匹配] 成局 %d 局（收到成局通知的玩家 %d/%d，约 %.1f 人/局）\n",
		out.battles, out.matched, len(ps), perBattleActual(out.matched, out.battles))

	active := matchedPlayers(ps)
	dialStart := time.Now()
	dialCtx, dialCancel := context.WithTimeout(ctx, cfg.connectTimeout)
	connected, failed := dialAndJoin(dialCtx, active, cfg)
	dialCancel()
	dialEnd := time.Now()
	fmt.Printf("[直连] via=%s transport=%s 建连成功 %d / 失败 %d（建流窗口 %.1fs）\n",
		cfg.via, cfg.transport, connected, failed, dialEnd.Sub(dialStart).Seconds())
	if connected == 0 {
		return nil, fmt.Errorf("没有任何帧连接建立成功（via=%s transport=%s）", cfg.via, cfg.transport)
	}

	start := time.Now()
	rates := startRateSampler(active, cfg.rateEvery, start)
	pumpAll(ctx, active, cfg.pump)
	elapsed := time.Since(start)
	win := windows{dialStart: dialStart, dialEnd: dialEnd, pumpEnd: time.Now(), steady: cfg.steady}
	rep := collect(active, cfg, out.battles, failed, elapsed, win)
	rep.Rates = rates()
	rep.QueueConc = cfg.queue.conc
	rep.QueueRTT = queueRTT(ps)
	out.attachPhases(rep)
	return rep, nil
}

// startRateSampler 每 interval 采一次「自压测开始以来的累计上行数」并算出窗口 pps；
// 返回的函数在负载结束后补一个尾点并停止采样（压测端上行 pps 时间线由此得到）。
func startRateSampler(ps []*loadPlayer, interval time.Duration, start time.Time) func() []ratePoint {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	var mu sync.Mutex
	var pts []ratePoint
	last, lastTime := 0.0, start
	stop := make(chan struct{})
	go func() {
		tk := time.NewTicker(interval)
		defer tk.Stop()
		for {
			select {
			case <-stop:
				return
			case now := <-tk.C:
				cur := totalInputs(ps)
				secs := now.Sub(lastTime).Seconds()
				pps := 0.0
				if secs > 0 {
					pps = (cur - last) / secs
				}
				last, lastTime = cur, now
				mu.Lock()
				pts = append(pts, ratePoint{T: now.Sub(start).Seconds(), PPS: pps})
				mu.Unlock()
				fmt.Printf("[速率] t=%5.1fs 上行 %.0f pkt/s（累计 %d）\n", now.Sub(start).Seconds(), pps, int64(cur))
			}
		}
	}()
	return func() []ratePoint {
		close(stop)
		mu.Lock()
		defer mu.Unlock()
		return append([]ratePoint(nil), pts...)
	}
}

// totalInputs 汇总全部连接的已发输入数。
func totalInputs(ps []*loadPlayer) float64 {
	var n int64
	for _, p := range ps {
		if p != nil {
			n += p.inputs.Load()
		}
	}
	return float64(n)
}

// createPlayers 并发建立压测客户端（业务通道 + 会话 + 注册登录）。
func createPlayers(ctx context.Context, gatewayTCP string, cfg loadRun) ([]*loadPlayer, error) {
	ps := make([]*loadPlayer, cfg.players)
	err := parallelFor(cfg.players, cfg.dialConc, func(i int) error {
		p, err := newLoadPlayer(ctx, gatewayTCP, fmt.Sprintf("p%d", i))
		if err != nil {
			return fmt.Errorf("%w（已成功 %d/%d）", err, countNonNil(ps), cfg.players)
		}
		ps[i] = p
		return nil
	})
	if err != nil {
		closePlayers(ps)
		return nil, fmt.Errorf("建客户端失败: %w", err)
	}
	return ps, nil
}

// countNonNil 统计已建立客户端数量（失败信息用）。
func countNonNil(ps []*loadPlayer) int {
	n := 0
	for _, p := range ps {
		if p != nil {
			n++
		}
	}
	return n
}

// dialAndJoin 并发建帧连接并入局，返回成功与失败连接数。
func dialAndJoin(ctx context.Context, ps []*loadPlayer, cfg loadRun) (int, int) {
	o := dialOpts{
		via: cfg.via, transport: cfg.transport,
		edgeWS: cfg.edgeWS, edgeUDP: cfg.edgeUDP, edgeKCP: cfg.edgeKCP,
		battleWS: cfg.battleWS, battleUDP: cfg.battleUDP,
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
		Via: cfg.via, Transport: cfg.transport, Gateway: cfg.gateway, EdgeWS: cfg.edgeWS, EdgeKCP: cfg.edgeKCP,
		PlayersWant: cfg.players, PerBattleWant: cfg.perBattle, Battles: battles, ConnectFailed: failed,
		DurationS: elapsed.Seconds(),
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
	rep.PerBattleReal = perBattleActual(rep.PlayersMatched, battles)
	return rep
}

// printReport 打印人读表格与末尾一行 JSON 汇总。
func printReport(r *report) {
	fmt.Printf("\n=== 压测结果（via=%s transport=%s 远程网关=%s）===\n", r.Via, r.Transport, r.Gateway)
	fmt.Printf("连接：请求 %d 玩家 → 成局 %d 局（约 %.1f 人/局），建连成功 %d，失败 %d；负载时长 %.1fs\n",
		r.PlayersWant, r.Battles, r.PerBattleReal, r.Connected, r.ConnectFailed, r.DurationS)
	fmt.Printf("包速率：上行 %.0f pkt/s（共 %d），下行帧广播 %.0f pkt/s（共 %d）；帧面失败 %d，出局通知 %d\n",
		r.InputPPS, r.Inputs, r.BroadcastPPS, r.Broadcasts, r.Failures, r.OutNotifies)
	fmt.Printf("入场窗口：入池 %.2fs（%.0f 票/秒，成功 %d 失败 %d，RTT p50=%.1f p99=%.1f）→ 成局 %.2fs（%.0f 玩家/秒）\n",
		r.QueueS, r.QueueQPS, r.QueueEntered, r.QueueFailed, r.QueueRTT.P50, r.QueueRTT.P99,
		r.MatchS, r.MatchPlayersPerS)
	fmt.Printf("          合计 %.2fs 收到成局通知 %d 人（%.0f 玩家/秒，入池并发 %d）；预灌 %d 票 %.2fs（%.0f 票/秒）\n",
		r.EntryWindowS, r.Matched, r.EntryPlayersPerS, r.QueueConc,
		r.PrefillEntered, r.PrefillS, r.PrefillQPS)
	fmt.Printf("延迟：入局往返 p50=%.2f p95=%.2f p99=%.2f（n=%d）\n",
		r.Handshake.P50, r.Handshake.P95, r.Handshake.P99, r.Handshake.Count)
	fmt.Printf("      SyncFrames 往返 p50=%.2f p95=%.2f p99=%.2f（n=%d）\n",
		r.Probe.P50, r.Probe.P95, r.Probe.P99, r.Probe.Count)
	fmt.Printf("      帧广播下行 p50=%.2f p95=%.2f p99=%.2f（n=%d）\n",
		r.Downlink.P50, r.Downlink.P95, r.Downlink.P99, r.Downlink.Count)
	fmt.Printf("分段 建流窗口 %.1fs：探针 p99=%.2f（n=%d）；稳态末 %.1fs：探针 p99=%.2f（n=%d）\n",
		r.DialS, r.DialProbe.P99, r.DialProbe.Count, r.SteadyS, r.SteadyProbe.P99, r.SteadyProbe.Count)
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

// fatalf 打印错误并退出（压测脚本的失败统一出口）。
func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "ltdriver 失败: "+format+"\n", args...)
	os.Exit(1)
}
