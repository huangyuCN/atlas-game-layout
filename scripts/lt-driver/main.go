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
	Probe     latency `json:"frame_rtt_ms"` // 仅成功样本（口径与历史各轮一致）
	Downlink  latency `json:"broadcast_downlink_ms"` // 未校正：含两端时钟差

	// 探针结果分类（把「超时」与「慢但成功」分开，见 probe.go）：
	// 成功率低 ⇒ 分位数被「超时」主导；成功率高而 p99 仍高 ⇒ 服务端真的慢。
	ProbeTotal      int            `json:"probe_total"`
	ProbeOK         int            `json:"probe_ok"`
	ProbeTimeout    int            `json:"probe_timeout"`
	ProbeError      int            `json:"probe_error"`
	ProbeSuccessPct float64        `json:"probe_success_pct"`
	ProbeErrKinds   map[string]int `json:"probe_err_kinds,omitempty"`
	ProbeOKLatency  latency        `json:"probe_ok_rtt_ms"`     // 仅成功样本 = 真实服务时延
	ProbeAllLatency latency        `json:"probe_all_rtt_ms"`    // 含超时（超时按实际等待计）
	ProbeTimeoutMS  latency        `json:"probe_timeout_wait_ms"` // 超时样本的等待时长

	// 广播成功率（收没收到）：分母是服务端权威帧号，不是实收条数。
	BroadcastReceived   int64   `json:"broadcast_received_frames"`
	BroadcastExpect     int64   `json:"broadcast_expected_frames"`
	BroadcastMissing    int64   `json:"broadcast_missing_frames"`
	BroadcastSuccessPct float64 `json:"broadcast_success_pct"`

	// 时钟校正（每轮开始/结束各测一次）：
	// Δ = 压测端 - 服务端；校正后 = 未校正 - Δ。漂移 >5ms 时该轮判不可用。
	ClockSource        string  `json:"clock_source"`
	ClockSSH           string  `json:"clock_ssh"`
	ClockOffsetStartMS float64 `json:"clock_offset_start_ms"`
	ClockOffsetEndMS   float64 `json:"clock_offset_end_ms"`
	ClockOffsetDriftMS float64 `json:"clock_offset_drift_ms"`
	ClockOffsetUsedMS  float64 `json:"clock_offset_used_ms"`
	ClockUncertaintyMS float64 `json:"clock_uncertainty_ms"`
	ClockRTTMS         float64 `json:"clock_rtt_ms"`
	ClockNTPClientMS   float64 `json:"ntp_offset_client_ms"`
	ClockNTPServerMS   float64 `json:"ntp_offset_server_ms"`
	ClockRoundUsable   bool    `json:"clock_round_usable"`
	ClockNote          string  `json:"clock_note,omitempty"`

	DownlinkCorrected   latency `json:"broadcast_downlink_corrected_ms"`
	DialDownCorrected   latency `json:"dial_broadcast_downlink_corrected_ms"`
	SteadyDownCorrected latency `json:"steady_broadcast_downlink_corrected_ms"`

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
	hello          helloPolicy
	clockSSH       string // 服务端 SSH 目标（测 Δ 用；空 = 只读两端 NTP）
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
	duration := flag.Duration("duration", 20*time.Second, "稳定负载时长（到点自动停止发送并打印 SUMMARY，无需人工 kill）")
	steady := flag.Duration("steady-window", 5*time.Second, "稳态窗口长度")
	probeJitter := flag.Bool("probe-jitter", false, "探针相位抖动（诊断用）")
	connectTimeout := flag.Duration("connect-timeout", 120*time.Second, "建连与入局阶段的总超时")
	matchTimeout := flag.Duration("match-timeout", 60*time.Second, "成局阶段的总超时（到点即用已匹配的玩家开打）")
	matchQuiet := flag.Duration("match-quiet", 3*time.Second, "成局静默期：连续这么久无新成局即认为成局收尾，立刻开打")
	conc := flag.Int("dial-concurrency", 64, "建连/注册的并发度")
	helloTimeout := flag.Duration("hello-timeout", 300*time.Millisecond,
		"数据报面 hello 单次读超时（接入层拒绝时不回错误，只能靠超时发现；旧值 3s 会把建流窗口放大到几十秒）")
	helloAttempts := flag.Int("hello-attempts", 3, "数据报面 hello 最大尝试次数（含首次，每次用新 socket）")
	helloBackoff := flag.Duration("hello-backoff", 150*time.Millisecond, "数据报面 hello 两次尝试之间的退避")
	helloUnavail := flag.Duration("hello-unavailable-backoff", 150*time.Millisecond,
		"收到接入层「暂不可用」回执后的重试间隔（夹到 100–200ms；回执只表示后端暂不可用，不必等满单次超时）")
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
	clockSSH := flag.String("clock-ssh", "ubuntu@10.10.1.23",
		"服务端 SSH 目标：轮首/轮尾各测一次双端时钟差 Δ（空 = 只读两端 NTP，精度低）")
	probeLog := flag.String("probe-log", "",
		"逐条探针记录落盘路径（NDJSON：发出/收到时刻、结果分类、err 类型）；空 = 不落盘")
	probeLogEveryFlag := flag.Int("probe-log-every", 1, "逐条探针记录的抽样间隔（每 N 条落 1 条）")
	flag.Parse()
	// 探针落盘开关必须在建连接前就位（loadPlayer 创建时据此决定是否保留逐条记录）。
	probeLogPath = *probeLog
	probeLogEvery.Store(int64(max(*probeLogEveryFlag, 1)))
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
		queue:    qc,
		clockSSH: *clockSSH,
		pump:     pumpCfg{pps: *pps, probe: *probe, duration: *duration, jitter: *probeJitter},
		hello: helloPolicy{
			timeout: *helloTimeout, attempts: *helloAttempts, backoff: *helloBackoff,
			unavailableBackoff: *helloUnavail,
		},
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
	clkStart := measureClock(cfg.clockSSH)
	fmt.Printf("[时钟] 轮首 Δ(压测端-服务端)=%.2fms（源 %s，最小 RTT %.1fms）\n",
		clkStart.Offset, clkStart.Source, clkStart.RTT)
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
	// 负载阶段自己的取消边界：-duration 到点即取消在途调用（泵另有自己的到点判断），
	// 于是「停止发送」不必等某条卡住的调用自然返回。
	pumpCtx, pumpCancel := context.WithTimeout(ctx, cfg.pump.duration)
	pumpAll(pumpCtx, active, cfg.pump)
	pumpCancel()
	elapsed := time.Since(start)
	clkEnd := measureClock(cfg.clockSSH)
	fmt.Printf("[时钟] 轮尾 Δ(压测端-服务端)=%.2fms（源 %s，最小 RTT %.1fms）\n",
		clkEnd.Offset, clkEnd.Source, clkEnd.RTT)
	win := windows{dialStart: dialStart, dialEnd: dialEnd, pumpEnd: time.Now(), steady: cfg.steady}
	rep := collect(active, cfg, out.battles, failed, elapsed, win, summarizeClock(clkStart, clkEnd))
	rep.Rates = rates()
	rep.QueueConc = cfg.queue.conc
	rep.QueueRTT = queueRTT(ps)
	out.attachPhases(rep)
	dumpProbeRecords(active)
	return rep, nil
}

// dumpProbeRecords 把本轮全部连接的逐条探针记录落盘（-probe-log 未配置时什么都不做）。
func dumpProbeRecords(ps []*loadPlayer) {
	if probeLogPath == "" {
		return
	}
	sets := make([]*probeSet, 0, len(ps))
	for _, p := range ps {
		if p != nil {
			sets = append(sets, p.probe)
		}
	}
	n, err := dumpProbeLog(probeLogPath, sets)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[探针] 逐条记录落盘失败（%s）: %v\n", probeLogPath, err)
		return
	}
	fmt.Printf("[探针] 逐条记录 %d 条 → %s\n", n, probeLogPath)
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
		hello: cfg.hello,
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

// collect 汇总全部连接的样本与计数，并把探针/下行样本切成「建流窗口 vs 稳态窗口」两段。
func collect(ps []*loadPlayer, cfg loadRun, battles, failed int, elapsed time.Duration, win windows, clk clockSummary) *report {
	rep := &report{
		Via: cfg.via, Transport: cfg.transport, Gateway: cfg.gateway, EdgeWS: cfg.edgeWS, EdgeKCP: cfg.edgeKCP,
		PlayersWant: cfg.players, PerBattleWant: cfg.perBattle, Battles: battles, ConnectFailed: failed,
		DurationS: elapsed.Seconds(),
	}
	hs, pr, dl := &samples{}, newProbeSet(false), &samples{}
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
		// 广播成功率：分母取服务端权威帧号（探针回执的 current_frame），分子取实收条数。
		if first := p.firstFrame.Load(); first > 0 {
			want, missing := broadcastGap(first, p.srvFrame.Load(), uint64(p.broadcasts.Load()))
			rep.BroadcastReceived += int64(p.broadcasts.Load())
			rep.BroadcastExpect += int64(want)
			rep.BroadcastMissing += int64(missing)
		}
	}
	if rep.BroadcastExpect > 0 {
		rep.BroadcastSuccessPct = float64(rep.BroadcastExpect-rep.BroadcastMissing) * 100 / float64(rep.BroadcastExpect)
	}
	if secs := elapsed.Seconds(); secs > 0 {
		rep.InputPPS = float64(rep.Inputs) / secs
		rep.BroadcastPPS = float64(rep.Broadcasts) / secs
	}
	rep.Handshake = summarize(hs.sorted())
	sum := pr.summary()
	rep.Probe = sum.OKLatency // 历史口径：只含成功样本（与 probe.add 仅在 err==nil 时调用一致）
	rep.ProbeTotal, rep.ProbeOK, rep.ProbeTimeout, rep.ProbeError = sum.Total, sum.OK, sum.Timeout, sum.Errors
	rep.ProbeSuccessPct, rep.ProbeErrKinds = sum.SuccessPct, sum.ErrKinds
	rep.ProbeOKLatency, rep.ProbeAllLatency, rep.ProbeTimeoutMS = sum.OKLatency, sum.AllLatency, sum.TimeoutWait
	rep.Downlink = summarize(dl.sorted())
	rep.DialS = win.dialEnd.Sub(win.dialStart).Seconds()
	rep.SteadyS = win.steady.Seconds()
	rep.DialProbe = summarize(pr.okWindow(win.dialStart, win.dialEnd))
	rep.DialDown = summarize(dl.window(win.dialStart, win.dialEnd))
	steadyFrom := win.pumpEnd.Add(-win.steady)
	rep.SteadyProbe = summarize(pr.okWindow(steadyFrom, win.pumpEnd))
	rep.SteadyDown = summarize(dl.window(steadyFrom, win.pumpEnd))
	rep.PerBattleReal = perBattleActual(rep.PlayersMatched, battles)
	rep.ClockSource, rep.ClockSSH = clk.Source, cfg.clockSSH
	rep.ClockOffsetStartMS, rep.ClockOffsetEndMS = clk.DeltaStart, clk.DeltaEnd
	rep.ClockOffsetDriftMS, rep.ClockOffsetUsedMS = clk.Drift, clk.Used
	rep.ClockUncertaintyMS, rep.ClockRTTMS = clk.Uncertainty, clk.RTT
	rep.ClockNTPClientMS, rep.ClockNTPServerMS = clk.NTPClient, clk.NTPServer
	rep.ClockRoundUsable, rep.ClockNote = clk.Usable, clk.Note
	rep.DownlinkCorrected = correctLatency(rep.Downlink, clk.Used)
	rep.DialDownCorrected = correctLatency(rep.DialDown, clk.Used)
	rep.SteadyDownCorrected = correctLatency(rep.SteadyDown, clk.Used)
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
	// 探针三行：分类（成功/超时/其它错误 + 成功率）→ 仅成功样本分位 → 含超时分位。
	// 只有「成功率 ≥95% 且成功样本 p99 仍高」才能判服务端真的慢；否则分位被超时主导。
	fmt.Printf("      探针：成功 %d、超时 %d、其它错误 %d、成功率 %.2f%%（总数 %d；报错类型 %s）\n",
		r.ProbeOK, r.ProbeTimeout, r.ProbeError, r.ProbeSuccessPct, r.ProbeTotal, errKindText(r.ProbeErrKinds))
	fmt.Printf("      探针（仅成功样本，真实服务时延）p50=%.2f p95=%.2f p99=%.2f max=%.2f（n=%d）\n",
		r.ProbeOKLatency.P50, r.ProbeOKLatency.P95, r.ProbeOKLatency.P99, r.ProbeOKLatency.Max, r.ProbeOKLatency.Count)
	fmt.Printf("      探针（含超时，超时按其实际等待计）p50=%.2f p95=%.2f p99=%.2f max=%.2f（n=%d）；超时等待 p50=%.0f max=%.0f\n",
		r.ProbeAllLatency.P50, r.ProbeAllLatency.P95, r.ProbeAllLatency.P99, r.ProbeAllLatency.Max,
		r.ProbeAllLatency.Count, r.ProbeTimeoutMS.P50, r.ProbeTimeoutMS.Max)
	fmt.Printf("      广播：实收 %d / 应到 %d，成功率 %.2f%%，缺口 %d 帧（分母=服务端 current_frame）\n",
		r.BroadcastReceived, r.BroadcastExpect, r.BroadcastSuccessPct, r.BroadcastMissing)
	fmt.Printf("      帧广播下行（未校正）p50=%.2f p95=%.2f p99=%.2f（n=%d）\n",
		r.Downlink.P50, r.Downlink.P95, r.Downlink.P99, r.Downlink.Count)
	fmt.Printf("      帧广播下行（校正 Δ=%+.2fms，漂移 %.2fms，不确定度 ±%.2fms）p50=%.2f p95=%.2f p99=%.2f；轮次可用=%v\n",
		r.ClockOffsetUsedMS, r.ClockOffsetDriftMS, r.ClockUncertaintyMS,
		r.DownlinkCorrected.P50, r.DownlinkCorrected.P95, r.DownlinkCorrected.P99, r.ClockRoundUsable)
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
