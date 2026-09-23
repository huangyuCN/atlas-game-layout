// loadtest 帧通道压测客户端（atlas-sdk-go 驱动）：对比 KCP 与 WS 战斗通道的帧上行延迟与帧广播吞吐。
//
// 场景：双玩家经 gateway 注册登录 + 入队（SDK 会话，业务通道 op 透传），
// driver 在战斗通道顺序发送 N 个帧输入，统计每次 SendFrameInput 的往返延迟
// （client → gateway → battle actor 帧通道）；再统计 T 秒窗口内收到的帧广播数量
// （fps）与广播延迟（收到时刻 - 帧 server_time）。
//
// 通道形态（-transport）：
//   - kcp：SDK dual 双通道（TCP 业务 + KCP 战斗，帧会话槽携带凭据）
//   - ws：SDK 单通道（WS 一条连接承载业务与战斗，连接绑定身份）
//
// 用法：
//
//	go run ./scripts/loadtest -transport kcp -inputs 500 -window 5s
//	go run ./scripts/loadtest -transport ws  -inputs 500 -window 5s
//
// 输出：末尾一行 JSON 汇总（bench 归档用，见 docs/superpowers/benchmarks 约定）。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	sdkclient "github.com/huangyuCN/atlas-sdk-go/client"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"google.golang.org/protobuf/encoding/protojson"
)

// summary 是压测汇总（JSON 输出）。
type summary struct {
	Transport string  `json:"transport"`
	Inputs    int     `json:"inputs"`
	InputAvg  float64 `json:"input_avg_ms"`
	InputP50  float64 `json:"input_p50_ms"`
	InputP95  float64 `json:"input_p95_ms"`
	InputP99  float64 `json:"input_p99_ms"`
	InputRate float64 `json:"input_rate_per_s"`
	Frames    int     `json:"broadcast_frames"`
	FrameFPS  float64 `json:"broadcast_fps"`
	FrameAvg  float64 `json:"broadcast_avg_ms"`
	FrameP95  float64 `json:"broadcast_p95_ms"`
}

// player 是压测客户端：SDK 会话 + 战斗域强类型 stub。
type player struct {
	id      string
	sess    *sdkclient.Session
	cli     *sdkclient.Client
	players *gamev1.PlayerServiceOpClient   // 匹配域 op（业务通道）
	battle  *battlev1.BattleServiceOpClient // 战斗域 op（KCP 战斗通道 / WS 单通道）
	step    byte

	started chan string // 开局通知（battleID）
	mu      sync.Mutex
	frames  []time.Duration // 帧广播延迟样本（收到时刻 - server_time）
}

// newSession 构造 SDK 会话管理器（关闭 SDK 内置 nil 心跳，改由真 DTO 心跳承担续租）。
func newSession() *sdkclient.Session {
	return sdkclient.NewSession(sdkclient.WithSessionHeartbeatInterval(0))
}

// commonDialOpts 公共拨号参数：传输保活 + 会话续租心跳（登录后生效）。
func commonDialOpts(sess *sdkclient.Session) []sdkclient.Option {
	return []sdkclient.Option{
		sdkclient.WithHeartbeatInterval(5 * time.Second),
		sdkclient.WithSessionHeartbeat(10*time.Second, func() (string, any) {
			if sess.Token() == "" {
				return "", nil // 未登录：跳过本轮
			}
			return sdkclient.OpSessionHeartbeat, &gatewayv1.HeartbeatRequest{Ts: time.Now().UnixMilli()}
		}),
	}
}

// newPlayer 建立 SDK 客户端并注册登录；挂接推送监听（开局通知先于战斗通道
// 绑定走业务通道回退，帧广播走战斗通道，两路都订阅）。
func newPlayer(ctx context.Context, tcpAddr, battleAddr, transport string) (*player, error) {
	sess := newSession()
	var cli *sdkclient.Client
	var err error
	switch transport {
	case "ws":
		// WS 单通道：业务与战斗共用一条连接（连接绑定身份）。
		cli, err = sdkclient.DialWS(battleAddr, "", append(commonDialOpts(sess), sess.ChannelOptions()...)...)
		if err != nil {
			return nil, fmt.Errorf("ws: %w", err)
		}
	default:
		// dual 双通道：TCP 业务 + KCP 战斗（帧会话槽携带凭据）。
		cli, err = sdkclient.DialDual(
			sdkclient.ChannelConfig{Addr: tcpAddr, Opts: sess.ChannelOptions()},
			sdkclient.ChannelConfig{
				Transport: sdkclient.TransportKCP,
				Addr:      battleAddr,
				Opts: []sdkclient.Option{
					sdkclient.WithSessionTokenProvider(func() string { return sess.Token() }),
				},
			},
			commonDialOpts(sess)...,
		)
		if err != nil {
			return nil, fmt.Errorf("dual: %w", err)
		}
	}
	sess.Bind(cli)
	p := &player{
		sess:    sess,
		cli:     cli,
		players: gamev1.NewPlayerServiceOpClient(sess),
		battle:  battlev1.NewBattleServiceOpClient(battleInvoker(cli)),
		started: make(chan string, 2),
	}
	p.watchNotifies()
	account := fmt.Sprintf("lt-%s-%d", transport, time.Now().UnixNano())
	if _, err := sess.Register(ctx, &gatewayv1.RegisterRequest{Account: account, Password: "pw"}); err != nil {
		return nil, fmt.Errorf("注册: %w", err)
	}
	if _, err := sess.Login(ctx, &gatewayv1.LoginRequest{PlayerId: sess.PlayerID(), Password: "pw"}); err != nil {
		return nil, fmt.Errorf("登录: %w", err)
	}
	p.id = sess.PlayerID()
	return p, nil
}

// battleInvoker 战斗域 op 的通道：dual 形态取战斗通道视图，单通道复用业务通道。
func battleInvoker(cli *sdkclient.Client) sdkclient.Invoker {
	if bv := cli.Channel(sdkclient.KindBattle); bv != nil {
		return bv
	}
	return cli
}

// watchNotifies 订阅开局通知、帧广播与结束通知（业务 + 战斗通道双挂）。
func (p *player) watchNotifies() {
	for _, sub := range []func(string, sdkclient.NotifyHandler) func(){p.cli.On} {
		sub(consts.PushOpMatchStarted, p.watch)
		sub(consts.PushOpFrameBroadcast, p.watch)
		sub(consts.PushOpBattleEnd, p.watch)
	}
	if bv := p.cli.Channel(sdkclient.KindBattle); bv != nil {
		bv.On(consts.PushOpMatchStarted, p.watch)
		bv.On(consts.PushOpFrameBroadcast, p.watch)
		bv.On(consts.PushOpBattleEnd, p.watch)
	}
}

// watch 收集开局通知、帧广播与结束通知。
func (p *player) watch(operation string, payload []byte) {
	switch operation {
	case consts.PushOpMatchStarted:
		var n gamev1.MatchStartedNotify
		if err := protojson.Unmarshal(payload, &n); err == nil && n.GetBattleId() != "" {
			select {
			case p.started <- n.GetBattleId():
			default:
			}
		}
	case consts.PushOpFrameBroadcast:
		var fb battlev1.FrameBroadcast
		if err := protojson.Unmarshal(payload, &fb); err != nil || fb.GetFrame().GetServerTime() == nil {
			return
		}
		latency := time.Since(fb.GetFrame().GetServerTime().AsTime())
		p.mu.Lock()
		p.frames = append(p.frames, latency)
		p.mu.Unlock()
	case consts.PushOpBattleEnd:
		// 压测场景战斗不结束（原地步进输入）；结束通知仅容错记录。
	}
}

// queueTwo 双玩家经 gateway 业务通道 op 入队，等待双方开局通知（battleID 一致）。
func queueTwo(ctx context.Context, ps ...*player) (string, error) {
	for _, p := range ps {
		if _, err := p.players.EnterMatchQueue(ctx, &gamev1.EnterMatchQueueReq{Ruleset: "casual"}); err != nil {
			return "", fmt.Errorf("%s 入队: %w", p.id, err)
		}
	}
	battleID := ""
	for _, p := range ps {
		select {
		case id := <-p.started:
			if battleID == "" {
				battleID = id
			}
			if id != battleID {
				return "", fmt.Errorf("双方开局 battleID 不一致: %q vs %q", id, battleID)
			}
		case <-time.After(10 * time.Second):
			return "", fmt.Errorf("%s 未收到开局通知", p.id)
		}
	}
	return battleID, nil
}

// joinBattle 加入战斗（懒激活期间重试）。
func (p *player) joinBattle(ctx context.Context, battleID string) error {
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := p.battle.JoinBattle(ctx, &battlev1.JoinBattleReq{BattleId: battleID})
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("加入战斗超时: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// frameLatencies 返回广播延迟样本副本。
func (p *player) frameLatencies() []time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]time.Duration(nil), p.frames...)
}

// percentiles 计算分位数（输入须已排序）。
func percentiles(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(q * float64(len(sorted)-1))
	return sorted[idx]
}

func main() {
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	driver, battleID := setupAndOpen(ctx)

	latencies, inputElapsed := measureInputLatency(ctx, driver, battleID)
	frameMs := observeBroadcast(driver)
	printSummary(latencies, frameMs, inputElapsed)
}

// setupAndOpen 建立双玩家连接（driver 实测 + dummy 陪跑）并等待成局，返回 driver 与 battleID。
func setupAndOpen(ctx context.Context) (*player, string) {
	// 战斗通道地址按传输形态取默认（kcp 端口 / ws URL）。
	battleAddr := *battleFlag
	if battleAddr == "" {
		if *transportFlag == "ws" {
			battleAddr = "ws://127.0.0.1:9002"
		} else {
			battleAddr = "127.0.0.1:9003"
		}
	}
	driver, err := newPlayer(ctx, *gwFlag, battleAddr, *transportFlag)
	if err != nil {
		fatalf("driver: %v", err)
	}
	dummy, err := newPlayer(ctx, *gwFlag, battleAddr, *transportFlag)
	if err != nil {
		fatalf("dummy: %v", err)
	}
	fmt.Printf("[压测] transport=%s driver=%s\n", *transportFlag, driver.id)

	// 开局：双玩家入队并等待成局。
	battleID, err := queueTwo(ctx, driver, dummy)
	if err != nil {
		fatalf("%v", err)
	}
	fmt.Printf("[开局] battle=%s\n", battleID)
	if err := driver.joinBattle(ctx, battleID); err != nil {
		fatalf("%v", err)
	}
	if err := dummy.joinBattle(ctx, battleID); err != nil {
		fatalf("%v", err)
	}
	return driver, battleID
}

// measureInputLatency 顺序发送 N 个帧输入并记录每次往返延迟。
// payload 步进 0（原地）：避免提前冲线结算，保证广播窗口期战斗持续。
func measureInputLatency(ctx context.Context, driver *player, battleID string) ([]float64, time.Duration) {
	var latencies []float64
	start := time.Now()
	for i := 0; i < *inputsFlag; i++ {
		send := time.Now()
		if err := driver.battle.SendFrameInput(ctx, &battlev1.FrameInputReq{
			BattleId: battleID,
			Input: &locksteppb.LockstepInput{
				FrameId: uint64(i%30 + 1), PlayerId: driver.id, Payload: []byte{0},
			},
		}); err != nil {
			fatalf("帧输入 %d: %v", i, err)
		}
		latencies = append(latencies, float64(time.Since(send).Microseconds())/1000)
	}
	inputElapsed := time.Since(start)
	fmt.Printf("[输入] %d 次完成，耗时 %v\n", *inputsFlag, inputElapsed)
	return latencies, inputElapsed
}

// observeBroadcast 观测 window 秒的帧广播窗口，返回帧广播延迟样本（毫秒）。
func observeBroadcast(driver *player) []float64 {
	time.Sleep(*windowFlag)
	samples := driver.frameLatencies()
	frameMs := make([]float64, 0, len(samples))
	for _, s := range samples {
		frameMs = append(frameMs, float64(s.Microseconds())/1000)
	}
	return frameMs
}

// printSummary 排序并输出压测汇总 JSON（bench 归档用）。
func printSummary(latencies, frameMs []float64, inputElapsed time.Duration) {
	sort.Float64s(latencies)
	sort.Float64s(frameMs)
	rate := float64(*inputsFlag) / inputElapsed.Seconds()

	sum := summary{
		Transport: *transportFlag,
		Inputs:    *inputsFlag,
		InputAvg:  avg(latencies),
		InputP50:  percentiles(latencies, 0.5),
		InputP95:  percentiles(latencies, 0.95),
		InputP99:  percentiles(latencies, 0.99),
		InputRate: rate,
		Frames:    len(frameMs),
		FrameFPS:  float64(len(frameMs)) / windowFlag.Seconds(),
		FrameAvg:  avg(frameMs),
		FrameP95:  percentiles(frameMs, 0.95),
	}
	out, _ := json.Marshal(sum)
	fmt.Printf("SUMMARY %s\n", out)
}

// avg 计算均值。
func avg(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	total := 0.0
	for _, x := range xs {
		total += x
	}
	return total / float64(len(xs))
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "loadtest 失败: "+format+"\n", args...)
	os.Exit(1)
}

var (
	transportFlag = flag.String("transport", "kcp", "战斗通道：kcp（dual：TCP 业务 + KCP 战斗）/ ws（WS 单通道）")
	gwFlag        = flag.String("gw", "127.0.0.1:9001", "gateway TCP 业务地址")
	battleFlag    = flag.String("battle", "", "gateway 战斗通道地址（空则按传输形态取默认：kcp 9003 / ws 9002；ws 形态即单通道地址）")
	inputsFlag    = flag.Int("inputs", 500, "帧输入次数")
	windowFlag    = flag.Duration("window", 5*time.Second, "帧广播观测窗口")
)
