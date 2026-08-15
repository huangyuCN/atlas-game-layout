// loadtest 帧通道压测客户端：对比 KCP 与 WS 战斗通道的帧上行延迟与帧广播吞吐。
//
// 场景：双玩家入局（driver 实测 + dummy 陪跑），driver 顺序发送 N 个帧输入，
// 统计每次 SendFrameInput 的往返延迟（client → gateway → battle actor 帧通道）；
// 再统计 T 秒窗口内收到的帧广播数量（fps）与广播延迟（收到时刻 - 帧 server_time）。
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

	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	kcpt "github.com/huangyuCN/atlas/transport/kcp"
	tcpt "github.com/huangyuCN/atlas/transport/tcp"
	wst "github.com/huangyuCN/atlas/transport/websocket"
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

// player 是压测客户端：认证（TCP）+ 战斗通道（KCP/WS）。
type player struct {
	id     string
	token  string
	battle battleAPI
	step   byte

	started chan string // 开局通知（battleID）
	mu      sync.Mutex
	frames  []time.Duration // 帧广播延迟样本（收到时刻 - server_time）
	end     chan string
}

// battleAPI 是战斗协议客户端最小接口。
type battleAPI interface {
	JoinBattle(context.Context, *gatewayv1.JoinBattleRequest) (*gatewayv1.JoinBattleReply, error)
	SendFrameInput(context.Context, *gatewayv1.SendFrameInputRequest) (*gatewayv1.SendFrameInputReply, error)
}

// newPlayer 建立认证 + 战斗通道并注册登录。
// 开局通知在绑定战斗通道前经业务通道（TCP）回退下发，故两路都挂接监听。
func newPlayer(ctx context.Context, tcpAddr, battleAddr, transport string) (*player, error) {
	tcpCli, err := tcpt.NewClient(tcpAddr)
	if err != nil {
		return nil, fmt.Errorf("tcp: %w", err)
	}
	auth := gatewayv1.NewGatewayAuthTCPClient(tcpCli)
	account := fmt.Sprintf("lt-%s-%d", transport, time.Now().UnixNano())
	reg, err := auth.Register(ctx, &gatewayv1.RegisterRequest{Account: account, Password: "pw"})
	if err != nil {
		return nil, fmt.Errorf("注册: %w", err)
	}
	login, err := auth.Login(ctx, &gatewayv1.LoginRequest{PlayerId: reg.GetPlayerId(), Password: "pw"})
	if err != nil {
		return nil, fmt.Errorf("登录: %w", err)
	}
	p := &player{
		id:      login.GetPlayerId(),
		token:   login.GetToken(),
		started: make(chan string, 2),
		end:     make(chan string, 2),
	}
	tcpCli.OnNotify(p.watch)
	switch transport {
	case "ws":
		cli, err := wst.NewClient(ctx, battleAddr)
		if err != nil {
			return nil, fmt.Errorf("ws: %w", err)
		}
		p.battle = gatewayv1.NewGatewayBattleWSClient(cli)
		cli.OnNotify(p.watch)
	default:
		cli, err := kcpt.NewClient(battleAddr)
		if err != nil {
			return nil, fmt.Errorf("kcp: %w", err)
		}
		p.battle = gatewayv1.NewGatewayBattleKCPClient(cli)
		cli.OnNotify(p.watch)
	}
	return p, nil
}

// watch 收集开局通知、帧广播与结束通知。
func (p *player) watch(operation string, payload []byte) {
	switch operation {
	case consts.PushOpMatchStarted:
		var n gatewayv1.MatchStartedNotify
		if err := protojson.Unmarshal(payload, &n); err == nil && n.GetBattleId() != "" {
			select {
			case p.started <- n.GetBattleId():
			default:
			}
		}
	case consts.PushOpFrameBroadcast:
		var fb gatewayv1.FrameBroadcast
		if err := protojson.Unmarshal(payload, &fb); err != nil || fb.GetFrame().GetServerTime() == nil {
			return
		}
		latency := time.Since(fb.GetFrame().GetServerTime().AsTime())
		p.mu.Lock()
		p.frames = append(p.frames, latency)
		p.mu.Unlock()
	case consts.PushOpBattleEnd:
		p.end <- "end"
	}
}

// queueTwo 双玩家入队并等待双方开局通知（battleID 一致）。
func queueTwo(ctx context.Context, matcherAddr string, ps ...*player) (string, error) {
	conn, err := atlasgrpc.DialInsecure(ctx, atlasgrpc.WithEndpoint(matcherAddr))
	if err != nil {
		return "", fmt.Errorf("matcher: %w", err)
	}
	defer conn.Close()
	svc := matcherv1.NewMatcherClient(conn)
	for i, p := range ps {
		if _, err := svc.QueueMatch(ctx, &matcherv1.QueueMatchRequest{
			PlayerId: p.id,
			Player:   &commonv1.PlayerSummary{PlayerId: p.id, Level: int32(10 + i)},
		}); err != nil {
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
		join, err := p.battle.JoinBattle(ctx, &gatewayv1.JoinBattleRequest{
			Token: p.token, PlayerId: p.id, BattleId: battleID,
		})
		if err == nil && join.GetOk() {
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
	matcherAddr := *matcherFlag
	battleID, err := queueTwo(ctx, matcherAddr, driver, dummy)
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

	// 帧输入往返延迟：driver 顺序发送 N 个帧输入。
	// payload 步进 0（原地）：避免提前冲线结算，保证广播窗口期战斗持续。
	var latencies []float64
	start := time.Now()
	for i := 0; i < *inputsFlag; i++ {
		send := time.Now()
		if _, err := driver.battle.SendFrameInput(ctx, &gatewayv1.SendFrameInputRequest{
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

	// 帧广播窗口：观测 window 秒。
	time.Sleep(*windowFlag)
	samples := driver.frameLatencies()
	frameMs := make([]float64, 0, len(samples))
	for _, s := range samples {
		frameMs = append(frameMs, float64(s.Microseconds())/1000)
	}
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
	transportFlag = flag.String("transport", "kcp", "战斗通道：kcp/ws")
	gwFlag        = flag.String("gw", "127.0.0.1:9001", "gateway TCP 地址")
	battleFlag    = flag.String("battle", "", "gateway 战斗通道地址（空则按传输形态取默认：kcp 9003 / ws 9002）")
	matcherFlag   = flag.String("matcher", "127.0.0.1:9200", "matcher gRPC 地址")
	inputsFlag    = flag.Int("inputs", 500, "帧输入次数")
	windowFlag    = flag.Duration("window", 5*time.Second, "帧广播观测窗口")
)
