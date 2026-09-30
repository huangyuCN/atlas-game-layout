// client.go 是压测客户端：业务通道走 SDK 会话（注册/登录/入队/收成局通知），
// 战斗帧走**直连帧面**（经接入层或直连 battle），沿途样本交给统计器。
//
// 阶段 3 批次 5 起战斗帧不再经网关：负载驱动用框架传输客户端 + 帧槽票据
// （接入层 hello/flow-id 由 edgeconn.go 承担），SDK 只负责业务链路与取票。
package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync/atomic"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-game-layout/api/battle/v1/opclient"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gamev1opclient "github.com/huangyuCN/atlas-game-layout/api/game/v1/opclient"
	gatewayv1 "github.com/huangyuCN/atlas-game-layout/api/gateway/v1"
	"github.com/huangyuCN/atlas-game-layout/scripts/sdksession"
	sdkclient "github.com/huangyuCN/atlas-sdk-go/client"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/contrib/edge"
	udpt "github.com/huangyuCN/atlas/transport/udp"
	wst "github.com/huangyuCN/atlas/transport/websocket"
	"google.golang.org/protobuf/encoding/protojson"
)

// frameClient 是一条直连帧连接的统一接缝：WS 面与 UDP 面、经接入层与直连 battle 都在此收口。
type frameClient interface {
	// Invoke 发起一次帧 op（一元调用，有回执的 op 才计入往返样本）。
	Invoke(ctx context.Context, operation string, req, resp any) error
	// OnNotify 挂接服务端推送分发。
	OnNotify(fn func(operation string, payload []byte))
	// Close 关闭连接（幂等）。
	Close() error
}

// wsFrameClient 把框架 WS 客户端适配为 frameClient（剥掉可选调用参数）。
type wsFrameClient struct{ cli *wst.Client }

// Invoke 发起一次帧 op。
func (w wsFrameClient) Invoke(ctx context.Context, operation string, req, resp any) error {
	return w.cli.Invoke(ctx, operation, req, resp)
}

// OnNotify 挂接推送分发。
func (w wsFrameClient) OnNotify(fn func(operation string, payload []byte)) { w.cli.OnNotify(fn) }

// Close 关闭连接。
func (w wsFrameClient) Close() error { return w.cli.Close() }

// dialOpts 是帧连接的装配参数（形态与地址由 main 按旗标算出）。
type dialOpts struct {
	via       string // edge（经接入层）| direct（直连 battle 帧端口）
	transport string // ws | udp
	edgeWS    string // 接入层 WS 面地址（host:port）
	edgeUDP   string // 接入层 UDP 面地址（host:port）
	battleWS  string // battle WS 帧端口（直连对照用）
	battleUDP string // battle UDP 帧端口（直连对照用）
}

// loadPlayer 是一个压测客户端：业务会话 + 直连帧连接 + 样本采集。
type loadPlayer struct {
	tag  string
	id   string
	sess *sdkclient.Session
	biz  *sdkclient.Client
	ops  *gamev1opclient.PlayerService

	started  chan *gamev1.MatchStartedNotify
	frame    frameClient
	battleID string
	ticket   []byte

	inputs     atomic.Int64  // 已发出的帧输入数（含失败）
	broadcasts atomic.Int64  // 已收到的帧广播数
	lastFrame  atomic.Uint64 // 已见到的最新帧号（补帧探针据此请求增量，避免回执无限增大）
	outs       atomic.Int64  // 出局通知数（压测中必须为 0：不误判掉线）
	failures   atomic.Int64  // 帧链路失败次数（连接失败/调用失败）

	handshake *samples // 入局（JoinBattle）往返
	probe     *samples // SyncFrames 探针往返
	downlink  *samples // 帧广播下行（服务端 server_time → 收到）
}

// newLoadPlayer 建立业务会话（注册/登录）并挂接成局通知；帧连接在拿到票据后另建。
func newLoadPlayer(ctx context.Context, gatewayTCP, tag string) (*loadPlayer, error) {
	sess := sdksession.NewSession(sdkclient.WithSessionHeartbeatInterval(10 * time.Second))
	opts := append([]sdkclient.Option{
		sdkclient.WithHeartbeatInterval(5 * time.Second),
	}, sess.ChannelOptions()...)
	cli, err := sdkclient.Dial(gatewayTCP, opts...)
	if err != nil {
		return nil, fmt.Errorf("%s 业务通道连接失败: %w", tag, err)
	}
	if err := sess.Bind(cli); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("%s 会话绑定失败: %w", tag, err)
	}
	p := &loadPlayer{
		tag: tag, sess: sess, biz: cli,
		ops:       gamev1opclient.NewPlayerService(sess),
		started:   make(chan *gamev1.MatchStartedNotify, 4),
		handshake: &samples{}, probe: &samples{}, downlink: &samples{},
	}
	cli.On(gamev1opclient.PlayerServicePushOps.MatchStartedNotify, p.watch)
	if err := p.registerLogin(ctx); err != nil {
		_ = cli.Close()
		return nil, err
	}
	return p, nil
}

// registerLogin 注册并登录（账号按标签与纳秒时间戳唯一），登录后 id 取会话里的玩家 ID。
func (p *loadPlayer) registerLogin(ctx context.Context) error {
	account := fmt.Sprintf("lt-%s-%d", p.tag, time.Now().UnixNano())
	rawReg, err := p.sess.Register(ctx, &gatewayv1.RegisterRequest{
		Account: account, Password: "pw", Nickname: account,
	})
	if err != nil {
		return fmt.Errorf("%s 注册失败: %w", p.tag, err)
	}
	reg, err := sdksession.ReplyAs[*gatewayv1.RegisterReply](rawReg)
	if err != nil {
		return fmt.Errorf("%s 注册回执异常: %w", p.tag, err)
	}
	if _, err := p.sess.Login(ctx, &gatewayv1.LoginRequest{PlayerId: reg.GetPlayerId(), Password: "pw"}); err != nil {
		return fmt.Errorf("%s 登录失败: %w", p.tag, err)
	}
	p.id = p.sess.PlayerID()
	return nil
}

// watch 只收取成局通知（本人票据 + 本局 battle_id 是直连的唯一来源）。
func (p *loadPlayer) watch(operation string, payload []byte) {
	if operation != gamev1opclient.PlayerServicePushOps.MatchStartedNotify {
		return
	}
	var n gamev1.MatchStartedNotify
	if protojson.Unmarshal(payload, &n) != nil || n.GetBattleId() == "" {
		return
	}
	p.ticket = append([]byte(nil), n.GetBattleTicket()...)
	select {
	case p.started <- &n:
	default:
	}
}

// enterQueue 经业务通道入队（1v1 casual；属性由服务端权威填充）。
func (p *loadPlayer) enterQueue(ctx context.Context) error {
	if _, err := p.ops.EnterMatchQueue(ctx, &gamev1.EnterMatchQueueReq{Ruleset: "casual"}); err != nil {
		return fmt.Errorf("%s 入队失败: %w", p.tag, err)
	}
	return nil
}

// waitStarted 等待成局通知（取 battle_id 与本人票据）；票据缺失即显式失败。
func (p *loadPlayer) waitStarted(ctx context.Context, timeout time.Duration) error {
	select {
	case n := <-p.started:
		if len(n.GetBattleTicket()) == 0 {
			return fmt.Errorf("%s 成局通知缺 battle_ticket", p.tag)
		}
		p.battleID = n.GetBattleId()
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("%s 未在 %v 内收到成局通知", p.tag, timeout)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// dialFrame 按形态建立直连帧连接（票据同时进接入层 hello 段与逐帧会话槽）。
func (p *loadPlayer) dialFrame(ctx context.Context, o dialOpts) error {
	if len(p.ticket) == 0 {
		return fmt.Errorf("%s 没有本局票据", p.tag)
	}
	slot := base64.RawURLEncoding.EncodeToString(p.ticket)
	cli, err := dialFrameByOpts(ctx, o, slot, p.ticket)
	if err != nil {
		p.failures.Add(1)
		return fmt.Errorf("%s 帧连接失败（%s/%s）: %w", p.tag, o.via, o.transport, err)
	}
	cli.OnNotify(p.onNotify)
	p.frame = cli
	return nil
}

// dialFrameByOpts 按「经接入层/直连 × WS/UDP」四象限建立帧连接。
func dialFrameByOpts(ctx context.Context, o dialOpts, slot string, ticket []byte) (frameClient, error) {
	switch {
	case o.via == "edge" && o.transport == "ws":
		hdr := http.Header{}
		hdr.Set(edge.TicketHeaderKey, slot)
		cli, err := wst.NewClient(ctx, "ws://"+o.edgeWS+"/",
			wst.WithClientSessionProvider(func() string { return slot }), wst.ClientHeader(hdr))
		if err != nil {
			return nil, err
		}
		return wsFrameClient{cli: cli}, nil
	case o.via == "edge" && o.transport == "udp":
		return dialEdgeUDP(ctx, o.edgeUDP, slot, ticket)
	case o.via == "direct" && o.transport == "ws":
		cli, err := wst.NewClient(ctx, "ws://"+o.battleWS+"/",
			wst.WithClientSessionProvider(func() string { return slot }))
		if err != nil {
			return nil, err
		}
		return wsFrameClient{cli: cli}, nil
	case o.via == "direct" && o.transport == "udp":
		cli, err := udpt.NewClient(o.battleUDP, udpt.WithClientSessionProvider(func() string { return slot }))
		if err != nil {
			return nil, err
		}
		return cli, nil
	default:
		return nil, fmt.Errorf("未知帧连接形态 via=%q transport=%q", o.via, o.transport)
	}
}

// join 入局（JoinBattle 有回执，故计入握手往返样本）并做一次补帧调用。
func (p *loadPlayer) join(ctx context.Context) error {
	start := time.Now()
	var join battlev1.JoinBattleReply
	if err := p.frame.Invoke(ctx, battlev1opclient.BattleServiceProtocolOps.JoinBattle,
		&battlev1.JoinBattleReq{BattleId: p.battleID}, &join); err != nil {
		p.failures.Add(1)
		return fmt.Errorf("%s 入局失败: %w", p.tag, err)
	}
	p.handshake.add(time.Since(start))
	if join.GetMeta().GetSessionId() != p.battleID {
		return fmt.Errorf("%s 入局回执不符: %+v", p.tag, join.GetMeta())
	}
	return p.probeOnce(ctx)
}

// onNotify 分发帧面推送：帧广播计入下行样本，出局通知单独计数（压测中应为 0）。
func (p *loadPlayer) onNotify(operation string, payload []byte) {
	switch operation {
	case battlev1opclient.BattleServicePushOps.FrameBroadcast:
		var fb battlev1.FrameBroadcast
		if protojson.Unmarshal(payload, &fb) != nil {
			return
		}
		p.broadcasts.Add(1)
		if st := fb.GetFrame().GetServerTime(); st != nil {
			p.downlink.add(time.Since(st.AsTime()))
		}
	case battlev1opclient.BattleServicePushOps.PlayerOutNotify:
		p.outs.Add(1)
	}
}

// pumpCfg 是压测负载参数。
type pumpCfg struct {
	pps      int           // 每连接每秒帧输入数（上行包速率）
	probe    time.Duration // SyncFrames 探针间隔（RTT 样本来源）
	duration time.Duration // 稳定负载时长
	jitter   bool          // 探针相位抖动开关（诊断：错开各连接的探针相位）
}

// pump 在 duration 内按目标包速率发帧输入（Tell 无回执），并按 probe 间隔打 RTT 探针。
//
// -probe-jitter 时先随机睡一个间隔再开探针 ticker：512 条连接的探针相位一旦对齐，
// 每秒会出现「512 个 SyncFrames 同刻齐发」的自造排队，p99 会被这一现象主导（诊断口径）。
func (p *loadPlayer) pump(ctx context.Context, cfg pumpCfg) {
	if cfg.jitter && cfg.probe > 0 {
		time.Sleep(rand.N(cfg.probe))
	}
	interval := time.Second / time.Duration(max(cfg.pps, 1))
	inputTick := time.NewTicker(max(interval, time.Millisecond))
	defer inputTick.Stop()
	probeTick := time.NewTicker(cfg.probe)
	defer probeTick.Stop()
	deadline := time.After(cfg.duration)
	seq := uint64(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline:
			return
		case <-inputTick.C:
			seq++
			p.sendInput(ctx, seq)
		case <-probeTick.C:
			_ = p.probeOnce(ctx)
		}
	}
}

// sendInput 发一帧输入：载荷步进 0（原地），避免压测中冲线结算提前结束对局。
func (p *loadPlayer) sendInput(ctx context.Context, seq uint64) {
	p.inputs.Add(1)
	err := p.frame.Invoke(ctx, battlev1opclient.BattleServiceProtocolOps.SendFrameInput,
		&battlev1.FrameInputReq{
			BattleId: p.battleID,
			Input:    &locksteppb.LockstepInput{FrameId: seq, Payload: []byte{0}},
		}, nil)
	if err != nil {
		p.failures.Add(1)
	}
}

// probeOnce 发一次 SyncFrames 探针并记录往返（这是帧面上唯一稳定的「往返延迟」样本来源：
// SendFrameInput 是 Tell 无回执，帧广播只有单向时间戳）。
//
// LastSeenFrame 取**已见到的最新帧号**（增量补帧，与 SDK 的重连补帧语义一致）：
// 若恒填 0，回执会带全部历史帧——数据报面上该回执会超过接入层单包上限（16KiB）被丢弃，
// 客户端就永远等不到回执（表现为帧面「卡死」）。
func (p *loadPlayer) probeOnce(ctx context.Context) error {
	start := time.Now()
	var sync battlev1.SyncFramesReply
	err := p.frame.Invoke(ctx, battlev1opclient.BattleServiceProtocolOps.SyncFrames,
		&battlev1.SyncFramesReq{BattleId: p.battleID, LastSeenFrame: p.lastFrame.Load()}, &sync)
	if err != nil {
		p.failures.Add(1)
		return err
	}
	if cur := sync.GetCurrentFrame(); cur > p.lastFrame.Load() {
		p.lastFrame.Store(cur)
	}
	p.probe.add(time.Since(start))
	return nil
}

// close 关闭帧连接与业务连接（幂等）。
func (p *loadPlayer) close() {
	if p.frame != nil {
		_ = p.frame.Close()
	}
	if p.biz != nil {
		_ = p.biz.Close()
	}
}
