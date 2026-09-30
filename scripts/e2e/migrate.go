// migrate.go 提供迁移形态（-mode migrate，规格 §8 / §11 第 4 项）：
// **两 battle 节点 + 接入层**，对局中触发一次战斗 actor 迁移，断言客户端经接入层
// 重连到新属主、SyncFrames 补帧后帧号一致、不判负、不卡死。
// 损伤形态见 damage.go。
package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	battlev1opclient "github.com/huangyuCN/atlas-game-layout/api/battle/v1/opclient"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	battleassemble "github.com/huangyuCN/atlas-game-layout/services/battle/assemble"
	edgeassemble "github.com/huangyuCN/atlas-game-layout/services/edge/assemble"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/contrib/edge"
	wst "github.com/huangyuCN/atlas/transport/websocket"
	"google.golang.org/protobuf/proto"
)

// 迁移形态的两个节点 ID（node-a 是 startStack 起的默认 battle 节点）。
const (
	migrateNodeA = "battle-e2e"
	migrateNodeB = "battle-e2e-2"
)

// migrateOpts 是迁移/损伤两形态的参数。
type migrateOpts struct {
	edgeAddr     string        // 接入层 WS 面地址（与 battle 的 edge_endpoints 一致）
	targetNode   string        // 迁移目标节点
	lossPercent  int           // 损伤形态：丢包百分比
	jitter       time.Duration // 损伤形态：抖动上限
	blackout     time.Duration // 损伤形态：断网时长
	preFrames    uint64        // 迁移前先打多少帧（触发迁移的时点）
	edgeTicketKB []byte        // 票据密钥（接入层与 battle 同值）
}

// edgePlayer 是一条**可重连**的接入层帧连接：接入层拆流后按同一地址重新 hello
// （生产路径：客户端只会看到一次重连，见规格 §8）。
type edgePlayer struct {
	tag     string
	addr    string
	slot    string
	watcher *directWatcher

	mu    sync.Mutex
	cli   *wst.Client
	dials atomic.Int64
}

// newEdgePlayer 建立一条接入层帧连接并返回可复用的直连句柄。
func newEdgePlayer(ctx context.Context, addr, slot, tag string) (*directClient, *edgePlayer, error) {
	ep := &edgePlayer{tag: tag, addr: addr, slot: slot, watcher: newDirectWatcher()}
	if err := ep.dial(ctx); err != nil {
		return nil, nil, err
	}
	return &directClient{tag: tag, watcher: ep.watcher, invoke: ep.invoke,
		notify: func(func(string, []byte)) {}, close: ep.close}, ep, nil
}

// dial 按接入层地址建立（或重建）WS 帧连接：票据走 X-Atlas-Ticket 头（接入层握手面），
// 逐帧会话槽沿用同一张票（battle 帧面验票并发起直连登记）。
func (e *edgePlayer) dial(ctx context.Context) error {
	hdr := http.Header{}
	hdr.Set(edge.TicketHeaderKey, e.slot)
	cli, err := wst.NewClient(ctx, "ws://"+e.addr+"/",
		wst.WithClientSessionProvider(func() string { return e.slot }),
		wst.ClientHeader(hdr))
	if err != nil {
		return fmt.Errorf("%s 经接入层连接失败（%s）: %w", e.tag, e.addr, err)
	}
	e.mu.Lock()
	old := e.cli
	e.cli = cli
	e.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	cli.OnNotify(e.watcher.watch)
	e.dials.Add(1)
	return nil
}

// invoke 在当前连接上发起帧 op（不含连接的往返重试，重连由用例显式驱动）。
func (e *edgePlayer) invoke(ctx context.Context, op string, req, resp any) error {
	e.mu.Lock()
	cli := e.cli
	e.mu.Unlock()
	if cli == nil {
		return fmt.Errorf("%s 当前无接入层连接", e.tag)
	}
	return cli.Invoke(ctx, op, req, resp)
}

// close 关闭当前连接（幂等）。
func (e *edgePlayer) close() error {
	e.mu.Lock()
	cli := e.cli
	e.cli = nil
	e.mu.Unlock()
	if cli == nil {
		return nil
	}
	return cli.Close()
}

// startMigrateStack 在基础四服务之上再起第二个 battle 节点与接入层（迁移形态专用）。
// 接入面固定在 battle 配置下发的地址（127.0.0.1:7100），因此该形态不可与本机其它占用冲突。
func startMigrateStack(ctx context.Context, mw middlewareAddrs, o directOpts, mo migrateOpts) (*stack, error) {
	st, err := startStack(mw, o)
	if err != nil {
		return nil, err
	}
	key, err := base64.StdEncoding.DecodeString(e2eTicketKey)
	if err != nil {
		return st, fmt.Errorf("e2e 票据密钥非法: %w", err)
	}
	batB, err := battleassemble.New(ctx, battleassemble.Options{
		NodeID: migrateNodeB, EtcdEndpoints: mw.etcdEndpoints, NatsURL: mw.natsURL,
		MongoURI: mw.mongoURI, MongoDB: mw.mongoDB, Namespace: e2eNS,
		TicketKey: e2eTicketKey, TicketTTL: e2eTicketTTL, EdgeEndpoints: e2eEdgeEndpoints,
	})
	if err != nil {
		st.stop()
		return nil, fmt.Errorf("启动第二个 battle 节点失败: %w", err)
	}
	st.stops = append(st.stops, batB.Stop)
	st.batB = batB

	ed, err := edgeassemble.New(ctx, edgeassemble.Options{
		Namespace: e2eNS, EtcdEndpoints: mw.etcdEndpoints, TicketKey: key,
		Listeners: []edge.Listener{{Name: "ws", Network: edge.NetworkTCP,
			Address: mo.edgeAddr, Carrier: edge.CarrierWSUpgrade}},
	})
	if err != nil {
		st.stop()
		return nil, fmt.Errorf("启动接入层失败（地址 %s 可能被占用）: %w", mo.edgeAddr, err)
	}
	st.stops = append(st.stops, ed.Stop)
	st.edge = ed
	return st, nil
}

// triggerMigrate 经**迁移收件箱 actor**（当前属主节点）触发一次战斗 actor 迁移：
// 这是管理面入口的进程内形态，走的是与线上同一套编排（drain → activate → verify + 状态搬运）。
// 两个 battle 节点对 matcher 都是候选，故先按目录读当前属主，再迁到另一个节点
// （收件箱必须寻址到属主节点本机，否则编排器拿不到本地目录视图）。
func (s *stack) triggerMigrate(ctx context.Context, battleID, prefer string) (*battlev1.MigrateBattleReply, string, error) {
	battlePID, err := types.NewPID(consts.ActorTypeBattle, battleID)
	if err != nil {
		return nil, "", err
	}
	snapshot, err := s.bat.Runtime.Ops().LookupSnapshot(ctx, []types.PID{battlePID})
	if err != nil || len(snapshot) == 0 {
		return nil, "", fmt.Errorf("读战斗 %s 的目录归属失败: %w", battleID, err)
	}
	from := snapshot[0].OwnerNode
	if from == "" {
		return nil, "", fmt.Errorf("战斗 %s 当前无属主节点", battleID)
	}
	target := prefer
	if target == from {
		target = migrateNodeA
		if from == migrateNodeA {
			target = migrateNodeB
		}
	}
	owner, err := s.runtimeOf(from)
	if err != nil {
		return nil, "", err
	}
	inbox, err := types.NewPID(consts.ActorTypeBattleMigrate, from)
	if err != nil {
		return nil, "", err
	}
	raw, err := owner.Ask(ctx, inbox, &battlev1.MigrateBattleRequest{BattleId: battleID, TargetNode: target})
	if err != nil {
		return nil, "", fmt.Errorf("触发迁移失败: %w", err)
	}
	reply, err := decodeMigrateReply(raw)
	if err != nil {
		return nil, "", err
	}
	return reply, target, nil
}

// decodeMigrateReply 归一 Ask 回执：同节点直投得到具体消息，跨节点得到线格式字节。
func decodeMigrateReply(raw any) (*battlev1.MigrateBattleReply, error) {
	if reply, ok := raw.(*battlev1.MigrateBattleReply); ok {
		return reply, nil
	}
	payload, ok := raw.([]byte)
	if !ok {
		return nil, fmt.Errorf("迁移回执类型 %T 不符", raw)
	}
	reply := &battlev1.MigrateBattleReply{}
	if err := proto.Unmarshal(payload, reply); err != nil {
		return nil, fmt.Errorf("解码迁移回执失败: %w", err)
	}
	return reply, nil
}

// runtimeOf 返回指定节点的 actor 运行时（迁移形态只有两个节点）。
func (s *stack) runtimeOf(node string) (*pkgactor.Runtime, error) {
	switch node {
	case migrateNodeA:
		return s.bat.Runtime, nil
	case migrateNodeB:
		if s.batB == nil {
			return nil, fmt.Errorf("第二个 battle 节点未启动，无法在 %s 上执行迁移", node)
		}
		return s.batB.Runtime, nil
	default:
		return nil, fmt.Errorf("未知 battle 节点 %q", node)
	}
}

// runMigrate 迁移形态闭环。
func runMigrate(ctx context.Context, st *stack, a addrs, mo migrateOpts, frames uint64) error {
	ps, err := connectDirectPlayers(a.tcp)
	if err != nil {
		return err
	}
	battleID, tickets, err := matchAndCollectTickets(ctx, ps)
	if err != nil {
		return err
	}
	clients, eps, err := connectEdgePlayers(ctx, ps, tickets, mo.edgeAddr)
	if err != nil {
		return err
	}
	defer closeDirectPlayers(clients)

	before, err := warmUpBattle(ctx, clients, battleID, mo.preFrames)
	if err != nil {
		return err
	}
	reply, target, err := st.triggerMigrate(ctx, battleID, mo.targetNode)
	if err != nil {
		return err
	}
	fmt.Printf("[迁移] 属主 %s → %s（epoch=%d）\n", reply.GetFromNode(), reply.GetToNode(), reply.GetEpoch())
	if reply.GetToNode() != target || reply.GetFromNode() == reply.GetToNode() {
		return fmt.Errorf("迁移目标不符: from=%s to=%s want=%s", reply.GetFromNode(), reply.GetToNode(), target)
	}
	// 拆流断言：属主变更后接入层必须把该局的流拆干净（规格 §8）。
	if err := waitActiveStreams(st, 0); err != nil {
		return err
	}
	fmt.Println("[拆流] 接入层已拆除该局全部流（活跃流归零）")
	if err := reconnectAll(ctx, clients, eps, battleID, before); err != nil {
		return err
	}
	if err := waitActiveStreams(st, len(clients)); err != nil {
		return err
	}
	return assertResumedTraffic(ctx, clients, battleID, frames)
}

// warmUpBattle 入局并先打一段帧（迁移发生在对局进行中），返回迁移前的帧号。
func warmUpBattle(ctx context.Context, clients [2]*directClient, battleID string, preFrames uint64) (uint64, error) {
	for i, dc := range clients {
		if _, err := edgeJoin(ctx, dc, battleID); err != nil {
			return 0, err
		}
		if err := sendDirectInputs(ctx, dc, battleID, preFrames, stepOf(i)); err != nil {
			return 0, err
		}
	}
	before, err := edgeJoin(ctx, clients[0], battleID)
	if err != nil {
		return 0, err
	}
	fmt.Printf("[迁移] 迁移前帧号=%d battle=%s\n", before, battleID)
	return before, nil
}

// stepOf 返回客户端序号对应的帧步长（A 前进 1，B 原地）。
func stepOf(i int) byte {
	if i == 0 {
		return 1
	}
	return 0
}

// connectEdgePlayers 为每个玩家建立经接入层的帧连接（票据来自成局推送）。
func connectEdgePlayers(ctx context.Context, ps [2]*player, tickets map[string][]byte,
	edgeAddr string) ([2]*directClient, [2]*edgePlayer, error) {
	var clients [2]*directClient
	var eps [2]*edgePlayer
	for i, p := range ps {
		raw, ok := tickets[p.id]
		if !ok {
			return clients, eps, fmt.Errorf("%s 没有本局票据", p.id)
		}
		dc, ep, err := newEdgePlayer(ctx, edgeAddr, base64.RawURLEncoding.EncodeToString(raw), p.id)
		if err != nil {
			return clients, eps, err
		}
		clients[i], eps[i] = dc, ep
	}
	fmt.Printf("[接入层] 双客户端经 %s 建立帧连接（WS 面）\n", edgeAddr)
	return clients, eps, nil
}

// edgeJoin 入局并返回服务端当前帧号（JoinBattle + SyncFrames）。
func edgeJoin(ctx context.Context, dc *directClient, battleID string) (uint64, error) {
	var join battlev1.JoinBattleReply
	if err := dc.invoke(ctx, battlev1opclient.BattleServiceProtocolOps.JoinBattle,
		&battlev1.JoinBattleReq{BattleId: battleID}, &join); err != nil {
		return 0, fmt.Errorf("%s 入局失败: %w", dc.tag, err)
	}
	var sync battlev1.SyncFramesReply
	if err := dc.invoke(ctx, battlev1opclient.BattleServiceProtocolOps.SyncFrames,
		&battlev1.SyncFramesReq{BattleId: battleID, LastSeenFrame: 0}, &sync); err != nil {
		return 0, fmt.Errorf("%s 补帧失败: %w", dc.tag, err)
	}
	return sync.GetCurrentFrame(), nil
}

// waitActiveStreams 等待接入层活跃流数达到期望值（拆流/重连的确定性断言）。
func waitActiveStreams(st *stack, want int) error {
	if st.edge == nil || st.edge.Proxy == nil {
		return fmt.Errorf("接入层句柄不可用，无法断言拆流")
	}
	deadline := time.Now().Add(10 * time.Second)
	var got int
	for time.Now().Before(deadline) {
		if got = st.edge.Proxy.ActiveStreams(); got == want {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("接入层活跃流数未达到期望：got=%d want=%d（属主变更未触发拆流或未重连）", got, want)
}

// reconnectAll 按**同一接入层地址 + 同一张票**重连并断言续接：
// 帧号不因迁移回退、补帧快照可用、补帧区间与当前帧一致。
func reconnectAll(ctx context.Context, clients [2]*directClient, eps [2]*edgePlayer,
	battleID string, before uint64) error {
	for i, dc := range clients {
		if err := eps[i].dial(ctx); err != nil {
			return err
		}
		after, err := edgeJoin(ctx, dc, battleID)
		if err != nil {
			return err
		}
		if after < before {
			return fmt.Errorf("%s 迁移后帧号回退：迁移前 %d，重连后 %d", dc.tag, before, after)
		}
		var sync battlev1.SyncFramesReply
		if err := dc.invoke(ctx, battlev1opclient.BattleServiceProtocolOps.SyncFrames,
			&battlev1.SyncFramesReq{BattleId: battleID, LastSeenFrame: before}, &sync); err != nil {
			return fmt.Errorf("%s 重连后补帧失败: %w", dc.tag, err)
		}
		if sync.GetSnapshot() == nil {
			return fmt.Errorf("%s 重连补帧回执缺少快照", dc.tag)
		}
		if sync.GetCurrentFrame() < before {
			return fmt.Errorf("%s 补帧回执帧号 %d 早于迁移前 %d", dc.tag, sync.GetCurrentFrame(), before)
		}
		fmt.Printf("[重连] %s 落到新属主，帧号 %d（迁移前 %d）\n", dc.tag, sync.GetCurrentFrame(), before)
	}
	if eps[0].dials.Load() < 2 || eps[1].dials.Load() < 2 {
		return fmt.Errorf("客户端未发生重连（拨号次数 %d/%d）", eps[0].dials.Load(), eps[1].dials.Load())
	}
	return nil
}

// assertResumedTraffic 断言迁移后帧继续推进（不卡死）且没有被判负（规格 §9.6）：
// 重连后继续输入 → 双方帧广播计数增长 → 全程无出局通知。
func assertResumedTraffic(ctx context.Context, clients [2]*directClient, battleID string, frames uint64) error {
	base := [2]int64{clients[0].watcher.frames.Load(), clients[1].watcher.frames.Load()}
	for i, dc := range clients {
		if err := sendDirectInputs(ctx, dc, battleID, frames, stepOf(i)); err != nil {
			return fmt.Errorf("%s 重连后帧输入失败: %w", dc.tag, err)
		}
	}
	for i, dc := range clients {
		if err := waitFrameCount(dc, base[i]); err != nil {
			return err
		}
		if n := dc.watcher.outs.Load(); n > 0 {
			return fmt.Errorf("%s 收到 %d 条出局通知（迁移拆流被误判为掉线）", dc.tag, n)
		}
	}
	fmt.Printf("[迁移] 重连后帧广播继续推进 A=%d→%d B=%d→%d；无出局通知（未误判掉线）\n",
		base[0], clients[0].watcher.frames.Load(), base[1], clients[1].watcher.frames.Load())
	fmt.Println("闭环通过（migrate 形态）")
	return nil
}

// waitFrameCount 等待该客户端的帧广播数超过 base（迁移后不卡死的判据）。
func waitFrameCount(dc *directClient, base int64) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if dc.watcher.frames.Load() > base {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("%s 迁移后帧广播停滞（仍为 %d）", dc.tag, base)
}
