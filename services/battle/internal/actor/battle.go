// Package actor 提供 battle 服务的 actor 装配：BattleActor（集群懒激活）承载
// 一局战斗：内嵌 lockstep 会话（帧引擎）+ 帧广播下发 + 结算落库与事件（M7）。
package actor

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz/simulator"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/data/models"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/data/repo"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/pubsub"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	lockstepimpl "github.com/huangyuCN/atlas/contrib/lockstep"
	"github.com/huangyuCN/atlas/lockstep"
	"google.golang.org/protobuf/proto"
)

// Config 是战斗装配参数（Props 工厂用）。
type Config struct {
	TickInterval  int64  // 帧间隔（纳秒）
	TrackLen      int32  // 赛道长度（胜负判定）
	MaxFrames     uint64 // 帧数上限
	SnapshotEvery uint64 // 快照周期（帧）
}

// 默认战斗参数（示例竞速对局：10fps、5 格赛道、60 帧上限）。
const (
	DefaultTickInterval  = int64(100 * time.Millisecond)
	DefaultTrackLen      = int32(5)
	DefaultMaxFrames     = uint64(60)
	DefaultSnapshotEvery = uint64(10)
)

// DefaultConfig 返回默认战斗参数。
func DefaultConfig() Config {
	return Config{
		TickInterval:  DefaultTickInterval,
		TrackLen:      DefaultTrackLen,
		MaxFrames:     DefaultMaxFrames,
		SnapshotEvery: DefaultSnapshotEvery,
	}
}

// Runtime 是 BattleActor 依赖的最小 actor 运行时接口：
// pkg/actor.Runtime（集群）与 core.LocalRuntime（单测）均满足。
type Runtime interface {
	Register(props core.Props) error
	Spawn(ctx context.Context, pid types.PID) (core.Ref, error)
	Stop(ctx context.Context, pid types.PID) error
	Tell(ctx context.Context, pid types.PID, msg any) error
	Ask(ctx context.Context, pid types.PID, req any) (any, error)
}

// Props 是 BattleActor 的注册规格（SpawnAuto 懒激活，matcher 开局拉起）。
type Props struct {
	Rt         Runtime
	Registry   *pubsub.Registry
	Storage    lockstep.Storage // 帧数据持久化（按 sessionID 分片，可共享）
	ResultRepo repo.ResultRepo
	Notifier   biz.BattleNotifier
	Publisher  biz.SettlePublisher
	Cfg        Config
}

// DefaultDeps 是默认战斗装配依赖（server fx 装配与 assemble 可编程装配共用）。
type DefaultDeps struct {
	Rt         Runtime
	Registry   *pubsub.Registry
	ResultRepo repo.ResultRepo
	Notifier   biz.BattleNotifier
	Publisher  biz.SettlePublisher
}

// NewRuntimeProps 以给定战斗参数组装 Props：
// MemoryStorage 按 sessionID 分片可共享，参数默认值由调用方给出（DefaultConfig）。
func NewRuntimeProps(d DefaultDeps, cfg Config) core.Props {
	return NewProps(Props{
		Rt:         d.Rt,
		Registry:   d.Registry,
		Storage:    lockstepimpl.NewMemoryStorage(),
		ResultRepo: d.ResultRepo,
		Notifier:   d.Notifier,
		Publisher:  d.Publisher,
		Cfg:        cfg,
	})
}

// NewProps 构造 BattleActor 注册规格。
func NewProps(p Props) core.Props {
	return core.Props{
		Type: consts.ActorTypeBattle,
		NewHandler: func(pid types.PID) core.Handler {
			return &BattleActor{
				pid:        pid,
				rt:         p.Rt,
				registry:   p.Registry,
				storage:    p.Storage,
				resultRepo: p.ResultRepo,
				notifier:   p.Notifier,
				publisher:  p.Publisher,
				cfg:        p.Cfg,
				players:    make(map[string]struct{}),
			}
		},
		SpawnMode: core.SpawnAuto,
		Tell:      pkgactor.DefaultTellChain(),
		Ask:       pkgactor.DefaultAskChain(),
	}
}

// BattleActor 是一局战斗的业务外壳：
// 帧引擎由内嵌 lockstep 会话（lockstep:<battleID>）承担。
type BattleActor struct {
	pid        types.PID
	cctx       core.ActorContext
	rt         Runtime
	registry   *pubsub.Registry
	resultRepo repo.ResultRepo
	notifier   biz.BattleNotifier
	publisher  biz.SettlePublisher
	cfg        Config

	battleID   string
	matchID    string
	storage    lockstep.Storage
	players    map[string]struct{} // 参战玩家
	sessionPID types.PID           // lockstep 会话 PID（每局唯一 type 保证 sim 隔离）
	settled    bool
}

// OnStart 实现 core.Handler：拉起 lockstep 会话（每局独立 type 保证 sim 状态隔离）并订阅帧广播。
func (b *BattleActor) OnStart(ctx core.ActorContext) error {
	b.cctx = ctx
	b.battleID = ctx.Self().UID()
	// contrib 的 NewSessionProps 按 type 注册且 sim 为共享单例：
	// 多局并发时同 type 会共享 sim 状态，故每局注册唯一 type（lockstep+短哈希）。
	// 代价是进程内 type 注册表随对局数增长（示例取舍；生产建议为 contrib 增加 sim 工厂）。
	sessionType := lockstepType(b.battleID)
	sessionCfg := lockstepimpl.SessionConfig{
		SessionID:      b.battleID,
		Mode:           lockstep.ModeServerAuthoritative,
		TickInterval:   time.Duration(b.cfg.TickInterval),
		MaxPlayers:     2,
		SnapshotEvery:  lockstep.FrameID(b.cfg.SnapshotEvery),
		EnableRollback: false,
	}
	props := lockstepimpl.NewSessionProps(sessionCfg, simulator.New(b.cfg.TrackLen, b.cfg.MaxFrames), b.storage, b.registry)
	props.Type = sessionType
	if err := b.rt.Register(props); err != nil {
		return fmt.Errorf("actor: 注册 lockstep 会话失败: %w", err)
	}
	b.sessionPID, _ = types.NewPID(sessionType, b.battleID)
	if _, err := b.rt.Spawn(ctx.Context(), b.sessionPID); err != nil {
		return fmt.Errorf("actor: 拉起 lockstep 会话失败: %w", err)
	}
	return b.registry.Subscribe(frameTopic(b.battleID), ctx.Self())
}

// lockstepType 从战斗 ID 派生每局唯一的 lockstep 类型名：
// 取 FNV-1a 哈希的 8 位十六进制后缀，保证任意战斗 ID 都映射到合法类型段（[a-z][a-z0-9_]*）。
func lockstepType(battleID string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(battleID))
	return fmt.Sprintf("lockstep%08x", h.Sum32())
}

// OnStop 实现 core.Handler：停 lockstep 会话（目录归属释放）。
func (b *BattleActor) OnStop(ctx core.ActorContext, _ types.ExitReason) error {
	_ = b.rt.Stop(ctx.Context(), b.sessionPID)
	return nil
}

// OnTell 实现 core.Handler：帧广播到达 → 下发客户端 + 胜负检查与结算。
func (b *BattleActor) OnTell(ctx core.ActorContext, msg any) error {
	result, ok := msg.(lockstep.FrameResult)
	if !ok {
		return nil
	}
	// 帧广播下发全部参战玩家（v1 链路：nats → gateway → 客户端）。
	frame := &locksteppb.LockstepFrame{
		FrameId:  uint64(result.Frame),
		Snapshot: snapshotMeta(result.Snapshot),
	}
	for _, in := range result.Inputs {
		frame.Inputs = append(frame.Inputs, toPBInput(in))
	}
	for player := range b.players {
		_ = b.notifier.PublishFrame(ctx.Context(), player, b.battleID, frame)
	}
	// 快照携带胜负状态：分出胜负即结算。
	if result.Snapshot != nil && !b.settled {
		b.checkSettle(ctx, result)
	}
	return nil
}

// checkSettle 从快照解出胜负：有胜者即结算（落库 + 事件 + 自停）。
func (b *BattleActor) checkSettle(ctx core.ActorContext, result lockstep.FrameResult) {
	var st simulator.State
	if err := json.Unmarshal(result.Snapshot.State, &st); err != nil || st.Winner == "" {
		return
	}
	b.settled = true
	res := &models.BattleResult{
		BattleID:    b.battleID,
		MatchID:     b.matchID,
		TotalFrames: uint64(result.Frame),
		SettledAt:   time.Now().UnixMilli(),
	}
	for player := range b.players {
		res.Players = append(res.Players, models.PlayerResult{
			PlayerID: player,
			Win:      player == st.Winner,
			Score:    st.Scores[player],
		})
	}
	if err := b.resultRepo.Save(ctx.Context(), res); err != nil {
		ctx.Logger().Error("battle: 结算落库失败", "battle", b.battleID, "err", err)
	}
	ev := &battlev1.BattleSettledEvent{BattleId: b.battleID, MatchId: b.matchID}
	for _, p := range res.Players {
		ev.Players = append(ev.Players, &battlev1.PlayerResult{PlayerId: p.PlayerID, Win: p.Win, Score: p.Score})
	}
	_ = b.publisher.PublishSettled(ctx.Context(), ev)
	// 战斗结束通知下发全部参战玩家（含胜者），随后自停。
	for player := range b.players {
		_ = b.notifier.PublishEnd(ctx.Context(), player, b.battleID, st.Winner)
	}
	ctx.Stop(types.ExitNormal())
}

// OnAsk 实现 core.Handler：信封分发（开局/加入/帧输入/补帧/状态）。
func (b *BattleActor) OnAsk(ctx core.ActorContext, req any) (any, error) {
	env, err := decodeEnvelope(req)
	if err != nil {
		return nil, err
	}
	switch k := env.GetKind().(type) {
	case *battlev1.BattleActorMsg_Create:
		return b.onCreate(ctx, k.Create)
	case *battlev1.BattleActorMsg_Join:
		return b.onJoin(ctx, k.Join)
	case *battlev1.BattleActorMsg_FrameInput:
		return b.onFrameInput(ctx, k.FrameInput)
	case *battlev1.BattleActorMsg_Reconnect:
		return b.onReconnect(ctx, k.Reconnect)
	case *battlev1.BattleActorMsg_GetState:
		return b.onGetState(ctx)
	default:
		return nil, fmt.Errorf("actor: 未知战斗消息")
	}
}

// onCreate 开局：登记参战玩家（matcher 成局后调用）。
func (b *BattleActor) onCreate(ctx core.ActorContext, req *battlev1.CreateBattleRequest) (any, error) {
	b.matchID = req.GetMatchId()
	for _, p := range req.GetPlayerIds() {
		b.players[p] = struct{}{}
	}
	return &battlev1.CreateBattleReply{BattleId: b.battleID}, nil
}

// onJoin 玩家加入：登记 + lockstep JoinSession + 快照回执（断线重连恢复）。
func (b *BattleActor) onJoin(ctx core.ActorContext, req *battlev1.JoinBattleReq) (any, error) {
	playerID := req.GetPlayerId()
	if _, ok := b.players[playerID]; !ok {
		return &battlev1.JoinBattleReply{Ok: false, ErrorReason: "PLAYER_NOT_IN_BATTLE"}, nil
	}
	// lockstep 成员加入（幂等）。
	if err := b.rt.Tell(ctx.Context(), b.sessionPID, lockstep.JoinSession{Player: lockstep.PlayerID(playerID)}); err != nil {
		return nil, fmt.Errorf("actor: 加入会话失败: %w", err)
	}
	reconnect, err := b.askReconnect(ctx, 0)
	if err != nil {
		return nil, err
	}
	return &battlev1.JoinBattleReply{
		Ok:           true,
		Meta:         sessionMeta(b.battleID, b.cfg.TickInterval),
		CurrentFrame: reconnect.GetCurrentFrame(),
		Snapshot:     reconnect.GetSnapshot(),
	}, nil
}

// onFrameInput 帧输入：转发 lockstep 会话。
func (b *BattleActor) onFrameInput(ctx core.ActorContext, req *battlev1.FrameInputReq) (any, error) {
	if _, ok := b.players[req.GetPlayerId()]; !ok {
		return &battlev1.FrameInputReply{}, nil
	}
	in := req.GetInput()
	err := b.rt.Tell(ctx.Context(), b.sessionPID, lockstep.PlayerInput{
		Player:  lockstep.PlayerID(req.GetPlayerId()),
		Frame:   lockstep.FrameID(in.GetFrameId()),
		Payload: in.GetPayload(),
	})
	if err != nil {
		return nil, fmt.Errorf("actor: 帧输入失败: %w", err)
	}
	return &battlev1.FrameInputReply{}, nil
}

// onReconnect 断线重连：按参战名单复核玩家后回执补帧。
func (b *BattleActor) onReconnect(ctx core.ActorContext, req *battlev1.ReconnectReq) (any, error) {
	if _, ok := b.players[req.GetPlayerId()]; !ok {
		return &battlev1.ReconnectReply{Ok: false}, nil
	}
	reconnect, err := b.askReconnect(ctx, req.GetLastSeenFrame())
	if err != nil {
		return nil, err
	}
	return &battlev1.ReconnectReply{
		Ok:           true,
		CurrentFrame: reconnect.GetCurrentFrame(),
		Snapshot:     reconnect.GetSnapshot(),
		Missed:       reconnect.GetMissed(),
	}, nil
}

// onGetState 状态查询（grpc 管理接口转发）。
func (b *BattleActor) onGetState(ctx core.ActorContext) (any, error) {
	// 同节点会话：lockstep 消息为对象直传（非跨节点 proto 信封）。
	reply, err := b.rt.Ask(ctx.Context(), b.sessionPID, lockstep.QueryStats{})
	if err != nil {
		return nil, fmt.Errorf("actor: 状态查询失败: %w", err)
	}
	stats, ok := reply.(lockstep.SessionStats)
	if !ok {
		return nil, fmt.Errorf("actor: 状态回执类型 %T 不符", reply)
	}
	return &battlev1.GetStateReply{
		Ok:           true,
		State:        stateName(b.settled),
		CurrentFrame: uint64(stats.CurrentFrame),
		PlayerCount:  uint32(stats.PlayerCount),
	}, nil
}

// askReconnect 向 lockstep 会话查询补帧信息（同节点对象直传）。
func (b *BattleActor) askReconnect(ctx core.ActorContext, lastSeen uint64) (*battlev1.ReconnectReply, error) {
	raw, err := b.rt.Ask(ctx.Context(), b.sessionPID, lockstep.ReconnectRequest{
		LastSeenFrame: lockstep.FrameID(lastSeen),
	})
	if err != nil {
		return nil, fmt.Errorf("actor: 补帧查询失败: %w", err)
	}
	reply, ok := raw.(lockstep.ReconnectResponse)
	if !ok {
		return nil, fmt.Errorf("actor: 补帧回执类型 %T 不符", raw)
	}
	out := &battlev1.ReconnectReply{Ok: true, CurrentFrame: uint64(reply.CurrentFrame)}
	if reply.Snapshot != nil {
		out.Snapshot = &locksteppb.SnapshotMeta{
			FrameId:    uint64(reply.Snapshot.Frame),
			StateHash:  hashBytes(reply.Snapshot.Hash),
			SizeBytes:  uint64(len(reply.Snapshot.State)),
			StorageKey: snapshotKey(b.battleID),
		}
	}
	for _, ins := range reply.MissedInputs {
		group := &locksteppb.FrameInputs{}
		for _, in := range ins {
			group.Inputs = append(group.Inputs, toPBInput(in))
		}
		out.Missed = append(out.Missed, group)
	}
	return out, nil
}

// stateName 映射战斗状态描述。
func stateName(settled bool) string {
	if settled {
		return "settled"
	}
	return "running"
}

// decodeEnvelope 还原消息信封（跨节点字节 / 同节点对象）。
func decodeEnvelope(req any) (*battlev1.BattleActorMsg, error) {
	switch v := req.(type) {
	case *battlev1.BattleActorMsg:
		return v, nil
	case []byte:
		env := new(battlev1.BattleActorMsg)
		if err := proto.Unmarshal(v, env); err != nil {
			return nil, fmt.Errorf("actor: 战斗消息解码失败: %w", err)
		}
		return env, nil
	default:
		return nil, fmt.Errorf("actor: 不支持的战斗消息类型 %T", req)
	}
}

// frameTopic 是 lockstep 帧广播主题。
func frameTopic(battleID string) string { return "frame." + battleID }

// snapshotKey 是快照存储键（内存存储仅标识用）。
func snapshotKey(battleID string) string { return "snap/" + battleID }

// snapshotMeta 组装快照元信息。
func snapshotMeta(snap *lockstep.Snapshot) *locksteppb.SnapshotMeta {
	if snap == nil {
		return nil
	}
	return &locksteppb.SnapshotMeta{
		FrameId:   uint64(snap.Frame),
		StateHash: hashBytes(snap.Hash),
		SizeBytes: uint64(len(snap.State)),
	}
}

// hashBytes 把状态哈希编码为 8 字节（大端）。
func hashBytes(h uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, h)
	return buf
}

// sessionMeta 组装会话元信息。
func sessionMeta(battleID string, tickNanos int64) *locksteppb.SessionMeta {
	return &locksteppb.SessionMeta{
		SessionId:        battleID,
		MaxPlayers:       2,
		TickMillis:       uint32(tickNanos / 1e6),
		InputDelayFrames: 0,
		Mode:             locksteppb.LockstepMode_LOCKSTEP_MODE_SERVER_AUTHORITATIVE,
	}
}

// toPBInput 转换 lockstep 输入为协议帧输入（帧广播与补帧共用）。
func toPBInput(in lockstep.Input) *locksteppb.LockstepInput {
	return &locksteppb.LockstepInput{
		FrameId:  uint64(in.Frame),
		PlayerId: string(in.Player),
		Payload:  in.Payload,
	}
}
