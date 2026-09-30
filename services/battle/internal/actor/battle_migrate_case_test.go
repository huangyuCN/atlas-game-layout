// 战斗 actor 迁移用例的断言部分（环境与两节点假件见 battle_migrate_test.go）。

package actor

import (
	"testing"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz"
	locksteppb "github.com/huangyuCN/atlas/api/lockstep"
	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
)

// start 在 node-a 拉起战斗 actor（epoch=1）并登记内存归属表。
func (e *migrateEnv) start(t *testing.T) {
	t.Helper()
	if _, err := e.a.rt.Spawn(newTestContext(), e.pid, core.WithSpawnEpoch(1)); err != nil {
		t.Fatalf("Spawn(%s): %v", nodeA, err)
	}
	e.ops.mu.Lock()
	e.ops.owner[e.pid.String()] = nodeA
	e.ops.epoch[e.pid.String()] = 1
	e.ops.mu.Unlock()
}

// seedAndJoin 在 node-a 开局并让双人加入（参战名单 p-a / p-b）。
func (e *migrateEnv) seedAndJoin(t *testing.T) {
	t.Helper()
	ctx := newTestContext()
	if _, err := e.a.rt.Ask(ctx, e.pid, &battlev1.CreateBattleRequest{
		MatchId: "m-mig", PlayerIds: []string{"p-a", "p-b"}}); err != nil {
		t.Fatalf("开局: %v", err)
	}
	for _, p := range []string{"p-a", "p-b"} {
		if _, err := e.a.rt.Ask(ctx, e.pid, &battlev1.JoinBattleReq{BattleId: migrateBID},
			core.WithSender(mustPlayerPID(t, p))); err != nil {
			t.Fatalf("加入 %s: %v", p, err)
		}
	}
}

// mustPlayerPID 组装发起者 PID（帧输入与加入的身份载体）。
func mustPlayerPID(t *testing.T, playerID string) types.PID {
	t.Helper()
	pid, err := types.NewPID(playerType, playerID)
	if err != nil {
		t.Fatalf("sender PID %s: %v", playerID, err)
	}
	return pid
}

// sendInputs 在指定节点投递 n 帧输入（a 前进、b 原地）。
func sendInputs(t *testing.T, node *migrateNode, pid types.PID, from, n uint64) {
	t.Helper()
	ctx := newTestContext()
	for i := uint64(0); i < n; i++ {
		frame := from + i
		for _, p := range []string{"p-a", "p-b"} {
			step := byte(1)
			if p == "p-b" {
				step = 0
			}
			err := node.rt.Tell(ctx, pid, &battlev1.FrameInputReq{BattleId: migrateBID,
				Input: &locksteppb.LockstepInput{FrameId: frame, PlayerId: p, Payload: []byte{step}}},
				core.WithSender(mustPlayerPID(t, p)))
			if err != nil {
				t.Fatalf("帧输入 %s@%d: %v", p, frame, err)
			}
		}
	}
}

// stateOn 查询指定节点上战斗 actor 的状态。
func stateOn(t *testing.T, node *migrateNode, pid types.PID) *battlev1.GetStateReply {
	t.Helper()
	raw, err := node.rt.Ask(newTestContext(), pid, &battlev1.GetStateReq{BattleId: migrateBID})
	if err != nil {
		t.Fatalf("GetState(%s): %v", node.id, err)
	}
	reply, ok := raw.(*battlev1.GetStateReply)
	if !ok {
		t.Fatalf("GetState 回执类型 %T 不符", raw)
	}
	return reply
}

// maxFrame 返回某节点已广播的最大帧号（无广播返回 0）。
func maxFrame(node *migrateNode, playerID string) uint64 {
	node.pusher.mu.Lock()
	defer node.pusher.mu.Unlock()
	var maxID uint64
	for _, f := range node.pusher.frames[playerID] {
		if f.GetFrameId() > maxID {
			maxID = f.GetFrameId()
		}
	}
	return maxID
}

// waitFrameGrow 等待某节点的帧广播越过 want（迁移后帧继续推进的断言）。
func waitFrameGrow(t *testing.T, node *migrateNode, playerID string, want uint64) uint64 {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := maxFrame(node, playerID); got > want {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s 的帧广播未越过 %d（当前 %d）", node.id, want, maxFrame(node, playerID))
	return 0
}

// syncFramesOn 在指定节点发起补帧查询。
func syncFramesOn(t *testing.T, node *migrateNode, pid types.PID, lastSeen uint64) *battlev1.SyncFramesReply {
	t.Helper()
	raw, err := node.rt.Ask(newTestContext(), pid,
		&battlev1.SyncFramesReq{BattleId: migrateBID, LastSeenFrame: lastSeen},
		core.WithSender(mustPlayerPID(t, "p-a")))
	if err != nil {
		t.Fatalf("SyncFrames(%s): %v", node.id, err)
	}
	reply, ok := raw.(*battlev1.SyncFramesReply)
	if !ok {
		t.Fatalf("SyncFrames 回执类型 %T 不符", raw)
	}
	return reply
}

// notifyOfflineOn 在指定节点投递断开事件。
func notifyOfflineOn(t *testing.T, node *migrateNode, pid types.PID, playerID string) {
	t.Helper()
	if err := node.rt.Tell(newTestContext(), pid, biz.PlayerOffline{PlayerID: playerID}); err != nil {
		t.Fatalf("断开消息 %s@%s: %v", playerID, node.id, err)
	}
}

// outsOn 返回指定节点上某玩家收到的出局广播。
func outsOn(node *migrateNode, playerID string) []outRecord {
	node.pusher.mu.Lock()
	defer node.pusher.mu.Unlock()
	return append([]outRecord(nil), node.pusher.outs[playerID]...)
}

// TestMigrateBattleContinuesFramesOnNewOwner 验证迁移后新属主继续推进帧、
// 名单与帧号一致（规格 §8/§11 第 4 项）。
func TestMigrateBattleContinuesFramesOnNewOwner(t *testing.T) {
	env := newMigrateEnv(t, nil, testBattleConfig())
	env.start(t)
	env.seedAndJoin(t)

	env.sendInputsAt(t, env.a, 1, 6)
	before := stateOn(t, env.a, env.pid)
	if before.GetPlayerCount() != 2 {
		t.Fatalf("迁移前参战人数 %d，期望 2", before.GetPlayerCount())
	}
	if frame := waitFrameGrow(t, env.a, "p-a", 0); frame == 0 {
		t.Fatal("迁移前应有帧广播")
	}

	out, err := env.inner.Migrate(newTestContext(), migrateBID, nodeB)
	if err != nil {
		t.Fatalf("迁移失败: %v", err)
	}
	if out.ToNode != nodeB || out.FromNode != nodeA || out.Epoch <= 1 {
		t.Fatalf("迁移结果不符: %+v", out)
	}
	if _, alive := env.a.rt.Stats(env.pid); alive {
		t.Fatal("旧节点仍持有战斗 actor（drain 未生效）")
	}

	after := stateOn(t, env.b, env.pid)
	if after.GetPlayerCount() != 2 {
		t.Fatalf("迁移后参战人数 %d，期望 2", after.GetPlayerCount())
	}
	if after.GetCurrentFrame() < before.GetCurrentFrame() {
		t.Fatalf("帧号回退：迁移前 %d，迁移后 %d", before.GetCurrentFrame(), after.GetCurrentFrame())
	}
	if after.GetCurrentFrame() > before.GetCurrentFrame()+2 {
		t.Fatalf("帧号跳跃异常（疑似从第 0 帧重开）：迁移前 %d，迁移后 %d",
			before.GetCurrentFrame(), after.GetCurrentFrame())
	}

	env.sendInputsAt(t, env.b, after.GetCurrentFrame()+1, 4)
	grown := waitFrameGrow(t, env.b, "p-a", after.GetCurrentFrame())
	if grown <= after.GetCurrentFrame() {
		t.Fatalf("新属主未继续推进帧：%d", grown)
	}
	assertSyncConsistent(t, env, before.GetCurrentFrame(), after.GetCurrentFrame())
}

// assertSyncConsistent 断言补帧回执与迁移后的快照帧一致（规格 §11 第 4 项）。
func assertSyncConsistent(t *testing.T, env *migrateEnv, lastSeen, wantFrame uint64) {
	t.Helper()
	reply := syncFramesOn(t, env.b, env.pid, lastSeen)
	if reply.GetCurrentFrame() < wantFrame {
		t.Fatalf("补帧回执帧号 %d 早于迁移后帧号 %d", reply.GetCurrentFrame(), wantFrame)
	}
	if reply.GetSnapshot() == nil {
		t.Fatal("补帧回执缺少快照")
	}
	if reply.GetSnapshot().GetFrameId() > reply.GetCurrentFrame() {
		t.Fatalf("快照帧 %d 晚于当前帧 %d", reply.GetSnapshot().GetFrameId(), reply.GetCurrentFrame())
	}
	if lastSeen < reply.GetCurrentFrame() && len(reply.GetMissed()) == 0 {
		t.Fatal("补帧区间非空但未回带缺失帧输入")
	}
}

// TestMigrateWindowHoldsOfflineJudgement 验证迁移窗口内掉线不判负、窗口关闭后重新起算（规格 §9.6）。
func TestMigrateWindowHoldsOfflineJudgement(t *testing.T) {
	presence := newFakePresence() // 全部不在场（断开后无存活直连）
	env := newMigrateEnv(t, presence, offlineCfg(2))
	env.start(t)
	env.seedAndJoin(t)

	if _, err := env.a.rt.Ask(newTestContext(), env.pid,
		&battlev1.PrepareBattleMigrationRequest{BattleId: migrateBID}); err != nil {
		t.Fatalf("进入迁移窗口: %v", err)
	}
	notifyOfflineOn(t, env.a, env.pid, "p-a")
	time.Sleep(3 * time.Duration(offlineCfg(2).OfflineTimeout))
	if outs := outsOn(env.a, "p-b"); len(outs) != 0 {
		t.Fatalf("迁移窗口内不应判负，却收到出局广播: %+v", outs)
	}
	if inRosterOn(t, env.a, env.pid, "p-a") != true {
		t.Fatal("迁移窗口内玩家不应被移出名单")
	}

	if _, err := env.a.rt.Ask(newTestContext(), env.pid,
		&battlev1.ResumeBattleMigrationRequest{BattleId: migrateBID}); err != nil {
		t.Fatalf("退出迁移窗口: %v", err)
	}
	waitOutOn(t, env.a, "p-b", "p-a")
}

// inRosterOn 判断玩家是否仍在参战名单（经 GetState 的人数间接判定：名单含 2 人）。
func inRosterOn(t *testing.T, node *migrateNode, pid types.PID, _ string) bool {
	t.Helper()
	return stateOn(t, node, pid).GetPlayerCount() == 2
}

// waitOutOn 等待某玩家收到关于 outPlayer 的出局广播。
func waitOutOn(t *testing.T, node *migrateNode, playerID, outPlayer string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, rec := range outsOn(node, playerID) {
			if rec.playerID == outPlayer {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s 未收到 %s 的出局广播", playerID, outPlayer)
}

// TestMigrateRejectsStaleEpochState 验证 epoch 栅栏：source_epoch 不小于本实例 epoch 的
// 状态一律拒绝（目录 Claim 单调抬 epoch，陈旧状态恢复会造成状态回退）。
func TestMigrateRejectsStaleEpochState(t *testing.T) {
	env := newMigrateEnv(t, nil, testBattleConfig())
	stale := &battlev1.BattleMigrationState{
		BattleId: migrateBID, Players: []string{"p-a"}, SourceEpoch: 99, SnapshotFrame: 7,
	}
	if err := env.b.rt.Tell(newTestContext(), inboxPID(t, nodeB),
		&battlev1.StageBattleMigrationRequest{State: stale}); err != nil {
		t.Fatalf("预置陈旧状态: %v", err)
	}
	if _, err := env.b.rt.Spawn(newTestContext(), env.pid, core.WithSpawnEpoch(1)); err == nil {
		t.Fatal("陈旧 epoch 的迁移状态应被拒绝恢复")
	}
}

// TestMigrateWithoutStateStillActivates 验证无状态迁移的降级语义：
// 目标节点收到激活请求但没有预置状态时，actor 正常从零启动（不报错、不卡死）。
func TestMigrateWithoutStateStillActivates(t *testing.T) {
	env := newMigrateEnv(t, nil, testBattleConfig())
	if _, err := env.b.rt.Spawn(newTestContext(), env.pid, core.WithSpawnEpoch(2)); err != nil {
		t.Fatalf("无状态激活不应失败: %v", err)
	}
	if got := stateOn(t, env.b, env.pid); got.GetCurrentFrame() != 0 {
		t.Fatalf("无状态启动应从第 0 帧开始，实际 %d", got.GetCurrentFrame())
	}
}
