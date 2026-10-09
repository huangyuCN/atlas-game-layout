package actor

import (
	"context"
	"sync"
	"testing"
	"time"

	battlev1actor "github.com/huangyuCN/atlas-game-layout/api/battle/v1/actor"
	errorv1 "github.com/huangyuCN/atlas-game-layout/api/error/v1"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/biz"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/ledger"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	atlaserrors "github.com/huangyuCN/atlas/errors"
)

// ledgerRecord 是一次结算留档（帧面据此在懒激活之前拒绝迟到 op）。
type ledgerRecord struct {
	battleID string
	winner   string
	players  []string
}

// memLedger 是结算留档端口的内存实现（记录留档次数与内容，供幂等断言）。
type memLedger struct {
	mu      sync.Mutex
	records []ledgerRecord
}

// RecordEnded 实现 biz.SettleLedger：记录一次留档。
func (l *memLedger) RecordEnded(battleID, winner string, players []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, ledgerRecord{
		battleID: battleID, winner: winner, players: append([]string(nil), players...),
	})
}

// snapshot 返回留档快照（副本，避免断言与写入竞态）。
func (l *memLedger) snapshot() []ledgerRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]ledgerRecord(nil), l.records...)
}

// TestSettleRecordsEndedLedger 验证结算把结果留档且只留一次：胜负与参战名单齐全
// （帧面靠它判定「已结束」，缺胜者会让重连补投发出空结果）。
func TestSettleRecordsEndedLedger(t *testing.T) {
	ledger := new(memLedger)
	env := newBattleEnvDeps(t, testBattleConfig(), battleDeps{Ledger: ledger})
	ctx := context.Background()
	seedAndJoin(t, env, ctx)
	sendFrameInputs(t, env, ctx)
	waitSettled(t, env)

	records := ledger.snapshot()
	if len(records) != 1 {
		t.Fatalf("结算留档次数 = %d, 期望 1（结算路径可重入但只留一次）", len(records))
	}
	got := records[0]
	if got.battleID != "b-test01" {
		t.Fatalf("留档对局 = %q, 期望 b-test01", got.battleID)
	}
	if got.winner != "p-a" {
		t.Fatalf("留档胜者 = %q, 期望 p-a", got.winner)
	}
	if len(got.players) != 2 {
		t.Fatalf("留档参战名单 = %v, 期望 2 人（胜负双方都在）", got.players)
	}
}

// TestSettleWithoutLedger 验证未装配留档端口时结算照常（留档是可选依赖，nil 不 panic）。
func TestSettleWithoutLedger(t *testing.T) {
	env := newBattleEnvDeps(t, testBattleConfig(), battleDeps{Ledger: nil})
	ctx := context.Background()
	seedAndJoin(t, env, ctx)
	sendFrameInputs(t, env, ctx)
	waitSettled(t, env)
	assertActorStopped(t, env)
}

// 静态保证：内存留档实现满足 biz 接口。
var _ biz.SettleLedger = (*memLedger)(nil)

// TestSettleWritesSharedLedger 验证结算把留档写进**跨节点**存储，且该留档能被激活闸门读到
// （P1-7）：结算节点写一次、任意节点据此拒绝复活已结束的对局——漏了这笔，落到非结算节点的
// 迟到帧 op / rpc 面调用会按 SpawnAuto 重建空名单实例（A1 缺陷的跨节点形态）。
func TestSettleWritesSharedLedger(t *testing.T) {
	store := ledger.NewMemoryStore()
	env := newBattleEnvDeps(t, testBattleConfig(),
		battleDeps{SharedLedger: store, SharedLedgerTTL: time.Minute})
	ctx := context.Background()
	seedAndJoin(t, env, ctx)
	sendFrameInputs(t, env, ctx)
	waitSettled(t, env)

	entry, ok, err := store.Lookup(ctx, "b-test01")
	if err != nil || !ok {
		t.Fatalf("跨节点留档未写入: ok=%v err=%v", ok, err)
	}
	if entry.Winner != "p-a" || len(entry.Players) != 2 {
		t.Fatalf("跨节点留档内容不符: %+v", entry)
	}
	gate := ledger.NewGate(store, battlev1actor.BattleServiceActorType, nil)
	pid, err := types.NewPID(battlev1actor.BattleServiceActorType, "b-test01")
	if err != nil {
		t.Fatalf("NewPID: %v", err)
	}
	if err := gate.AllowActivation(ctx, pid); !errorv1.IsBattleEnded(err) {
		t.Fatalf("激活闸门未拒绝已结束的对局: err=%v（reason=%s）", err, atlaserrors.Reason(err))
	}
}

// TestSettleWithoutSharedLedger 验证未装配跨节点留档时结算照常（可选依赖，nil 不 panic）。
func TestSettleWithoutSharedLedger(t *testing.T) {
	env := newBattleEnvDeps(t, testBattleConfig(), battleDeps{SharedLedger: nil})
	ctx := context.Background()
	seedAndJoin(t, env, ctx)
	sendFrameInputs(t, env, ctx)
	waitSettled(t, env)
	assertActorStopped(t, env)
}
