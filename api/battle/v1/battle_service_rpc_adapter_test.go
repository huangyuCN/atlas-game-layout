package battlev1

import (
	"context"
	"errors"
	"testing"

	"github.com/huangyuCN/atlas-game-layout/internal/actortest"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"google.golang.org/grpc/metadata"
)

// TestRPCAdapterFieldAddressing 验证字段寻址（INTERNAL op 同样需要）：
// battle_id 取自请求字段 → 组装 battle:<id> 的 PID；无身份要求（服务间调用）。
func TestRPCAdapterFieldAddressing(t *testing.T) {
	inv := &actortest.Invoker{Reply: &CreateBattleReply{}}
	adapter := NewBattleServiceRPCAdapter(inv, relay.RPCAdapterOptions{})

	req := &CreateBattleRequest{BattleId: "b-9"}
	if _, err := adapter.Create(context.Background(), req); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := inv.AskedPID.String(); got != "battle:b-9" {
		t.Fatalf("PID = %q, 期望 battle:b-9（取自请求字段）", got)
	}

	// 字段为空即拒绝（不静默落到空 uid）。
	if _, err := adapter.Create(context.Background(), &CreateBattleRequest{}); err == nil {
		t.Fatal("空 battle_id 应被拒绝")
	}

	// 发起者与请求 ID：字段寻址下发起者类型可以与目标 actor 不同（玩家发起战斗），
	// 契约只要求发起者是合法 PID。
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(relay.MetadataPlayerID, "p-1", types.SenderHeaderKey, "player:p-1", relay.MetadataRequestID, "rid-1"))
	if _, err := adapter.Create(ctx, req); err != nil {
		t.Fatalf("带 metadata 的 Create: %v", err)
	}

	// 非法发起者 PID：拒绝且给出可判定的结构化 reason。
	bad := metadata.NewIncomingContext(context.Background(), metadata.Pairs(types.SenderHeaderKey, "not-a-pid"))
	if _, err := adapter.Create(bad, req); !errors.Is(err, relay.ErrInvalidSender) {
		t.Fatalf("非法发起者错误 = %v, 期望 ErrInvalidSender", err)
	}
}
