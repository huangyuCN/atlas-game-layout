package gamev1

import (
	"context"
	"errors"
	"testing"

	"github.com/huangyuCN/atlas-game-layout/internal/actortest"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	atlaserrors "github.com/huangyuCN/atlas/errors"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// incoming 构造带指定 metadata 的入站 ctx（模拟 gRPC 调用方携带的身份/发起者）。
func incoming(pairs ...string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(pairs...))
}

// TestRPCAdapterSessionAddressing 验证接入层的会话寻址与 metadata 契约：
// 身份来自 x-atlas-player-id → 组装 player:<id> 的 PID；缺失即拒绝（不回落匿名）。
func TestRPCAdapterSessionAddressing(t *testing.T) {
	t.Run("身份寻址与回执", func(t *testing.T) {
		inv := &actortest.Invoker{Reply: &PlayerDataReply{}}
		adapter := NewPlayerServiceRPCAdapter(inv, relay.RPCAdapterOptions{})
		rep, err := adapter.GetPlayerData(incoming(relay.MetadataPlayerID, "p-1"), &GetPlayerDataReq{})
		if err != nil {
			t.Fatalf("GetPlayerData: %v", err)
		}
		if rep == nil {
			t.Fatal("回执为空")
		}
		if got := inv.AskedPID.String(); got != "player:p-1" {
			t.Fatalf("PID = %q, 期望 player:p-1", got)
		}
	})

	t.Run("缺身份拒绝", func(t *testing.T) {
		adapter := NewPlayerServiceRPCAdapter(&actortest.Invoker{}, relay.RPCAdapterOptions{})
		_, err := adapter.GetPlayerData(context.Background(), &GetPlayerDataReq{})
		if !errors.Is(err, relay.ErrNoSession) {
			t.Fatalf("错误 = %v, 期望保留 ErrNoSession 判定", err)
		}
		// 边界上必须给客户端可判定的结构化 reason（裸 error 过 gRPC 会变 codes.Unknown）。
		se := atlaserrors.FromError(err)
		if se == nil || se.Code != 401 || se.Reason != relay.ReasonNoSession {
			t.Fatalf("结构化错误 = %+v, 期望 401/%s", se, relay.ReasonNoSession)
		}
	})

	t.Run("非法发起者拒绝", func(t *testing.T) {
		adapter := NewPlayerServiceRPCAdapter(&actortest.Invoker{}, relay.RPCAdapterOptions{})
		ctx := incoming(relay.MetadataPlayerID, "p-1", types.SenderHeaderKey, "not-a-pid")
		_, err := adapter.GetPlayerData(ctx, &GetPlayerDataReq{})
		se := atlaserrors.FromError(err)
		if se == nil || se.Code != 400 || se.Reason != relay.ReasonInvalidSender {
			t.Fatalf("结构化错误 = %+v, 期望 400/%s", se, relay.ReasonInvalidSender)
		}
	})
}

// TestRPCAdapterReplyForms 验证回执两种形态：同进程具体消息与跨节点 wire 字节
// （后者必须经路由条目 ReplyOf 解码，硬断言具体类型会在跨节点时误报）。
func TestRPCAdapterReplyForms(t *testing.T) {
	t.Run("跨节点 wire 字节", func(t *testing.T) {
		wire, err := proto.Marshal(&PlayerDataReply{})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		adapter := NewPlayerServiceRPCAdapter(&actortest.Invoker{Reply: wire}, relay.RPCAdapterOptions{})
		if _, err := adapter.GetPlayerData(incoming(relay.MetadataPlayerID, "p-1"), &GetPlayerDataReq{}); err != nil {
			t.Fatalf("跨节点回执应解码成功: %v", err)
		}
	})

	t.Run("回执类型不符报专用 reason", func(t *testing.T) {
		adapter := NewPlayerServiceRPCAdapter(&actortest.Invoker{Reply: &RegisterReply{}}, relay.RPCAdapterOptions{})
		_, err := adapter.GetPlayerData(incoming(relay.MetadataPlayerID, "p-1"), &GetPlayerDataReq{})
		if err == nil {
			t.Fatal("回执类型不符应报错")
		}
		if got := atlaserrors.FromError(err).Reason; got != relay.ReasonReplyTypeMismatch {
			t.Fatalf("reason = %q, 期望 %q", got, relay.ReasonReplyTypeMismatch)
		}
	})
}
