package serverutil

import (
	"context"
	"testing"
	"time"

	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	gamev1 "github.com/huangyuCN/atlas-game-layout/api/game/v1"
	gamev1rpc "github.com/huangyuCN/atlas-game-layout/api/game/v1/rpc"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas/contrib/actor/opcall"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// edgeMethodNames 是 game.v1.PlayerService 的 Edge 面方法名（access=CLIENT）。
var edgeMethodNames = []string{
	"Register", "GetPlayerData", "GetBackpack", "EnterMatchQueue", "CancelMatch", "GetMatchStatus",
	"CreateParty", "JoinParty", "LeaveParty", "GetParty", "QueueParty",
}

// internalMethodNames 是 game.v1.PlayerService 的 Internal 面方法名（access=INTERNAL）。
var internalMethodNames = []string{"Login", "Logout", "GrantItem", "GetPlayer"}

// faceStub 同时实现 PlayerServiceEdge 与 PlayerServiceInternal：
// 两个方法面各自回显 ctx 上的调用身份（Register/GetPlayer 读 opcall.CallInfoFrom），
// 用于证明 opgrpc 已挂载（入站 metadata → ctx 身份，生成的 rpc 方法体同此契约）；其余方法只返回空回执。
type faceStub struct{}

// Register 实现 Edge 面：回显 ctx 调用身份（经 opgrpc 从入站 metadata 搬运而来）。
func (faceStub) Register(ctx context.Context, _ *gamev1.RegisterReq) (*gamev1.RegisterReply, error) {
	info, _ := opcall.CallInfoFrom(ctx)
	return &gamev1.RegisterReply{PlayerId: info.PlayerID}, nil
}

// GetPlayerData 实现 Edge 面（回执内容不参与断言）。
func (faceStub) GetPlayerData(context.Context, *gamev1.GetPlayerDataReq) (*gamev1.PlayerDataReply, error) {
	return &gamev1.PlayerDataReply{}, nil
}

// GetBackpack 实现 Edge 面（回执内容不参与断言）。
func (faceStub) GetBackpack(context.Context, *gamev1.GetBackpackReq) (*gamev1.BackpackReply, error) {
	return &gamev1.BackpackReply{}, nil
}

// EnterMatchQueue 实现 Edge 面（回执内容不参与断言）。
func (faceStub) EnterMatchQueue(context.Context, *gamev1.EnterMatchQueueReq) (*gamev1.EnterMatchQueueReply, error) {
	return &gamev1.EnterMatchQueueReply{}, nil
}

// CancelMatch 实现 Edge 面（回执内容不参与断言）。
func (faceStub) CancelMatch(context.Context, *gamev1.CancelMatchReq) (*gamev1.CancelMatchReply, error) {
	return &gamev1.CancelMatchReply{}, nil
}

// GetMatchStatus 实现 Edge 面（回执内容不参与断言）。
func (faceStub) GetMatchStatus(context.Context, *gamev1.GetMatchStatusReq) (*gamev1.MatchStatusReply, error) {
	return &gamev1.MatchStatusReply{}, nil
}

// CreateParty 实现 Edge 面（回执内容不参与断言）。
func (faceStub) CreateParty(context.Context, *gamev1.CreatePartyReq) (*gamev1.PartyReply, error) {
	return &gamev1.PartyReply{}, nil
}

// JoinParty 实现 Edge 面（回执内容不参与断言）。
func (faceStub) JoinParty(context.Context, *gamev1.JoinPartyReq) (*gamev1.PartyReply, error) {
	return &gamev1.PartyReply{}, nil
}

// LeaveParty 实现 Edge 面（回执内容不参与断言）。
func (faceStub) LeaveParty(context.Context, *gamev1.LeavePartyReq) (*gamev1.PartyReply, error) {
	return &gamev1.PartyReply{}, nil
}

// GetParty 实现 Edge 面（回执内容不参与断言）。
func (faceStub) GetParty(context.Context, *gamev1.GetPartyReq) (*gamev1.PartyReply, error) {
	return &gamev1.PartyReply{}, nil
}

// QueueParty 实现 Edge 面（回执内容不参与断言）。
func (faceStub) QueueParty(context.Context, *gamev1.QueuePartyReq) (*gamev1.PartyQueueReply, error) {
	return &gamev1.PartyQueueReply{}, nil
}

// Login 实现 Internal 面（回执内容不参与断言）。
func (faceStub) Login(context.Context, *gamev1.LoginReq) (*gamev1.LoginReply, error) {
	return &gamev1.LoginReply{}, nil
}

// Logout 实现 Internal 面（回执内容不参与断言）。
func (faceStub) Logout(context.Context, *gamev1.LogoutMsg) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

// GrantItem 实现 Internal 面（回执内容不参与断言）。
func (faceStub) GrantItem(context.Context, *gamev1.GrantItemReq) (*gamev1.GrantItemReply, error) {
	return &gamev1.GrantItemReply{}, nil
}

// GetPlayer 实现 Internal 面：回显 ctx 调用身份（经 opgrpc 从入站 metadata 搬运而来）。
func (faceStub) GetPlayer(ctx context.Context, _ *gamev1.GetPlayerReq) (*gamev1.PlayerReply, error) {
	info, _ := opcall.CallInfoFrom(ctx)
	return &gamev1.PlayerReply{Player: &commonv1.PlayerSummary{PlayerId: info.PlayerID}}, nil
}

// serviceMethods 返回某个面上指定服务的已注册方法名集合（服务不存在返回 nil）。
func serviceMethods(srv *FaceServer, service string) map[string]bool {
	info, ok := srv.GetServiceInfo()[service]
	if !ok {
		return nil
	}
	out := make(map[string]bool, len(info.Methods))
	for _, m := range info.Methods {
		out[m.Name] = true
	}
	return out
}

// TestGRPCServersFacesIsolateServices 验证域 rpc/ 平面的注册按面隔离：
// Edge 接口只出现在 edge 面、Internal 接口只出现在 internal 面（互相不得出现对方的方法名）。
func TestGRPCServersFacesIsolateServices(t *testing.T) {
	const service = "game.v1.PlayerService"
	faces, err := GRPCServers(&configspb.Server_GRPC{EdgeAddr: "127.0.0.1:0", InternalAddr: "127.0.0.1:0"}, nil)
	if err != nil {
		t.Fatalf("GRPCServers() 错误 = %v", err)
	}
	stub := faceStub{}
	gamev1rpc.RegisterPlayerServiceEdge(faces.Edge, stub)
	gamev1rpc.RegisterPlayerServiceInternal(faces.Internal, stub)

	edge, internal := serviceMethods(faces.Edge, service), serviceMethods(faces.Internal, service)
	for _, name := range edgeMethodNames {
		if !edge[name] {
			t.Errorf("edge 面缺少 Edge 方法 %s", name)
		}
		if internal[name] {
			t.Errorf("internal 面出现了 Edge 方法 %s（注册串面）", name)
		}
	}
	for _, name := range internalMethodNames {
		if !internal[name] {
			t.Errorf("internal 面缺少 Internal 方法 %s", name)
		}
		if edge[name] {
			t.Errorf("edge 面出现了 INTERNAL 方法 %s（可信面泄漏到不可信区）", name)
		}
	}
	if len(edge) != len(edgeMethodNames) || len(internal) != len(internalMethodNames) {
		t.Fatalf("方法数 = (edge %d, internal %d)，期望 (%d, %d)",
			len(edge), len(internal), len(edgeMethodNames), len(internalMethodNames))
	}
}

// TestGRPCServersFacesRejectCrossCalls 验证跨面调用真实失败：
// 在 edge 面调 Internal 方法、在 internal 面调 Edge 方法都必须 Unimplemented
// （端口隔离即信任边界：不是靠约定，而是服务清单里根本没有对方的方法）。
func TestGRPCServersFacesRejectCrossCalls(t *testing.T) {
	faces, err := GRPCServers(&configspb.Server_GRPC{EdgeAddr: "127.0.0.1:0", InternalAddr: "127.0.0.1:0"}, nil)
	if err != nil {
		t.Fatalf("GRPCServers() 错误 = %v", err)
	}
	stub := faceStub{}
	gamev1rpc.RegisterPlayerServiceEdge(faces.Edge, stub)
	gamev1rpc.RegisterPlayerServiceInternal(faces.Internal, stub)
	edgeConn := dialFace(t, faces.Edge)
	internalConn := dialFace(t, faces.Internal)

	cases := []struct {
		name   string
		conn   *grpc.ClientConn
		method string
		req    any
	}{
		{"edge 面调 INTERNAL 方法 Login", edgeConn, "/game.v1.PlayerService/Login", &gamev1.LoginReq{}},
		{"internal 面调 Edge 方法 Register", internalConn, "/game.v1.PlayerService/Register", &gamev1.RegisterReq{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := c.conn.Invoke(ctx, c.method, c.req, &emptypb.Empty{}); status.Code(err) != codes.Unimplemented {
				t.Fatalf("跨面调用错误码 = %v，期望 Unimplemented（err=%v）", status.Code(err), err)
			}
		})
	}
}

// TestGRPCServersFacesMountOpgrpc 验证两个面都挂了 opgrpc 一元拦截器：
// 入站 metadata 的身份经拦截器写入 ctx，方法体（stub）能读到。
func TestGRPCServersFacesMountOpgrpc(t *testing.T) {
	faces, err := GRPCServers(&configspb.Server_GRPC{EdgeAddr: "127.0.0.1:0", InternalAddr: "127.0.0.1:0"}, nil)
	if err != nil {
		t.Fatalf("GRPCServers() 错误 = %v", err)
	}
	stub := faceStub{}
	gamev1rpc.RegisterPlayerServiceEdge(faces.Edge, stub)
	gamev1rpc.RegisterPlayerServiceInternal(faces.Internal, stub)

	const playerID = "p-opgrpc"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, opcall.MetadataPlayerID, playerID)

	edgeReply := &gamev1.RegisterReply{}
	if err := dialFace(t, faces.Edge).Invoke(ctx, "/game.v1.PlayerService/Register", &gamev1.RegisterReq{}, edgeReply); err != nil {
		t.Fatalf("edge 面调用失败: %v", err)
	}
	if edgeReply.GetPlayerId() != playerID {
		t.Errorf("edge 面 ctx 玩家身份 = %q，期望 %q（opgrpc 未挂载？）", edgeReply.GetPlayerId(), playerID)
	}

	internalReply := &gamev1.PlayerReply{}
	if err := dialFace(t, faces.Internal).Invoke(ctx, "/game.v1.PlayerService/GetPlayer", &gamev1.GetPlayerReq{}, internalReply); err != nil {
		t.Fatalf("internal 面调用失败: %v", err)
	}
	if got := internalReply.GetPlayer().GetPlayerId(); got != playerID {
		t.Errorf("internal 面 ctx 玩家身份 = %q，期望 %q（opgrpc 未挂载？）", got, playerID)
	}
}

// dialFace 启动该面（若未启动）并返回指向它的客户端连接（测试结束自动关闭）。
func dialFace(t *testing.T, srv *FaceServer) *grpc.ClientConn {
	t.Helper()
	ep := startFace(t, srv)
	conn, err := grpc.NewClient(ep.Host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("拨号 %s 面失败: %v", srv.Face(), err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
