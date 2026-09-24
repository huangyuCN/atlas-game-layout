package serverutil

import (
	"fmt"
	"net"
	"net/url"

	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	"github.com/huangyuCN/atlas/contrib/actor/opgrpc"
	"github.com/huangyuCN/atlas/transport"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	"google.golang.org/grpc"
)

// SchemeGRPCEdge 是 edge 面端点的 scheme（internal 面沿用 SchemeGRPC="grpc"）。
//
// 两个面必须能在端点归集与注册中心里被区分：框架发现解析器按 scheme "grpc" 寻址，
// 故**可信的 internal 面**占用 "grpc"（服务间调用自然落到可信面），对外暴露的 edge 面单列本 scheme。
const SchemeGRPCEdge = "grpc-edge"

// FaceServer 是绑定到某个面的 gRPC 服务端：内嵌 *atlasgrpc.Server（注册服务、查询服务信息、
// 启停直接可用），只覆盖 Endpoint() 的 scheme——两个面共用一个传输实现，但地址必须能区分是谁的。
type FaceServer struct {
	*atlasgrpc.Server
	face   string
	scheme string
}

// Face 返回面名（edge/internal；错误信息与日志用）。
func (s *FaceServer) Face() string { return s.face }

// Endpoint 返回带本面 scheme 的端点 URL（监听地址与底层一致）。
func (s *FaceServer) Endpoint() (*url.URL, error) {
	u, err := s.Server.Endpoint()
	if err != nil || u == nil {
		return u, err
	}
	out := *u
	out.Scheme = s.scheme
	return &out, nil
}

// Transport 返回该面作为 transport.Server 的值：面未启用（nil）时返回 **nil 接口**，
// 以便 fx 值组与 ActiveServers 正确过滤——非 nil 接口持空指针会绕过过滤并在 Start 时 panic。
// （不叫 Server 是因为内嵌字段 *atlasgrpc.Server 已占用该名字。）
func (s *FaceServer) Transport() transport.Server {
	if s == nil {
		return nil
	}
	return s
}

// GRPCFaces 是 server.grpc 两个面的构造结果：地址为空的面未启用，对应字段为 nil。
type GRPCFaces struct {
	Edge     *FaceServer // 不可信区：客户端 op 经网关转发到域 rpc/ 的 Edge 接口
	Internal *FaceServer // 可信区：服务间 gRPC + P7 管理面 AdminService
}

// GRPCServers 按 server.grpc 配置构造两个**互相独立**的 gRPC 服务端：
//   - edge 面监听 edge_addr、internal 面监听 internal_addr，各自独立地址与生命周期；
//   - 地址为空 = 不启用该面（返回 nil，符合 docs/config.md 的唯一缺省约定）；
//   - 两面地址相同 = 配置错误，装配期报错（端口 0 除外：内核分配必然不同）；
//   - 两面都挂 opgrpc 一元拦截器（入站 metadata→ctx 身份、出站错误→gRPC status）与注入的
//     中间件链（一元 + 流式），挂载点只此一处，服务侧无法漏挂。
func GRPCServers(c *configspb.Server_GRPC, mws Middlewares) (GRPCFaces, error) {
	if c == nil {
		return GRPCFaces{}, nil
	}
	if conflictAddr(c.GetEdgeAddr(), c.GetInternalAddr()) {
		return GRPCFaces{}, fmt.Errorf(
			"serverutil: server.grpc 的 edge_addr 与 internal_addr 不得相同（两个面必须独立监听）: %s", c.GetEdgeAddr())
	}
	edge, err := newFaceServer(c, "edge", SchemeGRPCEdge, c.GetEdgeAddr(), mws)
	if err != nil {
		return GRPCFaces{}, err
	}
	internal, err := newFaceServer(c, "internal", SchemeGRPC, c.GetInternalAddr(), mws)
	if err != nil {
		return GRPCFaces{}, err
	}
	return GRPCFaces{Edge: edge, Internal: internal}, nil
}

// RegisterFaces 在已启用的面上注册域 rpc/ 平面实现，并返回两个面的 transport.Server 值
// （未启用的面返回 nil 接口，供 fx 值组与 ActiveServers 过滤）。
//
// Edge 接口 → edge listener、Internal 接口 → internal listener 是**编译期**约束：
// registerEdge/registerInternal 的形参类型即各面接口，把另一个面的实现注册进来无法通过编译。
// 面未启用时对应的注册函数不会被调用，故调用方无需自行判空。
func RegisterFaces[E any, I any](faces GRPCFaces, edge E, internal I,
	registerEdge func(grpc.ServiceRegistrar, E), registerInternal func(grpc.ServiceRegistrar, I),
) (transport.Server, transport.Server) {
	if faces.Edge != nil {
		registerEdge(faces.Edge, edge)
	}
	if faces.Internal != nil {
		registerInternal(faces.Internal, internal)
	}
	return faces.Edge.Transport(), faces.Internal.Transport()
}

// newFaceServer 构造单个面：地址为空 = 该面不启用（返回 nil，不构造服务端、不监听）。
func newFaceServer(c *configspb.Server_GRPC, face, scheme, addr string, mws Middlewares) (*FaceServer, error) {
	if addr == "" {
		return nil, nil
	}
	opts, err := GRPCOptions(c, addr)
	if err != nil {
		return nil, err
	}
	if len(mws) > 0 {
		opts = append(opts, atlasgrpc.Middleware(mws...), atlasgrpc.StreamMiddleware(mws...))
	}
	opts = append(opts, atlasgrpc.UnaryInterceptor(opgrpc.UnaryServerInterceptor()))
	srv, err := build("gRPC "+face+" 面服务端", atlasgrpc.NewServer, opts...)
	if err != nil {
		return nil, err
	}
	return &FaceServer{Server: srv, face: face, scheme: scheme}, nil
}

// conflictAddr 判定两面地址是否真的会互相覆盖：地址为空或不同即不冲突；
// 端口 0 表示「内核分配随机端口」，两面各绑一个必然不同，故也不算冲突
// （进程内/测试形态显式写 127.0.0.1:0，见 docs/config.md）。
func conflictAddr(edge, internal string) bool {
	if edge == "" || internal == "" || edge != internal {
		return false
	}
	if _, port, err := net.SplitHostPort(edge); err == nil && port == "0" {
		return false
	}
	return true
}
