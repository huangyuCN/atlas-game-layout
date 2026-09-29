// Package domainclient 提供网关到域服务的调用装配。
//
// 两条路径各归其位：
//   - **客户端业务 op**：按注解生成的路由表条目寻址，走域 `rpc/` 平面的 **Edge 面**
//     （CLIENT 方法只注册在 edge listener），投递由 `opcall.DeliverRemote` 收口，
//     新增 op 网关零代码；
//   - **网关自身的会话联动**（Register/Login/Logout）：走 **Internal 面**的类型化客户端
//     （INTERNAL 方法只注册在 internal listener），调用点写死方法名、编译期可校验。
//
// 两个面在同一个服务实例上注册、由端口隔离区分（信任边界），故拨号时必须显式选面：
// 客户端 op 选 `SchemeGRPCEdge`、服务间调用选 `SchemeGRPC`。
//
// 「proto 服务名 → 注册中心服务名」按模板约定派生：proto 包名首段即注册中心服务名
// （`game.v1.PlayerService` → `game`，见 `lib/consts.Service*` 与各服务 config.yaml）。
package domainclient

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas/registry"
	"google.golang.org/grpc"
)

// dialFunc 拨号一个域服务的 Edge 面（返回连接与关闭函数；测试可注入假拨号）。
type dialFunc func(ctx context.Context, registryService string) (grpc.ClientConnInterface, func() error, error)

// dialTimeout 是单个域服务连接的建立上限：与调用方 ctx 的请求超时解耦——
// 连接是节点级资源（活得比单个请求久），首个 op 的短 deadline 不该把建连也一起掐掉。
const dialTimeout = 5 * time.Second

// connEntry 是一条缓存的连接。
type connEntry struct {
	conn  grpc.ClientConnInterface
	close func() error
}

// Resolver 按 proto 服务名提供域服务 **Edge 面**的 gRPC 连接。
// 连接惰性建立并按服务缓存：启动期不要求对端在线，首个 op 到达时才拨号。
type Resolver struct {
	dial dialFunc

	mu    sync.Mutex
	conns map[string]connEntry
}

// NewResolver 构造连接解析器（只保存装配参数，不拨号）。
func NewResolver(discovery registry.Discovery, mws serverutil.ClientMiddlewares) *Resolver {
	return newResolver(func(ctx context.Context, registryService string) (grpc.ClientConnInterface, func() error, error) {
		conn, err := serverutil.DialDomain(ctx, discovery, registryService, serverutil.SchemeGRPCEdge, mws)
		if err != nil {
			return nil, nil, err
		}
		return conn, conn.Close, nil
	})
}

// newResolver 以注入的拨号函数构造解析器（包内测试用）。
func newResolver(dial dialFunc) *Resolver {
	return &Resolver{dial: dial, conns: make(map[string]connEntry)}
}

// Conn 实现 opgrpc.ConnResolver：proto 服务名 → 注册中心服务名 → Edge 面连接（缓存复用）。
func (r *Resolver) Conn(ctx context.Context, service string) (grpc.ClientConnInterface, error) {
	name, err := registryNameOf(service)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry, ok := r.conns[service]; ok {
		return entry.conn, nil
	}
	dialCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dialTimeout)
	defer cancel()
	conn, closeFn, err := r.dial(dialCtx, name)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, fmt.Errorf("domainclient: 拨号 %s（%s 面）返回空连接", name, serverutil.SchemeGRPCEdge)
	}
	r.conns[service] = connEntry{conn: conn, close: closeFn}
	return conn, nil
}

// Close 关闭全部已建连接（接入 fx 生命周期 OnStop；重复调用安全）。
func (r *Resolver) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var firstErr error
	for service, entry := range r.conns {
		if entry.close != nil {
			if err := entry.close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		delete(r.conns, service)
	}
	return firstErr
}

// registryNameOf 派生注册中心服务名（proto 服务名首段，模板约定）并做基本校验：
// 派生不出即报错（拼错的服务名不会退化成"连不上"这类难查现象）。
func registryNameOf(service string) (string, error) {
	name, rest, ok := strings.Cut(service, ".")
	if !ok || name == "" || rest == "" {
		return "", fmt.Errorf("domainclient: 非法 proto 服务名 %q（期望 <服务>.<版本>.<Service>）", service)
	}
	return name, nil
}
