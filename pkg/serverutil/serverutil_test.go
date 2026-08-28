package serverutil

import (
	"context"
	"errors"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/huangyuCN/atlas/transport"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
)

// brokenServer 是端点永远不就绪的失败样例：
// 会真实监听端口（模拟 bind 成功但探测持续失败的诡异场景），
// 用于验证超时路径下已就绪服务端被正确回收。
type brokenServer struct {
	lis net.Listener
}

var _ transport.Server = (*brokenServer)(nil)

func (b *brokenServer) Start(_ context.Context) error {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	b.lis = lis
	select {} // 阻塞直至进程退出（模拟永不返回的 serve）
}

func (b *brokenServer) Stop(context.Context) error {
	if b.lis != nil {
		return b.lis.Close()
	}
	return nil
}

func (b *brokenServer) Endpoint() (*url.URL, error) { return nil, errors.New("not ready") }

// TestServeAsyncStartsAndStops 验证正常路径：
// 多协议服务端并发启动、端点按序返回、stop 后监听关闭。
func TestServeAsyncStartsAndStops(t *testing.T) {
	httpSrv, err := atlashttp.NewServer(atlashttp.WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("构造 http 服务端失败: %v", err)
	}
	grpcSrv, err := atlasgrpc.NewServer(atlasgrpc.WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("构造 gRPC 服务端失败: %v", err)
	}

	eps, stop, err := ServeAsync(5*time.Second, httpSrv, grpcSrv)
	if err != nil {
		t.Fatalf("ServeAsync() 错误 = %v", err)
	}
	defer func() { _ = stop(context.Background()) }()

	if len(eps) != 2 || eps[0] == nil || eps[1] == nil {
		t.Fatalf("应返回两个端点, 实际 %v", eps)
	}
	for i, ep := range eps {
		if ep.Host == "" || ep.Port() == "0" {
			t.Fatalf("端点[%d] 未获取到真实监听地址: %s", i, ep)
		}
	}
	if eps[0].Scheme != "http" || eps[1].Scheme != "grpc" {
		t.Fatalf("端点 scheme 不符合预期: %s / %s", eps[0], eps[1])
	}

	if err := stop(context.Background()); err != nil {
		t.Fatalf("stop() 错误 = %v", err)
	}
}

// TestServeAsyncFailureRecoversReadyOnes 验证失败路径：
// 任一端点未就绪时整体报错，且已就绪的服务端被停止（监听关闭）。
func TestServeAsyncFailureRecoversReadyOnes(t *testing.T) {
	httpSrv, err := atlashttp.NewServer(atlashttp.WithAddress("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("构造 http 服务端失败: %v", err)
	}
	broken := &brokenServer{}

	_, _, err = ServeAsync(200*time.Millisecond, httpSrv, broken)
	if err == nil {
		t.Fatal("ServeAsync() 含失败服务端时期望报错，实际为 nil")
	}

	// 已就绪的 http 服务端应已被停止：向其地址拨号应失败。
	ok, _ := httpSrv.Endpoint()
	if ok != nil {
		conn, derr := net.DialTimeout("tcp", ok.Host, time.Second)
		if derr == nil {
			_ = conn.Close()
			t.Fatal("已就绪的 http 服务端未被回收（仍可连接）")
		}
	}
}
