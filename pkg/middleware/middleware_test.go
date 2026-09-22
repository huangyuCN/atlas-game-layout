package middleware

import (
	"bytes"
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	"github.com/huangyuCN/atlas-game-layout/pkg/spanstest"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	atlaslog "github.com/huangyuCN/atlas/log"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

// 上游链路标识（测试用固定 traceparent）。
const (
	testTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	testSpanID  = "00f067aa0ba902b7"
)

// startHTTP 起一个挂指定中间件链的 HTTP 服务端，返回就绪端点（测试结束自动停止）。
func startHTTP(t *testing.T, mws serverutil.Middlewares) *url.URL {
	t.Helper()
	srv, err := serverutil.HTTPServer(&configspb.Server_HTTP{Addr: "127.0.0.1:0"}, mws, nil)
	if err != nil {
		t.Fatalf("构造 HTTP 服务端失败: %v", err)
	}
	// 与生成代码同构：中间件的应用点是「服务调用」——handler 里显式 ctx.Middleware(...)
	// 包住业务调用；裸 HandleFunc 处理器（如 /health）不经过中间件链，正是有意为之。
	srv.Route("/").GET("/ping", func(ctx atlashttp.Context) error {
		h := ctx.Middleware(func(context.Context, interface{}) (interface{}, error) {
			return map[string]string{"ok": "1"}, nil
		})
		out, err := h(ctx, nil)
		if err != nil {
			return err
		}
		return ctx.Result(http.StatusOK, out)
	})
	go func() { _ = srv.Start(context.Background()) }()
	ep, err := serverutil.WaitEndpoint(srv, 5*time.Second)
	if err != nil {
		t.Fatalf("等待 HTTP 服务端就绪失败: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })
	return ep
}

// TestServerChainExtractsUpstreamTrace 验证服务端链生成 server span 并延续上游链路
// （extract 是跨服务链路成立的前提：不 extract 则每个服务各自一个 root span）。
func TestServerChainExtractsUpstreamTrace(t *testing.T) {
	exp, stop := spanstest.Install()
	defer stop()
	mws, err := Server()
	if err != nil {
		t.Fatalf("Server() 错误 = %v", err)
	}
	ep := startHTTP(t, mws)

	req, err := http.NewRequest(http.MethodGet, ep.String()+"/ping", nil)
	if err != nil {
		t.Fatalf("构造请求失败: %v", err)
	}
	req.Header.Set("traceparent", "00-"+testTraceID+"-"+testSpanID+"-01")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	spans := spanstest.ByName(exp)
	stub, ok := spans["/ping"]
	if !ok {
		names := make([]string, 0, len(spans))
		for name := range spans {
			names = append(names, name)
		}
		t.Fatalf("未找到 /ping 的 server span，已导出: %v", names)
	}
	if stub.SpanKind != trace.SpanKindServer {
		t.Fatalf("span kind = %v，期望 server", stub.SpanKind)
	}
	if got := stub.SpanContext.TraceID().String(); got != testTraceID {
		t.Fatalf("trace id = %s，期望延续上游 %s（extract 未生效）", got, testTraceID)
	}
	if got := stub.Parent.SpanID().String(); got != testSpanID {
		t.Fatalf("父 span = %s，期望上游 %s", got, testSpanID)
	}
}

// TestClientChainInjectsTraceparent 验证客户端链把 traceparent 注入出站请求
// （跨服务传播的另一半；用服务端拦截器读取入站 metadata 断言）。
func TestClientChainInjectsTraceparent(t *testing.T) {
	_, stop := spanstest.Install() // 需真实 provider：noop span 不会注入
	defer stop()

	got := make(chan string, 1)
	record := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if v := md.Get("traceparent"); len(v) > 0 {
				got <- v[0]
			}
		}
		return h(ctx, req)
	}
	srv, err := atlasgrpc.NewServer(atlasgrpc.WithAddress("127.0.0.1:0"), atlasgrpc.UnaryInterceptor(record))
	if err != nil {
		t.Fatalf("构造 gRPC 服务端失败: %v", err)
	}
	go func() { _ = srv.Start(context.Background()) }()
	ep, err := serverutil.WaitEndpoint(srv, 5*time.Second)
	if err != nil {
		t.Fatalf("等待 gRPC 服务端就绪失败: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })

	conn, err := atlasgrpc.DialInsecure(context.Background(),
		atlasgrpc.WithEndpoint(ep.Host),
		atlasgrpc.WithMiddleware(Client()...))
	if err != nil {
		t.Fatalf("拨号失败: %v", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{}); err != nil {
		t.Fatalf("health Check 失败: %v", err)
	}
	select {
	case v := <-got:
		if !strings.HasPrefix(v, "00-") {
			t.Fatalf("traceparent = %q，格式不符", v)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("服务端未收到 traceparent：客户端链未注入")
	}
}

// TestServerChainRecordsMetrics 验证默认链记录请求指标
// （instrument 取自全局 MeterProvider，故先装内存 reader 再构造链）。
func TestServerChainRecordsMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	old := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	defer func() {
		otel.SetMeterProvider(old)
		_ = mp.Shutdown(context.Background())
	}()

	mws, err := Server()
	if err != nil {
		t.Fatalf("Server() 错误 = %v", err)
	}
	ep := startHTTP(t, mws)
	resp, err := http.Get(ep.String() + "/ping")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("采集指标失败: %v", err)
	}
	names := metricNames(rm)
	if !contains(names, "server_requests_code_total") {
		t.Fatalf("未记录请求计数指标，已采集: %v", names)
	}
	if !contains(names, serverSecondsHistogramName) {
		t.Fatalf("未记录请求耗时指标，已采集: %v", names)
	}
}

// TestServerChainLogsRequest 验证默认链输出请求日志（日志走 atlas 全局 logger）。
func TestServerChainLogsRequest(t *testing.T) {
	var buf bytes.Buffer
	old := atlaslog.GetLogger()
	atlaslog.SetLogger(atlaslog.New(atlaslog.WithWriter(&buf)))
	defer atlaslog.SetLogger(old)

	mws, err := Server()
	if err != nil {
		t.Fatalf("Server() 错误 = %v", err)
	}
	ep := startHTTP(t, mws)
	resp, err := http.Get(ep.String() + "/ping")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if !strings.Contains(buf.String(), "/ping") {
		t.Fatalf("未见请求日志，实际输出: %q", buf.String())
	}
}

// metricNames 收集采集结果里的指标名。
func metricNames(rm metricdata.ResourceMetrics) []string {
	var names []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names = append(names, m.Name)
		}
	}
	return names
}

// contains 判断切片是否含目标值。
func contains(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}
