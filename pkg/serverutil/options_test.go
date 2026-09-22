package serverutil

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	atlasmiddleware "github.com/huangyuCN/atlas/middleware"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// startHTTP 后台启动 HTTP 服务端并返回就绪端点（测试结束自动停止）。
func startHTTP(t *testing.T, srv *atlashttp.Server) *url.URL {
	t.Helper()
	go func() { _ = srv.Start(context.Background()) }()
	ep, err := WaitEndpoint(srv, 5*time.Second)
	if err != nil {
		t.Fatalf("等待 HTTP 服务端就绪失败: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })
	return ep
}

// getBody 发起 GET 并返回状态码与响应体。
func getBody(t *testing.T, rawURL string) (int, string) {
	t.Helper()
	resp, err := http.Get(rawURL)
	if err != nil {
		t.Fatalf("GET %s 失败: %v", rawURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读取响应体失败: %v", err)
	}
	return resp.StatusCode, string(body)
}

// TestHTTPOptionsUnset 验证未配置任何字段时不追加选项（保持底层默认，不改变行为）。
func TestHTTPOptionsUnset(t *testing.T) {
	for _, c := range []*configspb.Server_HTTP{nil, {}} {
		opts, err := HTTPOptions(c)
		if err != nil {
			t.Fatalf("HTTPOptions(%v) 错误 = %v", c, err)
		}
		if len(opts) != 0 {
			t.Fatalf("未配置字段时不应追加选项，实际 %d 个", len(opts))
		}
	}
}

// TestHTTPOptionsBadDuration 验证非法时长在启动期报错。
func TestHTTPOptionsBadDuration(t *testing.T) {
	if _, err := HTTPOptions(&configspb.Server_HTTP{Timeout: "abc"}); err == nil {
		t.Fatal("timeout=abc 期望报错，实际为 nil")
	}
}

// TestHTTPOptionsTimeout 验证 handler 超时按配置生效（handler ctx 剩余时间受配置约束）。
func TestHTTPOptionsTimeout(t *testing.T) {
	opts, err := HTTPOptions(&configspb.Server_HTTP{Addr: "127.0.0.1:0", Timeout: "150ms"})
	if err != nil {
		t.Fatalf("HTTPOptions() 错误 = %v", err)
	}
	srv, err := atlashttp.NewServer(opts...)
	if err != nil {
		t.Fatalf("构造 HTTP 服务端失败: %v", err)
	}
	remain := make(chan time.Duration, 1)
	srv.HandleFunc("/deadline", func(w http.ResponseWriter, r *http.Request) {
		d, ok := r.Context().Deadline()
		if !ok {
			remain <- 0
		} else {
			remain <- time.Until(d)
		}
		w.WriteHeader(http.StatusOK)
	})
	ep := startHTTP(t, srv)

	if code, _ := getBody(t, ep.String()+"/deadline"); code != http.StatusOK {
		t.Fatalf("GET /deadline 状态码 = %d", code)
	}
	got := <-remain
	// 未配置时会吃到底层默认 30s，配置 150ms 后剩余时间必须远小于它。
	if got <= 0 || got > time.Second {
		t.Fatalf("handler ctx 剩余时间 = %v，期望受 timeout=150ms 约束", got)
	}
}

// TestHTTPOptionsPathPrefix 验证路由前缀生效，且前缀下的请求仍受超时约束
// （前缀不得绕过内置的请求包装）。
func TestHTTPOptionsPathPrefix(t *testing.T) {
	opts, err := HTTPOptions(&configspb.Server_HTTP{
		Addr: "127.0.0.1:0", Timeout: "150ms", PathPrefix: "/api",
	})
	if err != nil {
		t.Fatalf("HTTPOptions() 错误 = %v", err)
	}
	srv, err := atlashttp.NewServer(opts...)
	if err != nil {
		t.Fatalf("构造 HTTP 服务端失败: %v", err)
	}
	hasDeadline := make(chan bool, 1)
	srv.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		_, ok := r.Context().Deadline()
		hasDeadline <- ok
		w.WriteHeader(http.StatusOK)
	})
	ep := startHTTP(t, srv)

	if code, _ := getBody(t, ep.String()+"/api/ping"); code != http.StatusOK {
		t.Fatalf("GET /api/ping 状态码 = %d，期望 200", code)
	}
	if code, _ := getBody(t, ep.String()+"/ping"); code != http.StatusNotFound {
		t.Fatalf("GET /ping 状态码 = %d，期望 404（前缀外不可达）", code)
	}
	if ok := <-hasDeadline; !ok {
		t.Fatal("前缀下的 handler ctx 无 deadline：前缀绕过了内置请求包装")
	}
}

// TestGRPCOptionsUnset 验证未配置任何字段时不追加选项。
func TestGRPCOptionsUnset(t *testing.T) {
	for _, c := range []*configspb.Server_GRPC{nil, {}} {
		opts, err := GRPCOptions(c)
		if err != nil {
			t.Fatalf("GRPCOptions(%v) 错误 = %v", c, err)
		}
		if len(opts) != 0 {
			t.Fatalf("未配置字段时不应追加选项，实际 %d 个", len(opts))
		}
	}
}

// TestGRPCOptionsBadDuration 验证非法时长在启动期报错。
func TestGRPCOptionsBadDuration(t *testing.T) {
	if _, err := GRPCOptions(&configspb.Server_GRPC{Timeout: "1"}); err == nil {
		t.Fatal("timeout=1（无单位）期望报错，实际为 nil")
	}
	if _, err := GRPCOptions(&configspb.Server_GRPC{StreamTimeout: "-1s"}); err == nil {
		t.Fatal("stream_timeout=-1s 期望报错，实际为 nil")
	}
}

// TestGRPCOptionsServiceToggles 验证 reflection/metadata/admin 开关真实生效
// （以服务端已注册的服务名断言，而非只看选项个数）。
func TestGRPCOptionsServiceToggles(t *testing.T) {
	base, err := atlasgrpc.NewServer()
	if err != nil {
		t.Fatalf("构造 gRPC 服务端失败: %v", err)
	}
	assertServiceAbsent(t, base, "Reflection", "Metadata", "Channelz")

	opts, err := GRPCOptions(&configspb.Server_GRPC{
		Addr: "127.0.0.1:0", Timeout: "1s", Reflection: true, Metadata: true, Admin: true,
	})
	if err != nil {
		t.Fatalf("GRPCOptions() 错误 = %v", err)
	}
	srv, err := atlasgrpc.NewServer(opts...)
	if err != nil {
		t.Fatalf("构造 gRPC 服务端失败: %v", err)
	}
	assertServicePresent(t, srv, "reflection")
	assertServicePresent(t, srv, "metadata")
	assertServicePresent(t, srv, "channelz")
}

// serviceNames 返回服务端已注册的服务名集合。
func serviceNames(srv *atlasgrpc.Server) []string {
	infos := srv.GetServiceInfo()
	names := make([]string, 0, len(infos))
	for name := range infos {
		names = append(names, name)
	}
	return names
}

// assertServicePresent 断言存在名字包含 sub（忽略大小写）的服务。
func assertServicePresent(t *testing.T, srv *atlasgrpc.Server, sub string) {
	t.Helper()
	for _, name := range serviceNames(srv) {
		if strings.Contains(strings.ToLower(name), sub) {
			return
		}
	}
	t.Fatalf("未找到包含 %q 的服务，已注册: %v", sub, serviceNames(srv))
}

// assertServiceAbsent 断言不存在名字包含任一 sub（忽略大小写）的服务。
func assertServiceAbsent(t *testing.T, srv *atlasgrpc.Server, subs ...string) {
	t.Helper()
	names := serviceNames(srv)
	for _, sub := range subs {
		for _, name := range names {
			if strings.Contains(strings.ToLower(name), strings.ToLower(sub)) {
				t.Fatalf("默认不应注册包含 %q 的服务，已注册: %v", sub, names)
			}
		}
	}
}

// TestTLSConfigDisabled 验证未启用时返回 nil（明文）。
func TestTLSConfigDisabled(t *testing.T) {
	for _, c := range []*configspb.Server_TLS{nil, {}, {Enabled: false, CertFile: "/nope"}} {
		conf, err := TLSConfig(c)
		if err != nil || conf != nil {
			t.Fatalf("TLSConfig(%v) = (%v, %v)，期望 (nil, nil)", c, conf, err)
		}
	}
}

// TestTLSConfigInvalid 验证缺字段、缺文件与非法 CA 报错。
func TestTLSConfigInvalid(t *testing.T) {
	pki := newTestPKI(t)
	dir := filepath.Dir(pki.serverCert)
	if _, err := TLSConfig(&configspb.Server_TLS{Enabled: true}); err == nil {
		t.Fatal("缺 cert_file/key_file 期望报错")
	}
	if _, err := TLSConfig(&configspb.Server_TLS{
		Enabled: true, CertFile: filepath.Join(dir, "nope.crt"), KeyFile: filepath.Join(dir, "nope.key"),
	}); err == nil {
		t.Fatal("证书文件不存在期望报错")
	}
	if _, err := TLSConfig(&configspb.Server_TLS{
		Enabled: true, CertFile: pki.serverCert, KeyFile: pki.serverKey, ClientAuth: true,
	}); err == nil {
		t.Fatal("client_auth 缺 ca_file 期望报错")
	}
	if _, err := TLSConfig(&configspb.Server_TLS{
		Enabled: true, CertFile: pki.serverCert, KeyFile: pki.serverKey, CaFile: filepath.Join(dir, "nope.pem"),
	}); err == nil {
		t.Fatal("CA 文件不存在期望报错")
	}
}

// TestTLSConfigEnabled 验证证书加载与 mTLS 校验强度。
func TestTLSConfigEnabled(t *testing.T) {
	pki := newTestPKI(t)
	conf, err := TLSConfig(&configspb.Server_TLS{
		Enabled: true, CertFile: pki.serverCert, KeyFile: pki.serverKey,
	})
	if err != nil {
		t.Fatalf("TLSConfig() 错误 = %v", err)
	}
	if len(conf.Certificates) != 1 || conf.MinVersion != tls.VersionTLS12 {
		t.Fatalf("TLS 配置不符合预期: certs=%d minVersion=%d", len(conf.Certificates), conf.MinVersion)
	}
	if conf.ClientAuth != tls.NoClientCert {
		t.Fatalf("未配 CA 时不应要求客户端证书，实际 %v", conf.ClientAuth)
	}

	conf, err = TLSConfig(&configspb.Server_TLS{
		Enabled: true, CertFile: pki.serverCert, KeyFile: pki.serverKey, CaFile: pki.caCert, ClientAuth: true,
	})
	if err != nil {
		t.Fatalf("TLSConfig() 错误 = %v", err)
	}
	if conf.ClientAuth != tls.RequireAndVerifyClientCert || conf.ClientCAs == nil {
		t.Fatalf("client_auth 应要求并校验客户端证书，实际 auth=%v ca=%v", conf.ClientAuth, conf.ClientCAs)
	}

	// 配了 CA 但未强制：给了客户端证书就校验，不给也放行（规格 §4）。
	conf, err = TLSConfig(&configspb.Server_TLS{
		Enabled: true, CertFile: pki.serverCert, KeyFile: pki.serverKey, CaFile: pki.caCert,
	})
	if err != nil {
		t.Fatalf("TLSConfig() 错误 = %v", err)
	}
	if conf.ClientAuth != tls.VerifyClientCertIfGiven {
		t.Fatalf("配 CA 未强制时应为 VerifyClientCertIfGiven，实际 %v", conf.ClientAuth)
	}
}

// TestHealthHandler 验证统一健康检查响应。
func TestHealthHandler(t *testing.T) {
	rec := httptest.NewRecorder()
	HealthHandler("game")(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d，期望 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q", ct)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"status":"ok"`) || !strings.Contains(body, `"service":"game"`) {
		t.Fatalf("响应体 = %q", body)
	}
}

// TestHTTPServerAppliesFilters 验证注入的过滤器包住整个路由
// （与中间件不同：过滤器是 stdlib http.Handler 包装，裸处理器也生效）。
func TestHTTPServerAppliesFilters(t *testing.T) {
	filter := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Filter", "1")
			next.ServeHTTP(w, r)
		})
	}
	srv, err := HTTPServer(&configspb.Server_HTTP{Addr: "127.0.0.1:0"}, nil, Filters{filter})
	if err != nil {
		t.Fatalf("HTTPServer() 错误 = %v", err)
	}
	srv.HandleFunc("/health", HealthHandler("game"))
	ep := startHTTP(t, srv)

	resp, err := http.Get(ep.String() + "/health")
	if err != nil {
		t.Fatalf("请求失败: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if got := resp.Header.Get("X-Filter"); got != "1" {
		t.Fatalf("过滤器未生效：X-Filter = %q", got)
	}
}

// TestNetworkOf 验证网络枚举映射：未设置交给底层默认，合法值映射为 net.Listen 网络名，
// 未登记取值报错（不静默回落 tcp）。
func TestNetworkOf(t *testing.T) {
	if got, err := networkOf(nil); err != nil || got != "" {
		t.Fatalf("未设置应返回空串，实际 %q/%v", got, err)
	}
	cases := map[configspb.Server_Network]string{
		configspb.Server_NETWORK_TCP:  "tcp",
		configspb.Server_NETWORK_TCP4: "tcp4",
		configspb.Server_NETWORK_TCP6: "tcp6",
		configspb.Server_NETWORK_UNIX: "unix",
	}
	for in, want := range cases {
		got, err := networkOf(&in)
		if err != nil || got != want {
			t.Fatalf("networkOf(%v) = %q/%v，期望 %q", in, got, err, want)
		}
	}
	bad := configspb.Server_Network(99)
	if _, err := networkOf(&bad); err == nil {
		t.Fatal("未登记的网络类型期望报错")
	}
}

// boolPtr 返回 bool 指针（proto optional 字段用）。
func boolPtr(v bool) *bool { return &v }

// TestHTTPOptionsStrictSlash 验证 strict_slash 的空值语义：
// 未设置保持底层默认（true，/ping/ 301 重定向到 /ping），显式关闭则 /ping/ 直接 404。
func TestHTTPOptionsStrictSlash(t *testing.T) {
	cases := []struct {
		name   string
		strict *bool
		want   int
	}{
		{"未设置保持底层默认 true", nil, http.StatusMovedPermanently},
		{"显式关闭", boolPtr(false), http.StatusNotFound},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			opts, err := HTTPOptions(&configspb.Server_HTTP{Addr: "127.0.0.1:0", StrictSlash: c.strict})
			if err != nil {
				t.Fatalf("HTTPOptions() 错误 = %v", err)
			}
			srv, err := atlashttp.NewServer(opts...)
			if err != nil {
				t.Fatalf("构造 HTTP 服务端失败: %v", err)
			}
			srv.HandleFunc("/ping", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
			ep := startHTTP(t, srv)

			client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			}}
			resp, err := client.Get(ep.String() + "/ping/")
			if err != nil {
				t.Fatalf("请求失败: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != c.want {
				t.Fatalf("GET /ping/ 状态码 = %d，期望 %d", resp.StatusCode, c.want)
			}
		})
	}
}

// streamerServer 是流式服务桩的接口类型（RegisterService 要求 HandlerType 为接口指针）。
type streamerServer interface{}

// TestGRPCServerAppliesStreamMiddleware 验证同一中间件链也挂在流式 RPC 上：
// 只挂一元会让流式 RPC 静默失去日志/追踪/指标。
func TestGRPCServerAppliesStreamMiddleware(t *testing.T) {
	called := make(chan struct{}, 1)
	mw := func(next atlasmiddleware.Handler) atlasmiddleware.Handler {
		return func(ctx context.Context, req any) (any, error) {
			select {
			case called <- struct{}{}:
			default:
			}
			return next(ctx, req)
		}
	}
	srv, err := GRPCServer(&configspb.Server_GRPC{Addr: "127.0.0.1:0"}, Middlewares{mw})
	if err != nil {
		t.Fatalf("GRPCServer() 错误 = %v", err)
	}
	desc := &grpc.ServiceDesc{
		ServiceName: "test.v1.Streamer",
		HandlerType: (*streamerServer)(nil),
		Streams: []grpc.StreamDesc{{
			StreamName:    "Echo",
			Handler:       func(any, grpc.ServerStream) error { return nil },
			ServerStreams: true,
		}},
		Metadata: "test.proto",
	}
	srv.RegisterService(desc, struct{}{})
	go func() { _ = srv.Start(context.Background()) }()
	ep, err := WaitEndpoint(srv, 5*time.Second)
	if err != nil {
		t.Fatalf("等待 gRPC 服务端就绪失败: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })

	conn, err := grpc.NewClient(ep.Host, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("拨号失败: %v", err)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := conn.NewStream(ctx, &desc.Streams[0], "/test.v1.Streamer/Echo"); err != nil {
		t.Fatalf("建立流失败: %v", err)
	}
	select {
	case <-called:
	case <-time.After(3 * time.Second):
		t.Fatal("流式 RPC 未经过中间件链")
	}
}
