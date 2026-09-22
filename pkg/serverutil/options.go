package serverutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"

	"github.com/huangyuCN/atlas-game-layout/pkg/config"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	atlasmiddleware "github.com/huangyuCN/atlas/middleware"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
	atlashttp "github.com/huangyuCN/atlas/transport/http"
)

// Middlewares 是注入服务端的中间件链：配置表达不了函数值，故由 fx 提供默认链
// （pkg/middleware），业务用 fx.Decorate 追加自己的中间件。顺序即执行顺序。
type Middlewares []atlasmiddleware.Middleware

// ClientMiddlewares 是注入客户端的中间件链（gRPC 拨号时挂载，用于链路传播等）。
type ClientMiddlewares []atlasmiddleware.Middleware

// Filters 是注入 HTTP 服务端的过滤器链（stdlib http.Handler 包装，包住整个路由）：
// 与 Middlewares 的分工是「HTTP 协议层的东西（CORS/gzip/pprof）走 Filters，
// 传输无关的业务关注点（鉴权/限流/日志）走 Middlewares」。
type Filters []atlashttp.FilterFunc

// HTTPOptions 把 server.http 配置映射为 HTTP 服务端选项：
// 空串 / 0 / 未设置的字段一律不追加对应选项，交给底层默认值，避免「不配就变行为」；
// 时长字段解析失败直接报错（启动期暴露，不静默降级）。
func HTTPOptions(c *configspb.Server_HTTP) ([]atlashttp.ServerOption, error) {
	if c == nil {
		return nil, nil
	}
	opts := make([]atlashttp.ServerOption, 0, 8)
	network, err := networkOf(c.Network)
	if err != nil {
		return nil, fmt.Errorf("serverutil: server.http.network 无效: %w", err)
	}
	if network != "" {
		opts = append(opts, atlashttp.WithNetwork(network))
	}
	if c.GetAddr() != "" {
		opts = append(opts, atlashttp.WithAddress(c.GetAddr()))
	}
	timeout, err := config.ParseDuration(c.GetTimeout())
	if err != nil {
		return nil, fmt.Errorf("serverutil: server.http.timeout 无效: %w", err)
	}
	if timeout > 0 {
		opts = append(opts, atlashttp.Timeout(timeout))
	}
	if c.GetPathPrefix() != "" {
		opts = append(opts, atlashttp.WithPathPrefix(c.GetPathPrefix()))
	}
	if c.StrictSlash != nil {
		opts = append(opts, atlashttp.WithStrictSlash(c.GetStrictSlash()))
	}
	if c.GetMaxRequestBody() > 0 {
		opts = append(opts, atlashttp.WithMaxRequestBody(c.GetMaxRequestBody()))
	}
	conf, err := TLSConfig(c.GetTls())
	if err != nil {
		return nil, err
	}
	if conf != nil {
		opts = append(opts, atlashttp.TLSConfig(conf))
	}
	return opts, nil
}

// GRPCOptions 把 server.grpc 配置映射为 gRPC 服务端选项（空值语义同 HTTPOptions）。
func GRPCOptions(c *configspb.Server_GRPC) ([]atlasgrpc.ServerOption, error) {
	if c == nil {
		return nil, nil
	}
	opts := make([]atlasgrpc.ServerOption, 0, 10)
	network, err := networkOf(c.Network)
	if err != nil {
		return nil, fmt.Errorf("serverutil: server.grpc.network 无效: %w", err)
	}
	if network != "" {
		opts = append(opts, atlasgrpc.WithNetwork(network))
	}
	if c.GetAddr() != "" {
		opts = append(opts, atlasgrpc.WithAddress(c.GetAddr()))
	}
	timeout, err := config.ParseDuration(c.GetTimeout())
	if err != nil {
		return nil, fmt.Errorf("serverutil: server.grpc.timeout 无效: %w", err)
	}
	if timeout > 0 {
		opts = append(opts, atlasgrpc.Timeout(timeout))
	}
	streamTimeout, err := config.ParseDuration(c.GetStreamTimeout())
	if err != nil {
		return nil, fmt.Errorf("serverutil: server.grpc.stream_timeout 无效: %w", err)
	}
	if streamTimeout > 0 {
		opts = append(opts, atlasgrpc.WithStreamTimeout(streamTimeout))
	}
	if c.GetMaxRecvMsgSize() > 0 {
		opts = append(opts, atlasgrpc.WithMaxRecvMsgSize(int(c.GetMaxRecvMsgSize())))
	}
	if c.GetReflection() {
		opts = append(opts, atlasgrpc.WithReflection())
	}
	if c.GetMetadata() {
		opts = append(opts, atlasgrpc.WithMetadata())
	}
	if c.GetAdmin() {
		opts = append(opts, atlasgrpc.WithAdmin())
	}
	conf, err := TLSConfig(c.GetTls())
	if err != nil {
		return nil, err
	}
	if conf != nil {
		opts = append(opts, atlasgrpc.TLSConfig(conf))
	}
	return opts, nil
}

// HTTPServer 按 server.http 配置构造 HTTP 服务端，并挂载中间件与过滤器
// （业务路由由调用方注册）。
func HTTPServer(c *configspb.Server_HTTP, mws Middlewares, filters Filters) (*atlashttp.Server, error) {
	opts, err := HTTPOptions(c)
	if err != nil {
		return nil, err
	}
	if len(mws) > 0 {
		opts = append(opts, atlashttp.Middleware(mws...))
	}
	if len(filters) > 0 {
		opts = append(opts, atlashttp.WithFilter(filters...))
	}
	return build("HTTP 服务端", atlashttp.NewServer, opts...)
}

// GRPCServer 按 server.grpc 配置构造 gRPC 服务端，并挂载中间件
// （业务服务由调用方注册）。同一链同时挂一元与流式：只挂一元会让新增的
// 流式 RPC 静默失去日志/追踪/指标。
func GRPCServer(c *configspb.Server_GRPC, mws Middlewares) (*atlasgrpc.Server, error) {
	opts, err := GRPCOptions(c)
	if err != nil {
		return nil, err
	}
	if len(mws) > 0 {
		opts = append(opts, atlasgrpc.Middleware(mws...), atlasgrpc.StreamMiddleware(mws...))
	}
	return build("gRPC 服务端", atlasgrpc.NewServer, opts...)
}

// build 调用服务端构造函数并统一包装错误（构造失败在启动期暴露）。
func build[S any, O any](what string, ctor func(...O) (S, error), opts ...O) (S, error) {
	srv, err := ctor(opts...)
	if err != nil {
		var zero S
		return zero, fmt.Errorf("serverutil: 构造%s失败: %w", what, err)
	}
	return srv, nil
}

// TLSConfig 把 server.tls 配置映射为 *tls.Config：未启用（缺失或 enabled=false）返回 nil（明文），
// 启用时加载证书链与私钥；client_auth 要求 ca_file 并强制校验客户端证书。
func TLSConfig(c *configspb.Server_TLS) (*tls.Config, error) {
	if c == nil || !c.GetEnabled() {
		return nil, nil
	}
	if c.GetCertFile() == "" || c.GetKeyFile() == "" {
		return nil, fmt.Errorf("serverutil: 启用 TLS 时必须配置 cert_file 与 key_file")
	}
	if c.GetClientAuth() && c.GetCaFile() == "" {
		return nil, fmt.Errorf("serverutil: 启用客户端证书校验（client_auth）时必须配置 ca_file")
	}
	cert, err := tls.LoadX509KeyPair(c.GetCertFile(), c.GetKeyFile())
	if err != nil {
		return nil, fmt.Errorf("serverutil: 加载服务端证书失败: %w", err)
	}
	// MinVersion 固定 TLS 1.2：低于此版本的协议已不安全，不提供配置项避免误配。
	conf := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if c.GetCaFile() == "" {
		return conf, nil
	}
	caPEM, err := os.ReadFile(c.GetCaFile())
	if err != nil {
		return nil, fmt.Errorf("serverutil: 读取客户端 CA 失败: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("serverutil: 客户端 CA %s 未包含有效证书", c.GetCaFile())
	}
	conf.ClientCAs = pool
	if c.GetClientAuth() {
		conf.ClientAuth = tls.RequireAndVerifyClientCert
	} else {
		// 配置了 CA 但未强制：客户端给了证书就校验，不给也放行。
		conf.ClientAuth = tls.VerifyClientCertIfGiven
	}
	return conf, nil
}

// HealthHandler 返回统一健康检查处理器：JSON 响应 {"status":"ok","service":"<name>"}。
func HealthHandler(service string) http.HandlerFunc {
	body := fmt.Sprintf(`{"status":"ok","service":%q}`, service)
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
}
