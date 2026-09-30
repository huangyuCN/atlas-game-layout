package assemble

import (
	"errors"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/huangyuCN/atlas/namespace"
)

// TestNewBootstrap 验证 Options 到 *conf.Bootstrap 的映射与监听地址缺省。
func TestNewBootstrap(t *testing.T) {
	opts := Options{
		ID:            "a",
		EtcdEndpoints: []string{"127.0.0.1:12379"},
		NatsURL:       "nats://127.0.0.1:14222",
		RedisAddrs:    []string{"127.0.0.1:16379"},
		Namespace:     "p5-a",
	}
	cfg, err := newBootstrap(opts)
	if err != nil {
		t.Fatalf("newBootstrap: %v", err)
	}

	// 实例 ID 统一加 gw- 前缀（会话路由与跨实例踢人依赖该约定）。
	if got := cfg.GetRuntime().GetId(); got != "gw-a" {
		t.Errorf("runtime.id = %q, 期望 gw-a", got)
	}

	// 命名空间单源：runtime.namespace 显式传入，五面前缀全由框架 namespace.Derive 派生。
	assertNamespaceFaces(t, cfg.GetRuntime().GetNamespace(), "p5-a")

	eps := cfg.GetRegistry().GetEtcd().GetEndpoints()
	if len(eps) != 1 || eps[0] != "127.0.0.1:12379" {
		t.Errorf("registry.etcd.endpoints = %v", eps)
	}
	if got := cfg.GetData().GetRedis().GetAddrs(); len(got) != 1 || got[0] != "127.0.0.1:16379" {
		t.Errorf("data.redis.addrs = %v", got)
	}
	if got := cfg.GetData().GetNats().GetUrl(); got != "nats://127.0.0.1:14222" {
		t.Errorf("data.nats.url = %q", got)
	}
	// 进程内形态业务协议、http 与 gRPC edge 面监听地址固定随机端口。
	for name, got := range map[string]string{
		"tcp":            cfg.GetServer().GetTcp().GetAddr(),
		"websocket":      cfg.GetServer().GetWebsocket().GetAddr(),
		"grpc.edge_addr": cfg.GetServer().GetGrpc().GetEdgeAddr(),
		"http":           cfg.GetServer().GetHttp().GetAddr(),
	} {
		if got != "127.0.0.1:0" {
			t.Errorf("%s.addr = %q, 期望缺省 127.0.0.1:0", name, got)
		}
	}
	// 破坏性断言（配置层）：战斗帧面（kcp/udp）**刻意不配**——网关不再监听它们
	//（阶段 3 批次 5：客户端凭 battle_ticket 直连接入层 → battle 帧面）。
	if got := cfg.GetServer().GetKcp().GetAddr(); got != "" {
		t.Errorf("server.kcp.addr = %q, 期望留空（网关不再承载战斗帧）", got)
	}
	if got := cfg.GetServer().GetUdp().GetAddr(); got != "" {
		t.Errorf("server.udp.addr = %q, 期望留空（网关不再承载战斗帧）", got)
	}
	// gateway 本轮只启用 edge 面：internal 面留空 = 不启用（P7 管理面再启用）。
	if got := cfg.GetServer().GetGrpc().GetInternalAddr(); got != "" {
		t.Errorf("server.grpc.internal_addr = %q, 期望留空（不启用）", got)
	}
}

// TestNoBattleFrameListeners 验证装配期实际监听的 scheme 集合里没有战斗帧面：
// ListenSchemes 取自 bootstrap.Boot 的就绪端点表（真实运行时的监听面，不是配置抄写），
// 故「网关还在监听 9003/9004」会让本用例失败。
func TestNoBattleFrameListeners(t *testing.T) {
	schemes := sortedSchemes(map[string]*url.URL{
		"tcp":       {Scheme: "tcp", Host: "127.0.0.1:1"},
		"ws":        {Scheme: "ws", Host: "127.0.0.1:2"},
		"http":      {Scheme: "http", Host: "127.0.0.1:3"},
		"grpc-edge": {Scheme: "grpc-edge", Host: "127.0.0.1:4"},
	})
	want := []string{"grpc-edge", "http", "tcp", "ws"}
	if !slices.Equal(schemes, want) {
		t.Fatalf("ListenSchemes = %v, 期望 %v", schemes, want)
	}
	for _, s := range schemes {
		if s == "kcp" || s == "udp" {
			t.Fatalf("网关不应监听战斗帧面 %s（schemes=%v）", s, schemes)
		}
	}
	// 空端点（未启用的面）不入集合：nil 值不得被当成"在监听"。
	if got := sortedSchemes(map[string]*url.URL{"kcp": nil}); len(got) != 0 {
		t.Fatalf("nil 端点不应计入监听面: %v", got)
	}
}

// TestNewBootstrapRequiresNamespace 验证 R9 严格模式：runtime.namespace 缺失即装配失败
// （**不回落 default/env**），错误可用 errors.Is 判定且含字段全名与示例值。
func TestNewBootstrapRequiresNamespace(t *testing.T) {
	_, err := newBootstrap(Options{
		ID:            "a",
		EtcdEndpoints: []string{"127.0.0.1:12379"},
		NatsURL:       "nats://127.0.0.1:14222",
	})
	if err == nil {
		t.Fatal("runtime.namespace 缺失时期望装配失败，实际为 nil（说明回落了缺省）")
	}
	if !errors.Is(err, namespace.ErrInvalid) {
		t.Errorf("错误应可用 errors.Is(err, namespace.ErrInvalid) 判定，实际 %v", err)
	}
	if !strings.Contains(err.Error(), "runtime.namespace") || !strings.Contains(err.Error(), "test") {
		t.Errorf("错误信息应含字段全名 runtime.namespace 与示例值 test，实际 %v", err)
	}
}

// assertNamespaceFaces 断言 runtime.namespace 经框架派生出的五面前缀与预期逐面一致：
// 注册中心键前缀 / 集群 NATS subject 前缀 / 业务 topic 前缀 / redis 键前缀 / etcd 目录。
func assertNamespaceFaces(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Fatalf("runtime.namespace = %q, 期望 %q", got, want)
	}
	derived, err := namespace.Derive(got)
	if err != nil {
		t.Fatalf("namespace.Derive(%q): %v", got, err)
	}
	faces := []struct{ name, got, want string }{
		{"注册中心键前缀", derived.RegistryPrefix, "/atlas/services/" + want},
		{"集群 subject 前缀", derived.ClusterSubjectPrefix, "atlas_actor." + want},
		{"业务 topic 前缀", derived.TopicPrefix, "atlas." + want},
		{"redis 键前缀", derived.RedisKeyPrefix, "atlas:" + want + ":"},
		{"etcd 目录", derived.EtcdDirectory, "/atlas/actors/" + want},
	}
	for _, f := range faces {
		if f.got != f.want {
			t.Errorf("%s = %q, 期望 %q", f.name, f.got, f.want)
		}
	}
}
