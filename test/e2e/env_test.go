// Package e2e 提供跨服务的端到端集成测试（真 etcd/redis/nats/mongo，
// 本地 Docker 端口与集成服务器约定一致；不可达时跳过，Skipf 风格）。
package e2e

import (
	"context"
	"fmt"
	battlev1actor "github.com/huangyuCN/atlas-game-layout/api/battle/v1/actor"
	"testing"
	"time"

	admingamev1 "github.com/huangyuCN/atlas-game-layout/api/admin/game/v1"
	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	"github.com/huangyuCN/atlas-game-layout/pkg/etcd"
	pkgmongo "github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	pkgnats "github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	battleassemble "github.com/huangyuCN/atlas-game-layout/services/battle/assemble"
	gameassemble "github.com/huangyuCN/atlas-game-layout/services/game/assemble"
	gwassemble "github.com/huangyuCN/atlas-game-layout/services/gateway/assemble"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	"github.com/huangyuCN/atlas/namespace"
	atlasgrpc "github.com/huangyuCN/atlas/transport/grpc"
)

// 集成环境地址（本地 Docker 端口约定，与 10.10.9.36 集成服务器一致）。
const (
	itEtcdEndpoints = "127.0.0.1:12379"
	itRedisAddr     = "127.0.0.1:16379"
	itNatsURL       = "nats://127.0.0.1:14222"
	itMongoURI      = "mongodb://127.0.0.1:27018"
	itMongoDB       = "game_it"
)

// itNS 是本次测试运行独占的命名空间 token（形如 it-<纳秒>）：与常驻进程
// （runtime.namespace: test）隔离，也不会命中上一次运行残留的实例键（租约未过期）
// 导致注册冲突。路径形态的前缀（/atlas/services/<ns> 等）一律由框架 namespace.Derive 拼，
// 代码里不再出现路径字面量。
var itNS = fmt.Sprintf("it-%d", time.Now().UnixNano())

// 集成测试的出票配置（阶段 3 批次 2C）：battle 装配必须给出票据密钥与接入层各面地址，
// 否则 app.newActorConfig 直接失败（缺失即启动失败是刻意设计）。密钥为 base64 的 32 字节。
const itTicketKey = "AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA="

// itClientVersion 是 e2e 客户端上报的版本：网关的 min_client_version 门槛在登录期强制
// （模板配置 0.1.0，发布时会上调为首个支持直连的 SDK 版本），故取一个远高于任何门槛的值，
// 避免门槛上调后这些用例再次变红；门槛本身的行为由 version_gate_test.go 专门覆盖。
const itClientVersion = "9.9.9"

// itEdgeEndpoints 是接入层的「面→地址」列表（与 services/edge 的监听面一致：
// ws=tcp:7100、kcp=udp:7101、udp=udp:7102；单地址无法让 SDK 知道该拨哪个端口）。
var itEdgeEndpoints = []*battlev1.EdgeEndpoint{
	{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_WS, Address: "127.0.0.1:7100"},
	{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_KCP, Address: "127.0.0.1:7101"},
	{Transport: battlev1.EdgeTransport_EDGE_TRANSPORT_UDP, Address: "127.0.0.1:7102"},
}

// itDerived 是本次运行的五面派生结果（注册键前缀 / actor subject / 业务 topic /
// redis 键 / etcd 目录全由它取），与进程内装配（assemble → newBootstrap）同源。
var itDerived = mustDerive(itNS)

// mustDerive 派生命名空间；测试夹具里非法值直接 panic（token 由本文件生成，不会非法）。
func mustDerive(ns string) namespace.Derived {
	derived, err := namespace.Derive(ns)
	if err != nil {
		panic(err)
	}
	return derived
}

// e2eTopics 是本次测试运行独占的业务 topic 构造器：与进程内装配派生规则同源（同一 token），
// 发布方与订阅方必须一致，否则消息落在不同 subject。
var e2eTopics = mustTopics(itDerived)

// mustTopics 构造业务 topic 构造器（夹具里非法值直接 panic）。
func mustTopics(derived namespace.Derived) consts.Topics {
	topics, err := consts.NewTopics(derived)
	if err != nil {
		panic(err)
	}
	return topics
}

// probeCore 探测基础中间件（etcd/redis/nats）；不可用返回 skip 原因。
func probeCore(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cli, err := pkredis.NewClient(pkredis.Options{Addrs: []string{itRedisAddr}, Namespace: itNS})
	if err != nil {
		return "redis 构造失败: " + err.Error()
	}
	if err := cli.Ping(ctx); err != nil {
		_ = cli.Close()
		return "redis 不可用: " + err.Error()
	}
	_ = cli.Close()

	nc, err := pkgnats.Connect(pkgnats.Options{URL: itNatsURL, Name: "probe"})
	if err != nil {
		return "nats 不可用: " + err.Error()
	}
	nc.Close()

	ec, err := etcd.NewClient(etcd.Options{Endpoints: []string{itEtcdEndpoints}})
	if err != nil {
		return "etcd 不可用: " + err.Error()
	}
	_ = ec.Close()
	return ""
}

// probeMiddlewares 探测四中间件（含 mongo）；不可用返回 skip 原因。
func probeMiddlewares(t *testing.T) string {
	t.Helper()
	if reason := probeCore(t); reason != "" {
		return reason
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	mc, err := pkgmongo.NewClient(ctx, pkgmongo.Options{URI: itMongoURI, Database: itMongoDB})
	if err != nil {
		return "mongo 不可用: " + err.Error()
	}
	_ = mc.Close(ctx)
	return ""
}

// newGame 起 game 服务进程内装配。
func newGame(t *testing.T) *gameassemble.Game {
	t.Helper()
	g, err := gameassemble.New(context.Background(), gameassemble.Options{
		NodeID:        "game-it",
		EtcdEndpoints: []string{itEtcdEndpoints},
		NatsURL:       itNatsURL,
		RedisAddrs:    []string{itRedisAddr},
		MongoURI:      itMongoURI,
		MongoDB:       itMongoDB,
		Namespace:     itNS,
	})
	if err != nil {
		t.Fatalf("game 装配: %v", err)
	}
	t.Cleanup(func() { _ = g.Stop(context.Background()) })
	return g
}

// dialAdmin 拨号 game 的 internal 面（管理面 AdminService 只在此面可达）并返回管理面客户端。
// 需要验证安全边界时改用 game.EdgeGRPCURL 拨号，见 admin_test.go。
func dialAdmin(t *testing.T, addr string) admingamev1.AdminServiceClient {
	t.Helper()
	cli, err := atlasgrpc.DialInsecure(context.Background(), atlasgrpc.WithEndpoint(addr))
	if err != nil {
		t.Fatalf("管理面拨号(%s): %v", addr, err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return admingamev1.NewAdminServiceClient(cli)
}

// adminCtx 构造管理面请求信封：operator 必填（写操作另需幂等键），dryRun 只校验不改档。
func adminCtx(operator, key string, dryRun bool) *admingamev1.AdminContext {
	return &admingamev1.AdminContext{Operator: operator, IdempotencyKey: key, Reason: "e2e", DryRun: dryRun}
}

// newGateway 起一个 gateway 实例进程内装配。
func newGateway(t *testing.T, id string) *gwassemble.Gateway {
	t.Helper()
	gw, err := gwassemble.New(context.Background(), gwassemble.Options{
		ID:            id,
		EtcdEndpoints: []string{itEtcdEndpoints},
		NatsURL:       itNatsURL,
		RedisAddrs:    []string{itRedisAddr},
		Namespace:     itNS,
	})
	if err != nil {
		t.Fatalf("gateway 装配: %v", err)
	}
	t.Cleanup(func() { _ = gw.Stop(context.Background()) })
	return gw
}

// newBattle 起 battle 服务进程内装配（cfg 可覆盖战斗参数，nil 用默认）。
func newBattle(t *testing.T, cfg *battleassemble.BattleConfig) *battleassemble.Battle {
	t.Helper()
	b, err := battleassemble.New(context.Background(), battleassemble.Options{
		NodeID:        "battle-it",
		EtcdEndpoints: []string{itEtcdEndpoints},
		NatsURL:       itNatsURL,
		MongoURI:      itMongoURI,
		MongoDB:       itMongoDB,
		BattleCfg:     cfg,
		Namespace:     itNS,
		TicketKey:     itTicketKey,
		EdgeEndpoints: itEdgeEndpoints,
	})
	if err != nil {
		t.Fatalf("battle 装配: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })
	return b
}

// seedBattle 预置战斗成员：直接开局战斗 actor（懒激活），供战斗通道测试使用。
func seedBattle(t *testing.T, ctx context.Context, b *battleassemble.Battle, battleID string, playerIDs ...string) {
	t.Helper()
	pid, err := types.NewPID(battlev1actor.BattleServiceActorType, battleID)
	if err != nil {
		t.Fatalf("seedBattle PID: %v", err)
	}
	cli := battlev1actor.NewBattleServiceClusterClient(b.Runtime)
	if _, err := cli.Create(ctx, pid, &battlev1.CreateBattleRequest{
		MatchId:   "m-seed-" + battleID,
		PlayerIds: playerIDs,
	}); err != nil {
		t.Fatalf("seedBattle 开局: %v", err)
	}
}
