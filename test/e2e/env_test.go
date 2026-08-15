// Package e2e 提供跨服务的端到端集成测试（真 etcd/redis/nats/mongo，
// 本地 Docker 端口与集成服务器约定一致；不可达时跳过，Skipf 风格）。
package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/huangyuCN/atlas-game-layout/pkg/etcd"
	pkgmongo "github.com/huangyuCN/atlas-game-layout/pkg/mongo"
	pkgnats "github.com/huangyuCN/atlas-game-layout/pkg/nats"
	pkredis "github.com/huangyuCN/atlas-game-layout/pkg/redis"
	gameassemble "github.com/huangyuCN/atlas-game-layout/services/game/assemble"
	gwassemble "github.com/huangyuCN/atlas-game-layout/services/gateway/assemble"
)

// 集成环境地址（本地 Docker 端口约定，与 10.10.9.36 集成服务器一致）。
const (
	itEtcdEndpoints = "127.0.0.1:12379"
	itRedisAddr     = "127.0.0.1:16379"
	itNatsURL       = "nats://127.0.0.1:14222"
	itMongoURI      = "mongodb://127.0.0.1:27018"
	itMongoDB       = "game_it"
)

// probeMiddlewares 探测四中间件；不可用返回 skip 原因。
func probeMiddlewares(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cli, err := pkredis.NewClient(pkredis.Options{Addr: itRedisAddr})
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
		RedisAddr:     itRedisAddr,
		MongoURI:      itMongoURI,
		MongoDB:       itMongoDB,
	})
	if err != nil {
		t.Fatalf("game 装配: %v", err)
	}
	t.Cleanup(func() { _ = g.Stop(context.Background()) })
	return g
}

// newGateway 起一个 gateway 实例进程内装配。
func newGateway(t *testing.T, id string) *gwassemble.Gateway {
	t.Helper()
	gw, err := gwassemble.New(context.Background(), gwassemble.Options{
		ID:            id,
		EtcdEndpoints: []string{itEtcdEndpoints},
		NatsURL:       itNatsURL,
		RedisAddr:     itRedisAddr,
	})
	if err != nil {
		t.Fatalf("gateway 装配: %v", err)
	}
	t.Cleanup(func() { _ = gw.Stop(context.Background()) })
	return gw
}
