package domainclient

import (
	"context"
	stderrors "errors"
	"testing"

	"google.golang.org/grpc"
)

// fakeConn 是 grpc.ClientConnInterface 的最小实现（本包只用它做连接身份标记；
// 内嵌接口满足方法集，测试不会真正发起调用）。
type fakeConn struct {
	grpc.ClientConnInterface
	closed bool
}

// TestResolverCachesPerService 验证：按 proto 服务名派生注册中心服务名、
// 每个服务只拨一次、同服务复用同一连接、不同服务各自缓存。
func TestResolverCachesPerService(t *testing.T) {
	var dialed []string
	r := newResolver(func(_ context.Context, service string) (grpc.ClientConnInterface, func() error, error) {
		dialed = append(dialed, service)
		conn := &fakeConn{}
		return conn, func() error { conn.closed = true; return nil }, nil
	})

	first, err := r.Conn(context.Background(), "game.v1.PlayerService")
	if err != nil {
		t.Fatalf("Conn(game) 错误 = %v", err)
	}
	if _, err := r.Conn(context.Background(), "battle.v1.BattleService"); err != nil {
		t.Fatalf("Conn(battle) 错误 = %v", err)
	}
	again, err := r.Conn(context.Background(), "game.v1.PlayerService")
	if err != nil {
		t.Fatalf("Conn(game) 二次错误 = %v", err)
	}
	if first != again {
		t.Errorf("同服务应复用连接（同指针）")
	}
	want := []string{"game", "battle"}
	if len(dialed) != len(want) || dialed[0] != want[0] || dialed[1] != want[1] {
		t.Fatalf("拨号服务名 = %v, 期望 %v（proto 服务名首段）", dialed, want)
	}

	if err := r.Close(); err != nil {
		t.Fatalf("Close 错误 = %v", err)
	}
	if !first.(*fakeConn).closed {
		t.Errorf("Close 应关闭已建连接")
	}
}

// TestResolverRejectsBadServiceName 验证服务名派生失败即报错（不降级成"连不上"这类难查现象）。
func TestResolverRejectsBadServiceName(t *testing.T) {
	r := newResolver(func(context.Context, string) (grpc.ClientConnInterface, func() error, error) {
		t.Fatal("非法服务名不应触发拨号")
		return nil, nil, nil
	})
	for _, name := range []string{"", "game", ".v1.S", "game."} {
		if _, err := r.Conn(context.Background(), name); err == nil {
			t.Errorf("服务名 %q 应报错", name)
		}
	}
}

// TestResolverPropagatesDialError 验证拨号失败原样上抛（调用方按错误处理，不静默降级）。
func TestResolverPropagatesDialError(t *testing.T) {
	boom := stderrors.New("dial boom")
	r := newResolver(func(context.Context, string) (grpc.ClientConnInterface, func() error, error) {
		return nil, nil, boom
	})
	if _, err := r.Conn(context.Background(), "game.v1.PlayerService"); !stderrors.Is(err, boom) {
		t.Fatalf("错误 = %v, 期望 %v", err, boom)
	}
}

// TestResolverRejectsNilConn 验证拨号返回空连接即报错（装配缺陷不静默成功）。
func TestResolverRejectsNilConn(t *testing.T) {
	r := newResolver(func(context.Context, string) (grpc.ClientConnInterface, func() error, error) {
		return nil, nil, nil
	})
	if _, err := r.Conn(context.Background(), "game.v1.PlayerService"); err == nil {
		t.Fatal("空连接应报错")
	}
}
