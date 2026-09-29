package serverutil

import (
	"context"
	stderrors "errors"
	"testing"

	"google.golang.org/grpc"
)

// fakeConn 是 grpc.ClientConnInterface 的最小实现：记录调用次数与关闭状态。
type fakeConn struct {
	invokes int
	streams int
	closed  bool
}

// Invoke 记录一元调用。
func (f *fakeConn) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	f.invokes++
	return nil
}

// NewStream 记录开流调用（本测试不实际开流）。
func (f *fakeConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	f.streams++
	return nil, stderrors.New("not implemented")
}

// Close 记录关闭。
func (f *fakeConn) Close() error {
	f.closed = true
	return nil
}

// TestLazyConnDialsOnFirstUse 验证惰性拨号语义：
// 构造不拨号；首个调用才拨号；成功即缓存复用（同连接服务多次调用）；Close 关闭连接。
func TestLazyConnDialsOnFirstUse(t *testing.T) {
	calls := 0
	conn := &fakeConn{}
	c := NewLazyConn(func(context.Context) (grpc.ClientConnInterface, error) {
		calls++
		return conn, nil
	})
	if calls != 0 {
		t.Fatalf("构造即拨号（应惰性），calls=%d", calls)
	}
	for i := 1; i <= 2; i++ {
		if err := c.Invoke(context.Background(), "/game.v1.PlayerService/GetPlayerData", nil, nil); err != nil {
			t.Fatalf("第 %d 次 Invoke 错误 = %v", i, err)
		}
	}
	if calls != 1 {
		t.Fatalf("拨号次数 = %d, 期望 1（首个调用拨号并缓存）", calls)
	}
	if conn.invokes != 2 {
		t.Fatalf("底层连接调用次数 = %d, 期望 2", conn.invokes)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close 错误 = %v", err)
	}
	if !conn.closed {
		t.Fatal("Close 应关闭底层连接")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("重复 Close 应安全: %v", err)
	}
}

// TestLazyConnRejectsMissingDialer 验证装配缺失即报错（不 panic、不静默成功）。
func TestLazyConnRejectsMissingDialer(t *testing.T) {
	if err := NewLazyConn(nil).Invoke(context.Background(), "/x", nil, nil); err == nil {
		t.Fatal("缺拨号函数应报错")
	}
}

// TestLazyConnPropagatesDialError 验证拨号失败上抛、且失败结果不缓存（下次调用重试）。
func TestLazyConnPropagatesDialError(t *testing.T) {
	boom := stderrors.New("dial boom")
	calls := 0
	c := NewLazyConn(func(context.Context) (grpc.ClientConnInterface, error) {
		calls++
		return nil, boom
	})
	for i := 1; i <= 2; i++ {
		if err := c.Invoke(context.Background(), "/x", nil, nil); !stderrors.Is(err, boom) {
			t.Fatalf("第 %d 次调用错误 = %v, 期望 %v", i, err, boom)
		}
		if calls != i {
			t.Fatalf("第 %d 次调用后拨号次数 = %d（失败不应缓存）", i, calls)
		}
	}
}

// TestLazyConnRejectsNilConn 验证拨号返回空连接即报错（装配缺陷不静默成功）。
func TestLazyConnRejectsNilConn(t *testing.T) {
	c := NewLazyConn(func(context.Context) (grpc.ClientConnInterface, error) { return nil, nil })
	if err := c.Invoke(context.Background(), "/x", nil, nil); err == nil {
		t.Fatal("空连接应报错")
	}
}
