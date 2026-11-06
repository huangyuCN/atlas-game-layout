package actor

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/huangyuCN/atlas/contrib/actor/core"
	"github.com/huangyuCN/atlas/contrib/actor/types"
	atlaslog "github.com/huangyuCN/atlas/log"
)

// quietCtx 用「嵌入接口」的方式做最小桩：只实现本用例真正会走到的三个方法，
// 其余方法一旦被调用就 panic（嵌入的 nil 接口）——这正好也能证明中间件没碰别的能力。
type quietCtx struct {
	core.ActorContext
	ctx context.Context
}

// Self 返回零值 PID（本用例不校验身份）。
func (c quietCtx) Self() types.PID { return types.PID{} }

// Header 表示没有携带任何投递头。
func (c quietCtx) Header(string) (string, bool) { return "", false }

// Context 返回调用方给的上下文。
func (c quietCtx) Context() context.Context { return c.ctx }

// TestLoggingMiddlewareSkipsWorkWhenDebugDisabled 钉住热路径：debug 关掉时中间件**一次分配都不该有**。
//
// 为什么关键：原实现在每次 actor 调用里都做 ctx.Header 查询 + fmt.Sprintf("%T") + atlaslog.Ctx
// （后者会 WithContext），即使 debug 关闭也照付——4,096 流实测该中间件占 battle CPU 8%
// （约 0.33 核/进程）。改为「先判级别，未开启直接放行」。
func TestLoggingMiddlewareSkipsWorkWhenDebugDisabled(t *testing.T) {
	old := atlaslog.GetLogger()
	atlaslog.SetLogger(atlaslog.New(atlaslog.WithMinLevel(atlaslog.LevelInfo)))
	t.Cleanup(func() { atlaslog.SetLogger(old) })

	calls := 0
	h := LoggingMiddleware(func(core.ActorContext, any) error {
		calls++
		return nil
	})
	// 用指针：值类型装进接口参数本身要堆分配，会把「测试自身的一次分配」算进判据。
	ctx := &quietCtx{ctx: context.Background()}
	msg := &struct{ X int }{}

	if err := h(ctx, msg); err != nil {
		t.Fatalf("中间件不应报错: %v", err)
	}
	if calls != 1 {
		t.Fatalf("业务处理应被调用 1 次，实际 %d", calls)
	}
	allocs := testing.AllocsPerRun(500, func() { _ = h(ctx, msg) })
	if allocs > 0 {
		t.Fatalf("debug 关闭时日志中间件不得分配（fmt.Sprintf/WithContext 每次都会分配），实测 %.2f 次/调用", allocs)
	}
}

// TestLoggingMiddlewarePassesThroughNilContext 钉住 nil 上下文的兼容：原实现在 ctx 为 nil 时直接放行。
func TestLoggingMiddlewarePassesThroughNilContext(t *testing.T) {
	called := false
	h := LoggingMiddleware(func(core.ActorContext, any) error {
		called = true
		return nil
	})
	if err := h(nil, struct{}{}); err != nil {
		t.Fatalf("nil 上下文不应报错: %v", err)
	}
	if !called {
		t.Fatal("nil 上下文也必须调用业务处理")
	}
}

// TestLoggingMiddlewareLogsWhenDebugEnabled 钉住"闸门不得把该有的日志也关掉"：
// debug 级别开启时，一次 actor 调用必须产出一条含消息类型的日志行。
func TestLoggingMiddlewareLogsWhenDebugEnabled(t *testing.T) {
	old := atlaslog.GetLogger()
	var buf lineBuffer
	atlaslog.SetLogger(atlaslog.New(atlaslog.WithMinLevel(atlaslog.LevelDebug), atlaslog.WithWriter(&buf)))
	t.Cleanup(func() { atlaslog.SetLogger(old) })

	h := LoggingMiddleware(func(core.ActorContext, any) error { return nil })
	if err := h(&quietCtx{ctx: context.Background()}, struct{ X int }{}); err != nil {
		t.Fatalf("中间件不应报错: %v", err)
	}
	if out := buf.String(); !strings.Contains(out, "actor tell") {
		t.Fatalf("debug 开启时应输出 actor tell 日志，实际输出 %q", out)
	}
}

// lineBuffer 是并发安全的最小日志落点（日志器可能在工作协程写）。
type lineBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write 追加一段日志输出。
func (b *lineBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// String 返回当前已写入的内容。
func (b *lineBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
