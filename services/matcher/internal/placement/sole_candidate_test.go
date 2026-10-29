package placement

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/huangyuCN/atlas-game-layout/lib/consts"
	atlaslog "github.com/huangyuCN/atlas/log"
	"github.com/huangyuCN/atlas/registry"
)

// 本文件钉住「候选集只剩 1 台时必须告警」：轮询在单候选上等价于「全部对局落到同一台」，
// 而这类偏斜（实测 501 局里 486 局同台）在选点代码里完全看不出来——代码没错、计数没错，
// 错的是候选集本身（实例 id 缺省导致同名键被顶掉、注册冲突、帧面过滤把某台摘掉）。
// 故把「候选集大小」做成可观测信号，下次偏斜先看 WARN，不必再从选点算法怀疑起。

// captureLogger 是只收集 WARN 的日志桩（实现 atlaslog.Logger 全集，其余级别丢弃）。
type captureLogger struct {
	mu    sync.Mutex
	warns []string
}

// IsLevelEnabled 恒真：桩不做级别过滤。
func (l *captureLogger) IsLevelEnabled(atlaslog.Level) bool { return true }

// Log 收集 WARN 及更高级别的消息。
func (l *captureLogger) Log(level atlaslog.Level, msg string, _ ...any) {
	if level >= atlaslog.LevelWarn {
		l.record(msg)
	}
}

// Debug 丢弃。
func (l *captureLogger) Debug(string, ...any) {}

// Info 丢弃。
func (l *captureLogger) Info(string, ...any) {}

// Warn 收集。
func (l *captureLogger) Warn(msg string, _ ...any) { l.record(msg) }

// Error 收集。
func (l *captureLogger) Error(msg string, _ ...any) { l.record(msg) }

// Fatal 丢弃（测试里不触发）。
func (l *captureLogger) Fatal(string, ...any) {}

// With 返回自身（桩不区分字段）。
func (l *captureLogger) With(...any) atlaslog.Logger { return l }

// WithContext 返回自身。
func (l *captureLogger) WithContext(context.Context) atlaslog.Logger { return l }

// record 追加一条 WARN 文本。
func (l *captureLogger) record(msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, msg)
}

// soleWarns 返回提及「候选集」的告警条数（只看本用例关心的那一条）。
func (l *captureLogger) soleWarns() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, m := range l.warns {
		if strings.Contains(m, "候选集") {
			n++
		}
	}
	return n
}

// installCaptureLogger 替换全局 Logger 并在用例结束后还原。
func installCaptureLogger(t *testing.T) *captureLogger {
	t.Helper()
	old := atlaslog.GetLogger()
	cl := &captureLogger{}
	atlaslog.SetLogger(cl)
	t.Cleanup(func() { atlaslog.SetLogger(old) })
	return cl
}

// TestPlannerWarnsWhenOnlyOneCandidate 断言候选集塌成 1 台时必须有一条 WARN，
// 且**不重复刷**（快照按 TTL 重建，连续多次选点只告警一次）。
func TestPlannerWarnsWhenOnlyOneCandidate(t *testing.T) {
	cl := installCaptureLogger(t)
	disc := &fakeDiscovery{ins: []*registry.ServiceInstance{
		battleInstance("battle-b2", "127.0.0.1:9311"),
	}}
	p := New(disc, Options{CacheTTL: 0}) // 快照每次都重建 = 最坏情况的重复告警路径
	for i := 0; i < 5; i++ {
		_ = p.Place(context.Background())
	}
	if got := cl.soleWarns(); got != 1 {
		t.Fatalf("单候选应恰好告警 1 次（只在候选数变化时打），实际 %d 次", got)
	}
}

// TestPlannerSilentWithTwoCandidates 断言两台候选（健康态）不得打候选集告警。
func TestPlannerSilentWithTwoCandidates(t *testing.T) {
	cl := installCaptureLogger(t)
	disc := &fakeDiscovery{ins: []*registry.ServiceInstance{
		battleInstance(consts.ServiceBattle+"-1", "127.0.0.1:9301"),
		battleInstance(consts.ServiceBattle+"-b2", "127.0.0.1:9311"),
	}}
	p := New(disc, Options{CacheTTL: 0})
	for i := 0; i < 5; i++ {
		_ = p.Place(context.Background())
	}
	if got := cl.soleWarns(); got != 0 {
		t.Fatalf("两台候选不应告警，实际 %d 次", got)
	}
}
