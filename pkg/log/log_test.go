package log

import (
	"bytes"
	"testing"

	atlaslog "github.com/huangyuCN/atlas/log"
)

// 捕获全局日志输出，验证初始化装配生效。
type bufWriter struct{ bytes.Buffer }

func (w *bufWriter) Write(p []byte) (int, error) { return w.Buffer.Write(p) }

// TestInitGlobalLogger 验证 Init 后全局 Logger 生效且级别过滤正确。
func TestInitGlobalLogger(t *testing.T) {
	var buf bufWriter
	if err := Init(Options{
		Level:   "info",
		Format:  "text",
		Service: "demo",
		Writer:  &buf,
	}); err != nil {
		t.Fatalf("Init() 错误 = %v", err)
	}

	atlaslog.Debug("debug-msg", "k", "v")
	atlaslog.Info("info-msg", "k", "v")

	got := buf.String()
	if got == "" {
		t.Fatal("Init 后全局日志无输出")
	}
	if bytes.Contains([]byte(got), []byte("debug-msg")) {
		t.Errorf("info 级别下不应输出 debug 日志: %s", got)
	}
	if !bytes.Contains([]byte(got), []byte("info-msg")) {
		t.Errorf("info 级别应输出 info 日志: %s", got)
	}
}

// TestInitJSONFormat 验证 JSON 格式输出。
func TestInitJSONFormat(t *testing.T) {
	var buf bufWriter
	if err := Init(Options{Level: "info", Format: "json", Writer: &buf}); err != nil {
		t.Fatalf("Init() 错误 = %v", err)
	}
	atlaslog.Info("hello", "k", "v")
	got := buf.String()
	if !bytes.Contains([]byte(got), []byte(`"msg":"hello"`)) {
		t.Errorf("JSON 格式输出不符合预期: %s", got)
	}
}

// TestInitDefaultWriter 验证未指定 Writer 时回退标准输出（不 panic）。
func TestInitDefaultWriter(t *testing.T) {
	if err := Init(Options{Level: "warn", Format: "text"}); err != nil {
		t.Fatalf("Init() 错误 = %v", err)
	}
	atlaslog.Warn("warn-msg")
}

// TestInitBadLevel 验证非法级别报错。
func TestInitBadLevel(t *testing.T) {
	if err := Init(Options{Level: "verbose", Format: "text"}); err == nil {
		t.Fatal("Init() 期望级别解析错误，实际为 nil")
	}
}

// TestParseLevel 验证级别字符串解析的全量映射。
func TestParseLevel(t *testing.T) {
	cases := map[string]atlaslog.Level{
		"debug": atlaslog.LevelDebug,
		"info":  atlaslog.LevelInfo,
		"warn":  atlaslog.LevelWarn,
		"error": atlaslog.LevelError,
		"fatal": atlaslog.LevelFatal,
	}
	for in, want := range cases {
		got, err := parseLevel(in)
		if err != nil {
			t.Errorf("parseLevel(%q) 错误 = %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("parseLevel(%q) = %v, 期望 %v", in, got, want)
		}
	}
}
