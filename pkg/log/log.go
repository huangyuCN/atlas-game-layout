// Package log 提供游戏模板统一的日志初始化装配：
// 解析配置中的级别/格式，构造 Atlas 结构化日志并设置为全局 Logger。
// 实现复用 Atlas 顶层 log 包（slog 基础），本包只做装配。
package log

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	atlaslog "github.com/huangyuCN/atlas/log"
)

// Options 是日志初始化选项（来自各服务配置文件）。
type Options struct {
	// Level 输出级别：debug/info/warn/error/fatal（默认 info）。
	Level string
	// Format 输出格式：text/json（默认 text）。
	Format string
	// Service 服务名，注入每条日志的 service 字段。
	Service string
	// Writer 输出目标（默认 os.Stdout）。
	Writer io.Writer
	// File 是可选的日志文件路径：非空时同时写 stdout 与该文件（文件供
	// promtail 等采集器读取进 Loki，测试环境经 Grafana 面板查询）。
	// 目录不存在自动创建；写盘失败回退仅 stdout（日志不得阻断服务）。
	File string
}

// Init 按 Options 初始化全局日志。
func Init(opts Options) error {
	level, err := parseLevel(opts.Level)
	if err != nil {
		return err
	}
	format, err := parseFormat(opts.Format)
	if err != nil {
		return err
	}
	if opts.Writer == nil {
		opts.Writer = os.Stdout
	}

	if opts.File != "" {
		if f, ferr := openLogFile(opts.File); ferr == nil {
			// stdout + 文件双写：本地终端可观测，文件供采集器（promtail）进 Loki。
			opts.Writer = io.MultiWriter(opts.Writer, f)
		}
	}
	ao := []atlaslog.Option{
		atlaslog.WithFormat(format),
		atlaslog.WithWriter(opts.Writer),
		atlaslog.WithMinLevel(level),
	}
	if opts.Service != "" {
		ao = append(ao, atlaslog.WithServiceName(opts.Service))
	}
	atlaslog.SetLogger(atlaslog.New(ao...))
	return nil
}

// parseLevel 解析级别字符串。
func parseLevel(s string) (atlaslog.Level, error) {
	switch s {
	case "debug":
		return atlaslog.LevelDebug, nil
	case "info", "":
		return atlaslog.LevelInfo, nil
	case "warn":
		return atlaslog.LevelWarn, nil
	case "error":
		return atlaslog.LevelError, nil
	case "fatal":
		return atlaslog.LevelFatal, nil
	default:
		return atlaslog.LevelInfo, fmt.Errorf("log: 未知级别 %q（支持 debug/info/warn/error/fatal）", s)
	}
}

// parseFormat 解析格式字符串。
func parseFormat(s string) (atlaslog.Format, error) {
	switch s {
	case "text", "":
		return atlaslog.FormatText, nil
	case "json":
		return atlaslog.FormatJSON, nil
	default:
		return atlaslog.FormatText, fmt.Errorf("log: 未知格式 %q（支持 text/json）", s)
	}
}

// openLogFile 打开（必要时创建目录）日志文件供追加写。
func openLogFile(path string) (io.Writer, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
}
