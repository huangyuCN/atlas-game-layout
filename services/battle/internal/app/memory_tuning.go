package app

import (
	"fmt"
	"math"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	atlaslog "github.com/huangyuCN/atlas/log"
)

// GC 调优档位与边界（配置校验、文档与测试共用同一份常量，避免口径漂移）。
const (
	// memoryModeOff 是缺省档位：不碰 GOGC / GOMEMLIMIT，现行为零变化。
	memoryModeOff = "off"
	// memoryModeTuned 是调优档位：GOGC=off 等价行为 + 进程内存软上限。
	memoryModeTuned = "tuned"
	// DefaultMemoryLimit 是 tuned 且未显式配 memory_limit 时的预设值（3GiB）：
	// 取自压测验证过的取值（每进程 RSS 实测 2.85GiB，上限仍绑定）；**推荐显式配置**，
	// 显式值可预测、不随实现改预设而漂移。
	DefaultMemoryLimit = int64(3) << 30
	// MinMemoryLimit 是内存上限的配置下限（512MiB）：更小的上限会让 GOGC=off 的进程
	// 被软上限反复触发 GC，延迟收益归零甚至反转，属于致命误配，故启动即失败。
	MinMemoryLimit = int64(512) << 20
)

// memoryTuning 是解析后的 GC 调优意图。enabled=false 时 limit 无意义，
// 应用层必须一次都不调用运行时 API（守「缺省关闭」的回归）。
type memoryTuning struct {
	enabled bool
	limit   int64
}

// runtimeTuner 把两个标准库 setter 收成可注入依赖：
// 测试注入记录器即可断言「缺省零调用 / 开启各调用一次」，实现侧只认这组函数签名。
type runtimeTuner struct {
	setGCPercent   func(int) int
	setMemoryLimit func(int64) int64
}

// memoryTuningOf 解析并校验 battle.memory_mode / battle.memory_limit：
//   - 缺省 / "off"：关闭（不设置任何运行时值）；此时配了 memory_limit 视为误配，启动失败；
//   - "tuned"：开启；memory_limit 未配取 DefaultMemoryLimit（3GiB），显式值须为
//     二进制单位且 ≥ MinMemoryLimit（512MiB）；
//   - 未知档位、非法容量、非正数、溢出：一律启动失败（「配了没生效」比启动失败更贵）。
func memoryTuningOf(b *conf.BattleConf) (memoryTuning, error) {
	mode := strings.TrimSpace(b.GetMemoryMode())
	rawLimit := strings.TrimSpace(b.GetMemoryLimit())
	if mode == "" {
		mode = memoryModeOff
	}
	switch mode {
	case memoryModeOff:
		if rawLimit != "" {
			return memoryTuning{}, fmt.Errorf(
				"app: battle.memory_limit=%q 已配置但 memory_mode 未开启（缺省/off 不设置内存上限；"+
					"要么配 memory_mode: tuned，要么删掉本项）", rawLimit)
		}
		return memoryTuning{}, nil
	case memoryModeTuned:
		limit := DefaultMemoryLimit
		if rawLimit != "" {
			parsed, err := parseMemoryLimit(rawLimit)
			if err != nil {
				return memoryTuning{}, err
			}
			limit = parsed
		}
		if limit < MinMemoryLimit {
			return memoryTuning{}, fmt.Errorf(
				"app: battle.memory_limit=%dMiB 小于下限 512MiB：GOGC=off 下内存余量不足会被软上限"+
					"反复触发 GC，延迟收益归零甚至反转", limit>>20)
		}
		return memoryTuning{enabled: true, limit: limit}, nil
	default:
		return memoryTuning{}, fmt.Errorf(
			"app: battle.memory_mode=%q 非法（缺省/off = 关闭；tuned = GOGC=off + memory_limit）", mode)
	}
}

// parseMemoryLimit 解析进程内存上限字符串：只接受「正整数 + 二进制单位」
// （KiB/MiB/GiB/TiB，如 512MiB、3GiB），拒绝裸数字与十进制单位（GB 的 10^9 口径
// 会与 GOMEMLIMIT 的字节语义差出 7%，配置面不提供这种含糊）。
func parseMemoryLimit(raw string) (int64, error) {
	units := []struct {
		suffix string
		factor int64
	}{
		{"KiB", 1 << 10},
		{"MiB", 1 << 20},
		{"GiB", 1 << 30},
		{"TiB", 1 << 40},
	}
	for _, u := range units {
		num, ok := strings.CutSuffix(raw, u.suffix)
		if !ok {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(num), 10, 64)
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("app: battle.memory_limit=%q 非法（需为 <正整数><KiB|MiB|GiB|TiB>，如 3GiB）", raw)
		}
		if n > math.MaxInt64/u.factor {
			return 0, fmt.Errorf("app: battle.memory_limit=%q 过大（换算成字节超出 int64 范围）", raw)
		}
		return n * u.factor, nil
	}
	return 0, fmt.Errorf("app: battle.memory_limit=%q 非法（需为 <正整数><KiB|MiB|GiB|TiB>，如 3GiB）", raw)
}

// applyMemoryTuning 把调优意图落到运行时 setter：关闭档位**一次都不调用**；
// 开启档位先立内存上限、再关 GC（顺序不可反：先关 GC 会留出一段没有上限的窗口）。
func applyMemoryTuning(t runtimeTuner, m memoryTuning) {
	if !m.enabled {
		return
	}
	t.setMemoryLimit(m.limit)
	t.setGCPercent(-1)
}

// newMemoryTuning 是 fx 提供者：从 Bootstrap 的 battle 段解析调优意图。
// battle 段缺失时按关闭返回（必填校验归 newActorConfig，避免这里抢跑报出误导性错误）。
func newMemoryTuning(cfg *conf.Bootstrap) (memoryTuning, error) {
	return memoryTuningOf(cfg.GetBattle())
}

// registerMemoryTuning 是 fx 启动钩子（在 invoke 列表首位执行）：开启档位时在进程内
// 设置 GOGC=off 等价行为与内存软上限。用 debug 标准库 API 而非环境变量——进程内即时生效，
// 且不依赖部署侧额外注入；关闭档位不产生任何副作用，也不打日志。
func registerMemoryTuning(m memoryTuning) {
	if !m.enabled {
		return
	}
	applyMemoryTuning(runtimeTuner{
		setGCPercent:   debug.SetGCPercent,
		setMemoryLimit: debug.SetMemoryLimit,
	}, m)
	atlaslog.Infof("battle GC 调优已启用: memory_mode=tuned, gc=off, memory_limit=%dMiB", m.limit>>20)
}
