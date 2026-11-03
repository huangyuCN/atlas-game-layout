package app

import (
	"strings"
	"testing"

	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
)

// tuningRecorder 记录注入 setter 的调用（GC 百分比与内存上限各一条流水）：
// 用注入而不是读进程真实状态，才能在「缺省关闭」用例里断言**一次都没调用**，
// 也让用例不必污染测试进程的 GC 行为。
type tuningRecorder struct {
	gcPercents []int
	memLimits  []int64
}

// setGCPercent 记录一次 debug.SetGCPercent 调用。
func (r *tuningRecorder) setGCPercent(v int) int {
	r.gcPercents = append(r.gcPercents, v)
	return 0
}

// setMemoryLimit 记录一次 debug.SetMemoryLimit 调用。
func (r *tuningRecorder) setMemoryLimit(v int64) int64 {
	r.memLimits = append(r.memLimits, v)
	return 0
}

// tuner 把记录器包装成被测函数需要的 setter 组合。
func (r *tuningRecorder) tuner() runtimeTuner {
	return runtimeTuner{setGCPercent: r.setGCPercent, setMemoryLimit: r.setMemoryLimit}
}

// TestMemoryTuningDefaultOff 验证缺省/显式 off 两种写法都**不调用**两个运行时 API：
// 不设 GOGC 也不设 GOMEMLIMIT，现行为零变化（这是本配置项最重要的回归守卫）。
func TestMemoryTuningDefaultOff(t *testing.T) {
	for _, mode := range []string{"", "off"} {
		rec := &tuningRecorder{}
		got, err := memoryTuningOf(&conf.BattleConf{MemoryMode: mode})
		if err != nil {
			t.Fatalf("memory_mode=%q：memoryTuningOf: %v", mode, err)
		}
		if got.enabled {
			t.Fatalf("memory_mode=%q：竟然解析为开启（limit=%d）", mode, got.limit)
		}
		applyMemoryTuning(rec.tuner(), got)
		if len(rec.gcPercents) != 0 || len(rec.memLimits) != 0 {
			t.Fatalf("memory_mode=%q：缺省/关闭路径调用了运行时 API（gc=%v, limit=%v），期望零调用",
				mode, rec.gcPercents, rec.memLimits)
		}
	}
}

// TestMemoryTuningTunedApplies 验证开启路径：GC 百分比置 -1（等同 GOGC=off），
// 内存上限取配置值（显式 3GiB / 512MiB 下界 / 未配走 3GiB 预设）。
func TestMemoryTuningTunedApplies(t *testing.T) {
	cases := []struct {
		name  string
		limit string
		want  int64
	}{
		{name: "显式 3GiB（压测口径）", limit: "3GiB", want: 3 << 30},
		{name: "下界 512MiB", limit: "512MiB", want: 512 << 20},
		{name: "未配走预设 3GiB", limit: "", want: DefaultMemoryLimit},
	}
	for _, c := range cases {
		rec := &tuningRecorder{}
		got, err := memoryTuningOf(&conf.BattleConf{MemoryMode: "tuned", MemoryLimit: c.limit})
		if err != nil {
			t.Fatalf("%s：memoryTuningOf: %v", c.name, err)
		}
		if !got.enabled || got.limit != c.want {
			t.Fatalf("%s：解析结果 = %+v, 期望 enabled=true limit=%d", c.name, got, c.want)
		}
		applyMemoryTuning(rec.tuner(), got)
		if len(rec.gcPercents) != 1 || rec.gcPercents[0] != -1 {
			t.Fatalf("%s：SetGCPercent 调用 = %v, 期望恰好一次 -1（GOGC=off）", c.name, rec.gcPercents)
		}
		if len(rec.memLimits) != 1 || rec.memLimits[0] != c.want {
			t.Fatalf("%s：SetMemoryLimit 调用 = %v, 期望恰好一次 %d", c.name, rec.memLimits, c.want)
		}
	}
}

// TestMemoryTuningRejectsTooSmallLimit 验证 memory_limit 过小（< 512MiB）即启动失败：
// GOGC=off 下没有足够内存余量兜底，进程会被内存上限反复触发 GC 拖死，属于致命误配。
func TestMemoryTuningRejectsTooSmallLimit(t *testing.T) {
	for _, limit := range []string{"256MiB", "511MiB", "1KiB"} {
		_, err := newMemoryTuning(&conf.Bootstrap{Battle: &conf.BattleConf{MemoryMode: "tuned", MemoryLimit: limit}})
		if err == nil {
			t.Fatalf("memory_limit=%q 竟然通过校验（期望启动失败）", limit)
		}
		if !strings.Contains(err.Error(), "512MiB") {
			t.Fatalf("memory_limit=%q 的错误未提示 512MiB 下界：%v", limit, err)
		}
	}
}

// TestMemoryTuningRejectsInvalid 验证非法配置一律启动失败：
// 未知档位、无法解析的容量、非正数、溢出、只有 limit 没有 tuned、超范围单位。
func TestMemoryTuningRejectsInvalid(t *testing.T) {
	cases := []struct {
		name  string
		mode  string
		limit string
	}{
		{name: "未知档位", mode: "aggressive", limit: "3GiB"},
		{name: "容量缺失单位", mode: "tuned", limit: "3"},
		{name: "非二进制单位（3GB）", mode: "tuned", limit: "3GB"},
		{name: "零值", mode: "tuned", limit: "0GiB"},
		{name: "负值", mode: "tuned", limit: "-1GiB"},
		{name: "超出 int64 的乘法溢出", mode: "tuned", limit: "9223372036854775807GiB"},
		{name: "关闭档位却配了上限（配了没生效）", mode: "off", limit: "3GiB"},
	}
	for _, c := range cases {
		if _, err := newMemoryTuning(&conf.Bootstrap{Battle: &conf.BattleConf{MemoryMode: c.mode, MemoryLimit: c.limit}}); err == nil {
			t.Fatalf("%s：mode=%q limit=%q 竟然通过校验（期望启动失败）", c.name, c.mode, c.limit)
		}
	}
}

// TestNewMemoryTuningFromBootstrap 验证 fx 提供者从 Bootstrap 取 battle 段：
// 缺少 battle 段时按关闭处理（真正的必填校验由 newActorConfig 负责，
// 这里不能因为先跑而报出误导性的内存错误）。
func TestNewMemoryTuningFromBootstrap(t *testing.T) {
	off, err := newMemoryTuning(&conf.Bootstrap{})
	if err != nil {
		t.Fatalf("缺少 battle 段：newMemoryTuning: %v", err)
	}
	if off.enabled {
		t.Fatalf("缺少 battle 段：期望关闭，实际 %+v", off)
	}
}
