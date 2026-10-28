// clock.go 量「压测端 - 服务端」的时钟差 Δ（毫秒），供广播延迟做**单一口径**校正。
//
// 为什么需要：broadcast_downlink_ms = time.Since(服务端 server_time) 里含两端时钟差，
// 而两端 NTP 各自同步（源不同、抖动不同），跨轮比较会凭空多出几十毫秒。
//
// 口径（每轮开始/结束各测一次）：
//  1. 两端各读一次 NTP 自评偏移（timedatectl timesync-status 的 Offset），只作旁证；
//  2. 主口径是**跨机实测**：在一条已建立的 SSH 多路复用连接上反复执行 date，
//     按 NTP 公式 offset = server - (t0+t3)/2 取 RTT 最小的样本，再取负得 Δ = client - server。
//     主连接先预热（ControlMaster）——否则每次 ssh 的握手时间会偏进 Δ（可达数十毫秒）。
//
// 校正：真实单程 ≈ 原始 - Δ。轮内漂移 >5ms 时该轮判「不可用」（Δ 的保守精度见 clock_uncertainty_ms）。
package main

import (
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// MaxClockDriftMS 是轮内 Δ 漂移的可用上限：超过即判该轮不可用（读数无法复现）。
const MaxClockDriftMS = 5.0

// clockReading 是一次时钟测量的结果；Offset 为「压测端 - 服务端」（毫秒）。
type clockReading struct {
	OK        bool
	Offset    float64 // 主口径：跨机直测的 Δ
	RTT       float64 // 主口径样本的最小往返（毫秒），偏差量级 ≈ RTT/2
	NTPClient float64 // 压测端 NTP 自评 Offset（毫秒）
	NTPServer float64 // 服务端 NTP 自评 Offset（毫秒）
	Source    string  // "ssh-date" | "ntp" | "none"
	Err       string
}

// measureClock 测一次两端时钟差；sshTarget 为空则只读两端 NTP（无法跨机直测）。
func measureClock(sshTarget string) clockReading {
	r := clockReading{NTPClient: ntpOffsetMS(""), NTPServer: ntpOffsetMS(sshTarget), Source: "none"}
	if sshTarget != "" {
		if off, rtt, err := sshOffsetMS(sshTarget); err == nil {
			r.OK, r.Offset, r.RTT, r.Source = true, off, rtt, "ssh-date"
			return r
		} else {
			r.Err = err.Error()
		}
	}
	// 兜底：两端 NTP 自评之差（源不同，只作旁证，精度受各自 jitter 限制）。
	if !math.IsNaN(r.NTPClient) && !math.IsNaN(r.NTPServer) {
		r.OK, r.Offset, r.Source = true, r.NTPClient-r.NTPServer, "ntp"
	}
	return r
}

// ntpOffsetMS 读取某端 NTP 自评偏移；target 为空表示本机，返回 NaN 表示不可用。
func ntpOffsetMS(target string) float64 {
	out, err := runSSH(target, "timedatectl timesync-status 2>/dev/null")
	if err != nil {
		return math.NaN()
	}
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "Offset:") {
			continue
		}
		return parseDurationMS(strings.TrimSpace(strings.TrimPrefix(line, "Offset:")))
	}
	return math.NaN()
}

// parseDurationMS 解析 timedatectl 的偏移串（如 +750us、-8.015ms、+1.2s）为毫秒。
func parseDurationMS(s string) float64 {
	unit := 1.0
	switch {
	case strings.HasSuffix(s, "us"):
		unit, s = 0.001, strings.TrimSuffix(s, "us")
	case strings.HasSuffix(s, "ms"):
		unit, s = 1, strings.TrimSuffix(s, "ms")
	case strings.HasSuffix(s, "s"):
		unit, s = 1000, strings.TrimSuffix(s, "s")
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return math.NaN()
	}
	return v * unit
}

// sshOffsetMS 在一条多路复用 SSH 连接上直测两端时钟差（取 RTT 最小的样本）。
func sshOffsetMS(target string) (float64, float64, error) {
	if _, err := exec.LookPath("ssh"); err != nil {
		return 0, 0, fmt.Errorf("没有 ssh 可执行文件: %w", err)
	}
	master := fmt.Sprintf("/tmp/ltclock-%d", os.Getpid())
	args := []string{"-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
		"-o", "ControlMaster=auto", "-o", "ControlPath=" + master, "-o", "ControlPersist=600"}
	if _, err := runSSHArgs(args, target, "true"); err != nil {
		return 0, 0, fmt.Errorf("SSH 预热失败（%s）: %w", target, err)
	}
	defer closeSSHMaster(master, target)

	bestRTT := time.Duration(1<<62 - 1)
	bestOff, n := 0.0, 0
	for i := 0; i < 9; i++ {
		t0 := time.Now()
		out, err := runSSHArgs(args, target, "date +%s%N")
		t3 := time.Now()
		if err != nil {
			continue
		}
		t1, perr := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
		if perr != nil {
			continue
		}
		rtt := t3.Sub(t0)
		// NTP 公式：offset(server-client) = t1 - (t0+t3)/2；Δ(client-server) 取负。
		off := -float64(t1-(t0.UnixNano()+t3.UnixNano())/2) / 1e6
		n++
		if rtt < bestRTT {
			bestRTT, bestOff = rtt, off
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n == 0 {
		return 0, 0, errors.New("SSH 直测一次都没成功")
	}
	// 最小 RTT 样本仍含约 RTT/2 的不对称上限；把 RTT 一并返回供 SUMMARY 记录。
	return bestOff, float64(bestRTT.Microseconds()) / 1000, nil
}

// runSSH 在目标上执行命令；target 为空即本机。
func runSSH(target, cmd string) (string, error) {
	if target == "" {
		out, err := exec.Command("bash", "-c", cmd).Output()
		return string(out), err
	}
	return runSSHArgs([]string{"-o", "BatchMode=yes", "-o", "ConnectTimeout=5"}, target, cmd)
}

// runSSHArgs 以给定 ssh 参数执行远端命令。
func runSSHArgs(args []string, target, cmd string) (string, error) {
	out, err := exec.Command("ssh", append(args, target, cmd)...).Output()
	return string(out), err
}

// closeSSHMaster 关闭多路复用主连接（幂等；失败不影响测量结果）。
func closeSSHMaster(master, target string) {
	_ = exec.Command("ssh", "-O", "exit", "-o", "ControlPath="+master, target).Run()
}

// clockSummary 把开始/结束两次测量合成一轮的校正参数与可用性判定。
type clockSummary struct {
	Usable      bool    // 开始/结束都测到且轮内漂移 ≤ MaxClockDriftMS
	DeltaStart  float64 // 毫秒（压测端 - 服务端）
	DeltaEnd    float64
	Drift       float64 // |Δend - Δstart|
	Used        float64 // 校正采用的 Δ（开始/结束均值）
	RTT         float64 // 两次测量中较大的最小往返（毫秒）
	Uncertainty float64 // 保守不确定度：max(RTT)/2 + Drift/2
	Source      string
	Note        string
	NTPClient   float64 // 压测端 NTP 自评（旁证）
	NTPServer   float64 // 服务端 NTP 自评（旁证）
}

// summarizeClock 合成一轮的校正参数；任一测量失败即不可用（Δ 仍可能来自 NTP 兜底）。
func summarizeClock(start, end clockReading) clockSummary {
	s := clockSummary{DeltaStart: start.Offset, DeltaEnd: end.Offset, Source: start.Source,
		NTPClient: start.NTPClient, NTPServer: end.NTPServer}
	if !start.OK || !end.OK {
		s.Source = "none"
		s.Note = "开始/结束至少一次没测到 Δ：" + start.Err + end.Err
		return s
	}
	s.Drift = abs(end.Offset - start.Offset)
	s.Used = (start.Offset + end.Offset) / 2
	s.RTT = max(start.RTT, end.RTT)
	s.Uncertainty = s.RTT/2 + s.Drift/2
	s.Usable = s.Drift <= MaxClockDriftMS
	if !s.Usable {
		s.Note = fmt.Sprintf("轮内 Δ 漂移 %.1fms > %.0fms，该轮不可用", s.Drift, MaxClockDriftMS)
	}
	return s
}

// abs 返回绝对值。
func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

// correctLatency 返回按 Δ 平移后的分位摘要（校正 = 原始 - Δ）。
// 分位数对整体平移同变，故逐分位减 Δ 与逐样本减 Δ 等价。
func correctLatency(l latency, deltaMS float64) latency {
	if l.Count == 0 {
		return l
	}
	return latency{Count: l.Count, P50: l.P50 - deltaMS, P95: l.P95 - deltaMS, P99: l.P99 - deltaMS, Max: l.Max - deltaMS}
}
