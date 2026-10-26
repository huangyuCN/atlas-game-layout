package assemble

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/huangyuCN/atlas/contrib/edge"
)

// 本文件守「接入层的 edge_* 指标真的能出到 /metrics」。
//
// 为什么值得单独守：2026-10-26 诊断里多轮「拒绝计数=0」的推断都建立在「抓不到 edge_*」之上，
// 而现场 /metrics 只有 go_* / process_*（client_golang 的运行时与进程采集器）——那是采集器
// 一路退化成 noop 的典型形态，不是「没有拒绝」。
//
// 另一条必须记住的口径：指标序列是**懒创建**的（OTel SDK 只在第一次观测后建序列），
// 空闲实例上 edge_* 一条都没有。故本文件的断言一律「先触发事件再抓」，
// 绝不用空集当证据（空集只能证明没触发过事件）。

// 指标家族名一律取自框架常量：名字漂移会让面板与告警静默失效。
var metricsFamilies = []string{
	edge.MetricStreamsActive,
	edge.MetricBytesTotal,
	edge.MetricTicketRejectedTotal,
	edge.MetricStreamsTearedDownTotal,
	edge.MetricBackendDialErrorsTotal,
}

// mapResolver 按 battle_id 返回后端地址（WS 面同时覆盖「可用后端」与「拨不通的后端」两种输入）。
type mapResolver struct {
	backends map[string]string
}

// Resolve 实现 edge.Resolver。
func (r mapResolver) Resolve(_ context.Context, req edge.ResolveRequest) (edge.Backend, error) {
	if addr, ok := r.backends[req.Ticket.BattleID]; ok {
		return edge.Backend{Address: addr}, nil
	}
	return edge.Backend{}, fmt.Errorf("mapResolver: 无 %s 的后端", req.Ticket.BattleID)
}

// deadBackend 是一个「本机必有、但没进程监听」的地址：拨号必然失败（拨号错误指标的产生点）。
const deadBackend = "127.0.0.1:1"

// startEmbeddedWithMetrics 起只开 WS 面的进程内接入层，并打开 Prometheus 抓取端点。
func startEmbeddedWithMetrics(t *testing.T, backends map[string]string) *Edge {
	t.Helper()
	e, err := New(context.Background(), Options{
		Namespace:     "edge-metrics",
		EtcdEndpoints: []string{"127.0.0.1:12379"},
		TicketKey:     testKey,
		MetricsAddr:   "127.0.0.1:0", // 0 = 内核分配：测试不与本机其它实例抢端口
		Resolver:      mapResolver{backends: backends},
		Listeners: []edge.Listener{
			{Name: "ws", Network: edge.NetworkTCP, Address: "127.0.0.1:0", Carrier: edge.CarrierWSUpgrade},
		},
	})
	if err != nil {
		t.Fatalf("进程内装配失败: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := e.Stop(ctx); err != nil {
			t.Errorf("停机失败: %v", err)
		}
	})
	return e
}

// TestEmbeddedMetricsExposeEdgeFamily 断言进程内装配形态下五个 edge_* 家族都能被抓到：
// 每个家族各触发一次事件（坏票 / 建流+字节+拆除 / 拨号失败），再从 /metrics 全文里找。
func TestEmbeddedMetricsExposeEdgeFamily(t *testing.T) {
	e := startEmbeddedWithMetrics(t, map[string]string{"b-1": echoTCPBackend(t), "b-dead": deadBackend})
	if e.MetricsURL == "" {
		t.Fatal("配了 MetricsAddr 却没给出抓取端点地址")
	}

	// 三个触发点各自对应一组指标：缺任何一个都说明该路径的打点没接上。
	triggerBadTicket(t, e.ListenerAddrs[0])
	triggerRoundTrip(t, e.ListenerAddrs[0])
	triggerDialFailure(t, e.ListenerAddrs[0])

	body := waitFamilies(t, e.MetricsURL)
	for _, name := range metricsFamilies {
		if !hasFamily(body, name) {
			t.Fatalf("/metrics 里没有指标家族 %s（抓取全文如下，共 %d 字节）:\n%s", name, len(body), body)
		}
	}
	// 值也要对得上：拒绝 1 次（坏票）、拆除 1 次（客户端断开）、拨号失败 1 次（端口不可达）。
	requireSample(t, body, edge.MetricTicketRejectedTotal, 1, `reason="`+string(edge.ReasonTicketInvalid)+`"`)
	requireSample(t, body, edge.MetricStreamsTearedDownTotal, 1, `reason="`+string(edge.TeardownClientClosed)+`"`)
	requireSample(t, body, edge.MetricBackendDialErrorsTotal, 1, `reason="`+string(edge.DialErrorOther)+`"`)
	if got := familySum(body, edge.MetricBytesTotal); got < 1 {
		t.Fatalf("%s 应累计到转发字节，实际 %v", edge.MetricBytesTotal, got)
	}
}

// triggerBadTicket 用坏票发起一次 WS 升级请求：接入层断开并记 edge_ticket_rejected_total。
func triggerBadTicket(t *testing.T, addr string) {
	t.Helper()
	conn := dialFace(t, addr)
	defer conn.Close()
	head := "GET /battle?ticket=garbage HTTP/1.1\r\nHost: edge.test\r\n" +
		"Upgrade: websocket\r\nConnection: Upgrade\r\n\r\n"
	if _, err := conn.Write([]byte(head)); err != nil {
		t.Fatalf("写升级请求失败: %v", err)
	}
	expectFaceClosed(t, conn)
}

// triggerRoundTrip 完成一次「建流 → 双向字节 → 客户端断开」：覆盖活跃数、字节数与拆流计数。
func triggerRoundTrip(t *testing.T, addr string) {
	t.Helper()
	conn := dialFace(t, addr)
	head := "GET /battle?ticket=" + signTicket(t, "b-1") + " HTTP/1.1\r\nHost: edge.test\r\n" +
		"Upgrade: websocket\r\nConnection: Upgrade\r\n\r\n"
	if _, err := conn.Write([]byte(head)); err != nil {
		t.Fatalf("写升级请求失败: %v", err)
	}
	if got := readN(t, conn, len(head)); got != head {
		t.Fatalf("升级请求未被原样转发: %q", got)
	}
	if _, err := conn.Write([]byte("metrics-frame")); err != nil {
		t.Fatalf("写帧字节失败: %v", err)
	}
	if got := readN(t, conn, len("metrics-frame")); got != "metrics-frame" {
		t.Fatalf("回程字节不一致: %q", got)
	}
	_ = conn.Close() // 客户端断开 → 拆流（reason=client_closed）
	// 拆流发生在转发循环退出之后，抓取前给它一点时间落地（否则会抓到「还没拆」的中间态）。
	time.Sleep(100 * time.Millisecond)
}

// triggerDialFailure 用一个「拨不通」的后端发起一次升级请求：解析成功、拨号失败。
func triggerDialFailure(t *testing.T, addr string) {
	t.Helper()
	conn := dialFace(t, addr)
	defer conn.Close()
	head := "GET /battle?ticket=" + signTicket(t, "b-dead") + " HTTP/1.1\r\nHost: edge.test\r\n" +
		"Upgrade: websocket\r\nConnection: Upgrade\r\n\r\n"
	if _, err := conn.Write([]byte(head)); err != nil {
		t.Fatalf("写升级请求失败: %v", err)
	}
	expectFaceClosed(t, conn)
}

// dialFace 向接入面拨一条 TCP 连接。
func dialFace(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("拨接入层失败: %v", err)
	}
	return conn
}

// expectFaceClosed 断言接入层已断开该连接（L4 拒绝语义：断开，不回应用层错误）。
func expectFaceClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 64)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("接入层应断开该连接")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("连接未被断开（读超时）")
	}
}

// waitFamilies 轮询抓取端点直到五个家族全部出现，返回最后一次抓取全文。
// 轮询而非一次性抓取：指标落地与序列懒创建都不是同步的。
func waitFamilies(t *testing.T, url string) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		last = scrape(t, url)
		if allFamilies(last) {
			return last
		}
		time.Sleep(20 * time.Millisecond)
	}
	return last
}

// allFamilies 报告抓取全文里五个 edge_* 家族是否都在。
func allFamilies(body string) bool {
	for _, name := range metricsFamilies {
		if !hasFamily(body, name) {
			return false
		}
	}
	return true
}

// scrape 抓一次 Prometheus 文本端点，失败即测试失败。
func scrape(t *testing.T, url string) string {
	t.Helper()
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("抓取指标端点失败（%s）: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读取指标端点响应失败: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("指标端点应 200，实际 %d", resp.StatusCode)
	}
	return string(body)
}

// hasFamily 报告抓取全文里是否有该指标家族（有序列即可，值可以是 0）。
func hasFamily(body, name string) bool { return len(familyLines(body, name)) > 0 }

// familyLines 返回属于该家族的全部样本行（`name值` 与 `name{...} 值` 两种形态）。
func familyLines(body, name string) []string {
	var out []string
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, name+"{") || strings.HasPrefix(line, name+" ") {
			out = append(out, line)
		}
	}
	return out
}

// familySum 返回该家族全部样本的取值之和（解析失败按 0 计，缺家族返回 0）。
func familySum(body, name string) float64 {
	total := 0.0
	for _, line := range familyLines(body, name) {
		total += sampleValueOf(line)
	}
	return total
}

// requireSample 断言标签选择器（labels 各片段全部出现）命中的序列值不小于 want。
func requireSample(t *testing.T, body, name string, want float64, labels ...string) {
	t.Helper()
	for _, line := range familyLines(body, name) {
		matched := true
		for _, l := range labels {
			if !strings.Contains(line, l) {
				matched = false
			}
		}
		if matched {
			if got := sampleValueOf(line); got < want {
				t.Fatalf("%s%s 应不小于 %v，实际 %v（样本：%s）", name, strings.Join(labels, ","), want, got, line)
			}
			return
		}
	}
	t.Fatalf("%s 里没有 %s 的序列（全文：\n%s", name, strings.Join(labels, ","), body)
}

// sampleValueOf 解析一行 Prometheus 文本样本的取值（最后一个字段）。
func sampleValueOf(line string) float64 {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0
	}
	v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
	if err != nil {
		return 0
	}
	return v
}
