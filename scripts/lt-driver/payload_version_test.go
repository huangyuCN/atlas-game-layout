// 驱动「声明能解」与框架「解码器接受」必须同源：本条是 2026-11-04 漂移事故的回归闸。
//
// 事故形态：驱动在 JoinBattle 里声明 accept_payload_versions=[1,2,3]，而框架生成物的
// 帧头白名单只认 ver=1/2 —— 服务端真发 ver=3 时驱动在帧头层就报 invalid version: 3
// 并断连（unexpected EOF 16,233 次、广播成功率 23%）。两处独立维护必然漂移，故这里
// 直接断言「声明集合 == frame.PayloadVersions()」，任一侧单改都会红。
package main

import (
	"sort"
	"testing"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas/transport/frame"
)

// TestAcceptVersionsMatchFrameWhitelist 断言驱动声明集合与框架帧头白名单逐项相等。
func TestAcceptVersionsMatchFrameWhitelist(t *testing.T) {
	declared := make([]int, 0, len(acceptVersions))
	for _, v := range acceptVersions {
		declared = append(declared, int(v))
	}
	sort.Ints(declared)
	want := make([]int, 0, len(frame.PayloadVersions()))
	for _, v := range frame.PayloadVersions() {
		want = append(want, int(v))
	}
	if len(declared) != len(want) {
		t.Fatalf("驱动声明 %v，框架解码白名单 %v：两集合不等（协商将被违反）", declared, want)
	}
	for i := range declared {
		if declared[i] != want[i] {
			t.Fatalf("驱动声明 %v，框架解码白名单 %v：第 %d 项不等", declared, want, i)
		}
	}
}

// TestAcceptVersionsCoverDelta 断言驱动确实声明了 ver=3，避免「能解却不声明」的反向漂移
// （那会让服务端永远不下发增量帧，白名单扩了也白扩）。
func TestAcceptVersionsCoverDelta(t *testing.T) {
	for _, v := range acceptPayloadVersions() {
		if uint8(v) == frame.Version3 {
			return
		}
	}
	t.Fatalf("驱动未声明 ver=3（DELTA）：服务端永远不会下发增量帧")
}

// TestAcceptVersionsNonZero 断言声明集合里没有 UNSPECIFIED(0)：协议禁发 ver=0。
func TestAcceptVersionsNonZero(t *testing.T) {
	for _, v := range acceptVersions {
		if v == battlev1.FramePayloadVersion_FRAME_PAYLOAD_VERSION_UNSPECIFIED {
			t.Fatalf("声明里出现 UNSPECIFIED(0)，协议规定不得下发")
		}
	}
}
