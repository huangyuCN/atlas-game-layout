package handler

import (
	"context"
	"testing"

	commonv1 "github.com/huangyuCN/atlas-game-layout/api/common/v1"
	matcherv1 "github.com/huangyuCN/atlas-game-layout/api/matcher/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/spanstest"
	"go.opentelemetry.io/otel/codes"
)

// TestMatcherHandlerSpans 验证撮合/组队埋点：入队、建队与失败路径都产出业务 span。
func TestMatcherHandlerSpans(t *testing.T) {
	exp, restore := spanstest.Install()
	t.Cleanup(restore)

	h, _, _, _ := newTestHandler()
	ctx := context.Background()
	req := &matcherv1.QueueMatchRequest{
		PlayerId: "p-1",
		Player:   &commonv1.PlayerSummary{PlayerId: "p-1", Level: 10},
		Ruleset:  "casual",
	}

	if _, err := h.QueueMatch(ctx, req); err != nil {
		t.Fatalf("QueueMatch: %v", err)
	}
	// 失败路径：重复入队被拒 → 同名 span 覆盖为 Error（ByName 保留最后一个）。
	if _, err := h.QueueMatch(ctx, req); err == nil {
		t.Fatal("重复入队应报错")
	}
	if _, err := h.CreateParty(ctx, &matcherv1.CreatePartyRequest{
		PlayerId: "p-leader",
		Player:   &commonv1.PlayerSummary{PlayerId: "p-leader", Level: 10},
	}); err != nil {
		t.Fatalf("CreateParty: %v", err)
	}

	spans := spanstest.ByName(exp)
	queue, ok := spans["matcher.Matcher.QueueMatch"]
	if !ok {
		t.Fatalf("缺少 matcher.Matcher.QueueMatch span: %v", spans)
	}
	if got := spanstest.Attr(queue, "ruleset"); got != "casual" {
		t.Fatalf("QueueMatch span 属性 ruleset = %q, 期望 casual", got)
	}
	if queue.Status.Code != codes.Error {
		t.Fatalf("重复入队应置 Error，实际 %v", queue.Status.Code)
	}
	if _, ok := spans["matcher.Matcher.CreateParty"]; !ok {
		t.Fatalf("缺少 matcher.Matcher.CreateParty span: %v", spans)
	}
}
