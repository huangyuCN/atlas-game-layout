package errorv1

import (
	"fmt"
	"testing"

	atlaserrors "github.com/huangyuCN/atlas/errors"
)

// TestGeneratedHelpers 校验 protoc-gen-atlas-errors 生成助手的基础语义：
// Code/Reason/BizCode/metadata 与 errors.proto 中的标注一一对应。
// 生成物入库后本测试防「重生成导致语义漂移」的回归。
func TestGeneratedHelpers(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		code   int32
		reason string
		biz    int32
	}{
		{"PlayerNotFound", ErrPlayerNotFound("玩家不存在"), 404, "PLAYER_NOT_FOUND", 1001},
		{"PlayerAlreadyExists", ErrPlayerAlreadyExists("玩家已存在"), 409, "PLAYER_ALREADY_EXISTS", 1002},
		{"InvalidToken", ErrInvalidToken("令牌无效"), 401, "INVALID_TOKEN", 1003},
		{"KickedOffline", ErrKickedOffline("被挤下线"), 401, "KICKED_OFFLINE", 1004},
		{"TokenExpired", ErrTokenExpired("令牌过期"), 401, "TOKEN_EXPIRED", 1005},
		{"SessionNotFound", ErrSessionNotFound("会话不存在"), 404, "SESSION_NOT_FOUND", 1006},
		{"AlreadyInMatch", ErrAlreadyInMatch("已在匹配中"), 409, "ALREADY_IN_MATCH", 2001},
		{"MatchNotFound", ErrMatchNotFound("匹配不存在"), 404, "MATCH_NOT_FOUND", 2002},
		{"BattleNotFound", ErrBattleNotFound("战斗不存在"), 404, "BATTLE_NOT_FOUND", 3001},
		{"BattleFull", ErrBattleFull("战斗已满"), 409, "BATTLE_FULL", 3002},
		{"BattleEnded", ErrBattleEnded("战斗已结束"), 409, "BATTLE_ENDED", 3003},
		{"Internal", ErrInternal("服务内部错误"), 500, "INTERNAL", 9001},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			se := atlaserrors.FromError(tc.err)
			if se == nil {
				t.Fatal("FromError 返回 nil")
			}
			if se.Code != tc.code {
				t.Fatalf("Code = %d, 期望 %d", se.Code, tc.code)
			}
			if se.Reason != tc.reason {
				t.Fatalf("Reason = %q, 期望 %q", se.Reason, tc.reason)
			}
			if got := se.Metadata["biz_code"]; got != fmt.Sprint(tc.biz) {
				t.Fatalf("biz_code = %q, 期望 %d", got, tc.biz)
			}
			if got := se.Metadata["biz_reason"]; got != tc.name {
				t.Fatalf("biz_reason = %q, 期望 %q", got, tc.name)
			}
		})
	}
}

// TestIsHelpers 校验 IsXxx 判定函数：命中返回 true、非命中返回 false、nil 不 panic。
func TestIsHelpers(t *testing.T) {
	err := ErrPlayerNotFound("玩家不存在")
	if !IsPlayerNotFound(err) {
		t.Fatal("IsPlayerNotFound(ErrPlayerNotFound) 应返回 true")
	}
	if IsInvalidToken(err) {
		t.Fatal("IsInvalidToken(ErrPlayerNotFound) 应返回 false")
	}
	if IsPlayerNotFound(nil) {
		t.Fatal("IsPlayerNotFound(nil) 应返回 false")
	}
}

// TestBizCodeHelpers 校验 BizCodeXxx/CodeXxx/ReasonXxx 便捷函数。
func TestBizCodeHelpers(t *testing.T) {
	if got := BizCodePlayerNotFound(); got != 1001 {
		t.Fatalf("BizCodePlayerNotFound() = %d, 期望 1001", got)
	}
	if got := CodePlayerNotFound(); got != 404 {
		t.Fatalf("CodePlayerNotFound() = %d, 期望 404", got)
	}
	if got := ReasonPlayerNotFound(); got != "PLAYER_NOT_FOUND" {
		t.Fatalf("ReasonPlayerNotFound() = %q, 期望 PLAYER_NOT_FOUND", got)
	}
}

// TestAdminErrors 校验管理面错误码（P7 步骤 1）：号段紧接 ClientVersionTooLow=1011
// 之后（1012–1016），HTTP code 与用途一一对应——缺操作人/缺幂等键/幂等键复用/
// 同键操作在途/单次发放超上限。
func TestAdminErrors(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		code   int32
		reason string
		biz    int32
	}{
		{"AdminOperatorMissing", ErrAdminOperatorMissing("缺少操作人"), 400, "ADMIN_OPERATOR_MISSING", 1012},
		{"AdminIdempotencyKeyMissing", ErrAdminIdempotencyKeyMissing("缺少幂等键"), 400, "ADMIN_IDEMPOTENCY_KEY_MISSING", 1013},
		{"AdminIdempotencyKeyReused", ErrAdminIdempotencyKeyReused("幂等键已用于其他参数"), 409, "ADMIN_IDEMPOTENCY_KEY_REUSED", 1014},
		{"AdminOperationInFlight", ErrAdminOperationInFlight("同键操作在途"), 409, "ADMIN_OPERATION_IN_FLIGHT", 1015},
		{"AdminGrantCountExceeded", ErrAdminGrantCountExceeded("超过单次发放上限"), 400, "ADMIN_GRANT_COUNT_EXCEEDED", 1016},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			se := atlaserrors.FromError(tc.err)
			if se == nil {
				t.Fatal("FromError 返回 nil")
			}
			if se.Code != tc.code {
				t.Fatalf("Code = %d, 期望 %d", se.Code, tc.code)
			}
			if se.Reason != tc.reason {
				t.Fatalf("Reason = %q, 期望 %q", se.Reason, tc.reason)
			}
			if got := se.Metadata["biz_code"]; got != fmt.Sprint(tc.biz) {
				t.Fatalf("biz_code = %q, 期望 %d", got, tc.biz)
			}
			if got := se.Metadata["biz_reason"]; got != tc.name {
				t.Fatalf("biz_reason = %q, 期望 %q", got, tc.name)
			}
		})
	}
}

// TestAdminErrorIsHelpers 校验管理面 Is*/Code*/Reason*/BizCode* 判定：
// 命中返回 true，相邻号段互不串号（同码段内 reason 必须精确匹配），nil 不 panic。
func TestAdminErrorIsHelpers(t *testing.T) {
	err := ErrAdminIdempotencyKeyReused("幂等键已用于其他参数")
	if !IsAdminIdempotencyKeyReused(err) {
		t.Fatal("IsAdminIdempotencyKeyReused(ErrAdminIdempotencyKeyReused) 应返回 true")
	}
	if IsAdminIdempotencyKeyMissing(err) {
		t.Fatal("IsAdminIdempotencyKeyMissing(ErrAdminIdempotencyKeyReused) 应返回 false（相邻号段不得串号）")
	}
	if IsAdminOperatorMissing(err) {
		t.Fatal("IsAdminOperatorMissing(ErrAdminIdempotencyKeyReused) 应返回 false")
	}
	if IsAdminIdempotencyKeyReused(nil) {
		t.Fatal("IsAdminIdempotencyKeyReused(nil) 应返回 false")
	}
	if got := BizCodeAdminGrantCountExceeded(); got != 1016 {
		t.Fatalf("BizCodeAdminGrantCountExceeded() = %d, 期望 1016", got)
	}
	if got := CodeAdminGrantCountExceeded(); got != 400 {
		t.Fatalf("CodeAdminGrantCountExceeded() = %d, 期望 400", got)
	}
	if got := ReasonAdminOperationInFlight(); got != "ADMIN_OPERATION_IN_FLIGHT" {
		t.Fatalf("ReasonAdminOperationInFlight() = %q, 期望 ADMIN_OPERATION_IN_FLIGHT", got)
	}
}

// TestGeneratedErrorsAreBusinessClass 验证业务错误码生成物自动标注业务类（P6 步骤 3）：
// 日志按类定级（业务类为 Warn 且可被过滤），并且不计入故障率。
func TestGeneratedErrorsAreBusinessClass(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"ErrPlayerNotFound", ErrPlayerNotFound("玩家不存在")},
		{"ErrAlreadyInMatch", ErrAlreadyInMatch("已在匹配中")},
		{"NewBattleNotFound", NewBattleNotFound("战斗不存在")},
		{"WrapInvalidToken", WrapInvalidToken(fmt.Errorf("底层原因"), "令牌无效")},
	}
	for _, c := range cases {
		if got := atlaserrors.ClassOf(c.err); got != atlaserrors.ClassBusiness {
			t.Errorf("%s 的 class = %v, 期望 business", c.name, got)
		}
	}
}
