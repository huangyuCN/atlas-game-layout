package frameroute

import (
	"errors"
	"sort"
	"testing"

	"github.com/huangyuCN/atlas/contrib/actor/frameops"
	"github.com/huangyuCN/atlas/contrib/actor/relay"
	"github.com/huangyuCN/atlas/transport/frame/engine"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// testTable 返回一张含 CLIENT / INTERNAL 两种 access 的路由表（注册口径只认 CLIENT）。
func testTable() relay.Table {
	newReq := func() proto.Message { return &emptypb.Empty{} }
	return relay.Table{
		"/svc/Client1":  {Operation: "/svc/Client1", Access: relay.AccessClient, NewRequest: newReq},
		"/svc/Internal": {Operation: "/svc/Internal", Access: relay.AccessInternal, NewRequest: newReq},
		"/svc/Client2":  {Operation: "/svc/Client2", Access: relay.AccessClient, NewRequest: newReq},
	}
}

// TestRegisterOnlyClientOps 验证逐 op 注册只覆盖 access=CLIENT 的条目，且每 op 一个非空 handler。
func TestRegisterOnlyClientOps(t *testing.T) {
	ops := frameops.NewHandler(testTable(), nil, nil)
	got := make(map[string]engine.MsgHandler)
	err := Register(ops, func(operation string, handler engine.MsgHandler) error {
		if handler == nil {
			t.Errorf("operation %s 的 handler 为空", operation)
		}
		got[operation] = handler
		return nil
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	names := make([]string, 0, len(got))
	for op := range got {
		names = append(names, op)
	}
	sort.Strings(names)
	want := []string{"/svc/Client1", "/svc/Client2"}
	if len(names) != len(want) {
		t.Fatalf("注册的 op = %v, 期望 %v", names, want)
	}
	for i, op := range want {
		if names[i] != op {
			t.Fatalf("注册的 op = %v, 期望 %v", names, want)
		}
	}
}

// TestRegisterStopsOnError 验证 registrar 报错即中断并原样上抛（装配期漏注册不得静默）。
func TestRegisterStopsOnError(t *testing.T) {
	ops := frameops.NewHandler(testTable(), nil, nil)
	calls := 0
	boom := errors.New("subscribe 失败")
	err := Register(ops, func(string, engine.MsgHandler) error {
		calls++
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Register 错误 = %v, 期望原样上抛 %v", err, boom)
	}
	if calls != 1 {
		t.Fatalf("registrar 调用次数 = %d, 期望 1（首次失败即中断）", calls)
	}
}
