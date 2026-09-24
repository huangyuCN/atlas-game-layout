// gmctl 是 game 管理面（admin.game.v1.AdminService）的 GM 命令行：把 pkg/gm 的 SDK
// 暴露为四个子命令，回执以 JSON 输出到 stdout（便于 GM 人工判读与脚本消费）。
//
// 用法示例：
//
//	gmctl -addr 127.0.0.1:9101 -operator gm-alice grant-item -player p-1001 -item 1001 -count 5 -reason 工单-9527
//	gmctl -operator gm-alice query-player -player p-1001
//	gmctl -operator gm-alice query-backpack -player p-1001
//	gmctl -operator gm-alice query-audits -player p-1001 -page-size 20
//
// 安全前提：管理面只注册在 game 服务的 internal listener（配置 server.grpc.internal_addr），
// edge 面调用只会得到 Unimplemented；本轮内网无鉴权，-operator 由调用方自报、可伪造，
// 仅用于事后审计追责（见 docs/superpowers/specs/2026-09-23-admin-face-convention.md）。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	admingamev1 "github.com/huangyuCN/atlas-game-layout/api/admin/game/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/gm"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// 命令行缺省值。
const (
	// defaultAddr 是 game 管理面 internal listener 的本机形态（模板默认监听 0.0.0.0:9101）。
	defaultAddr = "127.0.0.1:9101"
	// defaultTimeout 是单次 RPC 超时。
	defaultTimeout = 10 * time.Second
	// defaultRetries 是瞬时失败（Unavailable/DeadlineExceeded）的自动重试次数：
	// 管理面写操作幂等（重试复用同一幂等键）、读操作天然可重试，故 CLI 默认重试 2 次。
	defaultRetries = 2
	// defaultMaxRows 是 query-audits 一次输出的条数上限（保护性上限，避免拉爆终端）。
	defaultMaxRows = 200
	// envOperator 是 operator 的缺省来源（未设置且未传 -operator 时直接报错退出）。
	envOperator = "GM_OPERATOR"
)

// 用法文本（全局 -h 打印；各子命令 -h 打印自己的选项）。
const usageText = `gmctl 是 game 管理面的 GM 命令行（回执以 JSON 输出到 stdout）

用法：
  gmctl [全局选项] <子命令> [子命令选项]

子命令：
  grant-item       发放道具（写操作，幂等；重试请复用同一个 -idempotency-key）
  query-player     查询玩家
  query-backpack   查询背包
  query-audits     查询审计记录（游标翻页，一次输出全部条目）

全局选项：
`

// globalOptions 是四个子命令共用的全局选项。
type globalOptions struct {
	addr           string
	operator       string
	timeout        time.Duration
	dryRun         bool
	reason         string
	idempotencyKey string
}

// subcommand 是子命令定义：parse 构造已绑定变量的选项集合与执行体（-h 时不执行）。
type subcommand struct {
	name  string
	parse func() (*flag.FlagSet, func(context.Context, *gm.Client) error)
}

// auditList 是 query-audits 的输出信封（entries 逐条按 proto 字段名序列化）。
type auditList struct {
	Count   int               `json:"count"`
	Entries []json.RawMessage `json:"entries"`
}

// marshalOptions 是回执序列化选项：proto 字段名 + 输出零值字段（GM 需看到 applied=false 这类结果）。
var marshalOptions = protojson.MarshalOptions{UseProtoNames: true, EmitDefaultValues: true, Indent: "  "}

// main 是 gmctl 入口：解析参数、执行子命令，失败时写 stderr 并以非零码退出。
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "gmctl:", err)
		os.Exit(1)
	}
}

// run 解析全局选项与子命令选项，校验操作者后拨号执行（-h 一律视为成功退出）。
func run(ctx context.Context, args []string) error {
	opts, rest, err := parseGlobal(args)
	if err != nil {
		return helpAsSuccess(err)
	}
	if len(rest) == 0 {
		newFlagSet("gmctl").Usage()
		return errors.New("缺少子命令（可选：grant-item / query-player / query-backpack / query-audits）")
	}
	sub, ok := findSubcommand(rest[0])
	if !ok {
		return fmt.Errorf("未知子命令 %q（可选：grant-item / query-player / query-backpack / query-audits）", rest[0])
	}
	fs, exec := sub.parse()
	if err := fs.Parse(rest[1:]); err != nil {
		return helpAsSuccess(err)
	}
	if opts.operator == "" {
		return fmt.Errorf("缺少操作者：请传 -operator，或设置环境变量 %s", envOperator)
	}
	client, err := gm.Dial(ctx, opts.addr, clientOptions(opts)...)
	if err != nil {
		return fmt.Errorf("连接管理面 %s 失败（确认 internal listener 已开启，且地址不是 edge 面）: %w", opts.addr, err)
	}
	defer func() { _ = client.Close() }()
	return exec(ctx, client)
}

// parseGlobal 解析全局选项，返回剩余参数（子命令名 + 子命令选项）。
func parseGlobal(args []string) (globalOptions, []string, error) {
	fs := newFlagSet("gmctl")
	addr := fs.String("addr", defaultAddr, "game 管理面 internal listener 地址 host:port")
	operator := fs.String("operator", os.Getenv(envOperator), "操作者标识（缺省取环境变量 "+envOperator+"）")
	timeout := fs.Duration("timeout", defaultTimeout, "单次 RPC 超时（瞬时失败自动重试 "+strconv.Itoa(defaultRetries)+" 次）")
	dryRun := fs.Bool("dry-run", false, "只校验不改档（仍写审计，且不占用幂等键）")
	reason := fs.String("reason", "", "工单/纠纷单号或说明（写入审计）")
	key := fs.String("idempotency-key", "", "幂等键（缺省自动生成；仅写操作使用）")
	if err := fs.Parse(args); err != nil {
		return globalOptions{}, nil, err
	}
	opts := globalOptions{
		addr:           *addr,
		operator:       strings.TrimSpace(*operator),
		timeout:        *timeout,
		dryRun:         *dryRun,
		reason:         *reason,
		idempotencyKey: strings.TrimSpace(*key),
	}
	return opts, fs.Args(), nil
}

// clientOptions 把全局选项映射为 SDK 选项。
func clientOptions(o globalOptions) []gm.Option {
	opts := []gm.Option{
		gm.WithOperator(o.operator),
		gm.WithTimeout(o.timeout),
		gm.WithRetry(defaultRetries),
	}
	if o.reason != "" {
		opts = append(opts, gm.WithReason(o.reason))
	}
	if o.idempotencyKey != "" {
		opts = append(opts, gm.WithIdempotencyKey(o.idempotencyKey))
	}
	if o.dryRun {
		opts = append(opts, gm.WithDryRun(true))
	}
	return opts
}

// findSubcommand 按名字查找子命令。
func findSubcommand(name string) (subcommand, bool) {
	for _, sub := range subcommands() {
		if sub.name == name {
			return sub, true
		}
	}
	return subcommand{}, false
}

// subcommands 返回全部子命令（顺序即帮助文本中的顺序）。
func subcommands() []subcommand {
	return []subcommand{
		{name: "grant-item", parse: grantItemCommand},
		{name: "query-player", parse: queryPlayerCommand},
		{name: "query-backpack", parse: queryBackpackCommand},
		{name: "query-audits", parse: queryAuditsCommand},
	}
}

// grantItemCommand 构造 grant-item 的选项集合与执行体：发放道具（写操作，幂等）。
func grantItemCommand() (*flag.FlagSet, func(context.Context, *gm.Client) error) {
	fs := newFlagSet("grant-item")
	player := fs.String("player", "", "目标玩家 ID（必填）")
	item := fs.Uint("item", 0, "道具 ID（必填，1..4294967295）")
	count := fs.Uint("count", 0, "发放数量（必填，1..4294967295；受服务端单次上限约束）")
	exec := func(ctx context.Context, client *gm.Client) error {
		if err := requireNonEmpty("player", *player); err != nil {
			return err
		}
		itemID, err := positiveUint32("item", *item)
		if err != nil {
			return err
		}
		countVal, err := positiveUint32("count", *count)
		if err != nil {
			return err
		}
		reply, err := client.GrantItem(ctx, *player, itemID, countVal)
		if err != nil {
			return err
		}
		return printJSON(reply)
	}
	return fs, exec
}

// queryPlayerCommand 构造 query-player：查询玩家读模型。
func queryPlayerCommand() (*flag.FlagSet, func(context.Context, *gm.Client) error) {
	return playerQueryCommand("query-player", func(ctx context.Context, c *gm.Client, id string) (proto.Message, error) {
		return c.QueryPlayer(ctx, id)
	})
}

// queryBackpackCommand 构造 query-backpack：查询背包读模型。
func queryBackpackCommand() (*flag.FlagSet, func(context.Context, *gm.Client) error) {
	return playerQueryCommand("query-backpack", func(ctx context.Context, c *gm.Client, id string) (proto.Message, error) {
		return c.QueryBackpack(ctx, id)
	})
}

// playerQueryCommand 构造「按玩家 ID 查询」类子命令的公共实现：
// name 为子命令名，call 为具体查询（返回管理面读模型）。
func playerQueryCommand(name string, call func(context.Context, *gm.Client, string) (proto.Message, error)) (*flag.FlagSet, func(context.Context, *gm.Client) error) {
	fs := newFlagSet(name)
	player := fs.String("player", "", "目标玩家 ID（必填）")
	exec := func(ctx context.Context, client *gm.Client) error {
		if err := requireNonEmpty("player", *player); err != nil {
			return err
		}
		reply, err := call(ctx, client, *player)
		if err != nil {
			return err
		}
		return printJSON(reply)
	}
	return fs, exec
}

// queryAuditsCommand 构造 query-audits 的选项集合与执行体：游标翻页取出审计条目。
func queryAuditsCommand() (*flag.FlagSet, func(context.Context, *gm.Client) error) {
	fs := newFlagSet("query-audits")
	player := fs.String("player", "", "按目标玩家 ID 过滤（可空）")
	operator := fs.String("operator-filter", "", "按操作者过滤（可空）")
	pageSize := fs.Uint("page-size", 20, "每页条数（0 = 服务端缺省 20，上限 200）")
	maxRows := fs.Uint("max", defaultMaxRows, "最多输出的条数")
	exec := func(ctx context.Context, client *gm.Client) error {
		size, err := toUint32("page-size", *pageSize)
		if err != nil {
			return err
		}
		it, err := client.QueryAudits(ctx, gm.AuditFilter{TargetID: *player, Operator: *operator, PageSize: size})
		if err != nil {
			return err
		}
		defer func() { _ = it.Close() }()
		entries, err := collect(it, *maxRows)
		if err != nil {
			return err
		}
		return printAudits(entries)
	}
	return fs, exec
}

// collect 按上限收集迭代器条目（达到上限即停止翻页）。
func collect(it gm.Iterator, maxRows uint) ([]*admingamev1.AuditEntry, error) {
	entries := make([]*admingamev1.AuditEntry, 0, maxRows)
	for uint(len(entries)) < maxRows {
		entry, err := it.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// newFlagSet 构造 flag 集合：帮助与用法统一输出到 stdout（便于管道抓取），
// -h 由调用方归一为成功退出（flag.ErrHelp）。
func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(os.Stdout)
	fs.Usage = func() {
		if name == "gmctl" {
			fmt.Fprint(fs.Output(), usageText)
		} else {
			fmt.Fprintf(fs.Output(), "用法：gmctl [全局选项] %s [选项]\n\n选项：\n", name)
		}
		fs.PrintDefaults()
	}
	return fs
}

// helpAsSuccess 把「用户请求帮助」归一为成功（帮助已由 flag 打印到 stdout）。
func helpAsSuccess(err error) error {
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	return err
}

// requireNonEmpty 校验必填字符串选项（发出请求前早失败）。
func requireNonEmpty(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("缺少必填选项 -%s", name)
	}
	return nil
}

// positiveUint32 校验并收窄正整数选项（0 与超 uint32 范围都报错，不静默截断）。
func positiveUint32(name string, value uint) (uint32, error) {
	if value == 0 || value > math.MaxUint32 {
		return 0, fmt.Errorf("-%s 必须是 1..%d 的整数，实际 %d", name, uint64(math.MaxUint32), value)
	}
	return uint32(value), nil
}

// toUint32 把 flag 的 uint 值收窄为 uint32（超范围即报错；0 合法）。
func toUint32(name string, value uint) (uint32, error) {
	if value > math.MaxUint32 {
		return 0, fmt.Errorf("-%s 超出上限 %d，实际 %d", name, uint64(math.MaxUint32), value)
	}
	return uint32(value), nil
}

// printJSON 把管理面回执以 JSON 打印到 stdout（gmctl 的唯一输出契约）。
func printJSON(msg proto.Message) error {
	out, err := marshalOptions.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化回执失败: %w", err)
	}
	fmt.Println(string(out))
	return nil
}

// printAudits 把审计条目以 {count, entries} 的 JSON 打印（entries 字段名与 AuditEntry 同名）。
func printAudits(entries []*admingamev1.AuditEntry) error {
	items := make([]json.RawMessage, 0, len(entries))
	for _, entry := range entries {
		out, err := marshalOptions.Marshal(entry)
		if err != nil {
			return fmt.Errorf("序列化审计记录失败: %w", err)
		}
		items = append(items, out)
	}
	out, err := json.MarshalIndent(auditList{Count: len(items), Entries: items}, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化审计列表失败: %w", err)
	}
	fmt.Println(string(out))
	return nil
}
