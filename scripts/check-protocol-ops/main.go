// Command check-protocol-ops 校验三端协议包的 op 常量逐一相同。
//
// 依据：Go / TS / C# 三端协议包都由 protoc-gen-atlas-client 按同一份
// atlas.route.v1 注解生成，同一个 op 在三端必须是同一个字符串（客户端按它分发、
// 服务端按它路由）。三端产物格式不同（Go 匿名结构体、TS `as const` 对象、
// C# 静态类常量），故这里各自解析成「常量组 → 成员 → 值」，把组名与成员名
// 归一成小写后逐键比对——任何一端漏生成、改名或值漂移都会在这里被挡住。
//
// 用法：在仓库根执行 `go run ./scripts/check-protocol-ops`；不一致以退出码 1 失败。
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// opKey 是一条 op 的归一化标识（组名与成员名小写：TS 的推送组/成员名首字母
// 小写，Go/C# 用导出名，归一后才可逐键比对）。
type opKey struct {
	Group  string
	Member string
}

// String 返回排障用的 `组.成员` 形式。
func (k opKey) String() string { return k.Group + "." + k.Member }

// format 是一种语言产物的行格式：三端同构（组声明 → 成员行 → 组结束行），
// 只有具体写法和缩进不同，故共用一套扫描逻辑、只换正则。
type format struct {
	GroupRe *regexp.Regexp // 第 1 个捕获组是常量组名
	EntryRe *regexp.Regexp // 第 1 个捕获组是成员名，第 2 个是 op 值
	End     string         // 组结束行
}

// end 是一端产物的扫描配置：名称、扫描根、文件名后缀与行格式。
type end struct {
	Name   string
	Root   string
	Suffix string
	Format format
}

// 三端产物的常量组开头行、成员行（值为以 / 开头的 op 串）与组结束行。
var (
	goGroupRe = regexp.MustCompile(`^var ([A-Za-z0-9_]+) = struct \{$`)
	goEntryRe = regexp.MustCompile(`^\t([A-Za-z0-9_]+):\s+"(/[^"]*)",$`)

	tsGroupRe = regexp.MustCompile(`^export const ([A-Za-z0-9_]+) = \{$`)
	tsEntryRe = regexp.MustCompile(`^  ([A-Za-z0-9_]+): ['"](/[^'"]*)['"],$`)

	csGroupRe = regexp.MustCompile(`^public static class ([A-Za-z0-9_]+)$`)
	csEntryRe = regexp.MustCompile(`^    public const string ([A-Za-z0-9_]+) = "(/[^"]*)";$`)
)

// ends 是三端扫描配置。生成器把 op 常量放在 <proto 目录>/opclient/ 下，
// 故扫描根取各端 api 目录（google/ 等闭包产物不参与）。
var ends = []end{
	{
		Name: "go", Root: filepath.Join("protocol", "go", "api"), Suffix: ".pb.go",
		Format: format{GroupRe: goGroupRe, EntryRe: goEntryRe, End: "}"},
	},
	{
		Name: "ts", Root: filepath.Join("protocol", "ts", "src", "api"), Suffix: "_pb.ts",
		Format: format{GroupRe: tsGroupRe, EntryRe: tsEntryRe, End: "} as const;"},
	},
	{
		Name: "csharp", Root: filepath.Join("protocol", "csharp", "api"), Suffix: "_client.g.cs",
		Format: format{GroupRe: csGroupRe, EntryRe: csEntryRe, End: "}"},
	},
}

// main 跑完三端解析与比对，失败即退出码 1。
func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "check-protocol-ops: %v\n", err)
		os.Exit(1)
	}
}

// run 逐端解析后比对，成功时打印每组条数。
func run() error {
	got := make(map[string]map[opKey]string, len(ends))
	names := make([]string, 0, len(ends))
	for _, e := range ends {
		ops, err := collect(e)
		if err != nil {
			return err
		}
		got[e.Name] = ops
		names = append(names, e.Name)
	}
	if err := compare(names, got); err != nil {
		return err
	}
	total := len(got[names[0]])
	counts := make([]string, 0, len(names))
	for _, name := range names {
		counts = append(counts, fmt.Sprintf("%s=%d", name, len(got[name])))
	}
	fmt.Printf("跨端 op 常量一致：%d 组 / %d 条（%s）\n",
		groupCount(got[names[0]]), total, strings.Join(counts, "、"))
	return nil
}

// collect 扫描一端的产物目录，汇总该端全部 op 常量。
func collect(e end) (map[opKey]string, error) {
	out := make(map[opKey]string)
	err := filepath.WalkDir(e.Root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, e.Suffix) {
			return nil
		}
		if !strings.Contains(filepath.ToSlash(path), "/opclient/") {
			return nil // 只认 opclient 产物（DTO 等其它生成物无 op 常量）
		}
		return parseFile(path, e.Format, out)
	})
	if err != nil {
		return nil, fmt.Errorf("[%s] 扫描 %s 失败: %w", e.Name, e.Root, err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("[%s] 在 %s 下未解析到任何 op 常量（生成器参数或目录布局变了？）", e.Name, e.Root)
	}
	return out, nil
}

// parseFile 按行格式解析单个产物文件：组名与成员名归一后记入 out。
func parseFile(path string, f format, out map[opKey]string) error {
	lines, err := readLines(path)
	if err != nil {
		return err
	}
	group := ""
	for _, line := range lines {
		if m := f.GroupRe.FindStringSubmatch(line); m != nil {
			group = opsGroup(m[1])
			continue
		}
		if group == "" {
			continue
		}
		if m := f.EntryRe.FindStringSubmatch(line); m != nil {
			if err := add(out, path, group, m[1], m[2]); err != nil {
				return err
			}
			continue
		}
		if line == f.End {
			group = ""
		}
	}
	return nil
}

// opsGroup 返回归一化后的常量组名；不是 op 常量组（名字不以 Ops 结尾）时返回空串。
func opsGroup(name string) string {
	lower := strings.ToLower(name)
	if !strings.HasSuffix(lower, "ops") {
		return ""
	}
	return lower
}

// add 记入一条 op；同一端出现重复键且值不同即报错（生成器应保证唯一）。
func add(out map[opKey]string, path, group, member, value string) error {
	k := opKey{Group: group, Member: strings.ToLower(member)}
	if prev, ok := out[k]; ok && prev != value {
		return fmt.Errorf("%s: %s 重复且值不同（%q / %q）", path, k, prev, value)
	}
	out[k] = value
	return nil
}

// compare 以三端键的并集逐键比对，返回全部缺失/不一致项。
func compare(names []string, ends map[string]map[opKey]string) error {
	union := make(map[opKey]bool)
	for _, name := range names {
		for k := range ends[name] {
			union[k] = true
		}
	}
	keys := make([]opKey, 0, len(union))
	for k := range union {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })

	var problems []string
	for _, k := range keys {
		values := make(map[string]string, len(names))
		for _, name := range names {
			v, ok := ends[name][k]
			if !ok {
				problems = append(problems, fmt.Sprintf("%s 缺少 %s", name, k))
				continue
			}
			values[name] = v
		}
		if len(values) != len(names) {
			continue
		}
		for _, name := range names[1:] {
			if values[name] != values[names[0]] {
				problems = append(problems, fmt.Sprintf("%s 值不一致：%s", k, joinValues(names, values)))
				break
			}
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("三端 op 常量不一致：\n  %s", strings.Join(problems, "\n  "))
	}
	return nil
}

// joinValues 把各端取值拼成 `go=...、ts=...` 形式（差异定位用）。
func joinValues(names []string, values map[string]string) string {
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, name+"="+values[name])
	}
	return strings.Join(parts, "、")
}

// groupCount 统计不同常量组的数量（报告口径：两个 ProtocolOps + 两个 PushOps = 4 组）。
func groupCount(ops map[opKey]string) int {
	groups := make(map[string]bool)
	for k := range ops {
		groups[k.Group] = true
	}
	return len(groups)
}

// readLines 按行读取文件（产物都是小文件，整读即可）。
func readLines(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取 %s 失败: %w", path, err)
	}
	return strings.Split(string(data), "\n"), nil
}
