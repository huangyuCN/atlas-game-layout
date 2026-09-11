// check-dup 检查 Go 源码中的重复函数体（复制粘贴/结构克隆）：
// 对每个函数做「结构指纹」归一（标识符→_、字面量→#，仅保留控制流骨架），
// 指纹相同且行数达阈值即报告——归一后同构的函数即
// 使改名/改字段也能命中，能提醒「该抽公共函数了」。
//
// 跳过：生成文件（Code generated 标记）、vendor/testdata/third_party、
// 点开头目录、_test.go（表驱动测试天然相似）、单测/生成产物。
//
// 用法：
//
//	go run ./scripts/check-dup [目录]
//
// 目录默认当前目录（递归）。发现重复组时输出明细并以退出码 1 结束。
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	// minLines 是函数体纳入检查的最小行数（低于此的重复抽函数收益低）。
	minLines = 10
)

// skipDir 是按目录名跳过的集合（不递归进入）。
var skipDir = map[string]bool{
	".git": true, "vendor": true, "testdata": true,
	"third_party": true, "node_modules": true, "bin": true,
}

// finding 是一组结构重复的函数。
type finding struct {
	kind   string // 函数签名描述（首条成员）
	count  int
	member []string // 各成员 "file:line name"
}

// clone 记录单个函数的归一指纹与位置信息。
type clone struct {
	file   string
	line   int
	name   string
	normal string
	lines  int
}

func main() {
	root := "."
	allow := map[string]bool{}
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	if len(os.Args) > 2 {
		names, err := readAllowlist(os.Args[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "check-dup: 读取豁免清单失败: %v\n", err)
			os.Exit(2)
		}
		allow = names
	}

	var clones []clone
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			name := info.Name()
			if (strings.HasPrefix(name, ".") || skipDir[name]) && path != root {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil
		}
		head := string(src)
		if len(head) > 2048 {
			head = head[:2048]
		}
		if strings.Contains(head, "Code generated") && strings.Contains(head, "DO NOT EDIT") {
			return nil
		}
		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, path, src, 0)
		if perr != nil {
			return nil
		}
		for _, d := range f.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			start := fset.Position(fn.Body.Pos()).Line
			end := fset.Position(fn.Body.End()).Line
			lines := end - start + 1
			if lines < minLines {
				continue
			}
			name := fn.Name.Name
			if fn.Recv != nil && len(fn.Recv.List) > 0 {
				if t, ok := fn.Recv.List[0].Type.(*ast.StarExpr); ok {
					if id, ok := t.X.(*ast.Ident); ok {
						name = id.Name + "." + name
					}
				}
			}
			clones = append(clones, clone{
				file:   path,
				line:   start,
				name:   name,
				normal: normalize(fset, src, fn.Body),
				lines:  lines,
			})
		}
		return nil
	})

	groups := make(map[string][]clone)
	for _, c := range clones {
		groups[c.normal] = append(groups[c.normal], c)
	}

	var out []finding
	for _, g := range groups {
		if len(g) < 2 {
			continue
		}
		if groupAllowlisted(g, allow) {
			continue // 豁免清单命中（设计性重复）
		}
		sort.Slice(g, func(i, j int) bool {
			if g[i].file != g[j].file {
				return g[i].file < g[j].file
			}
			return g[i].line < g[j].line
		})
		out = append(out, finding{
			kind:   g[0].name,
			count:  len(g),
			member: membersOf(g),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].member[0] < out[j].member[0]
	})

	fmt.Printf("== 重复函数组 %d（结构指纹一致、≥%d 行；标识符已归一）\n", len(out), minLines)
	for _, f := range out {
		fmt.Printf("[dup] %s ×%d\n", f.kind, f.count)
		for _, m := range f.member {
			fmt.Printf("      %s\n", m)
		}
	}
	if len(out) > 0 {
		os.Exit(1)
	}
}

// normalize 对函数体做结构归一：标识符→_、字面量→#，
// 保留关键字/运算符/标点与语句形态（缩进与注释被剔除）。
func normalize(fset *token.FileSet, src []byte, body *ast.BlockStmt) string {
	start := fset.Position(body.Pos()).Offset
	end := fset.Position(body.End()).Offset
	raw := src[start:end]

	var b strings.Builder
	var s scanner.Scanner
	var fsetLocal token.FileSet
	file := fsetLocal.AddFile("", 1, len(raw))
	s.Init(file, raw, nil, 0)
	for {
		_, tok, _ := s.Scan()
		if tok == token.EOF {
			break
		}
		switch tok {
		case token.IDENT:
			b.WriteString("_")
		case token.INT, token.FLOAT, token.CHAR, token.STRING:
			b.WriteString("#")
		case token.COMMENT:
			// 跳过
		default:
			b.WriteString(tok.String())
		}
	}
	return b.String()
}

// groupAllowlisted 判断重复组的成员是否全部命中豁免（裸函数名比对）。
func groupAllowlisted(g []clone, allow map[string]bool) bool {
	for _, c := range g {
		bare := c.name
		if i := strings.LastIndex(bare, "."); i >= 0 {
			bare = bare[i+1:]
		}
		if !allow[bare] {
			return false
		}
	}
	return true
}

// readAllowlist 读取豁免清单（每行一条函数名，支持 # 注释）。
func readAllowlist(path string) (map[string]bool, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	allow := make(map[string]bool)
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		allow[strings.Fields(line)[0]] = true
	}
	return allow, nil
}

// membersOf 汇总成员位置描述。
func membersOf(g []clone) []string {
	out := make([]string, 0, len(g))
	for _, c := range g {
		out = append(out, fmt.Sprintf("%s:%d %s（%d 行）", c.file, c.line, c.name, c.lines))
	}
	return out
}
