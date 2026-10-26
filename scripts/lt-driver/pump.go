// pump.go 是稳态负载的**收尾边界**：到点停止发送，并保证整轮压测一定能走下去（打印 SUMMARY）。
//
// 与 main.go 分开的理由：收尾语义（到点、单次调用预算、强制收尾上限）是独立的一条约束，
// 混在报表与旗标逻辑里最容易在改动中被削掉——而它正是「-duration 到点却挂死」的修复点。

package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// pumpGrace 是负载阶段到点后、在「一次调用预算」之外再给的收尾余量。
const pumpGrace = time.Second

// pumpAll 对已入局的连接施加稳定负载，直到 duration 到点。
//
// 收尾有**上限**：单条连接的泵最迟在「到点 + 一次调用预算」内退出（每次调用都有预算，
// 见 callContext）；若调用连 ctx 都不听（连接半死的最坏形态），到达上限即强制进入汇总——
// 整轮压测绝不能因为一条连接而永远打不出 SUMMARY（诊断 §6：t=195s 仍 0 pps、需人工 kill）。
func pumpAll(ctx context.Context, ps []*loadPlayer, cfg pumpCfg) {
	var wg sync.WaitGroup
	for _, p := range ps {
		if p == nil || p.frame == nil {
			continue
		}
		wg.Add(1)
		go func(p *loadPlayer) {
			defer wg.Done()
			p.pump(ctx, cfg)
		}(p)
	}
	done := make(chan struct{})
	go func() { defer close(done); wg.Wait() }()
	limit := cfg.duration + callBudget(cfg) + pumpGrace
	select {
	case <-done:
	case <-time.After(limit):
		fmt.Printf("[负载] 到点 %v 后仍有连接未收尾（帧调用卡住），强制进入汇总\n", limit)
	}
}
