package app

import (
	"fmt"

	battlev1actor "github.com/huangyuCN/atlas-game-layout/api/battle/v1/actor"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/ledger"
	"github.com/huangyuCN/atlas/metrics"
	"github.com/huangyuCN/atlas/namespace"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ledgerPrefixSuffix 是留档键在命名空间目录前缀下的后缀（键形如
// `/atlas/actors/<ns>/battle_ended/<battleID>`）。
const ledgerPrefixSuffix = "/battle_ended"

// 本文件装配跨节点结算留档与激活闸门（P1-7）：留档落 etcd（命名空间隔离 + 租约 TTL），
// 闸门注入 actor 集群运行时——**任意节点**在为 battle PID 创建 cell 之前问一次。

// newLedgerStore 装配跨节点留档存储：etcd + 命名空间派生前缀（与 actor 目录同源隔离口径，
// 共用同一 etcd 的两套部署不共享留档），TTL 由租约承担（写入时给，见 actor.recordSharedEnded）。
func newLedgerStore(cfg *conf.Bootstrap, cli *clientv3.Client) (*ledger.EtcdStore, error) {
	prefix, err := ledgerPrefix(cfg)
	if err != nil {
		return nil, err
	}
	return ledger.NewEtcdStore(cli, prefix), nil
}

// newActivationGate 装配 battle 的激活闸门：留档命中即拒绝激活（已结束的对局不得被复活），
// 查询失败按放行处理（见 ledger.Gate 的失败开放说明）。
func newActivationGate(store *ledger.EtcdStore, meter metrics.Collector) *ledger.Gate {
	return ledger.NewGate(store, battlev1actor.BattleServiceActorType, meter)
}

// ledgerPrefix 返回留档键前缀：命名空间派生（R9：缺失或非法即报错，不回落默认），
// 与 actor 目录前缀同源——两处不同源会让「配了隔离却没隔离」从缝里溜进来。
func ledgerPrefix(cfg *conf.Bootstrap) (string, error) {
	derived, err := namespace.Derive(cfg.GetRuntime().GetNamespace())
	if err != nil {
		return "", fmt.Errorf("app: 跨节点留档前缀派生失败: %w", err)
	}
	return derived.EtcdDirectory + ledgerPrefixSuffix, nil
}
