package conf

import (
	"path/filepath"
	"testing"
	"time"

	pkgactor "github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/config"
	"github.com/huangyuCN/atlas-game-layout/pkg/directory"
)

// TestConfigActorLease 验证 battle 的配置链路：config.yaml 的 actor.lease_* 能被解析成
// 租约参数且与缺省口径一致（60s/20s）。字段拼写、单位缺失或非法组合（Renew ≥ TTL）
// 在这里就暴露，不必等进程启动（装配期同样会启动失败）。
func TestConfigActorLease(t *testing.T) {
	var cfg Bootstrap
	if err := config.Load(filepath.Join("..", "..", "configs", "config.yaml"), &cfg); err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}
	ttl, renew, err := pkgactor.LeaseFromConf(cfg.GetActor())
	if err != nil {
		t.Fatalf("租约配置解析失败: %v", err)
	}
	if ttl != 60*time.Second || renew != 20*time.Second {
		t.Fatalf("租约配置 = %s/%s，期望 60s/20s", ttl, renew)
	}
}

// TestConfigActorDirectory 验证 battle 的配置链路里 actor.directory 缺省走 etcd：
// 老部署升级后目录后端不得被悄悄换掉（否则运行中的归属记录读不到，整批 actor 会被重 Claim）。
func TestConfigActorDirectory(t *testing.T) {
	var cfg Bootstrap
	if err := config.Load(filepath.Join("..", "..", "configs", "config.yaml"), &cfg); err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}
	opts, err := pkgactor.DirectoryFromConf(cfg.GetActor())
	if err != nil {
		t.Fatalf("目录配置解析失败: %v", err)
	}
	if opts.Backend != directory.BackendEtcd {
		t.Fatalf("缺省目录后端 = %q，期望 etcd", opts.Backend)
	}
}
