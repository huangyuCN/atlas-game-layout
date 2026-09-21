package bootstrap

import (
	"testing"

	"github.com/huangyuCN/atlas-game-layout/lib/version"
	"github.com/huangyuCN/atlas-game-layout/pkg/observability"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
)

// TestServiceIdentityOf 验证 runtime → 服务身份映射：
// 版本配置优先，未配置回退构建注入值（lib/version）；其余字段直接透传。
func TestServiceIdentityOf(t *testing.T) {
	got := serviceIdentityOf(&testBootstrap{Runtime: &configspb.Runtime{
		Name: "game", Id: "game-1", Version: "v1.2.3", Env: "test",
	}})
	want := observability.ServiceIdentity{Name: "game", ID: "game-1", Version: "v1.2.3", Env: "test"}
	if got != want {
		t.Fatalf("serviceIdentityOf() = %+v, 期望 %+v", got, want)
	}

	// 配置未写 version：回退构建注入版本。
	got = serviceIdentityOf(&testBootstrap{Runtime: &configspb.Runtime{Name: "game"}})
	if got.Version != version.Version {
		t.Fatalf("version 缺省应回退构建注入值 %q，实际 %q", version.Version, got.Version)
	}

	// Runtime 缺失：仅版本回退，其余零值。
	got = serviceIdentityOf(&testBootstrap{})
	if got.Name != "" || got.ID != "" || got.Env != "" || got.Version != version.Version {
		t.Fatalf("Runtime 缺失时身份不符: %+v", got)
	}
}

// TestTracingOptionsOf 验证 observability 配置 + 服务身份 → TracingOptions 映射。
func TestTracingOptionsOf(t *testing.T) {
	ratio := 0.25
	cases := []struct {
		name string
		cfg  *testBootstrap
		want observability.TracingOptions
	}{
		{
			name: "缺省：无端点、跟随父级、比率 1.0、身份全量透传",
			cfg: &testBootstrap{Runtime: &configspb.Runtime{
				Name: "game", Id: "game-1", Version: "v1.2.3", Env: "test",
			}},
			want: observability.TracingOptions{
				Identity:    observability.ServiceIdentity{Name: "game", ID: "game-1", Version: "v1.2.3", Env: "test"},
				Sampler:     observability.TraceSamplerParentBasedRatio,
				SampleRatio: 1,
			},
		},
		{
			name: "显式端点/采样器/比率",
			cfg: &testBootstrap{
				Runtime: &configspb.Runtime{Name: "game"},
				obs: &configspb.Observability{
					Otlp:        "http://127.0.0.1:4318",
					Sampler:     configspb.TraceSampler_TRACE_SAMPLER_ALWAYS_ON,
					SampleRatio: &ratio,
				},
			},
			want: observability.TracingOptions{
				Identity:    observability.ServiceIdentity{Name: "game", Version: version.Version},
				Endpoint:    "http://127.0.0.1:4318",
				Sampler:     observability.TraceSamplerAlwaysOn,
				SampleRatio: 0.25,
			},
		},
		{
			name: "always_off",
			cfg: &testBootstrap{
				Runtime: &configspb.Runtime{Name: "game"},
				obs:     &configspb.Observability{Sampler: configspb.TraceSampler_TRACE_SAMPLER_ALWAYS_OFF},
			},
			want: observability.TracingOptions{
				Identity:    observability.ServiceIdentity{Name: "game", Version: version.Version},
				Sampler:     observability.TraceSamplerAlwaysOff,
				SampleRatio: 1,
			},
		},
		{
			name: "显式 ratio=0（全丢）不被缺省值覆盖",
			cfg: &testBootstrap{
				Runtime: &configspb.Runtime{Name: "game"},
				obs:     &configspb.Observability{SampleRatio: new(float64)},
			},
			want: observability.TracingOptions{
				Identity:    observability.ServiceIdentity{Name: "game", Version: version.Version},
				Sampler:     observability.TraceSamplerParentBasedRatio,
				SampleRatio: 0,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tracingOptionsOf(tc.cfg, serviceIdentityOf(tc.cfg))
			if err != nil {
				t.Fatalf("tracingOptionsOf() 错误 = %v", err)
			}
			if got != tc.want {
				t.Fatalf("tracingOptionsOf() = %+v, 期望 %+v", got, tc.want)
			}
		})
	}
}

// TestTracingOptionsOfUnknownSampler 验证未知采样器枚举快速失败。
func TestTracingOptionsOfUnknownSampler(t *testing.T) {
	cfg := &testBootstrap{
		Runtime: &configspb.Runtime{Name: "game"},
		obs:     &configspb.Observability{Sampler: configspb.TraceSampler(9)},
	}
	if _, err := tracingOptionsOf(cfg, serviceIdentityOf(cfg)); err == nil {
		t.Fatal("未知采样器应报错，实际为 nil")
	}
}
