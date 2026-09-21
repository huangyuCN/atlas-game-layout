package fxkit

import (
	"reflect"
	"testing"

	"github.com/huangyuCN/atlas-game-layout/pkg/redis"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
)

// fakeRedisConf 是满足 WithData 的最小测试配置。
type fakeRedisConf struct {
	data *configspb.Data
}

// GetData 返回测试用数据中间件配置段。
func (f *fakeRedisConf) GetData() *configspb.Data { return f.data }

// TestRedisOptions 验证 proto 配置 → redis.Options 的映射（缺省/三形态/全字段）。
func TestRedisOptions(t *testing.T) {
	cases := []struct {
		name string
		data *configspb.Data
		want redis.Options
	}{
		{
			name: "缺省 data 段",
			data: nil,
			want: redis.Options{},
		},
		{
			name: "单点（mode 缺省即零值 SINGLE）",
			data: &configspb.Data{Redis: &configspb.Data_Redis{
				Addrs:    []string{"127.0.0.1:16379"},
				Password: "pw",
				Db:       3,
			}},
			want: redis.Options{
				Addrs:    []string{"127.0.0.1:16379"},
				Mode:     redis.ModeSingle,
				Password: "pw",
				DB:       3,
			},
		},
		{
			name: "哨兵",
			data: &configspb.Data{Redis: &configspb.Data_Redis{
				Mode:       configspb.Data_REDIS_MODE_SENTINEL,
				Addrs:      []string{"127.0.0.1:26379", "127.0.0.1:26380"},
				MasterName: "mymaster",
				Password:   "pw",
				Db:         1,
			}},
			want: redis.Options{
				Addrs:      []string{"127.0.0.1:26379", "127.0.0.1:26380"},
				Mode:       redis.ModeSentinel,
				MasterName: "mymaster",
				Password:   "pw",
				DB:         1,
			},
		},
		{
			name: "集群",
			data: &configspb.Data{Redis: &configspb.Data_Redis{
				Mode:  configspb.Data_REDIS_MODE_CLUSTER,
				Addrs: []string{"127.0.0.1:7000", "127.0.0.1:7001"},
			}},
			want: redis.Options{
				Addrs: []string{"127.0.0.1:7000", "127.0.0.1:7001"},
				Mode:  redis.ModeCluster,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := RedisOptions(&fakeRedisConf{data: tc.data})
			if err != nil {
				t.Fatalf("RedisOptions() 错误 = %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("RedisOptions() = %+v, 期望 %+v", got, tc.want)
			}
		})
	}
}

// TestRedisOptionsUnknownMode 验证未知 proto 枚举值快速失败（不静默落回单点）。
func TestRedisOptionsUnknownMode(t *testing.T) {
	_, err := RedisOptions(&fakeRedisConf{data: &configspb.Data{Redis: &configspb.Data_Redis{
		Mode:  configspb.Data_RedisMode(9),
		Addrs: []string{"127.0.0.1:16379"},
	}}})
	if err == nil {
		t.Fatal("未知 mode 期望报错，实际为 nil")
	}
}

// TestNewRedisClient 验证按配置构造客户端（惰性连接）；缺 addrs 快速失败。
func TestNewRedisClient(t *testing.T) {
	cli, err := NewRedisClient(&fakeRedisConf{data: &configspb.Data{Redis: &configspb.Data_Redis{
		Addrs: []string{"127.0.0.1:1"},
	}}})
	if err != nil {
		t.Fatalf("NewRedisClient() 错误 = %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	if _, err := NewRedisClient(&fakeRedisConf{}); err == nil {
		t.Fatal("缺 redis.addrs 期望报错，实际为 nil")
	}
}
