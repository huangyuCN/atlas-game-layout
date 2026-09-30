// Package assemble 提供 battle 服务的进程内（嵌入式）装配入口：
// 与进程形态共用 internal/app 的同一张 fx 依赖图与同一套启停路径
// （bootstrap.Boot → atlas.App：启动服务端、注册实例、注销与停机），
// 仅不注册进程信号——宿主/测试进程的信号不能被本实例拦下。
// 供集成测试与嵌入式部署复用。
package assemble

import (
	"context"
	"net/url"
	"time"

	battlev1 "github.com/huangyuCN/atlas-game-layout/api/battle/v1"
	"github.com/huangyuCN/atlas-game-layout/pkg/actor"
	"github.com/huangyuCN/atlas-game-layout/pkg/bootstrap"
	"github.com/huangyuCN/atlas-game-layout/pkg/serverutil"
	configspb "github.com/huangyuCN/atlas-game-layout/protobuf/configs"
	battleactor "github.com/huangyuCN/atlas-game-layout/services/battle/internal/actor"
	battleapp "github.com/huangyuCN/atlas-game-layout/services/battle/internal/app"
	"github.com/huangyuCN/atlas-game-layout/services/battle/internal/conf"
	"go.uber.org/fx"
)

// BattleConfig 是战斗参数覆盖（供 e2e 等外部包注入；镜像 internal/actor.Config——
// Go internal 规则禁止外部包直接引用 actor.Config，故以本包类型暴露，toActor 单点映射）。
type BattleConfig struct {
	TickInterval  int64  // 帧间隔（纳秒）
	TrackLen      int32  // 赛道长度（胜负判定）
	MaxFrames     uint64 // 帧数上限
	SnapshotEvery uint64 // 快照周期（帧）
	// OfflineTimeout 是掉线判定窗口（规格 §9.2；零值 = 使用配置/默认 15s）。
	// 数据报面帧面的空闲读超时按它推导（1/3）并在装配期校验，故嵌入形态改窗口时两处同源。
	OfflineTimeout time.Duration
}

// applyOverride 把战斗参数覆盖应用到基础配置：只覆盖战斗参数，
// 出票三件（密钥/TTL/接入层地址）来自配置，不得被覆盖抹掉；
// override 为 nil 时原样透传基础配置。
func applyOverride(base battleactor.Config, override *BattleConfig) battleactor.Config {
	if override == nil {
		return base
	}
	base.TickInterval = override.TickInterval
	base.TrackLen = override.TrackLen
	base.MaxFrames = override.MaxFrames
	base.SnapshotEvery = override.SnapshotEvery
	// 零值视为「不覆盖」：外部调用方常只填部分字段，零值窗口会把掉线判定误关。
	if override.OfflineTimeout > 0 {
		base.OfflineTimeout = override.OfflineTimeout
	}
	return base
}

// DefaultBattleConfig 返回默认战斗参数（完整填充，可局部覆盖）。
func DefaultBattleConfig() BattleConfig {
	cfg := battleactor.DefaultConfig()
	return BattleConfig{
		TickInterval:   cfg.TickInterval,
		TrackLen:       cfg.TrackLen,
		MaxFrames:      cfg.MaxFrames,
		SnapshotEvery:  cfg.SnapshotEvery,
		OfflineTimeout: cfg.OfflineTimeout,
	}
}

// FrameAddrs 是三个直连帧面（KCP/UDP/WS）的监听地址：空串 = 该帧面不启用
// （不监听端口、不注册帧 op，与配置语义一致）。脚本/测试据此指定固定端口直连。
type FrameAddrs struct {
	KCP string
	UDP string
	WS  string
}

// Options 是进程内装配参数。
type Options struct {
	NodeID        string
	EtcdEndpoints []string
	NatsURL       string
	MongoURI      string
	MongoDB       string
	// BattleCfg 覆盖战斗默认参数（测试注入：短帧间隔/长赛道等）；nil 用默认。
	BattleCfg *BattleConfig
	// Namespace 是命名空间 token（如 e2e-1790，**不是**注册键前缀路径）：嵌入式/测试形态用它
	// 把实例与常驻进程隔离，并避免上一次运行残留的实例键（租约未过期）导致注册冲突。
	// 五面前缀（注册键/actor subject/业务 topic/redis 键/etcd 目录）全由框架 namespace.Derive
	// 按该 token 派生；**必填**，缺失即装配失败（R9：不回落 default/env）。
	Namespace string
	// TicketKey 是入场票据密钥（base64 的 32 字节；与接入层 edge.ticket_key 同值）。
	// **必填**：缺失/非法/长度不符即装配失败（见 app.newActorConfig）。
	TicketKey string
	// TicketTTL 是票据有效期（time.ParseDuration 字符串，如 45s；空 = 120s）。
	TicketTTL string
	// EdgeEndpoints 是接入层「传输面 → 地址」列表（随成局推送下发）。
	// **必填**：至少一面、面不重复、地址非空，违反即装配失败（见 app.newActorConfig）。
	EdgeEndpoints []*battlev1.EdgeEndpoint
	// FrameAddrs 覆盖三个直连帧面的监听地址：nil = 三面各起随机端口（与 gRPC 面同口径）；
	// 非 nil 时按字段启用（空串 = 该面不启用）——-mode direct 脚本据此只开所选传输面。
	FrameAddrs *FrameAddrs
}

// Battle 是装配完成的 battle 服务句柄。
type Battle struct {
	Runtime *actor.Runtime // 战斗 actor 宿主（测试/观测可直达）
	// GRPCURL 是 internal 面（可信区：服务间 gRPC，matcher 开局/取票调用与测试直连用）的 host:port；
	// edge 面自阶段 3 批次 5 起不启用（客户端战斗 op 直连帧面，见 KCPURL/UDPURL/WSURL）。
	GRPCURL string
	// KCPURL / UDPURL / WSURL 是三个直连帧面的 host:port（未启用的面为空串）；
	// 进程内形态据此做直连寻址（scripts/e2e -mode direct 与集成测试）。
	KCPURL string
	UDPURL string
	WSURL  string
	stop   func(ctx context.Context) error
}

// graphHandles 从依赖图回捞句柄所需组件。
type graphHandles struct {
	fx.In

	bootstrap.Servers
	Runtime *actor.Runtime
}

// New 装配并启动 battle 服务：映射配置 → bootstrap.Boot 启动依赖图（含 actor 运行时），
// 由 atlas.App 统一启动服务端并注册实例（返回时注册已完成）。
// 实例 ID 必须 == actor NodeID（matcher 懒激活按服务实例选 battle 节点的硬约束）。
func New(ctx context.Context, o Options) (*Battle, error) {
	var h graphHandles
	cfg, err := newBootstrap(o)
	if err != nil {
		return nil, err
	}
	inst, urls, err := bootstrap.Boot(ctx, cfg,
		fx.Options(battleapp.Module, overrideBattleConfig(o.BattleCfg)), &h,
		requiredSchemes(cfg)...)
	if err != nil {
		return nil, err
	}
	return &Battle{
		Runtime: h.Runtime,
		GRPCURL: urls[serverutil.SchemeGRPC].Host,
		KCPURL:  endpointHost(urls, serverutil.SchemeKCP),
		UDPURL:  endpointHost(urls, serverutil.SchemeUDP),
		WSURL:   endpointHost(urls, serverutil.SchemeWS),
		stop:    inst.Stop,
	}, nil
}

// requiredSchemes 归集本次装配必须就绪的端点 scheme：internal gRPC 面恒需要
// （服务间调用；edge gRPC 面自阶段 3 批次 5 起不启用，见 newBootstrap），
// 三个直连帧面按配置声明（未声明即该面不启用）。
func requiredSchemes(cfg *conf.Bootstrap) []string {
	schemes := []string{serverutil.SchemeGRPC}
	for _, s := range []struct {
		scheme string
		addr   string
	}{
		{serverutil.SchemeKCP, cfg.GetServer().GetKcp().GetAddr()},
		{serverutil.SchemeUDP, cfg.GetServer().GetUdp().GetAddr()},
		{serverutil.SchemeWS, cfg.GetServer().GetWebsocket().GetAddr()},
	} {
		if s.addr != "" {
			schemes = append(schemes, s.scheme)
		}
	}
	return schemes
}

// endpointHost 取指定 scheme 的 host:port（未启用或缺失返回空串）。
func endpointHost(urls map[string]*url.URL, scheme string) string {
	if u, ok := urls[scheme]; ok && u != nil {
		return u.Host
	}
	return ""
}

// Stop 停止 battle 服务并释放全部资源
// （注销实例 → 停服务端 → 组件逆序回收，均由 atlas.App 驱动）。
func (b *Battle) Stop(ctx context.Context) error {
	if b.stop == nil {
		return nil
	}
	return b.stop(ctx)
}

// overrideBattleConfig 以 fx.Decorate 覆盖依赖图中的战斗参数默认值；
// override 为 nil 时透传基础值（装饰器恒挂载、行为零差异）。
func overrideBattleConfig(override *BattleConfig) fx.Option {
	return fx.Decorate(func(base battleactor.Config) battleactor.Config {
		return applyOverride(base, override)
	})
}

// 装配图只认 *conf.Bootstrap 一种输入，两种驱动形态因此共享全部构造函数。
// 监听地址固定随机端口（进程内形态不做端口管理）。
// newBootstrap 合成进程内形态配置；返回 error 的唯一来源是命名空间缺失/非法（R9）。
func newBootstrap(o Options) (*conf.Bootstrap, error) {
	// R9 严格模式：命名空间缺失/非法即装配失败（不回落 default/env）。
	if err := bootstrap.RequireNamespace(o.Namespace); err != nil {
		return nil, err
	}
	const randomPort = "127.0.0.1:0"
	frames := o.FrameAddrs
	if frames == nil {
		// 与两个 gRPC 面同口径：默认三面各起随机端口（嵌入式/测试不做端口管理）。
		frames = &FrameAddrs{KCP: randomPort, UDP: randomPort, WS: randomPort}
	}
	return &conf.Bootstrap{
		Runtime: &configspb.Runtime{Name: "battle", Id: o.NodeID, Namespace: o.Namespace},
		Registry: &configspb.Registry{
			Etcd: &configspb.Registry_Etcd{Endpoints: o.EtcdEndpoints},
		},
		Server: &configspb.Server{
			// 只启用 internal 面（服务间 gRPC）：edge 面自阶段 3 批次 5 起停用——
			// 客户端战斗 op 走直连帧面（下方 kcp/udp/websocket），不再经网关转 Edge 面。
			Grpc: &configspb.Server_GRPC{InternalAddr: randomPort},
			Http: &configspb.Server_HTTP{Addr: randomPort},
			// 三个直连帧面：空串 = 该面不启用（配置语义见 docs/config.md）。
			Kcp:       &configspb.Server_KCP{Addr: frames.KCP},
			Udp:       &configspb.Server_UDP{Addr: frames.UDP},
			Websocket: &configspb.Server_WebSocket{Addr: frames.WS},
		},
		Data: &configspb.Data{
			Nats:  &configspb.Data_Nats{Url: o.NatsURL},
			Mongo: &configspb.Data_Mongo{Uri: o.MongoURI, Database: o.MongoDB},
		},
		// 出票三件由调用方显式给出（校验在 app.newActorConfig：缺失/非法即装配失败）。
		Battle: &conf.BattleConf{
			TicketKey:     o.TicketKey,
			TicketTtl:     o.TicketTTL,
			EdgeEndpoints: o.EdgeEndpoints,
		},
	}, nil
}
