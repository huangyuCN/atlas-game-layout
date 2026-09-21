# Redis 配置对齐设计（Mode 枚举 + Addrs 收敛 + proto 补全）

- 日期：2026-09-21
- 状态：已定稿（使用者确认：single 多地址报错 / 映射收敛到 fxkit / **本会话所有改动不做兼容处理，直接破坏性变更**）
- 范围：`pkg/redis`（Go API 枚举化）、`protobuf/configs/data.proto`（配置结构对齐启动参数）、
  `pkg/fxkit`（配置→客户端映射收敛）、四个服务的 `configs/config.yaml` 与装配、测试与脚本调用点。
  不改客户端协议，不改 Redis 键空间与业务语义。

## 1. 背景

- `pkg/redis.Options` 的 `Mode` 是裸字符串（`"single"`/`"sentinel"`/`"cluster"`），属魔法值；
  单点形态另设 `Addr` 字段，与 `Addrs` 语义重叠。
- `protobuf/configs/data.proto` 的 `Redis` 只有 `string addr = 1`：哨兵/集群所需的
  `mode`/`master_name`/`addrs`/`password`/`db` 都无法下发，配置能力与 `Options`（启动参数）不一致。
- 三个服务的 `internal/app/infra.go` 各自把 `addr` 映射进 `Options`，同一段代码复制 3 份；
  补全字段后若继续各写一份，重复会进一步放大。
- 配置加载是 YAML → JSON → protojson（`pkg/config`），proto 枚举在 YAML 中按**枚举名**书写。

## 2. 决策表

| 议题 | 决策 |
|------|------|
| Go 形态类型 | `type Mode int` + `ModeSingle`/`ModeSentinel`/`ModeCluster`，零值 = single；提供 `String()` |
| 单点地址 | 删除 `Addr`；`Addrs []string` 覆盖三形态，single 取首个 |
| single 多地址 | 报错（严格），避免配置笔误被静默忽略 |
| proto 形态 | 新增 `enum RedisMode`：`REDIS_MODE_SINGLE = 0`（零值即 single）/`SENTINEL = 1`/`CLUSTER = 2`，不做 UNSPECIFIED 态 |
| proto 字段 | 直接重排：`mode = 1`、`addrs = 2`、`master_name = 3`、`password = 4`、`db = 5`；旧 `addr` 不保留、不 reserved（无兼容处理） |
| 映射落点 | `pkg/fxkit` 泛型（`WithData` + `RedisOptions[B]` + `NewRedisClient[B]`），与既有 `NewEtcdClient` 同构；删除三份重复 |
| 模板配置 | `redis.addrs: [127.0.0.1:16379]`，mode 缺省即 single（注释给出哨兵/集群写法） |

## 3. Go 接口

```go
// Mode 是 redis 部署形态。
type Mode int

const (
    ModeSingle   Mode = iota // 单点（默认，零值）
    ModeSentinel             // 主从哨兵
    ModeCluster              // 集群分片
)

// String 返回形态名（日志/错误信息用）。
func (m Mode) String() string

// Options 是 Redis 连接选项。
type Options struct {
    Addrs      []string // 节点地址：single 取首个；sentinel/cluster 为全部节点
    Mode       Mode     // 部署形态（零值 = single）
    MasterName string   // sentinel 监控的主库名（sentinel 必填）
    Password   string   // 可选密码
    DB         int      // 库编号（仅 single/sentinel；cluster 不支持）
}

func NewClient(opts Options) (*Client, error)
```

`NewClient` 校验（错误信息含形态名，便于定位配置）：

| 形态 | 校验 |
|------|------|
| `ModeSingle` | `len(Addrs) == 1`；0 个或 >1 个均报错 |
| `ModeSentinel` | `MasterName != ""` 且 `len(Addrs) >= 1` |
| `ModeCluster` | `len(Addrs) >= 1` |
| 其它取值 | 报错（越界 Mode） |

## 4. proto 结构

```proto
message Data {
  // RedisMode 是 redis 部署形态；零值（0）即 single。
  enum RedisMode {
    REDIS_MODE_SINGLE = 0;   // 单点（默认）
    REDIS_MODE_SENTINEL = 1; // 主从哨兵
    REDIS_MODE_CLUSTER = 2;  // 集群分片
  }
  message Redis {
    RedisMode mode = 1;        // 部署形态（缺省 = single）
    repeated string addrs = 2; // single 一个；sentinel/cluster 为节点列表
    string master_name = 3;    // sentinel 必填
    string password = 4;       // 可选密码
    int32 db = 5;              // 仅 single/sentinel
  }
  // Nats / Mongo 本次不动。
  Redis redis = 1;
  Nats nats = 2;
  Mongo mongo = 3;
}
```

## 5. 破坏性变更与影响面

- **不做兼容处理**（本会话统一原则）：旧 `addr` 字段、旧 YAML 键 `redis.addr` 一律不再识别，
  不提供 deprecated 字段、不提供迁移回落；protojson 拒绝未知字段，旧配置会在启动期直接报错。
- 四个服务 `config.yaml` 的 `redis.addr` 改为 `addrs` 列表；battle 的 `data.redis` 段未被任何装配使用，直接删除。
- 影响面清单：
  - `pkg/redis/redis.go`（Mode 枚举、Options、校验）；
  - `pkg/fxkit/fxkit.go`（`WithData`、`RedisOptions[B]`、`NewRedisClient[B]`）；
  - `services/{game,gateway,matcher}/internal/app/{infra.go,graph.go}`（删本地 `NewRedisClient`，改泛型提供）；
  - `services/*/configs/config.yaml`（redis 段）；
  - `protobuf/configs/data.pb.go`（重新生成）；
  - 测试/脚本 `Options{Addr: ...}` 约 8 处（`pkg/redis`、`services/gateway/internal/{server,session}`、`test/e2e`、`scripts/e2e`、`services/*/assemble` 断言）。
- 服务器：需重新同步代码与配置并重启四服务。

## 6. 测试（TDD，先写用例）

- `pkg/redis`：`Mode.String()`；三形态构造成功；single 0 个/2 个地址报错；sentinel 缺 `master_name` 报错；
  cluster 空地址报错；越界 Mode 报错；miniredis 连通（single 走 `Addrs[0]`）。
- `pkg/fxkit`：`RedisOptions` 对缺省与三形态 + 全字段映射断言；缺 `addrs` 时 `NewRedisClient` 快速失败。
- `pkg/config`：YAML 中 `mode: REDIS_MODE_SENTINEL` 可被 protojson 解析（枚举名可用，含 `addrs` 列表）。
- 服务 `assemble` 测试：断言 `GetRedis().GetAddrs()` 与 `GetMode()`。

## 7. 不做（YAGNI）

- 不引入 `ParseMode(string)`（当前没有字符串来源；protojson 已负责解析枚举）。
- 不新增 go-redis 连接池/超时等可调参数——本次只对齐**已有**启动参数。
- 不动 `Nats`/`Mongo` 配置段（同类问题另议）。
- 不做任何兼容/迁移处理：不保留旧字段、不写迁移脚本、不为旧配置名加回落分支。
